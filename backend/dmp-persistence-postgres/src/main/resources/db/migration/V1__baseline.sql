-- =============================================================================
-- Baseline schema — definition-side data (ADR-0005).
--
-- PostgreSQL owns definitions: tenants, pipelines, versions, connector instances
-- and the audit trail. Execution data (runs, splits, checkpoints) lives in
-- MongoDB and is deliberately absent from this schema.
--
-- Identifiers are UUIDv7, generated in the application. Time-ordered, so index
-- inserts append to the right-hand edge of the B-tree rather than scattering.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- tenant
--
-- The isolation boundary. Present from the first migration even though
-- authentication is deferred, because adding a tenant column to every table and
-- a predicate to every query later is a migration plus an audit of every query.
-- -----------------------------------------------------------------------------
CREATE TABLE tenant (
    id          UUID         PRIMARY KEY,
    slug        VARCHAR(64)  NOT NULL,
    name        VARCHAR(255) NOT NULL,
    status      VARCHAR(32)  NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL,

    CONSTRAINT uq_tenant_slug UNIQUE (slug),
    CONSTRAINT ck_tenant_status CHECK (status IN ('ACTIVE', 'SUSPENDED'))
);

COMMENT ON TABLE tenant IS 'Isolation boundary for every other aggregate.';

-- -----------------------------------------------------------------------------
-- connector_instance
--
-- A configured connection ("finance-postgres-replica"), not a connector type.
--
-- config and secret_refs are opaque JSONB. The platform cannot know a
-- third-party connector's configuration shape at compile time; it is validated
-- at runtime against the JSON Schema the plugin declares (ADR-0006). Constraining
-- it here would mean a schema change for every new connector.
--
-- secret_refs holds REFERENCES ONLY, never values. Nothing in this table is
-- secret, which is what makes it safe to log, dump and return over the API.
-- -----------------------------------------------------------------------------
CREATE TABLE connector_instance (
    id              UUID         PRIMARY KEY,
    tenant_id       UUID         NOT NULL REFERENCES tenant (id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    connector_type  VARCHAR(128) NOT NULL,
    direction       VARCHAR(16)  NOT NULL,
    config          JSONB        NOT NULL DEFAULT '{}'::jsonb,
    secret_refs     JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status          VARCHAR(32)  NOT NULL,
    description     TEXT,
    last_tested_at  TIMESTAMPTZ,
    last_test_error TEXT,
    created_at      TIMESTAMPTZ  NOT NULL,
    updated_at      TIMESTAMPTZ  NOT NULL,
    row_version     BIGINT       NOT NULL DEFAULT 0,

    CONSTRAINT uq_connector_instance_name UNIQUE (tenant_id, name),
    CONSTRAINT ck_connector_direction CHECK (direction IN ('SOURCE', 'SINK', 'BOTH')),
    CONSTRAINT ck_connector_status CHECK (status IN ('UNTESTED', 'ACTIVE', 'FAILED', 'DISABLED'))
);

CREATE INDEX idx_connector_instance_tenant_status ON connector_instance (tenant_id, status);
CREATE INDEX idx_connector_instance_tenant_type   ON connector_instance (tenant_id, connector_type);

-- -----------------------------------------------------------------------------
-- pipeline
--
-- The mutable container: name, folder, tags, and which version is published.
-- The DAG itself lives in pipeline_version. Separating them is what makes run
-- history interpretable — see the comment on pipeline_version.
-- -----------------------------------------------------------------------------
CREATE TABLE pipeline (
    id                UUID         PRIMARY KEY,
    tenant_id         UUID         NOT NULL REFERENCES tenant (id) ON DELETE CASCADE,
    name              VARCHAR(255) NOT NULL,
    description       TEXT,
    folder            VARCHAR(512),
    tags              JSONB        NOT NULL DEFAULT '[]'::jsonb,
    status            VARCHAR(32)  NOT NULL,
    published_version INTEGER,
    latest_version    INTEGER      NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ  NOT NULL,
    updated_at        TIMESTAMPTZ  NOT NULL,
    row_version       BIGINT       NOT NULL DEFAULT 0,

    CONSTRAINT uq_pipeline_name UNIQUE (tenant_id, name),
    CONSTRAINT ck_pipeline_status CHECK (status IN ('DRAFT', 'ACTIVE', 'ARCHIVED')),
    CONSTRAINT ck_pipeline_versions CHECK (
        latest_version >= 0
        AND (published_version IS NULL
             OR (published_version >= 1 AND published_version <= latest_version))
    )
);

CREATE INDEX idx_pipeline_tenant_status ON pipeline (tenant_id, status);
CREATE INDEX idx_pipeline_tenant_folder ON pipeline (tenant_id, folder) WHERE folder IS NOT NULL;
-- GIN over the tag array supports the containment queries the UI's tag filter issues.
CREATE INDEX idx_pipeline_tags ON pipeline USING GIN (tags jsonb_path_ops);

-- -----------------------------------------------------------------------------
-- pipeline_version
--
-- Immutable once published. A run records the version id it executed, so a
-- frozen definition is what makes "what actually ran on Tuesday" answerable
-- after the pipeline has been edited since. Mutating a published row would make
-- every run referencing it a fiction.
--
-- Enforced by trigger below, not only by application code, because the guarantee
-- is worth more than the convenience of a manual fix.
-- -----------------------------------------------------------------------------
CREATE TABLE pipeline_version (
    id              UUID        PRIMARY KEY,
    pipeline_id     UUID        NOT NULL REFERENCES pipeline (id) ON DELETE CASCADE,
    tenant_id       UUID        NOT NULL REFERENCES tenant (id) ON DELETE CASCADE,
    version_number  INTEGER     NOT NULL,
    status          VARCHAR(32) NOT NULL,
    definition      JSONB       NOT NULL DEFAULT '{"nodes":[],"edges":[]}'::jsonb,
    chunking_policy JSONB       NOT NULL,
    -- Fleet-wide concurrency for this pipeline. maxConcurrentChunks = 1 means strictly
    -- sequential, which is a genuine requirement for lock-contended targets such as
    -- concurrent Salesforce bulk jobs against the same object.
    execution_policy JSONB      NOT NULL,
    audit_policy    JSONB       NOT NULL,
    mode            VARCHAR(32) NOT NULL,
    change_note     TEXT,
    created_by      VARCHAR(255),
    created_at      TIMESTAMPTZ NOT NULL,
    published_at    TIMESTAMPTZ,

    CONSTRAINT uq_pipeline_version UNIQUE (pipeline_id, version_number),
    CONSTRAINT ck_pipeline_version_number CHECK (version_number >= 1),
    CONSTRAINT ck_pipeline_version_status CHECK (status IN ('DRAFT', 'VALIDATED', 'PUBLISHED')),
    CONSTRAINT ck_pipeline_version_mode CHECK (mode IN ('FULL_LOAD', 'INCREMENTAL', 'STREAMING', 'CDC')),
    CONSTRAINT ck_pipeline_version_published_at CHECK (
        status <> 'PUBLISHED' OR published_at IS NOT NULL
    )
);

CREATE INDEX idx_pipeline_version_lookup ON pipeline_version (pipeline_id, version_number DESC);
CREATE INDEX idx_pipeline_version_tenant ON pipeline_version (tenant_id, status);

-- A published version is frozen. Only the transition into PUBLISHED is allowed
-- to touch such a row, and nothing may delete one.
CREATE OR REPLACE FUNCTION pipeline_version_immutable() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'pipeline_version %/% is PUBLISHED and cannot be deleted; runs reference it',
            OLD.pipeline_id, OLD.version_number
            USING ERRCODE = 'restrict_violation';
    END IF;

    RAISE EXCEPTION
        'pipeline_version %/% is PUBLISHED and is immutable; create a new version instead',
        OLD.pipeline_id, OLD.version_number
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_pipeline_version_immutable
    BEFORE UPDATE OR DELETE ON pipeline_version
    FOR EACH ROW
    WHEN (OLD.status = 'PUBLISHED')
    EXECUTE FUNCTION pipeline_version_immutable();

-- -----------------------------------------------------------------------------
-- audit_log
--
-- Append-only control-plane trail (ADR-0011). Written in the same transaction as
-- the change it describes: an audit log that can disagree with the data it
-- records is worse than none, because it is trusted.
--
-- Immutability is enforced by trigger rather than by GRANT alone. Grants can be
-- widened by a well-meaning operator during an incident; a trigger states the
-- intent in the schema where it is visible.
-- -----------------------------------------------------------------------------
CREATE TABLE audit_log (
    id            UUID         PRIMARY KEY,
    tenant_id     UUID         NOT NULL REFERENCES tenant (id) ON DELETE CASCADE,
    occurred_at   TIMESTAMPTZ  NOT NULL,
    actor         VARCHAR(255) NOT NULL,
    action        VARCHAR(64)  NOT NULL,
    resource_type VARCHAR(64)  NOT NULL,
    resource_id   VARCHAR(128),
    summary       VARCHAR(1000),
    before_state  JSONB,
    after_state   JSONB,
    request_id    VARCHAR(128),
    source_ip     VARCHAR(64)
);

-- The dominant query is the History tab: one resource, newest first.
CREATE INDEX idx_audit_log_resource ON audit_log (tenant_id, resource_type, resource_id, occurred_at DESC);
CREATE INDEX idx_audit_log_time     ON audit_log (tenant_id, occurred_at DESC);
CREATE INDEX idx_audit_log_actor    ON audit_log (tenant_id, actor, occurred_at DESC);

CREATE OR REPLACE FUNCTION audit_log_append_only() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only; % is not permitted', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_log_append_only
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION audit_log_append_only();

COMMENT ON TABLE audit_log IS
    'Append-only. UPDATE and DELETE are blocked by trigger. Never TTL''d.';
