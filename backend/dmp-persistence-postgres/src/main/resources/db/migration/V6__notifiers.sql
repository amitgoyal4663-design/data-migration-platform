-- =============================================================================
-- V6 — where to send word when a run ends badly.
--
-- A definition, so it lives here beside pipelines and schedules rather than with
-- execution state (ADR-0005).
--
-- The credential is a reference, never a value, exactly as connector instances
-- store theirs. A webhook URL that carries its own token in the query string is
-- the common shortcut and it is why `url` is not treated as harmless: it is
-- shown in the console and returned by the API, so a token placed there is a
-- token the user has chosen to expose. The header is the supported way.
-- =============================================================================

CREATE TABLE notifier (
    id             UUID         PRIMARY KEY,
    tenant_id      UUID         NOT NULL REFERENCES tenant (id) ON DELETE CASCADE,

    -- Null means every pipeline in the tenant. Both are wanted: an operations
    -- channel watches everything, the team that owns one migration watches one.
    pipeline_id    UUID         REFERENCES pipeline (id) ON DELETE CASCADE,

    name           VARCHAR(255) NOT NULL,
    url            TEXT         NOT NULL,

    -- Comma-separated event names rather than a join table. The set is small,
    -- fixed, always read whole and never queried by member; a table would add a
    -- join to every read to model something no query asks about.
    events         VARCHAR(512) NOT NULL,

    secret_header  VARCHAR(128),
    secret_ref     VARCHAR(255),
    enabled        BOOLEAN      NOT NULL DEFAULT TRUE,
    description    TEXT,

    -- The commonest way an alerting system fails is silently: a rotated URL
    -- returns 404 forever and nobody notices, because the thing that would have
    -- said so is the thing that is broken.
    last_attempt_at        TIMESTAMPTZ,
    last_attempt_succeeded BOOLEAN NOT NULL DEFAULT FALSE,
    last_attempt_error     TEXT,

    created_at     TIMESTAMPTZ  NOT NULL,
    updated_at     TIMESTAMPTZ  NOT NULL,
    row_version    BIGINT       NOT NULL DEFAULT 0,

    CONSTRAINT uq_notifier_name UNIQUE (tenant_id, name)
);

-- Serves the only hot query there is: every live notifier for a tenant, asked
-- once per run that ends.
CREATE INDEX idx_notifier_enabled ON notifier (tenant_id, enabled) WHERE enabled;
