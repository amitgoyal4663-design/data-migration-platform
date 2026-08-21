-- =============================================================================
-- Demo data for the migration walkthrough.
--
-- Runs once, on an empty PostgreSQL data directory. Creates a `demo` schema with a
-- populated source table and an empty target, so there is something real to migrate
-- the moment the stack comes up.
--
-- Deliberately in its own schema: the platform's own tables live in `public`, and
-- mixing demo data into them would make it unclear what is the product and what is
-- the example.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS demo;

-- ----------------------------------------------------------------------------
-- Source: 50,000 orders. Large enough to be split into several chunks, so the
-- run genuinely exercises parallelism rather than completing in one batch.
-- ----------------------------------------------------------------------------
CREATE TABLE demo.orders (
    id          BIGINT PRIMARY KEY,
    customer    TEXT           NOT NULL,
    email       TEXT           NOT NULL,
    amount      NUMERIC(18,2)  NOT NULL,
    currency    TEXT           NOT NULL,
    status      TEXT           NOT NULL,
    placed_at   TIMESTAMPTZ    NOT NULL
);

INSERT INTO demo.orders (id, customer, email, amount, currency, status, placed_at)
SELECT
    g,
    'Customer ' || g,
    'customer' || g || '@example.com',
    -- Deliberately awkward decimals. If any of these arrive rounded, the platform
    -- has silently converted through a double somewhere and the ledger will not
    -- reconcile.
    (g * 1.07)::numeric(18,2),
    (ARRAY['GBP', 'USD', 'EUR'])[1 + (g % 3)],
    (ARRAY['placed', 'shipped', 'delivered', 'cancelled'])[1 + (g % 4)],
    now() - (g || ' minutes')::interval
FROM generate_series(1, 50000) g;

CREATE INDEX idx_demo_orders_placed_at ON demo.orders (placed_at);

-- ----------------------------------------------------------------------------
-- Target: same shape, empty. The migration fills it.
-- ----------------------------------------------------------------------------
CREATE TABLE demo.orders_copy (
    id          BIGINT PRIMARY KEY,
    customer    TEXT           NOT NULL,
    email       TEXT           NOT NULL,
    amount      NUMERIC(18,2)  NOT NULL,
    currency    TEXT           NOT NULL,
    status      TEXT           NOT NULL,
    placed_at   TIMESTAMPTZ    NOT NULL
);

-- ----------------------------------------------------------------------------
-- A target that rejects some rows, for exercising the dead-letter queue.
--
-- The constraint refuses cancelled orders. Roughly a quarter of the source fails
-- it, which is what makes the rejected-records view worth looking at: each entry
-- carries the row and PostgreSQL's own error message, not just a count.
-- ----------------------------------------------------------------------------
CREATE TABLE demo.orders_active_only (
    id          BIGINT PRIMARY KEY,
    customer    TEXT           NOT NULL,
    email       TEXT           NOT NULL,
    amount      NUMERIC(18,2)  NOT NULL,
    currency    TEXT           NOT NULL,
    status      TEXT           NOT NULL,
    placed_at   TIMESTAMPTZ    NOT NULL,

    CONSTRAINT ck_not_cancelled CHECK (status <> 'cancelled')
);

COMMENT ON SCHEMA demo IS
    'Sample data for the migration walkthrough. Not used by the platform itself.';
