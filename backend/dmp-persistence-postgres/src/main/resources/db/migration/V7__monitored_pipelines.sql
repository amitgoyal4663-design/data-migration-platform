-- =============================================================================
-- V7 — the support team's watchlist.
--
-- A flag rather than a separate table: it is one boolean per pipeline, read on
-- every dashboard load and written by hand a few times a year. A join table
-- would model a relationship that has exactly one attribute and no history
-- anybody has asked to keep.
--
-- Deliberately not a tag. Tags are the user's own vocabulary and they are free
-- to reorganise them; a reserved tag would break the moment somebody tidied up,
-- and the breakage would be silent — a pipeline quietly dropping off the screen
-- the support team trusts to be complete.
-- =============================================================================

ALTER TABLE pipeline
    ADD COLUMN monitored BOOLEAN NOT NULL DEFAULT FALSE;

-- The dashboard's only query: every watched pipeline for a tenant. Partial,
-- because the watchlist is a handful of rows out of however many exist.
CREATE INDEX idx_pipeline_monitored ON pipeline (tenant_id) WHERE monitored;
