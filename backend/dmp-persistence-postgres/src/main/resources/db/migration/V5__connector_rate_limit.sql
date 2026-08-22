-- What the far end has agreed to accept: records per window, calls per window, or both.
--
-- On the connector instance rather than the pipeline because the limit belongs to the endpoint.
-- Three pipelines feeding one client draw on one budget; a per-pipeline column would multiply the
-- agreed rate by however many pipelines happened to exist, silently, on the day somebody added
-- the second one.
--
-- Nullable, and null means unlimited. Every connector configured before this column existed was
-- configured by somebody who had no such agreement to record, and a default here would invent one.
ALTER TABLE connector_instance ADD COLUMN rate_limit jsonb;
