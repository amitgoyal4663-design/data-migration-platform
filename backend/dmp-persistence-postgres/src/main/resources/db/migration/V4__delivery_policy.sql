-- How a batch is divided into calls on the sink: the whole batch, one record at a time,
-- fixed groups, or groups decided by a script.
--
-- Nullable rather than defaulted. Every version stored before this column existed was written by
-- somebody who never saw the setting, and stamping a value on their rows would present a default
-- as a decision. The domain record substitutes DeliveryPolicy.DEFAULT when the column is null,
-- which is precisely the behaviour those versions already had.
ALTER TABLE pipeline_version ADD COLUMN delivery_policy jsonb;
