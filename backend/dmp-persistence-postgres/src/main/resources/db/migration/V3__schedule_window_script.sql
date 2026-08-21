-- The range of data each firing of a schedule covers.
--
-- Nullable, and null is the existing behaviour: a schedule without one starts a run that reads
-- whatever its query says, exactly as every schedule did before this column existed. Nothing has
-- to be backfilled and no existing schedule changes what it does.
--
-- Text rather than a set of columns because the arithmetic varies per pipeline: one query wants
-- the previous calendar day, another the previous hour, another a date string in its own format.
-- A windowUnit/windowCount pair would cover the first two and be extended for every case after.
ALTER TABLE schedule ADD COLUMN window_script text;

COMMENT ON COLUMN schedule.window_script IS
    'JavaScript returning the parameters each run is started with, e.g. { from, to }. '
    'Evaluated at fire time with the fire time injected and no access to the wall clock, so a '
    'run''s window is a pure function of when it was due. Null means no parameters.';
