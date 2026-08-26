-- A schedule names the query it runs, the same way a manual run does.
--
-- Until now it could not, so every scheduled run took whichever query happened to be declared
-- first on the connection. That made a schedule's behaviour depend on the order of a list edited
-- somewhere else entirely: reordering the queries on a connector -- or pressing "Make default" on
-- a different one -- silently changed what every schedule of every pipeline using it read at 3am,
-- with nothing on the schedule itself showing that it had moved.
--
-- Null keeps the old behaviour deliberately, and means the same thing here as it does for a manual
-- run: use the first declared query. Existing schedules were created under that rule and are
-- reading the right data today; rewriting them to name a query would be this migration guessing at
-- an intent nobody recorded. They keep working, and naming the query is an edit somebody makes
-- when they next have a reason to.

ALTER TABLE schedule ADD COLUMN query_name text;

COMMENT ON COLUMN schedule.query_name IS
    'Which named query on the source connection this schedule runs. Null means the first declared '
    'query, which is what every schedule did before this column existed.';
