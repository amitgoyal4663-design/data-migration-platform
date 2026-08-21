# ADR-0010: Quartz for recurring schedules, on PostgreSQL

**Status:** Accepted (2026-08-06)

## Context

The platform needs two things that both look like "run this later" and are not the same problem:

| | Recurring schedule | One-shot timer |
|---|---|---|
| Example | "Every weekday at 03:00 Europe/London" | "Retry this record in 5 minutes" |
| Volume | Tens to thousands | Millions |
| Semantics | Cron, calendars, DST, misfire policy | Fire once at a time |
| Lifetime | Months or years | Minutes to hours |

ADR-0002 chose the MongoDB TTL delay queue. It is well suited to the right-hand column and badly
suited to the left: it has no concept of a repeating rule, no misfire policy, and no timezone
handling. Building cron semantics on top of it would mean reimplementing Quartz.

## Decision

Use **Quartz** for recurring schedules, with its JDBCJobStore on the **PostgreSQL instance the
platform already runs**. Retain the MongoDB delay queue for one-shot timers.

```
Recurring rule          ──►  Quartz (JDBCJobStore, PostgreSQL, clustered)
  "every weekday 3am"          │
                               ▼
                        publishes StartRunCommand to Kafka   ← and returns immediately
                               │
One-shot timer          ──►    │
  "retry in 5 minutes"   Mongo TTL delay queue (ADR-0002)
                               │
                               ▼
                          destination topic from meta.topic
```

### A Quartz job may only publish

A scheduled job's entire body is: resolve the pipeline's published version, build a
`StartRunCommand`, publish it to Kafka, return. It must never execute a migration.

This is a hard rule. Quartz's thread pool is sized for scheduling — typically ten threads. Running
a six-hour migration on one of them would exhaust the pool, block every other schedule in the
deployment, and trigger misfires across unrelated pipelines. The scheduler decides *when*; the data
plane decides *how* and *for how long*.

### Clustering

Quartz clustering (`org.quartz.jobStore.isClustered = true`) uses row locks in `QRTZ_LOCKS` so
that exactly one control-plane replica fires a given trigger. That is leader election for the
scheduler, already built and already tested, which would otherwise be bespoke code with a subtle
failure mode.

### Misfire policy

Default `MISFIRE_INSTRUCTION_DO_NOTHING` for cron triggers: if the control plane was down at
03:00, do not fire a burst of catch-up runs on recovery. For a migration platform, six queued
backfills starting simultaneously at 07:00 is worse than one skipped run — and a deliberate
catch-up is what BACKFILL runs are for. Overridable per schedule.

## Why PostgreSQL and not a separate MySQL

Quartz's JDBCJobStore supports PostgreSQL natively; the distribution ships `tables_postgres.sql`,
vendored here as a Flyway migration. Introducing MySQL solely for the `QRTZ_*` tables would add a
fifth datastore to operate, back up and secure, in exchange for nothing. Schedules are also
definition data, which ADR-0005 places in PostgreSQL — so this keeps the ownership line intact
rather than cutting across it.

If organisational standards later require MySQL, the change is a datasource and a different
Quartz DDL script. Nothing in the application layer depends on the dialect.

## Consequences

**Positive**
- Cron, calendar and DST semantics are solved by a library with two decades of production use,
  not by us.
- Clustered leader election for free.
- Misfire handling is explicit and configurable per schedule rather than emergent.
- No new datastore.

**Negative**
- Quartz's API is dated and its configuration is stringly-typed. Contained behind a
  `SchedulePort` in `dmp-application`, so no service imports `org.quartz`.
- Cluster check-in polls the database (default 20s). Negligible at this volume, but it is polling
  and worth naming rather than discovering.
- Quartz owns eleven tables in a schema we otherwise control. Isolated in their own Flyway
  migration and never referenced by application queries.
- Two scheduling mechanisms exist. Justified by the table above, but the boundary must stay
  explicit: recurring rules are Quartz, one-shot timers are the delay queue, and neither
  reimplements the other.

## Alternatives rejected

- **Delay queue only.** Would require building cron parsing, timezone handling, misfire policy and
  schedule persistence — Quartz, rebuilt worse.
- **Spring `@Scheduled`.** No persistence, no clustering, no per-tenant dynamic schedules. Fine for
  the platform's own housekeeping tasks; unusable for user-defined schedules.
- **Kubernetes CronJobs.** Pushes user-visible configuration into cluster manifests, which the UI
  cannot manage and which does not survive as tenant data.
- **Temporal.** Rejected for the same reason as in ADR-0002: an entire additional cluster.
