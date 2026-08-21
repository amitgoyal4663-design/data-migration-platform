# ADR-0005: Datastore ownership — definition versus execution

**Status:** Accepted (2026-08-06). Supersedes the initial split in which run state lived in PostgreSQL.

## Context

The platform runs PostgreSQL, MongoDB, Redis and Kafka. Each class of data needs exactly one
owning store, or the system rots into "we write it to both and hope".

The first version of this ADR put runs, splits and checkpoints in PostgreSQL on the grounds
that run state is transactional state. That reasoning was weaker than it appeared:

- A `Run` is a **single document**. A state transition is therefore
  `findOneAndUpdate({_id, state: "RUNNING"}, {$set: {state: "PAUSED"}})` — an atomic
  compare-and-swap in one round trip, which is a *stronger* concurrency primitive than
  read-modify-write with an optimistic version column, not a weaker one.
- Idempotency keys need a unique index, which MongoDB has.

Meanwhile the argument against PostgreSQL for this data was understated. Runs, splits and
checkpoints are the highest-churn writes in the platform: a 10,000-split run checkpointing every
five seconds is relentless UPDATE traffic, and every PostgreSQL update leaves a dead tuple. That
is precisely the vacuum thrash this ADR cited as the reason to keep DLQ payloads out of
PostgreSQL — while placing runs in the same store.

## Decision

The line is **definition versus execution**.

### PostgreSQL — definitions and configuration

tenants · pipelines · pipeline_versions · connector_instances · schedules · audit_log · RBAC ·
`QRTZ_*` (Quartz JobStore, see [ADR-0010](0010-quartz-scheduling.md))

Low write volume, long lived, strongly relational. Needs foreign keys, transactions and unique
constraints. A pipeline version referenced by a run must not vanish, and only a relational store
enforces that without application cooperation.

### MongoDB — execution data

runs · splits · checkpoints · execution telemetry · per-record errors and DLQ payloads ·
transformation traces · dry-run samples · discovered schema catalogs · delay-queue timers
([ADR-0002](0002-delay-queue-mongo-ttl.md))

High churn, document-shaped, governed by TTL retention. This also collapses a cross-store join
that the earlier split created for no benefit: a run and its per-record errors now live together.

### Redis — ephemeral acceleration

rate-limit token buckets (Lua) · live run progress counters, write-behind to MongoDB ·
hot metadata cache · idempotency dedup window

**Invariant: nothing in Redis may be the only copy of anything.** A full flush must degrade
performance and nothing else.

### Kafka — the log

control commands · split assignment · run events · DLQ · streaming data path
([ADR-0001](0001-dual-mode-channel.md))

## Consequences

**Positive**
- High-churn writes go to a store whose update model suits them. No vacuum pressure from
  checkpointing.
- Run documents are naturally shaped for nested metrics, split summaries and structured error
  detail, with no object-relational mapping in between.
- Retention is a TTL index rather than a partition-drop job.
- Runs and their per-record errors are co-located, so error drill-down is one query.
- Execution schema can evolve as the engine grows without a migration on a hot table.

**Negative — accepted**
- **`run.pipelineVersionId` integrity is application-enforced.** Mitigation: published pipeline
  versions are never hard-deleted, only archived. `PipelineVersionRepository` refuses deletion of
  a published version outright, so the dangling reference is unreachable rather than merely
  unlikely.
- **Reporting over run history loses SQL.** The aggregation pipeline is adequate for the run list
  and dashboards. If Phase 7 analytics outgrow it, the answer is a rollup job into PostgreSQL, not
  moving the write path back.
- **MongoDB must run as a replica set, including in development.** Change streams require an
  oplog, so a standalone `mongod` cannot support the delay queue at all. Docker Compose runs
  `--replSet rs0` with an init container. This is a hard requirement, not a production-only concern.
- **Two persistence adapters to maintain.** Contained by both implementing ports declared in
  `dmp-application`, so the split is invisible above the adapter layer.

## Enforcement

Repository interfaces are out-ports in `dmp-application`. Implementations live in
`dmp-persistence-postgres` and `dmp-persistence-mongo`. An ArchUnit rule forbids any module from
depending on both adapters, and forbids `dmp-application` from depending on either — which makes
an accidental cross-store write a build failure rather than a review catch.
