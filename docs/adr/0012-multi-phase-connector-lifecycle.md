# ADR-0012: Multi-phase connector lifecycle for asynchronous external jobs

**Status:** Accepted (2026-08-06). Amends the SPI sketched in [ADR-0006](0006-connector-spi-plugin-isolation.md).

## Context

The connector SPI in ADR-0006 assumed a connector can be opened and read from more or less
immediately:

```java
List<Split>  plan(SplitPlanRequest req);
RecordStream read(Split split, Checkpoint from);
```

That holds for JDBC, MongoDB, Kafka, files and most REST APIs. It does not hold for a whole class
of systems where reading requires submitting an asynchronous job first. Salesforce Bulk API v2 is
the canonical example:

```
1. POST   /jobs/query              → jobId                       instant
2. GET    /jobs/query/{id}         → poll until JobComplete      minutes to hours
3. GET    /jobs/query/{id}/results → paged via Sforce-Locator    the actual read
4. DELETE /jobs/query/{id}         → release the job             mandatory
```

Under the existing SPI, `plan()` could not return until step 2 completed, because the number of
result locators is unknown until then. A `plan()` call blocking a thread for hours is not a
tuning problem; it is a design error.

This is not a Salesforce quirk. The same submit-poll-fetch-release shape appears in Amazon Athena,
BigQuery extract jobs, Redshift `UNLOAD`, Snowflake `COPY INTO`, Databricks jobs and Elasticsearch
point-in-time setup. The original SPI accommodated one class of connector and silently excluded
another, and the excluded class contains most enterprise analytics systems.

## Decision

### The lifecycle is symmetric across source and sink

Both directions gain explicit asynchronous phases. Sinks need this at least as much as sources —
Salesforce Bulk v2 **ingest** is the canonical example, not query.

```java
public interface SourceSession extends AutoCloseable {
    SchemaCatalog     discover();

    /** Submits any external job. Returns immediately with a handle; never blocks. */
    Preparation       prepare(PrepareRequest request);

    /** Polled by the engine. Returns PENDING, READY or FAILED with a suggested re-check delay. */
    PreparationStatus checkPreparation(Preparation preparation);

    /** Called only once preparation reports READY. */
    List<Split>       plan(Preparation preparation);

    RecordStream      read(Split split, Checkpoint from);

    /** Releases external resources. Must be idempotent — it will be called more than once. */
    void              release(Preparation preparation);
}

public interface SinkSession extends AutoCloseable {
    SinkCapabilities  capabilities();

    /** Creates the external job.            Salesforce: POST /jobs/ingest                  */
    Preparation       prepare(PrepareRequest request);

    /** Uploads a batch.                     Salesforce: PUT /jobs/ingest/{id}/batches      */
    WriteResult       write(RecordBatch batch);

    /** Signals no more data. Returns a handle; may not have landed yet.
     *                                       Salesforce: PATCH state=UploadComplete         */
    Commit            commit(Preparation preparation);

    /** Polled by the engine until the external system finishes processing.
     *                                       Salesforce: GET /jobs/ingest/{id}              */
    CommitStatus      checkCommit(Commit commit);

    /** Retrieves per-record outcomes once processing completes.
     *                                       Salesforce: GET .../failedResults, .../successfulResults */
    ResultHarvest     harvest(Commit commit);

    /** Idempotent.                          Salesforce: DELETE /jobs/ingest/{id}           */
    void              release(Preparation preparation);
}
```

Connectors needing no preparation return `Preparation.none()`, whose status is immediately `READY`.
Synchronous sinks return a `Commit` whose status is immediately `COMPLETE`. The common case costs
extra method calls and no extra state.

### A successful `write()` does not mean the records landed

This is the correctness consequence, and it is easy to get wrong. Salesforce returns `201` from
step 2 immediately, having only *accepted* the upload; the records are processed at step 4, which
may fail wholesale on a validation rule, a lock contention error or a trigger exception.

ADR-0009 says a checkpoint is written "after a successful sink flush". For an asynchronous sink
that phrasing is a trap: checkpointing after `write()` returns would advance the source cursor past
records the sink later rejected, silently losing them on resume. The rule is therefore qualified:

> **The checkpoint advances after `checkCommit()` reports COMPLETE — not after `write()` returns.**

`SinkCapabilities.commitIsAsynchronous()` tells the engine which rule applies. A sink declaring it
falsely produces silent data loss, so the TCK asserts it by killing the worker between `write()`
and `checkCommit()` and verifying no records are lost.

### `harvest()` is not optional for a migration platform

Step 4 reports `numberRecordsFailed: 5000`. Only `failedResults` reports **which** five thousand
and **why** — a picklist validation, a required field, a duplicate rule. A platform that surfaces
only the count tells the user they have a problem and nothing about how to fix it.

`harvest()` therefore feeds the `ERRORS` audit tier of [ADR-0011](0011-audit-model.md) directly:
each failed row becomes a record-error document with its payload and the external system's own
error text. It also returns the assigned external ids from `successfulResults`, which any
downstream pipeline needs.

### Polling goes through the delay queue, not a sleeping thread

`prepare()` returns, the engine writes the handle to the run and schedules a re-check through the
MongoDB TTL delay queue ([ADR-0002](0002-delay-queue-mongo-ttl.md)). A consumer wakes, calls
`checkPreparation()`, and either advances the run or reschedules with the connector's suggested
delay.

The delay queue's ~60 second floor, accepted as a cost in ADR-0002, is irrelevant here: Salesforce
bulk jobs take minutes to hours, and polling one more often than once a minute would only consume
API quota. The two decisions fit together better than either was designed to.

Nothing sleeps. Nothing holds a thread. A worker may be killed at any point during a preparation
that lasts hours, and the run is unaffected.

### Preparation state is persisted on the run

`Run.preparationState` is a JSON object keyed by node id:

```json
{
  "sfdc-source": { "jobId": "750xx000000005LAAQ", "submittedAt": "...", "attempts": 7 },
  "athena-source": { "queryExecutionId": "a1b2c3...", "submittedAt": "..." }
}
```

Keyed by node because a pipeline may have several sources, each with its own external job. Stored
on the run rather than held by a worker because the worker that submitted the job will frequently
not be the one that observes it finishing.

### Two new run states

```
CREATED → VALIDATED → PREPARING → RUNNING → FINALIZING → COMPLETED
                          │                      │
                          └─ polled via          └─ releases external resources
                             delay queue
```

- **PREPARING** — external jobs submitted, being polled. May last hours. Distinct from RUNNING so
  that "waiting on Salesforce" and "moving data" are distinguishable in the UI, in alerting and in
  duration metrics. Collapsing them would make every dashboard lie about throughput.
- **FINALIZING** — final sink commits and `release()` calls. Distinct from COMPLETED because
  releasing an external resource is a network call that can fail and must be retried. Folding it
  into a state transition would make it unobservable and unretryable.

### Release is guaranteed twice over

Salesforce caps bulk jobs at 10,000 per rolling 24 hours **per org**. A leak does not degrade one
pipeline; it stops every integration the organisation has, including ones this platform does not
own. That justifies redundancy:

1. **Happy path** — `FINALIZING` calls `release()` for every entry in `preparationState`.
2. **Reaper** — a scheduled sweep finds runs in a terminal state whose `preparationState` still
   holds unreleased handles, and releases them. This covers the case where a worker dies between
   failing and cleaning up, which the happy path structurally cannot.

`release()` must therefore be idempotent, and the SPI says so. A connector treating an already
deleted job as an error would make the reaper produce noise instead of safety.

## Consequences

**Positive**
- An entire class of enterprise connectors becomes expressible: Salesforce, Athena, BigQuery,
  Redshift, Snowflake, Databricks.
- Long preparations consume no threads and survive worker restarts.
- "Waiting on an external system" is visible as a first-class state rather than appearing as a
  stalled run.
- External resource leaks are structurally prevented rather than left to correct shutdown.
- The delay queue earns its place a second time, for a use case whose latency requirements suit it
  exactly.

**Negative**
- Five methods rather than three on `SourceSession`. Mitigated by `Preparation.none()`, so simple
  connectors ignore the addition entirely.
- Two extra run states to model in the engine, the API and the UI.
- Preparation is a distributed state machine driven by an at-least-once delay queue, so
  `checkPreparation()` may be invoked concurrently for the same handle. Connectors must tolerate
  it; the TCK will test for it.
- The reaper is another scheduled job that must itself be monitored — an unrun reaper fails
  silently, which is the worst failure mode for a safety net. Phase 7 exports its last-success
  timestamp as an alertable metric.

## Impact on the Connector TCK

Three additional compliance tests:

| Contract | Assertion |
|---|---|
| Preparation resumability | A handle serialised, discarded and reloaded still polls to READY |
| Release idempotency | `release()` called twice, and on an already-released handle, does not throw |
| Concurrent status checks | Two simultaneous `checkPreparation()` calls on one handle agree |

## Alternatives rejected

- **Block inside `plan()`.** Simplest, and holds a thread for hours while being unable to survive a
  restart.
- **A dedicated `AsyncSource` interface alongside `Source`.** Two SPIs to maintain, two engine code
  paths, and connector authors forced to choose up front — a choice they would frequently get wrong,
  since "does this ever need async preparation" is not obvious until a large dataset arrives.
- **Let the connector manage its own polling on an internal thread.** Moves the concurrency problem
  into every connector, makes it invisible to the platform, and loses resumability entirely.
