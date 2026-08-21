# Run Lifecycle — Low-Level Design

What happens between "start this pipeline" and a finished migration: which component acts, what
is written where, and what any of it sends to anybody else.

The high-level design is in [README.md](README.md). This document is the level below it, and is
written against the code rather than the intent — where the two have diverged, the code wins and
the divergence is called out.

---

## 1. The short version

```
  trigger ─► Run document in MongoDB, state CREATED
                      │
                      │   (nothing is sent anywhere. no queue, no broker, no callback)
                      ▼
  every worker polls MongoDB:  "any CREATED run?  any PENDING chunk?"
                      │
                      ▼
  first worker to notice advances the run:  CREATED → VALIDATED → PREPARING → RUNNING
  and writes its chunks to MongoDB as PENDING
                      │
                      ▼
  every worker polls again and races to claim chunks
                      │
                      ▼
  the winner reads from the source, transforms, writes to the sink,
  and only then advances the checkpoint
                      │
                      ▼
  last chunk completes ─► run COMPLETED
```

The single most important property: **nothing pushes work to a worker.** A worker is not
registered, not addressed, and not known to the control plane. It finds work by asking.

---

## 2. Starting a run

Three things can create a run, and all three do the same small amount of work.

| Trigger | Entry point |
|---|---|
| `POST /api/v1/pipelines/{id}/runs` | `RunController` → `RunOrchestrator.start(...)` |
| A Quartz schedule firing | `ScheduledRunStarter` → evaluate window script → `RunOrchestrator.start(...)` |
| Retry or replay of an earlier run | `RunOrchestrator.retry(...)` / `.replay(...)` |

`start()` resolves the pipeline's **published** version, records an audit entry in PostgreSQL,
and inserts one document into MongoDB's `run` collection in state `CREATED`. Then it returns.

It does not plan chunks, open a connection, contact a worker, or publish anything that a worker
consumes. A scheduled run and an API run are indistinguishable a millisecond later.

### The run document

```json
{
  "_id":               "<uuid v7, binary>",
  "tenantId":          "<uuid>",
  "pipelineId":        "<uuid>",
  "pipelineVersionId": "<uuid>",
  "versionNumber":     6,
  "mode":              "FULL_LOAD",
  "trigger":           "SCHEDULED",
  "state":             "CREATED",
  "idempotencyKey":    "schedule:<scheduleId>:<fireTimeMillis>",
  "parameters":        { "from": "2026-08-19T00:00:00+05:30",
                         "to":   "2026-08-20T00:00:00+05:30" },
  "activeSlots":       0,
  "metrics":           { "recordsRead": 0, "recordsWritten": 0, "recordsFailed": 0,
                         "recordsFiltered": 0, "bytesRead": 0,
                         "splitsTotal": 0, "splitsCompleted": 0, "splitsFailed": 0 },
  "rowVersion":        0,
  "createdAt":         "...", "startedAt": null, "endedAt": null,
  "errorCode":         null,  "errorMessage": null
}
```

Two fields carry more weight than their size suggests.

**`idempotencyKey`** is what stops a Quartz misfire storm creating four identical runs. For a
schedule it is derived from the schedule id and the fire time, so re-firing the same instant is
a no-op rather than a duplicate migration.

**`activeSlots`** is the fleet-wide concurrency counter. It lives on the run rather than anywhere
central precisely so that reserving a slot is one atomic document update — see §5.

---

## 3. How a worker finds out

It doesn't find out. It asks, on a loop, forever.

```java
@EventListener(ApplicationReadyEvent.class)
public void start() {
    this.pollerThread = Thread.ofPlatform()
            .name("dmp-worker-poller")
            .daemon(true)
            .start(this::poll);
}
```

One platform thread per pod, started once at boot. Chunks then execute on **virtual** threads, so
a pod can hold several chunks without holding several OS threads.

Each pass does two things:

```java
boolean pollOnce() {
    boolean didWork = false;
    for (Run run : runs.findByStates(Set.of(RunState.CREATED), BATCH_SIZE)) {
        didWork |= advance(run);          // start runs nobody has started yet
    }
    didWork |= claimAndExecute();         // claim chunks this pod has room for
    return didWork;
}
```

### Self-pacing, and the latency it costs

```java
if (didWork) {
    wait = busyPollInterval.toMillis();          // 200 ms
    idleMillis = idlePollInterval.toMillis();    // busy again: forget the backoff
} else {
    wait = idleMillis;                           // 5s, then 10, capped at 15
    idleMillis = Math.min(idleMillis * 2, MAX_IDLE_POLL.toMillis());
}
```

| State | Interval |
|---|---|
| busy — claimed a chunk on the last pass | 200 ms (`dmp.worker.busy-poll-interval`) |
| idle, first miss | 5 s (`dmp.worker.idle-poll-interval`) |
| idle, doubling | 10 s |
| idle, capped | 15 s (`MAX_IDLE_POLL`) |

An idle pod used to ask every five seconds forever; twenty pods then spent four queries a second
establishing that nothing was running. The backoff only ever grows while there is provably nothing
to do, and collapses to 200 ms the moment a chunk is claimed. A fully idle pod costs two indexed
queries every fifteen seconds.

**The cost is real and worth stating**: a run created after a quiet spell waits up to fifteen
seconds before any pod notices it. Nothing wakes a worker when a run appears. This is the largest
source of start-up latency in the platform, and the obvious next improvement.

> The method's javadoc describes this ladder as "five seconds, then ten, twenty, forty, capped at
> a minute". That is not what the code does — `MAX_IDLE_POLL` is fifteen seconds, so the sequence
> stops at 5 → 10 → 15. The comment is wrong, not the constant.

### Fairness across runs

`pollOnce` visits each runnable run and takes **at most one chunk per run per pass**. Draining one
run before looking at the next would let a ten-thousand-chunk migration starve a ten-chunk one
queued behind it.

---

## 4. Advancing a run to RUNNING

The first worker to see a `CREATED` run calls `RunOrchestrator.advanceToRunning(run, workerId)`.
Every transition is a conditional update — `findOneAndUpdate` matching the expected current state
— so if two workers see the same run, one wins and the other's update matches nothing.

```
CREATED ──► VALIDATED ──► PREPARING ──► RUNNING
```

| Step | What happens | Where it is written |
|---|---|---|
| `VALIDATED` | the published version resolves; connector instances exist and their config resolves | `run.state` |
| `PREPARING` | the source's `prepare()` runs. Asynchronous sources (Salesforce Bulk, a warehouse export) submit a job and return a handle; the engine polls `checkPreparation` until ready. Synchronous sources return `none()` and pass straight through | `run.preparationState` |
| plan | `RunPlanner.planChunks(...)` decides the chunks and writes them | `split` documents, `PENDING` |
| `RUNNING` | chunk count is counted from what was saved and set once | `run.state`, `run.metrics.splitsTotal` |

A run derived from another skips planning: a retry seeds its chunks from the run being
re-attempted, a replay from the stored dead-letter records.

### Why the failure handler re-reads the run

The run is already at `PREPARING` by the time planning can throw, so the copy the failure handler
was handed is two states out of date. Using it as the precondition matched nothing and did so
silently — the run stayed in `PREPARING` for ever with no error on it.

```java
private void failRun(Run run, Exception cause) {
    Run current = runs.findById(run.tenantId(), run.id()).orElse(run);
    runs.transitionState(current.tenantId(), current.id(), current.state(),
                    current.fail("RUN_FAILED", cause.getMessage(), clock.instant()))
            .ifPresentOrElse(...);
}
```

---

## 5. How work is distributed

Two atomic MongoDB operations. That is the entire mechanism.

```javascript
// 1. reserve a slot — bounded across the whole fleet, not per pod
run.findAndModify(
   filter: { _id: runId, activeSlots: { $lt: maxConcurrentChunks } },
   update: { $inc: { activeSlots: 1 } })

// 2. claim the lowest-index PENDING chunk
split.findAndModify(
   filter: { runId, state: "PENDING" },  sort: { index: 1 },
   update: { $set: { state: "RUNNING", assignedTo: workerId,
                     leaseExpiresAt: now + chunkLease } })
```

**In that order, never the reverse.** Claiming first and then discovering the run is at its limit
would leave a chunk marked `RUNNING` with nobody executing it, stalled until the lease expired.

MongoDB serialises each `findAndModify`, so two workers racing cannot both receive the same chunk
— one gets it, the other gets the next, or nothing.

### What this buys

- **Self-balancing.** A pod only asks when it is ready. One finishing a fast chunk asks again
  immediately; one grinding a slow chunk asks for nothing. Nobody predicts chunk duration.
- **Elasticity.** Add a pod and it starts pulling. No rebalance, no registration, no config.
- **`maxChunksPerPod`** stops a single pod claiming an entire parallel run at startup.

### What sequential does and does not give

With `maxConcurrentChunks: 1`, exactly one chunk of that run executes anywhere at any instant. The
pod running it may still change chunk to chunk, and other runs occupy the remaining pods — the
limit is per-run, so an ordered migration never idles the cluster.

Measured on a 201-chunk run with two workers and a limit of 1:

```
chunks per worker:   worker-A: 167,  worker-B: 34
handovers:           68
overlapping pairs:   0          ← the limit held across the fleet
```

The split is uneven because the worker holding the run polls at 200 ms while an idle one has
backed off, so the incumbent usually wins the next chunk. On short runs this looks like a single
pod taking everything; it is bias, not pinning.

### Leases, not heartbeats

A claimed chunk carries `leaseExpiresAt`, extended every `chunkLease / 3` — a third, so two
consecutive missed beats do not cost a worker a chunk it is actively processing. A worker that
stops extending, because it died or was partitioned or is wedged, loses the chunk to a sweep that
returns it to `PENDING` **and releases its slot**.

The heartbeat is conditional on `assignedTo` still matching. A worker whose lease lapsed must fail
to extend it; otherwise two pods would each believe they hold the claim and write every record of
that chunk twice.

---

## 6. Executing one chunk

One worker owns read, transform and write for its chunk. There is no shuffle, no handoff between
stages, and no second pod involved.

```
claim chunk                     split.findAndModify
open source cursor              resuming from the checkpoint, if any
loop:
    read a batch                readFetchSize records
    transform                   GraalJS sandbox, if a transform is configured
    write the batch             sink.write(batch) — blocks until durably accepted
    index the records           OpenSearch, async, if the audit level says so
    advance the checkpoint      checkpoint.sourceCursor = stream.cursor()
release slot, mark COMPLETED
```

### The one ordering rule the platform is built on

`sink.write(batch)` must durably succeed **before** `checkpoint.advance(cursor)`. Every connector
is written to honour it. The Kafka sink is the clearest illustration:

```java
for (DataRecord record : batch.records()) {
    pending.add(producer.send(new ProducerRecord<>(topic, keyOf(record), payload)));
}
producer.flush();
for (var future : pending) {
    future.get();            // waits for every broker acknowledgement
}
return WriteResult.allWritten(batch.size(), batch.totalBytes());
```

> Waiting is the point. The engine advances the checkpoint once this returns, so returning before
> the broker has the records would let a crash resume past messages that were never durably stored.

Reversing the order converts a pod restart into silent data loss. Keeping it means a crash
re-delivers the last batch — at-least-once, which is the platform's declared contract everywhere.

### Read size and write size are independent

`readFetchSize` is the source round-trip size; `writeBatchSize` is the sink batch size. A JDBC
cursor and an OpenSearch bulk request want very different numbers, and one setting would be wrong
for one of them. The sink's own protocol limit wins over the user's setting — a user configuring
50,000 against a sink accepting 10,000 is corrected at execution rather than discovering it as a
rejection halfway through.

### Parking, for sinks that answer later

A destination that accepts a job and reports much later — Salesforce Bulk v2, for instance — does
not hold a worker while it thinks. The chunk **parks**: it surrenders its worker thread *and* its
lease, and the job handle is persisted onto the split (`state: WAITING_EXTERNAL`). A Quartz job
polls for readiness, and whichever pod is free harvests the result. Read and write for one chunk
can therefore land on different pods, minutes apart.

---

## 7. What is stored, and where

The line is **definition versus execution** ([ADR-0005](../adr/0005-datastore-ownership.md)).

### PostgreSQL — definitions

| Table | Holds |
|---|---|
| `tenant` | tenants |
| `pipeline` | pipeline identity |
| `pipeline_version` | the frozen definition: `definition` (nodes and edges), `chunking_policy`, `execution_policy`, `audit_policy`, status, change note |
| `connector_instance` | `config` and `secret_refs` — **references only, never secret values** |
| `schedule` | cron expression, timezone, window script, enabled flag |
| `audit_log` | who did what to a definition |
| `QRTZ_*` | Quartz's clustered job store |

There is **no `run` table.** Asking PostgreSQL about a run will always return nothing.

### MongoDB — execution state

| Collection | Holds | Lifetime |
|---|---|---|
| `run` | one document per run: state, metrics, parameters, `activeSlots` | until archived |
| `split` | one per chunk: `spec`, `state`, `assignedTo`, `leaseExpiresAt`, `attempt`, `externalJob` | with the run |
| `checkpoint` | one per split: `sourceCursor`, `recordsRead`, `lastSeq` | with the run |
| `record_error` | the dead-letter queue: payload, error code, message, `expiresAt` | TTL, 30 days by default |
| `record_error_signature` | per-fault counts and the sampling reservation | TTL |

A run is a single document, so a state transition is one atomic compare-and-swap:

```javascript
findOneAndUpdate({_id, state: "RUNNING"}, {$set: {state: "PAUSED"}})
```

— a stronger concurrency primitive than read-modify-write against a version column, not a weaker
one.

### OpenSearch — the record audit index

One document per record, id `chunkId:seq:ordinal`, written before the chunk's checkpoint advances
and never dropped. (Not to be confused with `RecordLogPort`, which *does* drop events when its
backend is slow — a migration must not wait on its own logging. That trade is unacceptable here: a
missing entry does not blur the answer, it inverts it, and the search reports "not transferred" for
a record that was.)

```json
{ "tenantId": "...", "pipelineId": "...", "runId": "...", "chunkId": "...",
  "seq": 11, "ordinal": 0,
  "recordKey": "2900", "outcome": "WRITTEN", "errorCode": null,
  "occurredAt": "...", "expiresAt": "...",
  "record": { ... } }
```

`outcome` is `WRITTEN`, `REJECTED`, or `SENT` (an async sink took the batch but has not ruled).
`record` is present only when the audit policy asks for payloads, and is redacted and size-capped
exactly as a dead-lettered payload is.

The id is the engine's coordinates, not the record's own key:

- `chunkId` — the chunk, which is also the unit that retries.
- `seq` — position within that chunk, from 1. Stable across retries: a resumed chunk starts from
  the checkpoint and re-reads the same rows into the same positions, so record 11 is record 11 on
  every attempt. **Re-indexing is therefore idempotent** — a retried chunk updates its entries
  rather than duplicating them.
- `ordinal` — which output of that position, from 0. Above zero only where a per-record transform
  turned one input into several; those outputs deliberately share the input's `seq`, because `seq`
  is where the checkpoint resumes.

`recordKey` is a field, not the identity. It was the identity until August 2026, and that was a
bug: a source is free to hold the same key twice, two such rows were filed as one, and a run that
moved forty records left thirty entries — the index disagreeing with the run it exists to explain.
Records with no key at all were skipped entirely, so a source without a key column indexed nothing.
Both are fixed; `RecordIndexIdentityTest` pins them.

**The same assumption still underlies value-range resume cursors**, and there it is unfixed: a
duplicate key can cause a resumed chunk to skip a record. That is a data-loss risk rather than an
audit gap, and it is listed under Known gaps below.

### OpenSearch — the stage log

Off by default, per pipeline, under **Audit → Whether to log the work itself**. Four switches:
reads, transforms, writes, and whether any of them carries request and response bodies.

One document per *stage*, not per record — so at a thousand records to a batch this index is
roughly a thousandth the size of the record index.

```
READ  ──►  TRANSFORM  ──►  WRITE
query      in → out        what the destination said
```

Each answers a question nothing else could:

| Stage | one entry per | answers |
|---|---|---|
| `READ` | window of reading | *why did this run move nothing?* — the query is here |
| `TRANSFORM` | pass of the scripts | *where did my records go?* — `recordsIn → recordsOut` |
| `WRITE` | call to the destination | *what did the destination say?* |

```json
{ "runId": "...", "chunkId": "...", "traceId": "01a02041-ef67-…#7",
  "stage": "WRITE", "nodeName": "splitdemo_sink", "connectorType": "mongodb",
  "sequence": 7, "attempt": 0, "recordsIn": 5, "recordsOut": 5,
  "durationMs": 1, "outcome": "OK",
  "details": { "operation": "INSERT", "inserted": 0, "matched": 0, "modified": 0 } }
```

**`outcome` is about the stage, not its contents.** A write the destination accepted while
refusing every record inside it is `OK`; the refusals are in the record index and the signatures.
Everything else the platform stores describes a record, and the work happens in batches — so five
hundred records refused in one request was five hundred rejections and one status code, and the
status code was the part nobody kept.

#### The trace id

`traceId` is `<chunkId>#<cycle>` — one read → transform → write cycle. **The record index stamps
the same value**, which is the join: a record can be traced to the query that fetched it and the
call that wrote it, and a cycle can be shown as one story rather than two unrelated lists.

Derived rather than generated, which buys three things at once:

- a retried chunk walks the same cycles in the same order, so it reuses its trace ids and
  re-indexing overwrites instead of duplicating;
- narrowing a run's log to one chunk is a prefix match, not a second query;
- the id is legible — somebody holding one in a support ticket can see which chunk it is.

`GET /api/v1/stages/by-run?runId=…` returns them oldest first, optionally narrowed by `chunkId`
(which also accepts a full trace id) or by `stage`. Filtering to one stage across a whole run is
how read time gets compared against write time.

**Never blocks, never fails a run, drops rather than waits.** The opposite rule to the record
index, deliberately: a missing index entry inverts an answer, a missing stage entry only blurs a
diagnosis. Both go to OpenSearch; only the record index goes there synchronously.

#### Mapping updates on an existing index

Creating an index is a no-op once it exists, so a field added to the mapping reaches only
deployments that start empty. Everywhere else the first document carrying it gets the field
*dynamically* mapped — and a keyword mapped dynamically becomes `text`, which is analysed, so an
exact-match query on it silently returns nothing while the field is visibly present in every
document. `OpenSearchRecordIndex` therefore PUTs `_mapping` on every startup. That is additive
only: OpenSearch will not change an existing field's type, and an index that has already
dynamically mapped one needs reindexing.

### Kafka — events only, and only if enabled

Disabled by default (`dmp.events.kafka.enabled: false`). **No record ever passes through Kafka.**

```json
{ "type": "CHUNK_COMPLETED",
  "occurredAt": "2026-08-20T12:17:21.658Z",
  "tenantId": "...", "runId": "...", "pipelineId": "...",
  "pipelineName": "M to Kafka", "versionNumber": 2,
  "details": { "chunkIndex": 0, "recordsWritten": 100, "workerId": "worker-A" } }
```

Headers carry `eventType` and `tenantId`; the message is **keyed by `runId`**, so one run's events
land on one partition and stay in order — a consumer seeing `CHUNK_COMPLETED` after
`RUN_COMPLETED` would have to reorder them itself.

Event types: `RUN_CREATED`, `RUN_STARTED`, `CHUNK_COMPLETED`, `CHUNK_FAILED`, `RUN_PAUSED`,
`RUN_RESUMED`, `RUN_STOP_REQUESTED`, `RUN_COMPLETED`, `RUN_FAILED`.

Published with `acks=1` and never waited on:

> Waiting on the future would put broker latency directly into the chunk execution path — so a
> slow broker would slow every migration, which inverts the relationship between the work and the
> description of it.

A full buffer is counted and dropped. An event bus under pressure must not become back-pressure
on the migration.

**The platform never creates a topic.** There is no `AdminClient.createTopics` call anywhere, and
an ArchUnit rule fails the build if the method is referenced at all — including in tests, where a
convenience helper would otherwise be added first and depended on later. A missing topic is
reported loudly and then tolerated for events; for a Kafka **connector**, it stops the run:

> Topic 'orders.v1' does not exist on localhost:9092. This platform never creates topics — ask
> whoever provisions them on your cluster to create it, then run this again.

---

## 8. State machines

### Run

```
CREATED ─► VALIDATED ─► PREPARING ─► RUNNING ─┬─► FINALIZING ─► COMPLETED ─► ARCHIVED
                                              ├─► PAUSED ─► RUNNING
                                              ├─► STOPPING ─► STOPPED
                                              └─► FAILED
```

### Split

```
PENDING ─► RUNNING ─┬─► COMPLETED
                    ├─► WAITING_EXTERNAL ─► PENDING        (parked, then resumed)
                    ├─► FAILED ─► PENDING                  (retry, up to maxAttemptsPerChunk)
                    │         └─► ABANDONED                (attempts exhausted)
                    └─► CANCELLED
```

`ABANDONED` is terminal and is what a chunk reaches when it has failed its last attempt. A run
whose chunks are abandoned fails with `CHUNKS_ABANDONED`.

---

## 9. Worked example — MongoDB → Kafka, 20,000 records

Real run, `01a01f1a-ade9-77b0-bd74-60deca644a02`.

```
source     demo.orders @ mongodb://…       no splitField → defaults to _id
sink       topic orders.v1 @ localhost:9092  (3 partitions)
execution  sequential, maxConcurrentChunks: 1, rowsPerChunk: 100
chunking   readFetchSize: 500, writeBatchSize: 1000, maxBatchBytes: 8 MB
result     20,000 read · 20,000 written · 201 chunks · ~40 s · 2 workers
```

`maxConcurrentChunks: 1` plus a cursor-pageable source selects lazy planning, so nothing was
counted: 20,000 ÷ 100 = 200 chunks, plus a final chunk that found the source dry. Per chunk:

1. reserve a slot, claim the chunk — two `findAndModify` calls
2. open a cursor `_id > <checkpoint>`, sorted ascending, `batchSize` 500
3. read until the 100-row budget is hit — one round trip
4. no transform configured — passthrough
5. one batch of 100 (`writeBatchSize` is 1000, but only 100 rows exist)
6. `producer.send()` × 100, `flush()`, then `future.get()` on every one
7. advance the checkpoint to `{after: <last _id>}`
8. create chunk N+1 seeded with that cursor; release the slot

The partition key falls back to `record.key()` — the Mongo `_id` — which is why the three
partitions filled evenly and why repeated updates to one document would stay in order relative to
each other.

Nothing polled Kafka. Nothing consumed the topic. The platform's responsibility ended when the
broker acknowledged the last record.

---

## 10. Known gaps

Recorded here because a design document that only describes the intent is the reason this one
needed rewriting.

| Gap | Effect |
|---|---|
| Nothing wakes a worker when a run is created | up to 15 s before a pod notices, after a quiet spell |
| Idle backoff biases chunks toward the incumbent worker | uneven distribution on short runs; not a correctness problem |
| No check that a value-cursor split field is unique | a resumed chunk can skip records, silently. The audit index no longer assumes uniqueness, so it will now report the records that were moved — but it cannot report ones the resume never read |
| `RecordLogPort` has an implementation and no caller | a startup line announces a writer that is never invoked. Its async transport is now shared with the call log via `AsyncBulkIndexer`, so the duplication is gone; the port itself is still unwired |
| The platform publishes no metrics of its own | `/actuator/prometheus` serves JVM, Tomcat and Mongo driver metrics and nothing about migrations. No throughput, no chunk duration, no queue depth, no worker idle ratio. Logs say what happened to one thing; nothing says whether the system is healthy |
| Redis is provisioned and unused | a container nobody needs |
| Batch transforms do not chain | a second one is refused at publish; the underlying limit is unchanged |
