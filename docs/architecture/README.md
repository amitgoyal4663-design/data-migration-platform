# Enterprise Data Migration Platform — Foundation Architecture

**Status:** Phase 0 — approved
**Last updated:** 2026-08-06

This document is the architectural baseline. Individual decisions are recorded as ADRs
under [`docs/adr/`](../adr/); this document explains how they fit together.

---

## 1. Product thesis

Existing platforms sit at one of two poles:

| Pole | Products | Strength | Fatal cost |
|---|---|---|---|
| Record-per-message on a broker | NiFi, Kafka Connect | Replay, durability, streaming semantics | 10–100× cost amplification on bulk loads |
| Direct pipe, batch only | Fivetran, Airbyte, Glue | Cheap and fast bulk movement | No real streaming, no replay, no mid-batch resume |

We refuse the choice. Transport is an implementation of a `Channel` port. Connectors and
transformations never know which one they were given.

```
Pipeline DAG  ──►  Channel (port)
                     ├── InProcessChannel   → bounded buffer, zero-copy, back-pressure by blocking
                     │                        ALL intermediate stage-to-stage edges,
                     │                        in batch and streaming mode alike
                     └── KafkaChannel       → the user's own pre-provisioned topic; replay and
                                              consumer-group scaling — only where a Kafka
                                              connector is named as a source or a sink
```

Kafka is **always** the control bus — commands, split assignment, run events, DLQ, delay-queue
firing. That is one message per *split* or per *event*, never per record. It is the **data** bus
only where a pipeline explicitly names a Kafka topic: a PostgreSQL to Databricks migration puts
**no records through Kafka at all**.

The platform never creates a topic. Topics are pre-provisioned by the platform team, and a missing
one fails at publish and again at run start with an actionable message rather than silently
producing nothing.

See [ADR-0001](../adr/0001-dual-mode-channel.md) and
[ADR-0013](../adr/0013-no-topic-auto-creation.md).

---

## 2. Runtime topology

One deployable artifact, two roles, selected by Spring profile
(see [ADR-0004](../adr/0004-single-artifact-profiles.md)).

```
                       ┌───────────────────────────────────────────┐
    React Console ────► │  CONTROL PLANE   (profile: control-plane) │
                        │  REST / OpenAPI · pipeline CRUD           │
                        │  versioning · validation · scheduling     │
                        │  run orchestration                        │
                        │  owns PostgreSQL                          │
                        └──────────────────┬────────────────────────┘
                                           │  writes chunks to MongoDB as PENDING
                                           │  (nothing is sent to a worker)
                        ┌──────────────────▼────────────────────────┐
                        │  DATA PLANE      (profile: worker)        │
                        │  plugin classloaders · DAG executor       │
                        │  connectors · GraalJS sandbox             │
                        │  checkpointing · back-pressure            │
                        │  polls MongoDB and claims chunks itself   │
                        └──────────────────┬────────────────────────┘
                                           │  run events (optional) · telemetry
                  PostgreSQL   ·   MongoDB   ·   OpenSearch   ·   Kafka (optional)
```

The arrow between the planes is not a channel. The control plane writes chunk documents and
stops; workers discover them by polling. Nothing is addressed to a worker, so no worker needs
to be reachable, registered or known about.

Both roles are stateless and horizontally scalable. They scale on different curves —
the control plane with the number of users and pipelines, the data plane with data
volume — which is why the boundary exists from day one even while they ship as one jar.

---

## 3. Execution model

A `Run` is planned into **Splits**. A Split is simultaneously:

- the unit of parallelism (one worker thread owns one split), and
- the unit of resumption (a checkpoint is scoped to a split).

| Source family | Split strategy |
|---|---|
| JDBC | Primary-key or partition-column ranges, bucketed by estimated distinct values |
| MongoDB | `_id` range chunks |
| Kafka | One split per topic-partition |
| Files / SFTP / object storage | One split per file; byte ranges for splittable formats |
| Salesforce Bulk API v2 | One split per Bulk job result set |
| REST / GraphQL | Usually a single split — cursor chains are not splittable. The connector declares this honestly rather than faking parallelism. |

Splits are written to MongoDB in `PENDING` state and **workers pull them**
([ADR-0014](../adr/0014-pull-based-work-distribution.md)). Nothing is assigned, and there is no
coordinator, no leader election and no rebalance protocol in the data plane.

> This supersedes the original design, in which splits were published to Kafka keyed by `runId`
> and consumer-group rebalance was the assignment mechanism. Kafka partitions by key hash, so
> every split of a run would have landed in one partition, been consumed by one consumer, and
> executed by **one pod** — the mechanism intended to distribute work would have concentrated it.

Each worker runs one poller thread that repeatedly asks MongoDB for work:

```
reserve a slot:  run.findAndModify({_id, activeSlots: {$lt: maxConcurrentChunks}}, $inc)
claim a chunk:   split.findAndModify(lowest PENDING → RUNNING, assignedTo: me)
```

Both are single atomic operations, so two workers racing cannot both win the same chunk.
`maxConcurrentChunks` therefore bounds the run **across the whole fleet**, not per pod — a limit
of 1 means exactly one chunk of that run executes anywhere at any instant, while the pod running
it may still change from chunk to chunk.

Each worker then executes its chunk end-to-end (source → transform → sink) in a chunked,
back-pressured loop, checkpointing `(splitId, sourceCursor)` after every batch the sink has
durably accepted. A worker killed mid-chunk stops renewing its lease; a sweep returns the chunk
to `PENDING` and releases its slot, and whichever worker claims it next resumes from the last
committed checkpoint.

### Two planning strategies

| | Planned up front | Generated as it goes |
|---|---|---|
| Chosen when | anything else | `maxConcurrentChunks == 1` **and** the source supports cursor pagination |
| Boundaries | `min`/`max` of the split key, sliced into ranges | none — the spec is `{_dmpOpenEnded: true}` |
| Parallelism | yes, all chunks are `PENDING` at once | **no** — chunk N+1 cannot exist until chunk N reports where it stopped |
| Chunk sizes | uneven; a key range may hold 5 rows or 50,000 | exactly one row budget each |
| Rows arriving mid-run | missed; the maximum was frozen at planning time | picked up |

Lazy generation is not a degraded mode. It counts nothing, guesses no boundaries, and needs no
usable split key — but it is inherently sequential, because the starting point of each chunk is
an output of the previous one.

### Chunking — read size and write size are independent

Nothing is ever materialised in bulk. Records stream through a bounded buffer, and the
efficient read size is almost never the efficient write size — they are constrained by
different things (source round-trip cost versus sink protocol limits). Both are configured
separately. See [ADR-0009](../adr/0009-chunking-and-flow-control.md).

```
  Source                    bounded buffer                    Sink
  ──────                    ──────────────                    ────
  read(fetchSize=100)  ──►  ▓▓▓▓▓▓▓░░░░░░░░  ──►  write(batch of 1000)
     page                   maxInFlightBatches         flush on:
     page  ─────────────────► accumulate ──────────────► • writeBatchSize reached
     ...                                                 • maxBatchBytes reached
                                                         • flushInterval elapsed
     ▲                                                        │
     └────────────── back-pressure: buffer full ◄──────────────┘
```

```java
record ChunkingPolicy(
    int      readFetchSize,        // records per source round-trip      (default 500)
    int      writeBatchSize,       // records per sink flush             (default 1000)
    DataSize maxBatchBytes,        // byte ceiling per batch             (default 8 MB)
    Duration flushInterval,        // linger, so low-volume never stalls (default 5s)
    int      maxInFlightBatches    // buffer depth, the memory knob      (default 2)
) {}
```

Three rules make this safe:

1. **Bytes bound memory, not record count.** A thousand records may be 1 KB or 10 MB.
   Whichever of `writeBatchSize` or `maxBatchBytes` is hit first triggers the flush, so
   worst-case heap per split is `maxInFlightBatches × maxBatchBytes` — a number an operator
   can size a worker against.
2. **Sink capability caps user configuration.** Effective batch is
   `min(writeBatchSize, sink.maxBatchSize())`. Configuring 50,000 records against Salesforce
   Bulk v2 clamps to 10,000 with a warning in the UI, rather than failing at runtime.
3. **Checkpoints ride on batch commit, never on records.** Resumption is always at a batch
   boundary. This is what makes the Phase 3 "kill -9 mid-run, resume cleanly" guarantee
   achievable.

Configuration precedence is per-pipeline override → connector default → platform default.
Adaptive sizing (growing the batch while sink latency stays flat) is a Phase 10 concern and
depends on Phase 7 metrics.

### Back-pressure

Back-pressure is structural, never a delay:

- `InProcessChannel` — bounded buffer; a full buffer blocks the source read loop directly.
- `KafkaChannel` — `consumer.pause()` on the partition when the downstream buffer is full.

Nothing is dropped and no queue is unbounded. Rate limiting is a separate concern, handled by
a Redis token bucket plus worker-local pacing. Conflating rate limiting with back-pressure, or
either with delay queues, is how these platforms acquire latency nobody can explain.

---

## 4. Data model in flight

Payloads are Jackson `JsonNode` ([ADR-0003](../adr/0003-record-model-jsonnode.md)),
wrapped in an envelope carrying the coordinates the engine needs:

```java
record DataRecord(
    JsonNode payload,        // the user-visible document
    String   key,            // natural key, for partitioning and upsert
    Headers  headers,        // lineage, trace ids, connector metadata
    SplitId  splitId,
    long     seq,            // monotonic within split
    Cursor   sourceCursor    // for checkpointing
) {}
```

`splitId` + `seq` form the idempotency key used for sink deduplication. These cannot live
inside the payload — the payload belongs to the user and their transformation may rewrite
it entirely.

---

## 5. Datastore ownership

The line is **definition versus execution**. See [ADR-0005](../adr/0005-datastore-ownership.md).

| Store | Owns | Rationale |
|---|---|---|
| **PostgreSQL** | tenants, pipelines, pipeline versions, connector instances, schedules, audit log, RBAC, `QRTZ_*` | Definitions: low churn, long lived, strongly relational. Needs foreign keys and transactions — a pipeline version referenced by a run must not vanish. |
| **MongoDB** | **runs, splits, checkpoints**, execution telemetry, per-record errors and DLQ payloads, transformation traces, dry-run samples, discovered schema catalogs, delay-queue timers | Execution: high churn, document-shaped, TTL retention. A 10,000-split run checkpointing every 5s is relentless UPDATE traffic, and every PostgreSQL update leaves a dead tuple. |
| **OpenSearch** | the per-record audit index — one entry per record, optionally carrying its payload | Searchable by record key or by run, years after a cutover. Written asynchronously; a slow cluster drops events rather than slowing a migration. |
| **Kafka** | run **events** only, and only when enabled | Optional outbound notification. A fifty-million-record migration produces a few hundred messages here. |

> **Redis and Kafka are both narrower than originally planned.**
>
> *Redis* was to hold rate-limit buckets, progress counters, a metadata cache and an idempotency
> window. None of that was built — there is no Redis dependency in any module and no reference in
> any source file. The container in the compose stack is currently unused.
>
> *Kafka* was to carry control commands, split assignment, the DLQ and the streaming data path. It
> carries none of them now. [ADR-0013](../adr/0013-no-topic-auto-creation.md) removed the streaming
> data path — the platform has no authority to create topics, so a topic per DAG edge is not
> possible and intermediate edges are in-process. [ADR-0014](../adr/0014-pull-based-work-distribution.md)
> removed split assignment. The DLQ lives in MongoDB, alongside the run state it belongs to.
>
> What remains is run events: one message per state change and per chunk, published with `acks=1`
> because losing one is a missed notification, not data loss. Records never pass through a broker.
> Turn it off and nothing about migration changes.

A `Run` is a single document, so a state transition is
`findOneAndUpdate({_id, state: "RUNNING"}, {$set: {state: "PAUSED"}})` — an atomic
compare-and-swap in one round trip, which is a stronger concurrency primitive than
read-modify-write against a version column, not a weaker one.

Two consequences worth stating up front: `run.pipelineVersionId` integrity is application-enforced
(published versions are never hard-deleted, only archived), and **MongoDB must run as a replica
set even in development**, because change streams require an oplog.

---

## 6. Scheduling and delayed execution

Two mechanisms, because these are two different problems that merely look alike:

| | Recurring schedule | One-shot timer |
|---|---|---|
| Example | "Every weekday at 03:00 Europe/London" | "Retry this record in 5 minutes" |
| Volume | Tens to thousands | Millions |
| Semantics | Cron, calendars, DST, misfire policy | Fire once at a time |
| Mechanism | **Quartz**, JDBCJobStore on PostgreSQL ([ADR-0010](../adr/0010-quartz-scheduling.md)) | **MongoDB TTL delay queue** ([ADR-0002](../adr/0002-delay-queue-mongo-ttl.md)) |

Quartz clustering (`QRTZ_LOCKS` row locks) also provides scheduler leader election, which would
otherwise be bespoke code with a subtle failure mode.

**A Quartz job may only create a run.** Its entire body is: evaluate the schedule's window script
to get this firing's parameters, call `RunOrchestrator.start(...)`, return. The run is left in
`CREATED` and a worker picks it up. Running a six-hour migration on one of Quartz's ten scheduler
threads would exhaust the pool and misfire every unrelated schedule in the deployment. The
scheduler decides *when*; the data plane decides *how* and *for how long*.

> ADR-0010 originally had this publish a start command to Kafka. That was written while Kafka was
> the assignment mechanism; work is now claimed from MongoDB, so publishing a command would
> reintroduce a broker dependency for a row the worker will find on its next poll anyway.

### The delay queue

MongoDB TTL + change streams, split across **two collections**, per
[ADR-0002](../adr/0002-delay-queue-mongo-ttl.md).

```
dq_timers   { _id, expireAt }                     ← TTL index, deliberately tiny
dq_payloads { _id, payload, meta{topic,…},        ← same _id, arbitrary size
              fireAt, published }

schedule(delay, payload, meta)
     │  1. insert dq_payloads   ← payload FIRST
     │  2. insert dq_timers     ← then the TTL trigger
     ▼
TTL monitor (~60s) deletes the dq_timers doc
     ▼
change stream sees the delete — documentKey._id is sufficient
     ▼                                    ┌─► direct KafkaProducer  (default)
publish id to dmp.delay.fired  ───────────┤   OR
     ▼                                    └─► Kafka Connect sink
consumer: fetch dq_payloads by _id → resolve meta.topic → publish → mark published
     ▲
reconciliation sweeper: { fireAt < now-2min, published: false }
```

**Why the split matters.** Keeping the TTL collection to two fields is not cosmetic:

- A delete change event carries only `documentKey._id` by default. Because `_id` *is* the join
  key to the payload, `changeStreamPreAndPostImages` is **unnecessary** — removing its storage
  cost, its oplog pressure and its MongoDB version floor entirely.
- The TTL monitor scans an index over ~40-byte documents, so it stays memory-resident even at
  tens of millions of pending timers. That directly reduces TTL lag, this design's main weakness.
- Payload size is fully decoupled from timer performance.

**Insert ordering is payload-first, deliberately.** The two writes are not atomic, so the
failure modes are made asymmetric on purpose: a payload without a timer is an orphan reaped by
TTL (harmless); a timer without a payload is data loss. Only the first is reachable.

Accepted consequence: the effective delay floor is ~60 seconds, set by MongoDB's TTL monitor
interval. The retry ladder is therefore `60s → 5m → 30m → 2h → DLQ` rather than starting at
5s. Acceptable for a migration platform — retries here exist for transient sink outages, not
tight request-level backoff.

Required hardenings, all part of Phase 4 scope:

1. Durable resume-token persistence after each processed change-stream batch.
2. A reconciliation sweeper over `dq_payloads` for overdue-and-unpublished timers. The oplog
   window will eventually be exceeded, and a resume-token gap loses timers with no error
   surfacing anywhere. The two-collection layout makes this query cheap — no join needed.
3. A long garbage-collection TTL on `dq_payloads` (`fireAt + 7 days`) to reap orphans from
   partial writes.
4. Idempotent firing — `meta.timerId` propagates as a Kafka header and consumers deduplicate.
   Publish first, mark `published` second; the reverse ordering risks marking a timer fired
   that never was.
5. A missing payload routes to the DLQ rather than being silently dropped.

The whole thing sits behind a `DelayQueue` port. If the 60-second floor becomes a problem in
production, the implementation is replaceable without touching the engine.

---

## 7. Connector SPI

```java
public interface Connector { ConnectorSpec spec(); }   // JSON Schema of its configuration

public interface Source extends Connector {
    SourceSession open(SourceContext ctx);
}
public interface SourceSession extends AutoCloseable {
    SchemaCatalog discover();
    List<Split>   plan(SplitPlanRequest req);
    RecordStream  read(Split split, Checkpoint from);   // pull-based, back-pressured
}

public interface Sink extends Connector {
    SinkSession open(SinkContext ctx);
}
public interface SinkSession extends AutoCloseable {
    SinkCapabilities capabilities();   // UPSERT | TRANSACTIONAL | APPEND_ONLY | BATCH_SIZE_HINT
    WriteResult      write(RecordBatch batch);
    void             commit(CommitContext ctx);
}
```

`ConnectorSpec` returns a JSON Schema. **The React console renders every connector's
configuration form from that schema at runtime.** Drop a plugin jar in, restart a worker,
and a complete configuration UI appears with no frontend change. This is the difference
between a plugin system and a plugin system anyone actually uses.

Isolation: each connector ships as a fat jar in `plugins/<name>-<version>/`, loaded by a
child-first `PluginClassLoader` and discovered via `ServiceLoader`. Without this, the
Oracle driver and the Mongo driver will eventually disagree about a transitive Netty
version. See [ADR-0006](../adr/0006-connector-spi-plugin-isolation.md).

**Connector TCK.** Every connector must pass a compliance suite covering schema-discovery
contract, split determinism, checkpoint resumability, at-least-once redelivery behaviour,
and error-taxonomy conformance. Ecosystems work when compliance is mechanical.

---

## 8. Transformation engine

GraalJS in a locked-down context — `HostAccess.NONE`, `allowAllAccess(false)`, no IO, no
native access, no thread creation, no reflection, pinned ECMAScript version. One fresh
`Context` per execution off a shared `Engine`, which shares the parsed-AST cache.

Known limitation: hard CPU-time and heap limits (`sandbox.MaxCPUTime`,
`sandbox.MaxHeapMemory`) are Oracle GraalVM features, not Community Edition. On CE we
enforce via a watchdog calling `Context.close(true)` plus statement-count instrumentation.
That stops infinite loops but not a single statement allocating 4 GB. Mitigation is
process-level: transforms run in a worker pool with its own memory ceiling, so the blast
radius is one process. See [ADR-0008](../adr/0008-transform-sandbox.md).

Alongside JavaScript, declarative nodes (Mapper, Filter, Flatten) backed by JSONata cover
the ~80% of real transformations that are field mapping and should not require code — or
pay the sandbox's cost.

---

## 9. Delivery semantics

Sinks declare capabilities. The engine computes the strongest guarantee actually
achievable for a given source/sink/mode combination and **displays it on the pipeline in
the UI**:

- `Exactly-once — transactional sink`
- `At-least-once — deduplicated by natural key`
- `At-least-once — duplicates possible on retry`

No competitor surfaces this. Every one of them makes the user read documentation and
guess. Mechanism: deterministic split boundaries and checkpoints on the source side;
idempotency key `hash(runId, splitId, seq)` or natural-key upsert on the sink side;
two-phase commit where the sink supports it.

---

## 10. Module layout

```
dmp-parent
├── dmp-bom                    dependency management exposed to plugin authors
├── dmp-common                 ids, errors, json, time, tracing
├── dmp-domain                 Pipeline, Run, Split, Checkpoint, DataRecord — no framework deps
├── dmp-application            use cases and in/out ports (hexagonal core)
├── dmp-connector-api          public SPI — semver-stable, the compatibility contract
├── dmp-connector-runtime      plugin classloading, registry, spec validation, secret injection
├── dmp-transform-api          transform SPI
├── dmp-transform-graaljs      sandboxed JavaScript engine
├── dmp-transform-declarative  mapper / filter / flatten via JSONata
├── dmp-engine                 DAG executor, channels, checkpointing, back-pressure, retry
├── dmp-messaging              Kafka topics, headers, serde, DLQ
├── dmp-scheduler              delay queue, cron, outbox dispatcher, reconciliation sweeper
├── dmp-persistence-postgres   out-port adapters
├── dmp-persistence-mongo      telemetry, DLQ, timer adapters
├── dmp-observability          Micrometer, OpenTelemetry, structured logging
├── connectors/                each independently released as a plugin jar
│   └── jdbc-postgres · mongodb · kafka · rest · file-csv · elasticsearch · salesforce · …
├── apps/
│   ├── dmp-app                Spring Boot — profiles: control-plane | worker | all
│   └── dmp-console            React + Vite + TypeScript
└── dmp-testkit                Connector TCK, Testcontainers fixtures, load harness
```

`dmp-domain` compiles with no Spring on the classpath. This is enforced by an ArchUnit
test, not by discipline.

---

## 11. GraalVM native image — scope

Native image cannot classload arbitrary jars at runtime, so it is mutually exclusive with
dynamic connector plugins in the same process. Resolution
([ADR-0007](../adr/0007-graalvm-native-image-scope.md)):

- **Control plane** → native image. Fast boot and ~50 MB RSS matter for Kubernetes scale-out.
- **Worker** → JVM. Needs dynamic plugins, and JIT beats AOT for sustained data crunching anyway.

---

## 12. Roadmap

See [ROADMAP.md](../../ROADMAP.md).

## 13. Deferred

Security and authentication are deferred pending company SSO. However, a `SecretsProvider`
SPI and a tenant boundary exist in the domain model from Phase 1 — retrofitting
multi-tenancy is a rewrite, not a feature.
