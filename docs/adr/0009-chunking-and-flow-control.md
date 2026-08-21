# ADR-0009: Chunking — independent read size and write size

**Status:** Accepted (2026-08-06)

## Context

Records must not be materialised in bulk. A 100-million-row migration has to stream through
bounded memory regardless of source or sink.

Critically, the efficient read size and the efficient write size are almost never the same
number, and they are constrained by different things:

| | Governed by |
|---|---|
| **Read size** | Source round-trip cost, cursor/page semantics, driver buffering, source-side memory. A JDBC cursor is happiest at 100–1000 rows; a REST API dictates its own page size; Salesforce Bulk v2 returns whatever it returns. |
| **Write size** | Sink protocol limits and amortisation. Elasticsearch `_bulk` wants 5–15 MB per request; a JDBC batch wants ~1000 statements; Kafka wants a linger-based batch; Salesforce Bulk v2 caps at 150 MB and 10,000 records. |

Coupling them means one side is always wrong. A single `batchSize` that satisfies a JDBC
source will hammer Elasticsearch with tiny bulk requests, and one that satisfies
Elasticsearch will hold an unbounded source cursor open.

## Decision

Read size and write size are separate, independently configured, and mediated by a bounded
buffer. This is the concrete shape of the `Channel` port from ADR-0001.

```
  Source                    bounded buffer                    Sink
  ──────                    ──────────────                    ────
  read(fetchSize=100)  ──►  ▓▓▓▓▓▓▓░░░░░░░░  ──►  write(batch of 1000)
     page                   maxInFlightBatches         flush on:
     page                                                • writeBatchSize reached
     page  ─────────────────► accumulate ──────────────► • maxBatchBytes reached
     ...                                                 • flushInterval elapsed
                                                         • split exhausted
     ▲                                                        │
     └────────────── back-pressure: buffer full ◄──────────────┘
                     stops the read loop; nothing is dropped
```

### ChunkingPolicy

```java
record ChunkingPolicy(
    int      readFetchSize,        // records per source round-trip      (default 500)
    int      writeBatchSize,       // records per sink flush             (default 1000)
    DataSize maxBatchBytes,        // byte ceiling per batch             (default 8 MB)
    Duration flushInterval,        // linger, so low-volume never stalls (default 5s)
    int      maxInFlightBatches    // buffer depth, the memory knob      (default 2)
) {}
```

The user's example — read 100 per query, send 1000 to the sink — is
`readFetchSize=100, writeBatchSize=1000`.

### Three rules that make this safe

**1. Bytes bound memory, not record count.** A thousand records may be 1 KB or 10 MB. Whichever
of `writeBatchSize` or `maxBatchBytes` is reached first triggers the flush. Worst-case heap
for a running split is therefore bounded and predictable:

```
peak ≈ maxInFlightBatches × maxBatchBytes
```

That is the number an operator sizes a worker against. Without the byte ceiling, record count
alone gives no memory guarantee at all.

**2. Sink capability caps user configuration.** `SinkCapabilities` declares
`preferredBatchSize` and `maxBatchSize`. The effective batch is
`min(writeBatchSize, sink.maxBatchSize())`, and a user configuring 50,000 records against
Salesforce Bulk v2 is clamped to 10,000 with a warning surfaced in the UI rather than
failing at runtime. Connectors know their own protocol limits; users should not have to.

**3. Checkpoints ride on batch commit, never on records.** The checkpoint is written
immediately after the sink has *durably accepted* the batch, recording the source cursor as of
the last record in it. Resumption is therefore always at a batch boundary, which is what makes the
"kill -9 mid-run, resume cleanly" guarantee in Phase 3 achievable. Checkpointing per record
would be prohibitively expensive; checkpointing on a timer would allow gaps.

> **Qualification for asynchronous sinks** (see [ADR-0012](0012-multi-phase-connector-lifecycle.md)).
> "Durably accepted" is not the same as "`write()` returned". Salesforce Bulk v2 answers `201` to a
> batch upload having only queued it; the records are processed later and may fail wholesale. For
> any sink declaring `commitIsAsynchronous()`, the checkpoint advances only once `checkCommit()`
> reports COMPLETE. Checkpointing on the `write()` return would advance the source cursor past
> records the sink subsequently rejected — silent loss, discovered on resume or never.

`flushInterval` matters for the same reason: without it a streaming pipeline with low volume
would hold records indefinitely waiting for `writeBatchSize`, and would also never checkpoint.

### Configuration precedence

Per-pipeline override → connector-declared default → platform default. All three are metadata,
none is hardcoded. Phase 10 adds adaptive sizing — growing the batch while sink latency stays
flat, shrinking it on rejection or timeout — which requires the metrics from Phase 7 and is
deliberately not attempted before then.

## Consequences

**Positive**
- Memory is bounded and calculable, independent of dataset size, for both batch and streaming.
- Each side of a pipeline can be tuned for its own protocol without compromise.
- Back-pressure is structural — a full buffer blocks the reader. No dropping, no unbounded
  queue, no delay-based throttling.
- Batch-boundary checkpointing gives clean resume semantics for free.

**Negative**
- Five knobs is more surface than one. Mitigated by connector-supplied defaults that are
  correct for most users, so the knobs exist for tuning rather than for setup.
- The buffer adds one copy between read and write. Accepted; it is the price of decoupling,
  and `maxInFlightBatches=2` keeps it small.
- Adaptive sizing is deferred, so early tuning is manual and guided by dashboards.

## Alternatives rejected

- **Single `batchSize`.** Simple, and wrong for one side of every pipeline.
- **Record-count-only limits.** Provides no memory bound. A single split of large documents
  would OOM a worker with a perfectly reasonable-looking configuration.
- **Reactive Streams (`Flux`) end to end.** Genuine back-pressure semantics, but it imposes a
  reactive programming model on every connector author, which conflicts with the low-barrier
  SPI goal in ADR-0006. A pull-based `RecordStream` over a bounded buffer achieves the same
  flow control with a far simpler contract.
