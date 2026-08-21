# ADR-0003: Jackson JsonNode as the in-flight payload model

**Status:** Accepted (2026-08-06) — chosen over the recommended alternative

## Context

Every record flowing through the engine needs an internal representation. The options span
a spectrum: untyped JSON trees, a typed row model with logical types (Kafka Connect's
`Struct`/`Schema`), or columnar batches (Apache Arrow).

## Decision

Payloads are Jackson `JsonNode`.

Records travel wrapped in an envelope that carries engine coordinates:

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

The envelope is not a hedge against the decision — it is a correctness requirement.
`splitId` and `seq` form the idempotency key used for sink deduplication, and they cannot
live inside `payload` because the payload belongs to the user and their transformation may
rewrite it entirely.

`ObjectMapper` is configured with `DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS` so
that numeric precision loss is an explicit configuration choice rather than a silent default.

## Consequences

**Positive**
- Zero marshalling at the GraalJS boundary. `JsonNode` is already what the sandbox expects,
  which makes the transformation engine (Phase 5) substantially simpler and faster.
- Trivial for connector authors. No schema plumbing to learn, low barrier to contribution.
- Schemaless sources (MongoDB, REST, files) map directly with no impedance mismatch.
- Fastest path to a working end-to-end pipeline.

**Negative — accepted**
- **Type fidelity.** No native `TIMESTAMP WITH TIME ZONE`, no `BYTES`. Timestamps become
  strings and binary becomes base64. Connectors targeting typed sinks (Postgres, Databricks,
  Parquet) must carry their own coercion rules.
- **No projection pushdown.** Without a schema the engine cannot prune columns before
  reading, so `SELECT *` is effectively always the source query shape unless the connector
  implements pruning itself.
- **No columnar optimisation path.** Vectorised transforms and zero-copy Parquet writes are
  off the table for the analytical connectors.
- **Cost of change.** Replacing this later touches every connector written up to that point.
  This is the principal risk being accepted.

## Containment

Two rules keep the blast radius bounded should this need to change:

1. Connectors emit and consume `DataRecord`, never bare `JsonNode`. The envelope is the
   stable surface.
2. Sink-side type coercion lives in a shared `TypeCoercion` utility in `dmp-common`, not
   duplicated per connector — so a future typed model has one place to integrate rather
   than N.

## Alternatives rejected

- **Typed interfaces with a row implementation now, Arrow later.** The architect's
  recommendation: define `Record`/`Schema`/`RecordBatch` as interfaces with proper logical
  types, ship row-based, add Arrow later without touching connectors. Rejected as
  unnecessary ceremony for Phase 1.
- **Apache Arrow from day one.** Best analytical throughput ceiling, but off-heap memory
  management and a difficult SPI would deter connector contributors.
