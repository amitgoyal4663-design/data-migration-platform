# ADR-0001: Dual-mode Channel port for the data path

**Status:** Accepted (2026-08-06). Amended by [ADR-0013](0013-no-topic-auto-creation.md) — the
"one topic per DAG edge" element is withdrawn, because the platform has no authority to create
topics. Intermediate edges are always in-process; Kafka carries data only where the user's own
pre-provisioned topic is named as a source or sink. The rest of this decision stands.

## Context

Records must move between pipeline stages. The industry splits into two camps: a message
broker between every stage (NiFi, Kafka Connect), or a direct in-process pipe (Fivetran,
Airbyte, Glue).

Broker-per-record gives replay, durability and genuine streaming semantics, at a cost of
roughly 10–100× amplification in throughput, storage and latency for bulk work. A
500-million-row backfill becomes 500 million Kafka messages plus serialisation, network
and retention.

Direct pipes are cheap and fast for batch but offer no replay, no cross-team fan-out and
no natural streaming model.

We need both. Migration platforms do bulk backfills *and* continuous sync, often for the
same pipeline definition.

## Decision

Transport is an out-port, `Channel`, with two implementations selected by pipeline mode:

- `InProcessChannel` — bounded queue, zero-copy handoff, back-pressure by blocking the
  producer. Used for batch, scheduled and incremental modes.
- `KafkaChannel` — one durable topic per DAG edge, consumer-group scaling, replay from
  offset. Used for streaming and CDC modes.

Kafka is **always** the control bus regardless of mode: run commands, split assignment,
run events, DLQ. It is the *data* bus only when the mode warrants it.

Connectors and transformations are written against `RecordStream` and never observe which
channel implementation they were handed.

## Consequences

**Positive**
- Batch economics comparable to Fivetran; streaming semantics comparable to NiFi; one codebase.
- The same pipeline definition can be run in either mode without modification, which makes
  "backfill then switch to streaming" a first-class flow rather than two separate products.
- Channel selection becomes a knob we can tune per pipeline, or later per edge.

**Negative**
- Two execution paths to test. Mitigated by making the Connector TCK exercise both.
- An extra abstraction layer between the engine and the transport.
- Failure semantics differ between the two: in-process failure loses the in-flight buffer
  and resumes from checkpoint; Kafka failure resumes from committed offset. The engine must
  reason about both, and the UI must report the resulting delivery guarantee accurately
  (see ADR-0005 and the delivery-semantics section of the architecture document).

## Alternatives rejected

- **Kafka for everything.** Uniform and fully replayable, but the amplification cost on
  bulk loads is not recoverable by tuning. It would make us structurally more expensive
  than every batch competitor.
- **In-process only.** Fastest to build, but defers the streaming half of the product
  vision and would require engine rework to add later.
