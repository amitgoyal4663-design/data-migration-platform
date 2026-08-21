# ADR-0002: MongoDB TTL + change stream as the delay queue

**Status:** Accepted (2026-08-06) — chosen over the recommended alternative, with mandatory hardenings

## Context

The platform needs delayed execution for retry backoff, scheduled and cron job triggers,
throttled redelivery, and workflow timers.

A design already proven in the team's production environment: a caller submits a payload,
metadata (including the destination topic) and a delay; MongoDB TTL expiry drives the fire
event; Kafka carries it onward with the destination resolved dynamically from metadata.

## Decision

Adopt the two-collection MongoDB TTL design, behind a `DelayQueue` port.

### Structure

The timer is split across two collections. This is the key design property and it is what
makes the approach viable.

```
dq_timers        (TTL-indexed, deliberately tiny)
  { _id: ObjectId, expireAt: ISODate }
        └── TTL index on expireAt, expireAfterSeconds: 0

dq_payloads      (no TTL pressure, arbitrary size)
  { _id: ObjectId,          ← same value as dq_timers._id
    payload: <document>,
    meta:    { topic, key, headers, attempt, timerId },
    fireAt:  ISODate,
    published: false }
```

### Flow

```
schedule(delay, payload, meta)
     │
     │  1. insert dq_payloads   ← payload FIRST
     │  2. insert dq_timers     ← then the TTL trigger
     ▼
TTL monitor (~60s) deletes the dq_timers document
     │
     ▼
change stream on dq_timers observes the delete
     │  documentKey._id is sufficient — no pre-image needed
     ▼
Kafka Connect (or direct producer) publishes the id to dmp.delay.fired
     │
     ▼
consumer: fetch dq_payloads by _id  →  resolve meta.topic  →  publish to destination
     │
     ▼
mark dq_payloads.published = true
```

### Why the split matters

Keeping the TTL collection to two fields is not a cosmetic choice:

- **`documentKey` alone is sufficient.** A delete change event carries only `_id` by default.
  Because `_id` *is* the join key to the payload, `changeStreamPreAndPostImages` is
  unnecessary — which removes the storage cost, the oplog pressure and the MongoDB 6.0+
  version constraint that a single-collection design would incur.
- **TTL scans stay fast.** The TTL monitor scans an index over a collection of ~40-byte
  documents. The index stays resident in memory even at tens of millions of pending timers,
  which directly reduces the TTL lag that is this design's main weakness.
- **Payload size is decoupled from timer performance.** A 2 MB payload costs nothing in the
  expiry path.

### Insert ordering

Payload first, then timer. The two inserts are not atomic and MongoDB is not being asked to
make them so, so the failure modes are made asymmetric on purpose:

- Payload written, timer insert fails → an orphaned payload, reaped by `dq_payloads`' own
  long TTL. Harmless.
- Timer written, payload insert fails → the timer fires and finds nothing. This is data loss.

The chosen ordering makes only the first case reachable.

### Final hop

Configurable. A direct `KafkaProducer` is the default because it removes a deployment and a
failure domain; the Apache Kafka Connect path remains available for environments that
standardise on it. Both publish to the same topic with the same record shape.

## Mandatory hardenings

Part of the Phase 4 definition of done:

1. **Durable resume tokens.** Persisted after each processed change-stream batch. A listener
   down longer than the oplog window otherwise skips timers silently.
2. **Reconciliation sweeper.** Periodically scans
   `dq_payloads: { fireAt: { $lt: now - 2min }, published: false }` and fires them directly.
   The oplog window *will* be exceeded eventually, and a resume-token gap loses timers with
   no error anywhere. This is the safety net that makes the design survivable, and the
   two-collection layout is what makes the query cheap — it needs no join.
3. **Garbage-collection TTL on `dq_payloads`.** A second, much longer TTL (`fireAt + 7 days`)
   reaps orphans from partial writes and from payloads whose consumer died mid-flight.
4. **Idempotent firing.** Every timer carries `meta.timerId`, propagated as a Kafka header;
   downstream consumers deduplicate. Publish first, then mark `published: true` — the reverse
   ordering risks marking a timer fired that never was. At-least-once is the honest guarantee.
5. **Missing-payload handling.** A fired timer whose payload is absent routes to the DLQ with
   its id rather than being silently dropped. It should be impossible given the insert
   ordering; if it happens, it indicates something worse and must be visible.

## Consequences

**Positive**
- Proven in this team's production environment. Operational familiarity has real value.
- Arbitrary delay values, no ladder constraint.
- Dynamic destination routing falls out of `meta.topic` naturally, so one delay queue serves
  retries, schedules and workflow timers without separate infrastructure per use case.
- No pre-images required, so no MongoDB version floor beyond change-stream support and no
  oplog amplification.
- Large payloads are free with respect to timer performance.

**Negative — accepted**
- **~60 second delay floor.** MongoDB's TTL monitor runs once per minute. The retry ladder is
  therefore `60s → 5m → 30m → 2h → DLQ` rather than starting at 5s. Acceptable here: retries
  in a migration platform exist for transient sink outages, not request-level backoff.
- **No back-pressure signal.** Under write load the TTL monitor batches and can fall
  arbitrarily behind with no SLA. The sweeper detects this; it cannot prevent it. Phase 7
  exports TTL lag as a first-class metric — measured as `now - min(fireAt)` over unpublished
  payloads — so it is alertable rather than discovered during an incident.
- **Two writes and two reads per timer**, plus index, oplog and change-stream cost. The extra
  read is a single `_id` lookup and is negligible; the extra write is real.
- **No transactional coupling.** A timer and the business state it relates to cannot commit
  atomically. Reconciliation must therefore treat divergence as expected, not exceptional.

## Mitigation of lock-in

The implementation sits behind a `DelayQueue` port:

```java
public interface DelayQueue {
    TimerId schedule(Duration delay, DelayedMessage msg);   // msg carries payload + meta.topic
    void    cancel(TimerId id);
}
```

If the 60-second floor becomes a production problem, an alternative implementation — tiered
Kafka delay topics for short backoff, or a PostgreSQL timer table with
`SELECT … FOR UPDATE SKIP LOCKED` for transactional coupling — can be substituted without
touching the engine, the scheduler API, or any connector.

## Alternatives rejected

- **Three-layer composite** (PostgreSQL timer table + Kafka delay ladder + in-process timing
  wheel). Recommended by the architect for sub-second precision and atomic timer/business-state
  commit, but rejected in favour of the proven in-house design.
- **Single-collection TTL.** Simpler, but requires pre-images to recover the payload from a
  delete event, or encoding metadata into `_id`. Both are worse than one indexed lookup.
- **Temporal / Cadence.** Purpose-built for durable timers and saga orchestration, but
  introduces an entire additional cluster and is the wrong tool for high-volume data-plane
  retries.
