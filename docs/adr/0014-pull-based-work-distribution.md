# ADR-0014: Workers pull chunks; concurrency is limited by atomic slot reservation

**Status:** Accepted (2026-08-07). Amends [ADR-0001](0001-dual-mode-channel.md).

## Context

ADR-0001 said splits are "published to Kafka keyed by `runId`, and Kafka's consumer-group rebalance
becomes our work-assignment protocol." Two problems with that, one fatal.

**Fatal: keying by `runId` defeats the purpose entirely.** Kafka partitions by key hash, so every
chunk of a run lands in one partition, is consumed by one consumer, and is executed by **one pod**.
The mechanism intended to distribute work would concentrate it.

**Structural: push assignment does not handle uneven work.** Keying by `splitId` spreads chunks,
but spreads them blindly. Chunks are not equal — one key range is dense, another is sparse; one
file is 4 KB, the next is 400 MB. Blind assignment produces stragglers: a pod that happens to
receive ten slow chunks finishes long after a pod that received ten fast ones, and the run's
duration is set by the unluckiest pod.

Separately, users need to constrain concurrency. Sequential execution is a real requirement, not a
degraded mode: concurrent Salesforce Bulk v2 jobs against the same object produce
`UNABLE_TO_LOCK_ROW` failures on rows that are otherwise valid; parent records must land before the
children referencing them; a modest read replica will not survive sixteen simultaneous range scans.

## Decision

### Workers pull; nothing pushes

Chunks are written to MongoDB in `PENDING` state. Every worker runs the same loop:

```
forever:
    if I have a free slot:
        chunk = claimNextPending(runId, workerId, lease)   ← one atomic operation
        if chunk: execute it
        else:     back off and retry
```

`claimNextPending` is a single `findAndModify`: match a `PENDING` chunk with the lowest index, mark
it `RUNNING` with this worker's id, return it. MongoDB serialises it, so two workers racing cannot
both receive the same chunk — one gets it, the other gets the next, or nothing.

**That single operation is the entire distribution mechanism.** No coordinator, no assignment
service, no rebalance protocol, and no Kafka involvement.

Distribution self-balances because a pod only asks when it is ready. A pod finishing a fast chunk
immediately asks again; a pod grinding through a slow one asks for nothing. Nobody predicts chunk
duration, and nobody needs to.

### Concurrency limits via atomic slot reservation

`ExecutionPolicy.maxConcurrentChunks` bounds how many chunks of a run execute across the entire
fleet. Enforced by making the limit part of the query:

```javascript
findAndModify(
   filter: { _id: runId, activeSlots: { $lt: maxConcurrentChunks } },
   update: { $inc: { activeSlots: 1 } }
)
```

The check and the increment are one operation, so two pods cannot both observe "3 of 4 in use" and
both proceed. With a limit of 1, exactly one worker in the cluster holds the slot. A worker reserves
before claiming and releases when the chunk finishes; reserving and finding no pending chunk is an
ordinary outcome, and the caller releases and moves on.

`maxConcurrentChunks = 0` means unlimited and skips reservation altogether, so the common case pays
no extra round trip.

### What sequential does and does not give

Strict sequencing and simultaneous work across many pods are contradictory. With a limit of 1,
exactly one chunk of that run executes at any instant, and no architecture changes that.

What is preserved:

- **The pod rotates.** Chunk 0 may run on pod A, chunk 1 on pod C, chunk 2 on pod B — whichever
  asks first.
- **The fleet stays busy.** The limit is per-run, so other runs occupy the remaining pods. A slow
  ordered migration never idles the cluster.

`maxChunksPerPod` bounds a single worker so one pod cannot claim an entire parallel run at startup
and leave the rest idle.

### Leases, not heartbeat protocols

A claimed chunk carries `leaseExpiresAt`. The owning worker extends it every
`chunkLease / 3` — a third, so two consecutive missed beats do not cost a worker a chunk it is
actively processing. A worker that stops extending, because it died or was partitioned or is wedged,
loses the chunk to a sweep that returns it to `PENDING` **and releases its slot**.

The heartbeat is conditional on `assignedTo` still matching. A worker whose lease lapsed and whose
chunk was reclaimed must fail to extend it; otherwise two pods would each believe they hold the
claim and write every record of that chunk twice.

### Reconciliation

Slot drift accumulates in one direction: workers that die holding a slot never decrement. The lease
sweep releases what it can see, but the terminal state of unchecked drift on a sequential run is a
permanent deadlock — counter reads 1, nothing is running, no worker can ever reserve again. A
periodic `reconcileSlots` sets the counter to the observed count of `RUNNING` chunks, removing the
possibility rather than reducing its likelihood.

## Consequences

**Positive**
- Load balances itself, including across chunks of wildly differing duration.
- No parallelism ceiling from partition counts.
- Adding a pod mid-run needs no rebalance; it starts claiming immediately.
- Removing a pod mid-run costs one lease interval, then its chunks are reclaimed and resumed from
  their checkpoints.
- Sequential execution is expressible and correctly enforced across a fleet with no shared lock.
- **Kafka is no longer required by the execution engine at all.** Given that the platform cannot
  create topics ([ADR-0013](0013-no-topic-auto-creation.md)), this removes the provisioning
  dependency from the core entirely. Kafka remains a connector and an optional event publisher.

**Negative**
- Workers poll rather than being notified. One indexed query per pod per interval — negligible —
  but it is polling, and idle pods discover new work up to one interval late. Mitigated by adaptive
  backoff: frequent while work is flowing, sparse when idle.
- Two round trips to start a chunk when a limit is set (reserve, then claim) instead of one.
  Unlimited runs are unaffected.
- The slot counter is derived state that can drift, requiring the reconciliation sweep above. This
  is the price of enforcing a fleet-wide invariant without a lock service.
- Fairness between runs is not yet implemented: a worker choosing which run to claim from must
  round-robin across active runs, or a large run will starve a small one. Phase 3 work, noted here
  so it is not discovered in production.

## Alternatives rejected

- **Kafka consumer-group assignment.** The mechanism that motivated this ADR. Concentrates work
  when keyed by run, produces stragglers when keyed by split, and caps parallelism at the partition
  count.
- **A central scheduler assigning chunks to pods.** Requires tracking pod liveness and capacity,
  becomes a single point of failure, and must re-derive exactly the information the pull model gets
  for free by having pods ask only when ready.
- **Advisory locks or a lock service for concurrency limits.** Correct, and adds a dependency plus a
  liveness problem — a pod dying while holding a lock needs the same lease machinery, so nothing is
  saved.
