# Target Design — Event-Driven Work Distribution

This is the design we are moving to. It replaces the polling model described in
[run-lifecycle.md](run-lifecycle.md).

One sentence: **Kafka hands out the work, one worker owns each chunk from start to finish, and
MongoDB remembers where everything got to.**

---

## 1. The picture

```
  Console / API / Schedule
        │
        │  create Run in MongoDB (state CREATED)
        │  publish  RUN_START
        ▼
  ┌──────────────────────────────────────────────┐
  │  dmp.work.v1          16 partitions           │
  │  dmp.work.sequential.v1   1 partition         │
  └──────────────────────────────────────────────┘
        │            │            │
        ▼            ▼            ▼
    Worker A     Worker B     Worker C          consumer group "dmp-workers"
        │
        │  RUN_START  → work out the chunks, publish one CHUNK_READY each
        │  CHUNK_READY → read, transform, write, checkpoint
        │
        ├── chunk done      → publish the next CHUNK_READY (lazy mode only)
        ├── chunk parked    → hand to the delay queue, it comes back later
        └── chunk failed    → republish with attempt + 1
```

Nothing polls. A worker sits waiting on Kafka and starts the moment work arrives.

---

## 2. Topics

Three. This is the whole list to give the platform team.

| Topic | Partitions | Carries |
|---|---|---|
| `dmp.work.v1` | 16 | run starts, and chunks for normal runs |
| `dmp.work.sequential.v1` | 1 | chunks for runs that must go one at a time |
| `dmp.run.events.v1` | 3 | notices for other teams — already in the config |

Plus the delay queue we already have.

**Why the second topic has one partition.** Some runs must do one chunk at a time — Salesforce
locks rows, parents must land before children, a read replica cannot take sixteen scans at once.
One partition means one consumer, which means one chunk at a time. Kafka enforces it. No lock, no
counter, no extra code.

**16 is the parallel limit.** Sixteen chunks at once across every pod. A seventeenth pod adds
nothing. Raising it later is painful, so agree the number before rollout.

**The platform still never creates a topic.** If a topic is missing the run stops with a clear
message naming it. This does not change ([ADR-0013](../adr/0013-no-topic-auto-creation.md)).

---

## 3. What a message looks like

```json
{ "type": "CHUNK_READY",
  "tenantId": "...",
  "runId":    "...",
  "splitId":  "...",
  "index":    47,
  "attempt":  0 }
```

**Ids only. No records, no config, no secrets.** The worker looks everything else up in MongoDB.
A message carrying data goes stale; a message carrying an id never does.

Keyed by `splitId`, so chunks spread evenly across the sixteen partitions.

---

## 4. What a worker does

```
poll()  →  gets ONE message                    max.poll.records = 1

if the chunk is already COMPLETED:
      commit the offset, do nothing            ← guard against Kafka redelivery

pause() this partition                         stop taking new work
hand the chunk to a worker thread
keep calling poll()                            heartbeat only, returns nothing

    read a batch  →  transform  →  write  →  save checkpoint
    ... repeat until the chunk's row budget is used, or the source runs dry

add this chunk's totals to the run
mark the chunk COMPLETED
commit the offset                              ← only now
resume() the partition
```

**The pause is not optional.** A Kafka consumer must come back to `poll()` every few minutes or the
group throws it out — in the middle of a chunk. Our chunks can run for hours. Pausing the partition
and polling only to say "still alive" is what stops that. `max.poll.interval.ms` set high as a
backstop.

**The offset is committed last, after the chunk is done.** A pod that dies half way never commits,
so Kafka gives the chunk to somebody else, and that worker carries on from the checkpoint. This is
at-least-once, the same promise the platform makes everywhere else.

---

## 5. Settings

The user sets **one number**. The other two have good defaults and belong under Advanced.

| Setting | Who sets it | Default | What it means |
|---|---|---|---|
| **Rows per chunk** | **the user** | **10,000** | how much work one worker takes at a time |
| Batch size | nobody, normally | 1,000 | how many rows go to the sink in one call, and how much is redone after a crash |
| Read fetch size | nobody, normally | 1,000 | how many rows come back from the source in one call |

**Why 10,000 and not 50,000.** With Kafka a worker is given whole partitions. If the big chunks all
land in one partition, that worker grinds while the others idle. Making many more chunks than
partitions averages that out. Aim for roughly **ten times as many chunks as partitions**.

**Why batch size stays.** Two reasons that cannot be designed away: sinks have their own hard
limits, and the checkpoint is saved after each batch, so batch size is how much work is repeated
after a crash. The sink's real limit always wins over the configured number.

---

## 6. How chunks are worked out when we do not know the size

We almost never know how many rows there are, and we should not try to find out. Three cases, in
order of preference.

**A. The source can give the smallest and largest key.** Divide the key range, not the row count:

```
min = 1, max = 1,000,000, target 10,000 rows
   →  chunk 0: keys      1 –  10,000
      chunk 1: keys 10,001 –  20,000
      ...
```

Two indexed lookups. Works on a billion-row table. Chunks come out uneven — one range dense,
another almost empty — and that is fine, because an empty chunk finishes instantly.

This is what JDBC already does, and what MongoDB should do. **MongoDB currently runs
`countDocuments` on every run**, which is a scan on a big filtered collection. That should be
dropped in favour of the same min/max approach.

**B. The source cannot give a range, but can be read in order.** Lazy mode: take 10,000 rows, stop,
and publish the next chunk starting from where this one stopped. Repeat until a chunk comes back
short. Nothing is counted and nothing is guessed — but it is **one chunk at a time**, because the
next chunk's starting point is an output of the previous one.

**C. Neither.** One chunk, read start to finish. A REST cursor chain is honest about this rather
than faking parallelism.

---

## 7. How the counts stay correct

Two places, and the split is the whole trick.

**The checkpoint counts one chunk.** Updated after every batch, together with the resume position:

```
checkpoint { splitId, sourceCursor,
             recordsRead, recordsWritten, recordsFailed, recordsFiltered, bytesRead }
```

**The run counts the whole job.** Updated **once**, when a chunk finishes, with a MongoDB `$inc`.
Many workers can do this at the same time without losing anything.

Why this is safe when a chunk runs twice:

- the chunk's totals are read from the checkpoint, not from memory
- a pod that dies half way never added anything to the run
- the replacement pod carries on from the checkpoint and adds the full total once

Chunk counts: `splitsTotal` rises as chunks are created — in lazy mode it grows during the run,
which is why the console shows "chunks finished" and not a percentage. There is no honest
denominator, and an invented one is worse than none.

**The one new risk with Kafka** is a finished chunk being delivered a second time. The guard in
step 4 handles it: if the chunk is already `COMPLETED`, commit and do nothing.

---

## 8. What gets deleted

Kafka's consumer group already does these jobs, so the hand-written versions go:

- chunk leases and the heartbeat that renews them
- the sweep that reclaims chunks from dead workers
- `activeSlots`, the slot reservation, and the drift-reconciliation sweep
- the whole poll loop and its backoff ladder

That is a meaningful amount of the hardest code in the engine.

---

## 9. What does not change

- **All seven connectors.** Not one line.
- **`ChunkExecutor`** — the read/transform/write/checkpoint loop.
- Transforms, the audit index, the dead-letter queue, run parameters, schedules.

This is the reason for keeping one owner per chunk. Salesforce sends a bulk job and comes back
later for the failed rows; Databricks submits a query and waits. Both need a single owner for the
whole life of a chunk. Splitting read, transform and write into separate services would break them.

**Records never travel through Kafka.** Only ids. Sending the data through a shared broker would
double the network cost, make that cluster the ceiling for every migration, and need a topic per
pipeline that we cannot get.

---

## 10. Build order

1. **Stop the 200 ms sleep between chunks.** Three lines. Roughly 3× faster today, and gives an
   honest number to measure the rest against.
2. **Build the Kafka consumer behind a flag.** Both paths in the code, one switch.
3. **Move run starts to Kafka.** Smallest piece. Removes the start-up delay straight away.
4. **Move chunks to Kafka.** The real change.
5. **Delete the leases and slots** once real migrations have run on the new path.
6. **Write ADR-0015**, marking ADR-0014 as replaced.

Steps 1 and 3 are worth doing on their own, whatever happens to the rest.
