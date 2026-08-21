# Example — one call per day, MongoDB to MongoDB

A working pipeline that divides each batch into one call per day, and proves it did.

Built and run against the local compose stack; the numbers at the bottom are from that run, not
from a description of one.

- **Pipeline:** `split-demo` (folder `/examples`)
- **Definition:** [`split-by-day.version.json`](split-by-day.version.json)

---

## What it does

```
partest_source  ──►  Tag each call  ──►  splitdemo_sink
   (MongoDB)      (batch transform)        (MongoDB)
```

Records are read from one collection, the batch is divided into one group per `day`, and each group
is written with its own call. A batch transform runs **per group** and stamps each record with which
call carried it — which is what makes the grouping visible afterwards rather than something you have
to take on trust.

## The split script

Delivery → *Split each batch by a script*:

```javascript
function split(records) {
  // One call per day. Records sharing a label travel together.
  return records.map(r => r.day)
}
```

One label per record, same order. Same label means the same call.

## The batch transform

A node on the canvas, between source and sink:

```javascript
function transformBatch(records) {
  // Runs ONCE PER GROUP. If it ran per batch, callSize would be the whole
  // batch and callGroup would be one day for every record.
  return records.map(r => ({
    ...r,
    callGroup: records[0].day,
    callSize: records.length
  }))
}
```

## Settings

```
Rows per chunk   10,000
Read size             0     — platform default
Write size            0     — whatever the destination prefers
Delivery              split script (above)
Sink write mode       UPSERT
```

**`UPSERT`, not `INSERT`, and it matters.** The batch transform spreads `...r`, which carries the
source `_id` through to the destination. Under `INSERT` the example works exactly once and every
later run fails with forty duplicate-key rejections:

```
code:    11000
message: E11000 duplicate key error collection: dmp.splitdemo_sink
         index: _id_ dup key: { _id: "aug18-0" }
```

Under `UPSERT` the same records land on themselves and the run is repeatable — which is what an
example ought to be, and what a pipeline you might retry ought to be too.

## Running it

```bash
curl -X POST "$API/pipelines/$PID/runs" \
  -H "X-Tenant-Id: $TENANT" -H 'Content-Type: application/json' \
  -d '{"parameters":{"from":"2026-08-18T00:00:00+05:30","to":"2026-08-21T00:00:00+05:30"}}'
```

The window spans three days, so the source yields three distinct labels.

## What came out

```
run state   : COMPLETED
read/written: 40 / 40

day 2026-08-18  →  callGroup 2026-08-18  callSize 10  |  10 records
day 2026-08-19  →  callGroup 2026-08-19  callSize 20  |  20 records
day 2026-08-20  →  callGroup 2026-08-20  callSize 10  |  10 records

total documents: 40
```

```json
{
  "day": "2026-08-18",
  "payload": "recent row 18/0",
  "seq": 2800,
  "updatedAt": "2026-08-18T12:00:00.000Z",
  "callGroup": "2026-08-18",
  "callSize": 10
}
```

Run it again and you get the same forty documents, not eighty:

```
run 1: COMPLETED written=40 failed=0 | sink holds 40 documents
run 2: COMPLETED written=40 failed=0 | sink holds 40 documents
run 3: COMPLETED written=40 failed=0 | sink holds 40 documents
```

**`callSize` is the assertion.** Forty records went through in one batch. Had the batch transform
run once for that batch — the old behaviour — every record would carry `callSize: 40` and
`callGroup: 2026-08-18`. Instead the three groups report 10, 20 and 10, each stamped with its own
day. Three calls on the destination, each seeing only its own records.

Note also that all forty were written under **one checkpoint**. Dividing the batch changed how many
times the destination was called; it did not change how much work a crash would repeat.

---

## Reusing it

1. Create your source and sink connector instances.
2. Take [`split-by-day.version.json`](split-by-day.version.json) and replace the two
   `<... connector instance id>` placeholders.
3. `POST /api/v1/pipelines/{id}/versions` with that body, then publish version 1.

Or copy the two scripts into the console: the batch transform onto a node, the split into
**Execution settings → Delivery → Split each batch by a script**. The **Try it** button beside the
editor runs it against sample records and shows the calls it would make, before any run.

## Adapting the split

```javascript
// one call per region
return records.map(r => r.region)

// one call per target table
return records.map(r => r.entityType)

// a new call whenever a running total would exceed a limit
let total = 0, group = 0
return records.map(r => {
  total += r.amount
  if (total > 10000) { group++; total = r.amount }
  return String(group)
})

// fixed size, without touching the batch setting
return records.map((r, i) => Math.floor(i / 50))
```

The last one is easier as **Delivery → Fixed groups**, which needs no script at all.

## Two things that surprise people

**A group never crosses a batch.** The engine holds a batch, never a whole chunk. Widen this
example past a batch's worth of records and `2026-08-19` would be written in two calls, not one —
each with its own `callSize`. If everything for one label must arrive together, sort by it at the
source.

**Records the script has no label for share a group.** A script written as a lookup returns
`undefined` for anything the lookup misses, and those records are written together in one call
rather than one call each — so a typo in a lookup table costs one extra call, not a thousand.
