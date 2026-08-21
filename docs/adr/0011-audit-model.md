# ADR-0011: Two-tier audit — control-plane actions and record-level lineage

**Status:** Accepted (2026-08-06)

## Context

"Audit" in a data platform means two unrelated things that are routinely conflated, with very
different volume, retention and storage requirements.

|  | Control-plane audit | Record-level audit |
|---|---|---|
| Question answered | "Who published v3 at 03:14?" | "What happened to customer 88291's row?" |
| Volume | Hundreds per day | One entry **per record** — 100M for a 100M-row run |
| Retention | Years, legally | Days to years, depending on purpose |
| Mutability | Must be immutable | Append-only |
| Contains customer data | No | **Yes — this is the whole point, and the whole problem** |

Treating them as one system produces either an audit log too expensive to keep or one too thin to
be useful.

## Decision

### Tier 1 — control-plane audit

Every mutation of a definition writes an `audit_log` row in **PostgreSQL**.

```
audit_log(id, tenant_id, occurred_at, actor, action, resource_type, resource_id,
          summary, before, after, request_id, source_ip)
```

- Append-only. The application's database role is granted `INSERT` and `SELECT` and nothing else,
  so "delete the evidence" is not expressible through the application at all.
- No TTL. Rows are never aged out. Volume does not justify it and compliance forbids it.
- `before` / `after` hold the JSON diff of the changed aggregate, which makes "what exactly did
  that edit change" answerable without reconstructing state from adjacent entries.
- Written in the **same transaction** as the change it records. An audit log that can disagree
  with the data it describes is worse than none, because it is trusted.

Optional and not built in Phase 1: hash-chaining each entry over its predecessor's digest, for
tamper *evidence* rather than tamper resistance. Recorded here so the column can be added before
there is history to migrate.

### Tier 2 — record-level audit

A per-pipeline policy with four levels, because the right answer genuinely differs by pipeline.

| Level | Captured | Store | Retention | Cost |
|---|---|---|---|---|
| `COUNTERS` | read / written / failed / filtered per split | MongoDB, inside the run document | with the run | free |
| `ERRORS` *(default)* | every rejected record: full payload, node id, error, timestamp | MongoDB `record_errors` | 30d TTL | ~1% of volume |
| `INDEXED` | plus one entry per record — key, outcome, run; content when opted in | OpenSearch | 1y+ | ~140 B/record, ~250 B with content |
| `FULL` | every record, before and after | Kafka audit topic → **object storage, Parquet** | years | high volume, low unit cost |

`SAMPLED` was removed in August 2026. It promised one-in-N successful records with before/after
payloads, was accepted at validation, and the engine never implemented it — a pipeline set to it
behaved exactly as `ERRORS` with no warning. The need behind it, checking that a transform does what
its author intended, belongs in a preview while the script is being written rather than in a runtime
audit level discovered to be wrong after a twenty-million-row run.

`INDEXED` replaced its slot. It answers a question the other levels cannot — *was record 88291
transferred, and what happened to it* — which counters cannot answer and reading the destination
cannot either, because the destination says only whether the record is there now, not whether this
platform put it there or when. Identities alone cost about 140 bytes a record and hold no personal
data, so they need no redaction and are retained for a year by default; adding record content makes
every field searchable and is a separate per-pipeline opt-in, because it puts customer data into a
second store and inherits that pipeline's redaction rules.

**`FULL` deliberately does not write to MongoDB.** A hundred million TTL'd documents is sustained
write pressure, index bloat and a deletion backlog, in exchange for a query pattern nobody uses —
full lineage is read during investigations and compliance requests, not on dashboards. The same
data as Parquet on object storage compresses roughly 10:1, costs almost nothing to retain, and is
directly queryable by Athena, DuckDB or Databricks. MongoDB holds the operational slice being
actively debugged; object storage holds the archive.

The `ERRORS` tier doubles as the dead-letter queue. A rejected record and an audited failure are
the same event, and writing it twice would be storage spent to create a reconciliation problem.

### Redaction is part of the writer, not a later feature

Record-level audit stores real customer data. A migration platform that logs every record verbatim
is a data-protection incident with a delay fuse — and one that cannot be fixed after the fact,
because by the time anyone notices, the unredacted data is already on disk and in backups.

Therefore `AuditPolicy` carries `redactedFields` and a `RedactionMode`
(`MASK`, `HASH`, `DROP`), applied **before** the audit record is serialised — never as a
post-processing pass over stored data.

`HASH` is deliberately available alongside `DROP`: hashing preserves the ability to correlate the
same value across runs, which is often the actual investigative need, without storing the value.

## Consequences

**Positive**
- Compliance-grade control-plane audit at negligible cost.
- Record-level cost scales with what the pipeline actually needs, rather than being one global
  decision that is wrong for most pipelines.
- The DLQ and the error audit are one thing, so they cannot disagree.
- Redaction is structurally impossible to forget, because the writer requires a policy.

**Negative**
- Four levels is configuration surface. Mitigated by `ERRORS` being a sane default that most users
  never change.
- `FULL` requires object storage and a Parquet writer, which is Phase 11 work and unavailable
  before then. Attempting `FULL` earlier is rejected at validation rather than silently degraded.
- Writing the audit row in the same transaction as the change couples audit availability to write
  availability: if `audit_log` is unwritable, the mutation fails. This is the intended trade-off —
  an unaudited change is not an acceptable fallback.
- Redaction is only as good as the field metadata. Unmarked PII is stored unredacted. Phase 12
  should add pattern-based detection as a backstop, but metadata remains the primary mechanism.

## Alternatives rejected

- **Audit everything, always.** ~50 GB of audit for a 100M-row migration, in an operational store,
  to answer questions almost nobody asks.
- **Application log files as the audit trail.** Not queryable, not tenant-scoped, not immutable,
  and routinely rotated away exactly when needed.
- **Record-level audit in PostgreSQL.** Same write-amplification and vacuum objection that moved
  runs to MongoDB in ADR-0005, at 100× the volume.
