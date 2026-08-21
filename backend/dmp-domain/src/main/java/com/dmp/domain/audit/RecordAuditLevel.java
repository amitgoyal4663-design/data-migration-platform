package com.dmp.domain.audit;

/**
 * How much per-record detail a pipeline captures, per ADR-0011.
 *
 * <p>The right answer genuinely differs by pipeline, which is why this is policy rather than a
 * platform-wide setting. Moving anonymised analytics events and moving payment records warrant
 * different answers, and forcing one of them is either wasteful or negligent.
 */
public enum RecordAuditLevel {

    /**
     * Aggregate counters only, held in the run and split documents.
     *
     * <p>Always captured; this level means nothing beyond it is. Free.
     */
    COUNTERS(false, false),

    /**
     * Every rejected record with its full payload, failing node and error.
     *
     * <p>The default. Also serves as the dead-letter queue — a rejected record and an audited
     * failure are the same event, and storing it twice would only create a reconciliation problem.
     */
    ERRORS(true, false),

    /**
     * Errors, plus one index entry per record: its key, what happened to it, and where.
     *
     * <p>Answers "was record 88291 transferred, and what is its status" across every run — the
     * question a business asks months after a cutover, which counters cannot answer and which
     * reading the target cannot either, because the target says only whether the record is there
     * now, not whether this platform put it there or when.
     *
     * <p><b>No payloads.</b> A key, an outcome, a run and a timestamp is roughly a hundred bytes,
     * so a hundred million records cost gigabytes; the same records with their payloads cost
     * terabytes and answer almost nothing further — a rejected record's content is already in the
     * dead-letter queue, and a written record's content is in the destination.
     *
     * <p>Holding no record content, it also needs no redaction and carries no erasure obligation,
     * which is what lets it be retained for years while payloads are measured in days.
     */
    INDEXED(true, false),

    /**
     * Every record, before and after.
     *
     * <p>Written to a Kafka audit topic and archived to object storage as Parquet — never to
     * MongoDB. See ADR-0011. Requires the archival pipeline delivered in Phase 11; selecting it
     * before then is rejected at validation rather than silently downgraded.
     */
    FULL(true, true);

    private final boolean capturesFailures;
    private final boolean capturesSuccesses;

    RecordAuditLevel(boolean capturesFailures, boolean capturesSuccesses) {
        this.capturesFailures = capturesFailures;
        this.capturesSuccesses = capturesSuccesses;
    }

    public boolean capturesFailures() {
        return capturesFailures;
    }

    public boolean capturesSuccesses() {
        return capturesSuccesses;
    }

    /** Whether this level writes record payloads, and therefore requires a redaction policy. */
    public boolean capturesPayloads() {
        return capturesFailures || capturesSuccesses;
    }

    /**
     * Whether every record gets a searchable index entry, written or rejected.
     *
     * <p>Distinct from {@link #capturesSuccesses()}, which asks whether successful record
     * <em>payloads</em> are stored. This asks only whether their identities are, which is a
     * different question with a cost three orders of magnitude apart.
     */
    public boolean indexesEveryRecord() {
        return this == INDEXED || this == FULL;
    }
}
