package com.dmp.application.port.out;

import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

/**
 * Dead-letter queue for individual rejected records.
 *
 * <p>This is both the DLQ and the {@code ERRORS} tier of the audit policy. A rejected record and an
 * audited failure are the same event, and storing it twice would only create a reconciliation
 * problem between two accounts of the same thing.
 *
 * <p>Entries carry the record payload, because a message saying "5,000 records failed" without
 * saying which ones tells a user they have a problem and nothing about how to fix it. Payloads are
 * redacted according to the pipeline's audit policy before they reach this port — an unredacted
 * payload must never be written, and by the time it is here it is too late to fix that.
 */
public interface RecordErrorPort {

    void recordAll(List<RecordErrorEntry> errors);

    List<RecordErrorEntry> findByRun(TenantId tenantId, RunId runId, int limit);

    long countByRun(TenantId tenantId, RunId runId);

    /**
     * A page of a run's rejected records, for replaying them through the pipeline again.
     *
     * <p>Ordered by the position the records held in the original run and paged by offset, so a
     * replay divides into chunks that between them cover every entry exactly once. The ordering has
     * to be total and stable for that to hold — {@code seq} repeats across chunks of the original
     * run, so the chunk is part of the sort rather than {@code seq} alone.
     *
     * <p>Reads what is still stored: entries the audit retention has already expired are gone, and
     * the replay covers what remains rather than failing because the rest is unrecoverable.
     */
    List<RecordErrorEntry> findForReplay(TenantId tenantId, RunId runId, int skip, int limit);

    /**
     * Counts a group of records that failed the same way, and says how many payloads may be kept.
     *
     * <p>The count is applied atomically, so the total is exact whichever pods observed which parts
     * of it. The allowance is not: two pods can each be told there is room for the last few samples
     * and both take it. That is deliberate — serialising the decision would put a lock on the
     * failure path, and the cost of the race is storing thirteen examples of a fault instead of ten.
     * What matters is that twenty million become a handful, not that the handful is exactly ten.
     *
     * <p>The cap is passed in rather than read from the caller's own count because only this port
     * knows how many payloads previous batches — on this pod or any other — already stored.
     *
     * @param occurrences how many records hit this fault in this batch
     * @param wanted      how many of their payloads the caller would store if permitted
     * @param cap         total payloads allowed for this fault across the run; {@code 0} for no cap
     * @return how many payloads to store, between zero and {@code wanted}
     */
    int reserveSamples(SignatureKey key, long occurrences, int wanted, int cap,
                       Instant now, Instant expiresAt);

    /** The distinct faults in a run, with exact counts, ordered by how many records each cost. */
    List<SignatureSummary> summariseByRun(TenantId tenantId, RunId runId, int limit);

    /**
     * Identifies one distinct fault within one run.
     *
     * @param signature normalised key the records were grouped on
     * @param code      the target's own error code, verbatim
     * @param message   representative message with per-record identifiers replaced
     */
    record SignatureKey(TenantId tenantId, RunId runId, String nodeId, String signature,
                        String code, String message) {
    }

    /**
     * One fault, and what it cost.
     *
     * @param count         records that hit this fault, exact regardless of how many were stored
     * @param samplesStored payloads available to look at
     */
    record SignatureSummary(String signature, String code, String message, String nodeId,
                            long count, long samplesStored,
                            Instant firstSeenAt, Instant lastSeenAt) {
    }

    /**
     * @param nodeId    which pipeline node rejected it
     * @param seq       position within the chunk, so the record can be located exactly
     * @param code      the external system's error code, verbatim
     * @param message   the external system's message, verbatim
     * @param payload   redacted before it arrives here
     * @param expiresAt TTL boundary, set from the pipeline's audit retention
     */
    record RecordErrorEntry(
            TenantId tenantId,
            RunId runId,
            SplitId splitId,
            String nodeId,
            long seq,
            String key,
            String code,
            String message,
            JsonNode payload,
            Instant occurredAt,
            Instant expiresAt) {
    }
}
