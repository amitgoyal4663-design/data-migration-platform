package com.dmp.application.port.out;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;

import java.time.Instant;
import java.util.List;

/**
 * A searchable record of what happened to each individual record.
 *
 * <p>Answers one question: <em>was this record transferred, and what is its status?</em> Asked
 * months after a cutover, about one identifier out of crores, by someone who needs an answer rather
 * than a percentage. Counters cannot answer it. Reading the destination cannot either — that says
 * whether the record is there now, not whether this platform put it there, in which run, or when.
 *
 * <p><b>Identities, never payloads.</b> A key, an outcome, a run and a timestamp is about a hundred
 * bytes; the same record with its content is thousands. At a hundred million records that is the
 * difference between gigabytes and terabytes, in exchange for almost nothing: a rejected record's
 * content is already in {@link RecordErrorPort}, and a written record's content is in the
 * destination. Holding no content, entries also need no redaction and carry no erasure obligation,
 * which is what lets them be kept for years while payloads are kept for days.
 *
 * <p><b>This port must not lose entries.</b> That is the whole difference between it and
 * {@link RecordLogPort}, which drops events when its backend is slow because a migration must never
 * wait on its own logging. The same trade is unacceptable here: a dropped entry does not degrade
 * the answer, it inverts it — the search reports "not transferred" for a record that was, and
 * somebody migrates it twice or tells a customer their data is missing. An implementation that
 * cannot write must fail loudly enough that the chunk is retried.
 */
public interface RecordIndexPort {

    /**
     * Records what happened to a batch of records.
     *
     * <p>Called before the chunk's checkpoint advances, so a chunk that dies mid-flight is replayed
     * from a position at or behind what is indexed. Entries are therefore at-least-once and are
     * keyed to be idempotent: re-indexing the same record of the same run overwrites rather than
     * duplicates.
     */
    void indexAll(List<RecordIndexEntry> entries);

    /**
     * Everything known about one record key within one pipeline, newest first.
     *
     * <p>Scoped to a pipeline rather than searching the tenant, and required rather than optional.
     * A record key is only unique in the context of the source it came from — two pipelines moving
     * different systems can legitimately both hold a record numbered 88291, and answering with both
     * is answering a question nobody asked.
     *
     * <p>It is also what makes the query authorizable. Access will be granted per pipeline, and a
     * query with no pipeline in it cannot be checked against that grant — so the scope is part of
     * the signature now, while changing it is free, rather than after callers depend on the
     * unscoped shape.
     */
    Page<RecordIndexEntry> findByKey(TenantId tenantId, PipelineId pipelineId, String recordKey,
                                     PageQuery pageQuery);

    /** The index for one run, for reconciling a single migration. */
    Page<RecordIndexEntry> findByRun(TenantId tenantId, RunId runId, Outcome outcome,
                                     PageQuery pageQuery);

    /** How many entries a run indexed, to compare against what the run says it moved. */
    long countByRun(TenantId tenantId, RunId runId);

    /**
     * Free-text and field search across indexed records.
     *
     * <p>Only meaningful where the pipeline indexes payloads; an identity-only index has nothing to
     * match on but the key, which {@link #findByKey} already answers more cheaply. An
     * implementation with no search capability may answer this by key alone, and must say so rather
     * than returning an empty page that reads as "no such record".
     */
    Page<RecordIndexEntry> search(TenantId tenantId, Query query, PageQuery pageQuery);

    /** Whether this implementation can match on record content rather than only on the key. */
    boolean supportsContentSearch();

    /**
     * @param recordKey exact key, when the caller already knows it
     * @param text      matched across the record's fields; requires payload indexing
     * @param field     a specific field to match {@code text} against, e.g. {@code email}
     */
    record Query(PipelineId pipelineId, String recordKey, String text, String field, RunId runId,
                 Outcome outcome, Instant after, Instant before) {
    }

    /**
     * What happened to one record.
     *
     * <p>{@code splitId}, {@code seq} and {@code ordinal} are this entry's identity, and the
     * reason the index can be trusted as a count. An implementation must make an entry with the
     * same three values replace its predecessor rather than sit beside it: entries are written
     * before the checkpoint advances, so a chunk that dies between the two re-indexes on its next
     * attempt what it had already indexed. {@code recordKey} cannot serve — a source may hold the
     * same key twice, and keying on it would file two genuinely different records as one.
     *
     * @param splitId   the chunk, which is also the unit that retries
     * @param traceId   the read → transform → write cycle that carried this record, as built by
     *                  {@link StageLogPort.Trace}. The join between this index and the stage log:
     *                  without it the two are a list of records and a list of calls with no way to
     *                  say which records were in which call
     * @param seq       position within that chunk, stable across retries of it
     * @param ordinal   which output of that position this is, from 0, for a transform that turned
     *                  one input into several
     * @param recordKey the source's own identifier — an {@code _id}, a primary key, a message
     *                  key. Null where the source has none; searching by key then cannot find
     *                  this entry, but the run's own list still contains it
     * @param outcome   whether it was written, rejected or deliberately filtered out
     * @param errorCode the destination's code when it was rejected; null otherwise
     * @param payload   the record's content, redacted per the pipeline's policy — null unless the
     *                  pipeline opted into payload indexing
     * @param expiresAt when this entry is deleted, normally far later than its payload's expiry
     */
    record RecordIndexEntry(
            TenantId tenantId,
            PipelineId pipelineId,
            RunId runId,
            SplitId splitId,
            String traceId,
            long seq,
            int ordinal,
            String recordKey,
            Outcome outcome,
            String errorCode,
            com.fasterxml.jackson.databind.JsonNode payload,
            Instant occurredAt,
            Instant expiresAt) {
    }

    /** The three ways a record leaves a pipeline. */
    enum Outcome {
        /** Accepted by the destination. */
        WRITTEN,
        /** Refused by the destination. Its payload is in the dead-letter queue. */
        REJECTED,
        /** Dropped by a transform on purpose. A success, not a failure. */
        FILTERED,
        /**
         * Handed to a destination that decides later, and no per-record verdict came back.
         *
         * <p>Exists because the alternative was a lie. A Salesforce bulk write reports every record
         * as written the moment it is staged — the org has not looked at any of them yet — and the
         * index was recording that as {@code WRITTEN}. A run where five thousand of ten thousand
         * records were refused indexed all ten thousand as written, and "was record 8,432
         * transferred?" answered yes for records the destination had thrown out. That is the one
         * question this index exists to answer, so answering it wrongly is worse than not indexing
         * at all.
         *
         * <p>Reaching this state is not an error. It means the sink was asked for a count rather
         * than a list of rejections, which is a deliberate and often correct choice — see
         * {@code Sink.Harvest}. What it says is exactly true: this record was sent, the destination
         * accepted the batch, and nobody asked it record by record what became of this one.
         */
        SENT
    }
}
