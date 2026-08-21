package com.dmp.application.port.out;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

/**
 * A log of the work, as distinct from a log of the records.
 *
 * <p>{@link RecordIndexPort} answers <em>"what happened to record 88291?"</em>. This answers
 * <em>"what did the platform do, in what order, and how long did each part take?"</em> — and they
 * are different questions, because the work happens in batches while the index is per record.
 *
 * <p>Three stages, which is what a chunk does:
 *
 * <pre>
 *   READ  ──►  TRANSFORM  ──►  WRITE
 *   query      in → out        what the destination said
 * </pre>
 *
 * <p>Each answers a question nothing else could. A run that moved no rows is a READ entry with the
 * query in it. A run that read forty and wrote thirty-one is a TRANSFORM entry naming the node that
 * dropped nine. A batch refused whole is a WRITE entry with one status code explaining all five
 * hundred rejections — the fact the platform used to discard while keeping the five hundred.
 *
 * <p><b>Never blocks and never fails the run.</b> The opposite rule to {@link RecordIndexPort},
 * deliberately. A missing index entry inverts an answer — the search says "not transferred" for a
 * record that was — so that port must fail loudly enough to retry the chunk. A missing stage entry
 * only blurs a diagnosis. Between slowing a migration and losing a log line, the log line loses.
 *
 * <p><b>Never receives an unredacted body.</b> Redaction happens before this port is called.
 */
public interface StageLogPort {

    /**
     * Queues entries for indexing.
     *
     * <p>Returns immediately. Whether they reach the index is not the caller's concern and must not
     * affect its control flow.
     */
    void log(List<StageEntry> entries);

    /** Whether a backend is configured at all, so callers can skip building entries. */
    boolean isEnabled();

    /** One run's stages in the order they happened, optionally narrowed to one chunk or stage. */
    Page<StageEntry> find(TenantId tenantId, RunId runId, SplitId splitId, Stage stage,
                          PageQuery pageQuery);

    /**
     * One thing the platform did.
     *
     * @param traceId    the one read → transform → write cycle this belongs to. Formatted
     *                   {@code <chunkId>#<cycle>} — derived rather than random so a retried chunk
     *                   reuses the trace it had, and so narrowing to a chunk is a prefix match.
     *                   The record index stamps the same value, which is what lets a cycle be
     *                   shown as one story instead of two unrelated lists
     * @param stage      READ, TRANSFORM or WRITE
     * @param nodeId     which node on the canvas, so a pipeline with two transforms is unambiguous
     * @param sequence   position within the chunk for this stage, from 0
     * @param attempt    the chunk's attempt, so a retry's entries are distinguishable
     * @param recordsIn  how many records went in
     * @param recordsOut how many came out. Differs from {@code recordsIn} only at TRANSFORM, where
     *                   a filter drops and a splitter multiplies — and where that difference is
     *                   the entire reason the stage is worth logging
     * @param bytes      approximate serialised size of the records involved
     * @param durationMs wall-clock time, which is where a slow stage shows up
     * @param outcome    whether the stage itself succeeded, not whether its records did
     * @param query      READ only: the query the connector ran, with values redacted
     * @param cursorIn   READ only: where the stream was positioned before
     * @param cursorOut  READ only: where it was positioned after
     * @param details    whatever the connector could report that the counts cannot
     * @param request    what was sent, redacted and capped; null unless bodies are captured
     * @param response   what came back, redacted and capped; null unless bodies are captured
     */
    record StageEntry(
            TenantId tenantId,
            PipelineId pipelineId,
            RunId runId,
            SplitId splitId,
            String traceId,
            Stage stage,
            String nodeId,
            String nodeName,
            String connectorType,
            int sequence,
            int attempt,
            int recordsIn,
            int recordsOut,
            long bytes,
            long durationMs,
            Outcome outcome,
            String errorCode,
            String errorMessage,
            String query,
            JsonNode cursorIn,
            JsonNode cursorOut,
            JsonNode details,
            JsonNode request,
            JsonNode response,
            Instant occurredAt,
            Instant expiresAt) {

        /** How many records this stage lost or gained. Zero everywhere except a transform. */
        public int delta() {
            return recordsOut - recordsIn;
        }
    }

    /** The three things a chunk does. */
    enum Stage {
        /** Rows pulled from the source. */
        READ,
        /** Scripts applied to them, which may drop records or turn one into several. */
        TRANSFORM,
        /** Records handed to the destination. */
        WRITE
    }

    /**
     * How the stage itself ended.
     *
     * <p>About the stage, not its contents. A write the destination accepted while refusing half
     * the records inside it is {@code OK} here — the request succeeded — and the refusals are in
     * the record index and the dead-letter queue where they belong. Conflating the two is how the
     * platform came to report five hundred failures and no reason for any of them.
     */
    enum Outcome {
        OK,
        FAILED
    }

    /**
     * How a trace id is built, and read back.
     *
     * <p>Derived from the chunk and the cycle rather than generated, which buys three things at
     * once: a retried chunk reuses the ids it had, so re-indexing overwrites instead of
     * duplicating; narrowing a run's log to one chunk is a prefix match rather than a second
     * query; and the id is legible, so somebody reading one in a support ticket can see which
     * chunk it belongs to without a lookup.
     */
    final class Trace {

        private static final String SEPARATOR = "#";

        private Trace() {
        }

        public static String of(SplitId splitId, int cycle) {
            return splitId.value() + SEPARATOR + cycle;
        }

        /** The chunk portion, for narrowing a search. */
        public static String chunkOf(String traceId) {
            int at = traceId == null ? -1 : traceId.indexOf(SEPARATOR);
            return at < 0 ? traceId : traceId.substring(0, at);
        }
    }
}
