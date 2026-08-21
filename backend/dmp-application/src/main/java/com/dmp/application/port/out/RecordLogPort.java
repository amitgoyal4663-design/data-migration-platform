package com.dmp.application.port.out;

import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

/**
 * A searchable log of what happened to individual records.
 *
 * <p>Distinct from {@link RecordErrorPort}, which is the operational dead-letter queue: that
 * answers "show me this run's failures" and is indexed by run. This answers "find every record
 * whose email contains @acme.com, across every run, over the last ninety days" — a full scan in a
 * document store, and the thing a search engine exists for.
 *
 * <p>Three rules govern every implementation, and they are not negotiable:
 *
 * <ul>
 *   <li><b>Never blocks.</b> Logging is observability, not the work. A slow index must not slow a
 *       migration.</li>
 *   <li><b>Never fails the run.</b> If the log backend is unreachable, the implementation warns and
 *       drops. A migration failing because its logging was down would be absurd.</li>
 *   <li><b>Never receives an unredacted payload.</b> Redaction happens before this port is called.
 *       A search index full of personal data is worse than a database one, because it is designed
 *       to be searched.</li>
 * </ul>
 */
public interface RecordLogPort {

    /**
     * Queues events for indexing.
     *
     * <p>Returns immediately. Whether the events reach the index is not the caller's concern and
     * must not affect its control flow.
     */
    void log(List<RecordEvent> events);

    /** Whether a log backend is configured at all, so callers can skip building events. */
    boolean isEnabled();

    /**
     * What happened to one record.
     *
     * @param pipelineName denormalised so a search result is readable without a join
     * @param payload      already redacted per the pipeline's audit policy
     * @param durationMs   time spent on this record, where the connector can report it
     */
    record RecordEvent(
            TenantId tenantId,
            RunId runId,
            SplitId splitId,
            String pipelineName,
            String nodeId,
            String connectorType,
            long seq,
            String key,
            Outcome outcome,
            String errorCode,
            String errorMessage,
            JsonNode payload,
            long durationMs,
            Instant occurredAt) {
    }

    /** The three ways a record can leave a pipeline. */
    enum Outcome {
        /** Accepted by the destination. */
        WRITTEN,
        /** Rejected by the destination, and also sent to the dead-letter queue. */
        REJECTED,
        /** Dropped deliberately by a filter, which is a success rather than a failure. */
        FILTERED
    }
}
