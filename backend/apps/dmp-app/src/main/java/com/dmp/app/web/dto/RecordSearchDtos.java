package com.dmp.app.web.dto;

import com.dmp.application.port.out.RecordIndexPort;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/** Web contract for the record index. */
public final class RecordSearchDtos {

    private RecordSearchDtos() {
    }

    @Schema(name = "RecordIndexResponse",
            description = "What one run did with one record. No record content — the index holds "
                    + "identities only. A rejected record's payload is on the run's Failures tab.")
    public record Response(
            @Schema(description = "The source's own identifier, exactly as it reported it. Null "
                    + "where the source has no key — the entry is still here, and still counts.",
                    nullable = true)
            String recordKey,
            String pipelineId,
            String runId,
            String chunkId,
            @Schema(description = "The read → transform → write cycle that carried this record. "
                    + "The same value appears on the stage log's entries, which is what lets a "
                    + "record be traced to the query that fetched it and the call that wrote it.")
            String traceId,
            @Schema(description = "Which record of the chunk this was, from 1. Together with "
                    + "chunkId and ordinal it identifies the entry — recordKey does not, because "
                    + "a source may hold the same key twice.")
            long seq,
            @Schema(description = "Which output of that record this was, from 0. Above zero only "
                    + "where a transform turned one record into several.")
            int ordinal,
            @Schema(description = "WRITTEN, SENT, REJECTED, FILTERED or TRANSFORM_FAILED. FILTERED "
                    + "means a transform dropped it on purpose, which is a success rather than a "
                    + "loss; TRANSFORM_FAILED means a script threw on it, which is not.")
            String outcome,
            @Schema(description = "The destination's own code when it refused the record")
            String errorCode,

            @Schema(description = "The record as the platform handled it, redacted and size-capped "
                    + "by the pipeline's audit policy. Null when that policy does not index "
                    + "payloads — which is a policy decision, not a missing record.",
                    nullable = true)
            com.fasterxml.jackson.databind.JsonNode payload,

            @Schema(description = "The record as the source produced it, when a transform changed "
                    + "it. Null when nothing changed it — the platform does not store an identical "
                    + "copy beside every record.",
                    nullable = true)
            com.fasterxml.jackson.databind.JsonNode sourcePayload,

            @Schema(description = "What the destination or the script actually said. For a failed "
                    + "transform this is the exception's own message, which is the difference "
                    + "between knowing it failed and knowing why.",
                    nullable = true)
            String errorMessage,

            Instant occurredAt) {

        public static Response from(RecordIndexPort.RecordIndexEntry entry) {
            return new Response(
                    entry.recordKey(),
                    entry.pipelineId().toString(),
                    entry.runId().toString(),
                    entry.splitId().toString(),
                    entry.traceId(),
                    entry.seq(),
                    entry.ordinal(),
                    entry.outcome().name(),
                    entry.errorCode(),
                    entry.payload(),
                    entry.sourcePayload(),
                    entry.errorMessage(),
                    entry.occurredAt());
        }
    }
}
