package com.dmp.app.web.dto;

import com.dmp.application.port.out.StageLogPort;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/** Web contract for the stage log. */
public final class StageLogDtos {

    private StageLogDtos() {
    }

    @Schema(name = "StageLogResponse",
            description = "One thing the platform did — a window of reading, a pass of the "
                    + "transforms, or a call to the destination.")
    public record Response(
            String runId,
            String chunkId,
            @Schema(description = "The one read → transform → write cycle this belongs to. "
                    + "Formatted <chunkId>#<cycle>. Every record the cycle carried is stamped "
                    + "with the same value, which is what joins this log to the record index.")
            String traceId,
            @Schema(description = "FETCH, READ, TRANSFORM or WRITE")
            String stage,
            String nodeId,
            String nodeName,
            String connectorType,
            @Schema(description = "Position within the chunk for this stage alone — the third "
                    + "read, the third write. Not comparable across stages.")
            int sequence,
            @Schema(description = "Position among all of this chunk's entries, and what the log is "
                    + "ordered by. Sorting on sequence interleaved the stages of a chunk fast "
                    + "enough to put several in one millisecond.")
            int position,
            @Schema(description = "The chunk's attempt, so a retry's entries are distinguishable")
            int attempt,
            int recordsIn,
            @Schema(description = "Differs from recordsIn only at TRANSFORM, where a filter drops "
                    + "and a splitter multiplies — and where that difference is the whole reason "
                    + "the stage is worth logging.")
            int recordsOut,
            long bytes,
            @Schema(description = "Wall-clock time. Where a slow stage shows up.")
            long durationMs,
            @Schema(description = "Whether the stage succeeded — not whether its records did. A "
                    + "write the destination accepted while refusing half the records inside it "
                    + "is OK here; those refusals are on the run's Failures tab.")
            String outcome,
            String errorCode,
            String errorMessage,
            @Schema(description = "READ only: the query the connector ran", nullable = true)
            String query,
            JsonNode cursorIn,
            JsonNode cursorOut,
            @Schema(description = "Whatever the connector could report that the counts cannot",
                    nullable = true)
            JsonNode details,
            @Schema(description = "What was sent, redacted and capped. Null unless the pipeline "
                    + "captures bodies.", nullable = true)
            JsonNode request,
            @Schema(description = "What came back, redacted and capped. Null unless the pipeline "
                    + "captures bodies.", nullable = true)
            JsonNode response,
            Instant occurredAt) {

        public static Response from(StageLogPort.StageEntry entry) {
            return new Response(
                    entry.runId().toString(),
                    entry.splitId().toString(),
                    entry.traceId(),
                    entry.stage().name(),
                    entry.nodeId(),
                    entry.nodeName(),
                    entry.connectorType(),
                    entry.sequence(),
                    entry.position(),
                    entry.attempt(),
                    entry.recordsIn(),
                    entry.recordsOut(),
                    entry.bytes(),
                    entry.durationMs(),
                    entry.outcome().name(),
                    entry.errorCode(),
                    entry.errorMessage(),
                    entry.query(),
                    entry.cursorIn(),
                    entry.cursorOut(),
                    entry.details(),
                    entry.request(),
                    entry.response(),
                    entry.occurredAt());
        }
    }
}
