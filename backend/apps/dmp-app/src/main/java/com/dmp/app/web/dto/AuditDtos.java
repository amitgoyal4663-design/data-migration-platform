package com.dmp.app.web.dto;

import com.dmp.domain.audit.AuditEntry;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/** Web contract for the control-plane audit trail. */
public final class AuditDtos {

    private AuditDtos() {
    }

    @Schema(name = "AuditEntryResponse",
            description = "One recorded change to a definition, or one run-lifecycle command.")
    public record Response(
            String id,
            Instant occurredAt,
            @Schema(description = "Who did it. A system identity such as 'system:scheduler' for "
                    + "anything the platform initiated on its own.")
            String actor,
            @Schema(description = "CREATE, UPDATE, DELETE, ARCHIVE, RESTORE, PUBLISH, ROLLBACK, "
                    + "TEST_CONNECTION, ENABLE, DISABLE, RUN_START, RUN_PAUSE, RUN_RESUME, "
                    + "RUN_STOP or SCHEDULE_CHANGE")
            String action,
            @Schema(description = "pipeline, version, connector, schedule or run")
            String resourceType,
            String resourceId,
            String summary,
            @Schema(description = "The resource's JSON state before the change; null for a create.")
            JsonNode before,
            @Schema(description = "Its state after; null for a delete.")
            JsonNode after,
            String requestId,
            String sourceIp) {

        public static Response from(AuditEntry entry) {
            return new Response(
                    entry.id().toString(),
                    entry.occurredAt(),
                    entry.actor(),
                    entry.action().name(),
                    entry.resourceType(),
                    entry.resourceId(),
                    entry.summary(),
                    entry.before(),
                    entry.after(),
                    entry.requestId(),
                    entry.sourceIp());
        }
    }
}
