package com.dmp.app.web.dto;

import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.pipeline.DeliveryPolicy;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.PipelineDefinition;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.pipeline.ValidationResult;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.List;

/**
 * Web contract for pipeline versions.
 *
 * <p>{@link PipelineDefinition}, {@link ChunkingPolicy} and {@link AuditPolicy} are exposed
 * directly rather than mirrored into parallel DTOs. Unlike the aggregates, these are value objects
 * whose JSON form is already their persisted form and already their contract with the designer
 * canvas — three representations of the same structure would be three places to keep in step, for
 * no independence gained. The aggregates themselves are still mapped, because their shape is an
 * internal decision.
 */
public final class PipelineVersionDtos {

    private PipelineVersionDtos() {
    }

    @Schema(name = "PipelineVersionResponse")
    public record Response(
            String id,
            String pipelineId,
            int versionNumber,
            @Schema(description = "DRAFT, VALIDATED or PUBLISHED. PUBLISHED is immutable.")
            String status,
            PipelineDefinition definition,
            ChunkingPolicy chunkingPolicy,
            ExecutionPolicy executionPolicy,
            AuditPolicy auditPolicy,
            DeliveryPolicy deliveryPolicy,
            String mode,
            @Schema(description = "Transport derived from the mode: IN_PROCESS for batch, KAFKA for streaming")
            String channelType,
            String changeNote,
            String createdBy,
            Instant createdAt,
            Instant publishedAt) {

        public static Response from(PipelineVersion version) {
            return new Response(
                    version.id().toString(),
                    version.pipelineId().toString(),
                    version.versionNumber(),
                    version.status().name(),
                    version.definition(),
                    version.chunkingPolicy(),
                    version.executionPolicy(),
                    version.auditPolicy(),
                    version.deliveryPolicy(),
                    version.mode().name(),
                    version.channelType().name(),
                    version.changeNote(),
                    version.createdBy(),
                    version.createdAt(),
                    version.publishedAt());
        }
    }

    /** Summary for the version list, which does not need the whole DAG in every row. */
    @Schema(name = "PipelineVersionSummary")
    public record Summary(
            String id,
            int versionNumber,
            String status,
            String mode,
            int nodeCount,
            String changeNote,
            String createdBy,
            Instant createdAt,
            Instant publishedAt) {

        public static Summary from(PipelineVersion version) {
            return new Summary(
                    version.id().toString(),
                    version.versionNumber(),
                    version.status().name(),
                    version.mode().name(),
                    version.definition().nodes().size(),
                    version.changeNote(),
                    version.createdBy(),
                    version.createdAt(),
                    version.publishedAt());
        }
    }

    @Schema(name = "CreatePipelineVersionRequest")
    public record CreateRequest(
            @Schema(description = "The DAG. Omit to start from an empty canvas.")
            PipelineDefinition definition,

            @Schema(description = "Read and write sizing. Omit for platform defaults.")
            ChunkingPolicy chunkingPolicy,

            @Schema(description = "Fleet-wide concurrency. maxConcurrentChunks=1 is strictly "
                    + "sequential, 0 is unlimited. Omit for unlimited.")
            ExecutionPolicy executionPolicy,

            @Schema(description = "Record-level audit policy. Omit for ERRORS with 30-day retention.")
            AuditPolicy auditPolicy,

            @Schema(description = "How a batch is divided into calls on the sink: the whole batch "
                    + "(the default), one record at a time, fixed groups, or groups decided by a "
                    + "script. Separate from the batch size, which decides how much is buffered "
                    + "and how much is redone after a crash.")
            DeliveryPolicy deliveryPolicy,

            @Schema(description = "FULL_LOAD, INCREMENTAL, STREAMING or CDC. Determines the transport.")
            PipelineMode mode,

            @Size(max = 2000)
            String changeNote) {
    }

    @Schema(name = "UpdatePipelineDefinitionRequest")
    public record UpdateDefinitionRequest(PipelineDefinition definition) {
    }

    @Schema(name = "UpdatePipelinePoliciesRequest",
            description = "Any field omitted is left unchanged, so one policy can be edited "
                    + "without restating the others.")
    public record UpdatePoliciesRequest(ChunkingPolicy chunkingPolicy,
                                        ExecutionPolicy executionPolicy,
                                        @Schema(description = "What to keep about individual "
                                                + "records: the level, how many payloads to store "
                                                + "per distinct fault, the size cap on one payload, "
                                                + "how long they live, and which fields to redact "
                                                + "before anything is written down.")
                                        com.dmp.domain.audit.AuditPolicy auditPolicy,
                                        @Schema(description = "How the batch is divided into calls "
                                                + "on the sink. groupSize 0 is the whole batch, 1 "
                                                + "is one record per call, N is fixed groups; or "
                                                + "give a split script instead. Not both.")
                                        DeliveryPolicy deliveryPolicy,
                                        PipelineMode mode) {
    }

    /**
     * Validation outcome.
     *
     * <p>Warnings and errors are returned together and separately. Only errors block publication;
     * warnings exist so a half-built pipeline can be saved without the platform arguing about it.
     */
    @Schema(name = "ValidationResponse")
    public record ValidationResponse(boolean valid, List<Issue> errors, List<Issue> warnings) {

        public static ValidationResponse from(ValidationResult result) {
            return new ValidationResponse(
                    result.isValid(),
                    result.errors().stream().map(Issue::from).toList(),
                    result.warnings().stream().map(Issue::from).toList());
        }

        @Schema(name = "ValidationIssue")
        public record Issue(
                @Schema(description = "Stable machine-readable code", example = "CYCLE_DETECTED")
                String code,
                String message,
                @Schema(description = "Node to highlight on the canvas, if the issue has one")
                String nodeId,
                @Schema(description = "Edge to highlight on the canvas, if the issue has one")
                String edgeId) {

            static Issue from(com.dmp.domain.pipeline.ValidationIssue issue) {
                return new Issue(issue.code(), issue.message(), issue.nodeId(), issue.edgeId());
            }
        }
    }
}
