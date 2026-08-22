package com.dmp.app.web.dto;

import com.dmp.domain.pipeline.Pipeline;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Set;

/**
 * Web contract for pipelines.
 *
 * <p>Separate from the domain aggregate. Returning {@link Pipeline} directly would make the API
 * shape a consequence of the domain model, so that any internal refactor became a breaking change
 * for the console and for every integration built against it. The DTO is where the public contract
 * is decided and where it can be held stable independently.
 */
public final class PipelineDtos {

    private PipelineDtos() {
    }

    @Schema(name = "PipelineResponse", description = "A pipeline and its version pointers")
    public record Response(
            String id,
            String name,
            String description,
            String folder,
            Set<String> tags,
            String status,
            @Schema(description = "On the support team's daily operations dashboard")
            boolean monitored,
            @Schema(description = "Version currently published and runnable; null if never published")
            Integer publishedVersion,
            @Schema(description = "Highest version number allocated, published or not")
            int latestVersion,
            @Schema(description = "Whether a run can be started right now")
            boolean runnable,
            Instant createdAt,
            Instant updatedAt) {

        public static Response from(Pipeline pipeline) {
            return new Response(
                    pipeline.id().toString(),
                    pipeline.name(),
                    pipeline.description(),
                    pipeline.folder(),
                    pipeline.tags(),
                    pipeline.status().name(),
                    pipeline.monitored(),
                    pipeline.publishedVersion(),
                    pipeline.latestVersion(),
                    pipeline.isRunnable(),
                    pipeline.createdAt(),
                    pipeline.updatedAt());
        }
    }

    @Schema(name = "CreatePipelineRequest")
    public record CreateRequest(
            @NotBlank(message = "Name is required")
            @Size(max = 255, message = "Name must not exceed 255 characters")
            String name,

            @Size(max = 4000, message = "Description must not exceed 4000 characters")
            String description,

            @Schema(description = "Slash-separated path, for example /finance/daily", example = "/finance/daily")
            String folder,

            @Schema(description = "Lowercase tags; normalised by the server")
            Set<String> tags) {
    }

    @Schema(name = "UpdatePipelineRequest")
    public record UpdateRequest(
            @NotBlank(message = "Name is required")
            @Size(max = 255, message = "Name must not exceed 255 characters")
            String name,

            @Size(max = 4000, message = "Description must not exceed 4000 characters")
            String description,

            String folder,

            Set<String> tags) {
    }
}
