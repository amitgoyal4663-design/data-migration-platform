package com.dmp.app.web.dto;

import com.dmp.domain.schedule.Schedule;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/** Wire shapes for schedules. */
public final class ScheduleDtos {

    private ScheduleDtos() {
    }

    @Schema(name = "CreateScheduleRequest")
    public record CreateRequest(
            @NotBlank String pipelineId,
            @NotBlank @Size(max = 255) String name,

            @Schema(description = "Quartz cron, six or seven fields. '0 0 3 * * ?' is daily at 03:00.",
                    example = "0 0 3 * * ?")
            @NotBlank String cronExpression,

            @Schema(description = "IANA timezone. Required — '3am' is not a moment without one.",
                    example = "Asia/Kolkata")
            @NotBlank String timezone,
            @Schema(description = "JavaScript returning the values each run is started with, e.g. \"const to = fireTime.startOf('day'); return { from: to.minus({days: 1}), to }\". Leave empty to read whatever the query says.")
            String windowScript,

            @Schema(description = "Which named query on the source connection this schedule runs. Empty means the first one declared, which is what every schedule did before this field existed.")
            String queryName,

            String description) {
    }

    @Schema(name = "UpdateScheduleRequest")
    public record UpdateRequest(
            @NotBlank @Size(max = 255) String name,
            @NotBlank String cronExpression,
            @NotBlank String timezone,
            @Schema(description = "JavaScript returning the values each run is started with, e.g. \"const to = fireTime.startOf('day'); return { from: to.minus({days: 1}), to }\". Leave empty to read whatever the query says.")
            String windowScript,
            @Schema(description = "Which named query on the source connection this schedule runs. Empty means the first one declared, which is what every schedule did before this field existed.")
            String queryName,
            String description) {
    }

    @Schema(name = "ScheduleResponse")
    /** A cron expression and a script, asked about rather than saved. */
    public record PreviewRequest(
            @NotBlank String cronExpression,
            @NotBlank String timezone,
            String windowScript) {
    }

    /** What the next few firings would cover. */
    public record WindowPreview(java.util.List<Firing> firings) {

        /**
         * One firing, and the values it would produce.
         *
         * <p>{@code error} carries a script failure for that firing rather than failing the whole
         * preview, because a script can be right for most firings and wrong for one — the first of
         * the month, a Monday — and seeing which is the entire point of looking.
         */
        public record Firing(java.time.Instant firesAt,
                             java.util.Map<String, String> parameters,
                             String error) {
        }
    }

    public record Response(
            String id,
            String pipelineId,
            String name,
            String cronExpression,
            String timezone,
            String windowScript,
            String queryName,
            boolean enabled,
            String description,

            @Schema(description = "When this rule last started a run. Null if it never has.")
            Instant lastFiredAt,

            @Schema(description = "When it fires next. Null when disabled or unregistered. Shown "
                    + "because few people can evaluate a cron expression in their head, and it "
                    + "catches a mistyped rule before it costs a missed nightly load.")
            Instant nextFireAt,

            Instant createdAt,
            Instant updatedAt) {

        public static Response from(Schedule schedule, Instant nextFireAt) {
            return new Response(
                    schedule.id().toString(),
                    schedule.pipelineId().toString(),
                    schedule.name(),
                    schedule.cronExpression(),
                    schedule.timezone().getId(),
                    schedule.windowScript(),
                    schedule.queryName(),
                    schedule.enabled(),
                    schedule.description(),
                    schedule.lastFiredAt(),
                    nextFireAt,
                    schedule.createdAt(),
                    schedule.updatedAt());
        }
    }
}
