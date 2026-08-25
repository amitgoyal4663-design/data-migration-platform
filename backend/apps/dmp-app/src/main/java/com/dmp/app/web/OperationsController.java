package com.dmp.app.web;

import com.dmp.app.web.dto.RunDtos;
import com.dmp.application.service.OperationsDashboard;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** The support team's daily screen. */
@RestController
@RequestMapping("/api/v1/operations")
@Tag(name = "Operations", description = "Is anything wrong with the pipelines being watched")
public class OperationsController {

    private final OperationsDashboard dashboard;
    private final Clock clock;

    public OperationsController(OperationsDashboard dashboard, Clock clock) {
        this.dashboard = dashboard;
        this.clock = clock;
    }

    @GetMapping("/dashboard")
    @Operation(summary = "Every watched pipeline, judged against its own history",
            description = """
                    Answers the question somebody actually arrives with — "is anything wrong?" —
                    which a run list cannot, because a number is only readable next to what that
                    same pipeline usually does. Five thousand records is healthy for one and a
                    catastrophe for another.

                    The comparison is a median of the last ten completed runs and a wide band
                    around it. Deliberately arithmetic anybody can restate: the first question
                    after a flag is "why is that abnormal", and a judgement nobody on the support
                    desk can explain gets ignored within a fortnight — taking the real alerts with
                    it.

                    Nothing is stored. Computed on demand, so there is no second copy of the truth
                    to drift from the runs it describes.
                    """)
    public DashboardResponse dashboard(
            @Parameter(description = "How far back a run still counts as recent, in hours")
            @RequestParam(defaultValue = "24") int hours,
            @Parameter(description = "The watchlist only, or every pipeline that has been published")
            @RequestParam(defaultValue = "true") boolean watched) {

        var data = dashboard.dashboard(Duration.ofHours(Math.clamp(hours, 1, 24 * 30)), watched);
        var t = data.totals();

        return new DashboardResponse(
                data.pipelines().stream()
                        .map(entry -> PipelineHealthResponse.from(entry, clock)).toList(),
                data.pipelines().size(),
                (int) data.pipelines().stream()
                        .filter(OperationsDashboard.PipelineHealth::healthy).count(),
                data.live().stream().map(l -> new LiveResponse(l.runId(), l.pipeline(), l.state(),
                        l.progress(), l.recordsRead(), l.recordsWritten(), l.seconds())).toList(),
                new TotalsResponse(t.completed(), t.failed(), t.recordsRead(), t.recordsWritten(),
                        t.recordsFailed(), t.running()),
                data.headlines().stream().map(h -> new HeadlineResponse(h.severity().name(),
                        h.headline(), h.detail(), h.pipelineId(), h.runId())).toList(),
                data.generatedAt());
    }

    @Schema(name = "OperationsDashboard")
    public record DashboardResponse(
            @Schema(description = "Worst first — a screen read every morning is scanned from the top")
            List<PipelineHealthResponse> pipelines,
            int watched,
            int healthy,
            @Schema(description = "Runs actually in flight, for the progress bars. Excludes "
                    + "paused runs — they hold a slot but nothing is happening.")
            List<LiveResponse> live,
            @Schema(description = "Every run in the window, watched or not, so the headline "
                    + "figures describe the platform rather than the watchlist")
            TotalsResponse totals,
            @Schema(description = "The screen in sentences, biggest first. A figure has to be "
                    + "interpreted before it means anything; a headline has already done that.")
            List<HeadlineResponse> headlines,
            Instant generatedAt) {
    }

    @Schema(name = "OperationsHeadline")
    public record HeadlineResponse(String severity, String headline, String detail,
                                   String pipelineId, String runId) {
    }

    @Schema(name = "OperationsLiveRun")
    public record LiveResponse(String runId, String pipeline, String state,
                               @Schema(description = "0 to 1; null before planning finishes")
                               Double progress, long recordsRead, long recordsWritten,
                               long seconds) {
    }

    @Schema(name = "OperationsTotals")
    public record TotalsResponse(long completed, long failed, long recordsRead, long recordsWritten,
                                 long recordsFailed, int running) {
    }

    @Schema(name = "PipelineHealth")
    public record PipelineHealthResponse(
            String pipelineId,
            String name,
            @Schema(description = "On the support desk's watchlist. False for a pipeline that "
                    + "only appears because the scope was widened to every published one.")
            boolean watched,
            @Schema(description = "The most recent real run. Null for a pipeline that has never "
                    + "run, which is itself worth seeing on a watchlist.")
            RunDtos.Response latest,
            @Schema(description = "Median records read over recent completed runs. Null until "
                    + "there are enough runs to say anything.")
            Long typicalRows,
            Long typicalSeconds,
            @Schema(description = "How many runs the comparison rests on. Shown, because a "
                    + "judgement from three runs and one from ten deserve different belief.")
            int baselineRuns,
            @Schema(description = "INFO, WARNING or CRITICAL")
            String worst,
            boolean healthy,
            List<FindingResponse> findings,
            @Schema(description = "Why records failed on the last run, biggest first. Brought up "
                    + "from the run because \"222 failed\" makes somebody open it, while "
                    + "\"180 Policy_Number__c is required\" already says whose problem it is.")
            List<ReasonResponse> reasons,
            @Schema(description = "The last seven runs, newest first, for the trend beside today")
            List<AttemptResponse> trend,
            ScheduleResponse schedule,
            @Schema(description = "What this pipeline moved across the whole window, not on its "
                    + "last run — the figure a product team asks for, which no single run holds")
            VolumeResponse volume) {

        static PipelineHealthResponse from(OperationsDashboard.PipelineHealth health, Clock clock) {
            return new PipelineHealthResponse(
                    health.pipelineId(),
                    health.name(),
                    health.watched(),
                    health.latest() == null
                            ? null : RunDtos.Response.from(health.latest(), clock.instant()),
                    health.typicalRows(),
                    health.typicalSeconds(),
                    health.baselineRuns(),
                    health.worst().name(),
                    health.healthy(),
                    health.findings().stream().map(FindingResponse::from).toList(),
                    health.reasons().stream()
                            .map(r -> new ReasonResponse(r.count(), r.code(), r.reason())).toList(),
                    health.trend().stream()
                            .map(a -> new AttemptResponse(a.runId(), a.state(), a.at(), a.read(),
                                    a.written(), a.failed(), a.seconds())).toList(),
                    health.schedule() == null ? null : new ScheduleResponse(
                            health.schedule().name(), health.schedule().cron(),
                            health.schedule().timezone(), health.schedule().lastFiredAt(),
                            health.schedule().nextDueAt()),
                    new VolumeResponse(health.volume().runs(), health.volume().completed(),
                            health.volume().failed(), health.volume().read(),
                            health.volume().written(), health.volume().recordsFailed(),
                            health.volume().seconds(), health.volume().successRate()));
        }
    }

    @Schema(name = "OperationsVolume")
    public record VolumeResponse(int runs, int completed, int failed, long read, long written,
                                 long recordsFailed, long seconds,
                                 @Schema(description = "Of the runs that ended, the share that "
                                         + "ended cleanly. Null when none has ended yet.")
                                 Double successRate) {
    }

    @Schema(name = "OperationsFailureReason")
    public record ReasonResponse(long count, String code, String reason) {
    }

    @Schema(name = "OperationsAttempt")
    public record AttemptResponse(String runId, String state, Instant at, long read, long written,
                                  long failed, long seconds) {
    }

    @Schema(name = "OperationsSchedule")
    public record ScheduleResponse(String name, String cron, String timezone, Instant lastFiredAt,
                                   @Schema(description = "So \"it has not run\" reads as late or "
                                           + "as simply not due yet")
                                   Instant nextDueAt) {
    }

    @Schema(name = "OperationsFinding")
    public record FindingResponse(String severity, String code, String message, String detail) {

        static FindingResponse from(OperationsDashboard.Finding finding) {
            return new FindingResponse(finding.severity().name(), finding.code(),
                    finding.message(), finding.detail());
        }
    }
}
