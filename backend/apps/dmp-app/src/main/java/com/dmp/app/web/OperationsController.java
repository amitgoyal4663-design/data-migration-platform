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
            @RequestParam(defaultValue = "24") int hours) {

        List<OperationsDashboard.PipelineHealth> health =
                dashboard.today(Duration.ofHours(Math.clamp(hours, 1, 24 * 30)));

        return new DashboardResponse(
                health.stream().map(entry -> PipelineHealthResponse.from(entry, clock)).toList(),
                health.size(),
                (int) health.stream().filter(OperationsDashboard.PipelineHealth::healthy).count(),
                clock.instant());
    }

    @Schema(name = "OperationsDashboard")
    public record DashboardResponse(
            @Schema(description = "Worst first — a screen read every morning is scanned from the top")
            List<PipelineHealthResponse> pipelines,
            int watched,
            int healthy,
            Instant generatedAt) {
    }

    @Schema(name = "PipelineHealth")
    public record PipelineHealthResponse(
            String pipelineId,
            String name,
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
            List<FindingResponse> findings) {

        static PipelineHealthResponse from(OperationsDashboard.PipelineHealth health, Clock clock) {
            return new PipelineHealthResponse(
                    health.pipelineId(),
                    health.name(),
                    health.latest() == null
                            ? null : RunDtos.Response.from(health.latest(), clock.instant()),
                    health.typicalRows(),
                    health.typicalSeconds(),
                    health.baselineRuns(),
                    health.worst().name(),
                    health.healthy(),
                    health.findings().stream().map(FindingResponse::from).toList());
        }
    }

    @Schema(name = "OperationsFinding")
    public record FindingResponse(String severity, String code, String message, String detail) {

        static FindingResponse from(OperationsDashboard.Finding finding) {
            return new FindingResponse(finding.severity().name(), finding.code(),
                    finding.message(), finding.detail());
        }
    }
}
