package com.dmp.app.web;

import com.dmp.app.web.dto.ScheduleDtos;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.transform.api.WindowScript;
import org.springframework.scheduling.support.CronExpression;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.dmp.application.service.ScheduleService;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.schedule.Schedule;
import com.dmp.domain.schedule.ScheduleId;
import com.dmp.engine.schedule.ScheduleRegistrar;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/**
 * Recurring schedules for pipelines.
 *
 * <p>Creating one only records a rule. What it does when it fires is create a run and return —
 * scheduling decides <em>when</em>, the engine decides how and for how long (ADR-0010).
 */
@RestController
@RequestMapping("/api/v1/schedules")
@Tag(name = "Schedules", description = "Run a pipeline on a recurring cron schedule")
public class ScheduleController {

    private final ScheduleService schedules;

    /**
     * Optional, because the next fire time is a scheduler detail and a control plane without one
     * still manages schedule records. Absent means the field is simply null.
     */
    private final ObjectProvider<ScheduleRegistrar> registrar;

    private final WindowScript windowScript;

    public ScheduleController(ScheduleService schedules, ObjectProvider<ScheduleRegistrar> registrar,
                              WindowScript windowScript) {
        this.schedules = schedules;
        this.registrar = registrar;
        this.windowScript = windowScript;
    }

    @GetMapping
    @Operation(summary = "List schedules",
            description = "Every schedule, or those for one pipeline when pipelineId is given.")
    public List<ScheduleDtos.Response> list(@RequestParam(required = false) String pipelineId) {
        List<Schedule> found = pipelineId == null
                ? schedules.list()
                : schedules.listForPipeline(PipelineId.parse(pipelineId));

        return found.stream().map(this::withNextFire).toList();
    }

    @GetMapping("/{scheduleId}")
    public ScheduleDtos.Response get(@PathVariable String scheduleId) {
        return withNextFire(schedules.get(ScheduleId.parse(scheduleId)));
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Create a schedule",
            description = "Rejected if the pipeline has no published version — a schedule on one "
                    + "would fail every time it fired, silently, at whatever hour it was set for.")
    public ScheduleDtos.Response create(@Valid @RequestBody ScheduleDtos.CreateRequest request) {
        return withNextFire(schedules.create(
                PipelineId.parse(request.pipelineId()), request.name(), request.cronExpression(),
                request.windowScript(), request.timezone(), request.description()));
    }

    @PutMapping("/{scheduleId}")
    public ScheduleDtos.Response update(@PathVariable String scheduleId,
                                        @Valid @RequestBody ScheduleDtos.UpdateRequest request) {
        return withNextFire(schedules.update(
                ScheduleId.parse(scheduleId), request.name(), request.cronExpression(),
                request.timezone(), request.windowScript(), request.description()));
    }

    @PostMapping("/{scheduleId}/enable")
    @Operation(summary = "Resume a schedule")
    public ScheduleDtos.Response enable(@PathVariable String scheduleId) {
        return withNextFire(schedules.setEnabled(ScheduleId.parse(scheduleId), true));
    }

    @PostMapping("/{scheduleId}/disable")
    @Operation(summary = "Pause a schedule",
            description = "Keeps the rule and its history. Deleting during an incident loses both.")
    public ScheduleDtos.Response disable(@PathVariable String scheduleId) {
        return withNextFire(schedules.setEnabled(ScheduleId.parse(scheduleId), false));
    }

    @DeleteMapping("/{scheduleId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String scheduleId) {
        schedules.delete(ScheduleId.parse(scheduleId));
    }

    @PostMapping("/preview-window")
    @Operation(summary = "Show what a window script would produce",
            description = """
                    Runs the script against the next few firings of a cron expression and returns
                    the values each would produce, without saving anything or starting a run.

                    Worth checking before saving: a window script is only wrong in ways you find
                    out about the following morning, when a run has already covered the wrong day.
                    """)
    public ScheduleDtos.WindowPreview previewWindow(
            @Valid @RequestBody ScheduleDtos.PreviewRequest request) {

        ZoneId zone = ZoneId.of(request.timezone());
        CronExpression cron;
        try {
            cron = CronExpression.parse(request.cronExpression());
        } catch (IllegalArgumentException e) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "That is not a valid cron expression: " + e.getMessage());
        }

        List<ScheduleDtos.WindowPreview.Firing> firings = new ArrayList<>();
        ZonedDateTime at = ZonedDateTime.now(zone);

        for (int i = 0; i < 4; i++) {
            at = cron.next(at);
            if (at == null) {
                break;
            }
            // Each firing is evaluated independently, exactly as it will be in production — one
            // that would throw shows as a failure against its own time rather than hiding the
            // three that worked.
            try {
                firings.add(new ScheduleDtos.WindowPreview.Firing(at.toInstant(),
                        windowScript.evaluate(request.windowScript(), at.toInstant(), zone), null));
            } catch (RuntimeException e) {
                firings.add(new ScheduleDtos.WindowPreview.Firing(
                        at.toInstant(), Map.of(), e.getMessage()));
            }
        }
        return new ScheduleDtos.WindowPreview(firings);
    }

    private ScheduleDtos.Response withNextFire(Schedule schedule) {
        Instant next = registrar.getIfAvailable() == null || !schedule.enabled()
                ? null
                : registrar.getObject().nextFireTime(schedule.id()).orElse(null);
        return ScheduleDtos.Response.from(schedule, next);
    }
}
