package com.dmp.engine.schedule;

import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.ScheduleRepository;
import com.dmp.common.json.Json;
import com.dmp.domain.run.Run;
import com.dmp.transform.api.WindowScript;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import com.dmp.domain.run.RunTrigger;
import com.dmp.domain.schedule.Schedule;
import com.dmp.domain.schedule.ScheduleId;
import com.dmp.engine.RunOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

/**
 * What a schedule does when it fires: create a run, and stop.
 *
 * <p><b>It must never execute a migration.</b> Quartz's thread pool is sized for scheduling — five
 * threads here. Running a six-hour migration on one would occupy it for six hours, exhaust the
 * pool, block every other schedule in the deployment and trigger misfires across unrelated
 * pipelines. The scheduler decides <em>when</em>; the data plane decides how and for how long
 * (ADR-0010).
 *
 * <p>So the body is one insert. The run is created in CREATED state and a worker's pull loop picks
 * it up on its next pass, exactly as for a run started from the console. Nothing here waits.
 *
 * <p>ADR-0010 originally had this publish a start command to Kafka. That was written while Kafka
 * was still on the engine's critical path; ADR-0013 and ADR-0014 moved work distribution to
 * pull-based claiming in MongoDB, so publishing a command would reintroduce a broker dependency for
 * something a single insert already achieves.
 *
 * <p>Separate from the Quartz {@code Job} on purpose. Quartz constructs job classes reflectively
 * through a no-arg constructor, which rules out constructor injection; keeping the logic in an
 * ordinary bean means it is injected and unit-tested like everything else, and the job is a
 * three-line adapter.
 */
@Component
public class ScheduledRunStarter {

    private static final Logger log = LoggerFactory.getLogger(ScheduledRunStarter.class);

    private final ScheduleRepository schedules;
    private final RunOrchestrator orchestrator;
    private final TenantContext tenantContext;
    private final WindowScript windowScript;
    private final Clock clock;

    public ScheduledRunStarter(ScheduleRepository schedules,
                               RunOrchestrator orchestrator,
                               TenantContext tenantContext,
                               WindowScript windowScript,
                               Clock clock) {
        this.schedules = schedules;
        this.orchestrator = orchestrator;
        this.tenantContext = tenantContext;
        this.windowScript = windowScript;
        this.clock = clock;
    }

    /**
     * Starts a run for a schedule.
     *
     * <p>Never throws. A pipeline whose version was unpublished, or a database blip, must not put
     * the trigger into an error state and stop every future firing — the next occurrence should
     * simply try again.
     *
     * @param scheduledFor the instant the trigger was <em>due</em>, not when it ran
     */
    public void start(ScheduleId scheduleId, Instant scheduledFor) {
        Optional<Schedule> live = schedules.findAllEnabled().stream()
                .filter(candidate -> candidate.id().equals(scheduleId))
                .findFirst();

        if (live.isEmpty()) {
            // Re-read rather than trusting the copy Quartz has held since registration: a rule
            // disabled an hour ago must not fire.
            log.info("Schedule {} fired but is disabled or deleted; not starting a run", scheduleId);
            return;
        }
        Schedule schedule = live.get();

        try {
            tenantContext.runAs(schedule.tenantId(), "system:scheduler", () -> {
                // Keyed on the due time, so two replicas racing on one trigger derive the same key
                // and the second is rejected by the unique index rather than duplicating a
                // migration. Clustering should prevent the race; this survives it if it happens.
                // The window is computed here, once, from the time the trigger was DUE rather
                // than the time it ran. A pod that starts twenty minutes late must still process
                // the period it was scheduled for, not one shifted by the delay.
                JsonNode parameters = windowFor(schedule, scheduledFor);

                Run run = orchestrator.start(schedule.pipelineId(), RunTrigger.SCHEDULED,
                        schedule.idempotencyKeyFor(scheduledFor), parameters);

                schedules.update(schedule.fired(clock.instant()));
                log.info("Schedule '{}' started run {} of pipeline {}",
                        schedule.name(), run.id(), schedule.pipelineId());
                return run;
            });
        } catch (Exception e) {
            log.error("Schedule '{}' could not start a run of pipeline {}: {}",
                    schedule.name(), schedule.pipelineId(), e.getMessage(), e);
        }
    }

    /**
     * Runs the schedule's window script, if it has one.
     *
     * <p>A script that throws stops this firing rather than starting a run without parameters. The
     * alternative is worse than a missed run: a query written as {@code WHERE ts > :from} with no
     * value either fails inside the connector or, if a placeholder happened to be optional, reads
     * the entire table — and doing that on a schedule, silently, is how a nightly incremental
     * becomes a nightly full load nobody notices until the bill arrives.
     */
    private JsonNode windowFor(Schedule schedule, Instant scheduledFor) {
        if (schedule.windowScript() == null || schedule.windowScript().isBlank()) {
            return Json.emptyObject();
        }

        Map<String, String> window =
                windowScript.evaluate(schedule.windowScript(), scheduledFor, schedule.timezone());

        ObjectNode parameters = Json.newObject();
        window.forEach(parameters::put);

        log.info("Schedule '{}' due at {} covers {}", schedule.name(), scheduledFor, window);
        return parameters;
    }
}
