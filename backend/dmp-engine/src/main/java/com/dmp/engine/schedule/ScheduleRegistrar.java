package com.dmp.engine.schedule;

import com.dmp.application.port.out.ScheduleRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.schedule.Schedule;
import com.dmp.domain.schedule.ScheduleId;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

/**
 * Keeps Quartz's triggers in step with the {@code schedule} table.
 *
 * <p>The table is the source of truth and Quartz is a projection of it. That direction matters: a
 * schedule someone edited while a replica was down must take effect when it comes back, and the
 * only way to guarantee that is to rebuild the projection from the table at startup rather than
 * trusting what is already in {@code QRTZ_TRIGGERS}.
 *
 * <p>Runs on the control plane only. A worker pod executes jobs — it must never own the schedule,
 * or scaling workers would multiply the firings.
 */
@Component
@Profile({"control-plane", "all", "default"})
public class ScheduleRegistrar {

    private static final Logger log = LoggerFactory.getLogger(ScheduleRegistrar.class);

    private static final String GROUP = "dmp-schedules";

    private final Scheduler scheduler;
    private final ScheduleRepository schedules;

    public ScheduleRegistrar(Scheduler scheduler, ScheduleRepository schedules) {
        this.scheduler = scheduler;
        this.schedules = schedules;
    }

    /**
     * Loads every enabled rule once the application is up.
     *
     * <p>Idempotent by construction: each schedule maps to one job key, and registering replaces
     * whatever was there. Several replicas doing this simultaneously converge on the same state,
     * and clustering decides which of them actually fires.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void loadSchedules() {
        try {
            int loaded = 0;
            for (Schedule schedule : schedules.findAllEnabled()) {
                register(schedule);
                loaded++;
            }
            log.info("Registered {} schedule(s) with Quartz", loaded);
        } catch (Exception e) {
            // Never fatal. A malformed rule must not stop the control plane from starting and
            // serving every other pipeline.
            log.error("Could not load schedules; the API is unaffected and they can be re-saved", e);
        }
    }

    /**
     * Creates or replaces the trigger for a schedule.
     *
     * <p>The job stores only the schedule id, not the rule. The job re-reads the schedule when it
     * fires, so a rule disabled an hour ago does not fire from a stale copy Quartz has been holding
     * since registration.
     */
    public void register(Schedule schedule) {
        requireValidCron(schedule.cronExpression());

        JobKey jobKey = keyOf(schedule.id());
        try {
            JobDetail job = JobBuilder.newJob(StartRunJob.class)
                    .withIdentity(jobKey)
                    .withDescription(schedule.name())
                    .usingJobData(StartRunJob.SCHEDULE_ID, schedule.id().toString())
                    .storeDurably()
                    .build();

            Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity(new TriggerKey(jobKey.getName(), GROUP))
                    .forJob(jobKey)
                    .withSchedule(CronScheduleBuilder
                            .cronSchedule(schedule.cronExpression())
                            .inTimeZone(TimeZone.getTimeZone(schedule.timezone()))
                            // If the control plane was down at 03:00, do not fire a burst of
                            // catch-up runs on recovery. Six backfills starting at once when a
                            // replica returns is worse than one skipped nightly load, and a
                            // deliberate catch-up is what a BACKFILL run is for (ADR-0010).
                            .withMisfireHandlingInstructionDoNothing())
                    .build();

            scheduler.scheduleJob(job, java.util.Set.of(trigger), true);
            log.info("Schedule '{}' registered: {} ({})",
                    schedule.name(), schedule.cronExpression(), schedule.timezone());

        } catch (SchedulerException e) {
            throw new DmpException(ErrorCode.INTERNAL,
                    "Could not register schedule '" + schedule.name() + "': " + e.getMessage(),
                    Map.of("scheduleId", schedule.id().toString()));
        }
    }

    /** Removes a schedule's trigger. Safe to call for one that was never registered. */
    public void unregister(ScheduleId id) {
        try {
            if (scheduler.deleteJob(keyOf(id))) {
                log.info("Schedule {} unregistered", id);
            }
        } catch (SchedulerException e) {
            throw new DmpException(ErrorCode.INTERNAL,
                    "Could not unregister schedule " + id + ": " + e.getMessage(),
                    Map.of("scheduleId", id.toString()));
        }
    }

    /**
     * When this schedule fires next.
     *
     * <p>Shown in the console because a cron expression is not something most people can evaluate
     * in their head, and "next run: tomorrow 03:00" catches a mistyped rule before it costs a
     * missed nightly load.
     */
    public Optional<Instant> nextFireTime(ScheduleId id) {
        try {
            return scheduler.getTriggersOfJob(keyOf(id)).stream()
                    .map(Trigger::getNextFireTime)
                    .filter(java.util.Objects::nonNull)
                    .map(java.util.Date::toInstant)
                    .min(Instant::compareTo);
        } catch (SchedulerException e) {
            log.debug("Could not read the next fire time for schedule {}", id, e);
            return Optional.empty();
        }
    }

    /**
     * Rejects a cron expression before it is stored.
     *
     * <p>Validated here rather than discovered when the trigger fails to build, so the user sees
     * the problem while they are looking at the field they typed it into.
     */
    public static void requireValidCron(String expression) {
        try {
            new CronExpression(expression);
        } catch (ParseException e) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "'" + expression + "' is not a valid cron expression: " + e.getMessage()
                            + ". Quartz uses six or seven fields — second, minute, hour, day of "
                            + "month, month, day of week, and optionally year. '0 0 3 * * ?' is "
                            + "every day at 03:00.",
                    Map.of("cronExpression", expression));
        }
    }

    private static JobKey keyOf(ScheduleId id) {
        return new JobKey(id.toString(), GROUP);
    }
}
