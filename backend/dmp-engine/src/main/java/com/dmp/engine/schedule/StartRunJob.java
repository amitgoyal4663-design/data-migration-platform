package com.dmp.engine.schedule;

import com.dmp.domain.schedule.ScheduleId;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;

/**
 * The Quartz entry point for a schedule.
 *
 * <p>Deliberately three lines of logic. Quartz constructs this reflectively through a no-arg
 * constructor and then autowires it, which rules out constructor injection — so everything worth
 * testing lives in {@link ScheduledRunStarter}, which is an ordinary bean, and this only translates
 * a firing into a call.
 *
 * <p>{@code @DisallowConcurrentExecution} because a schedule that fires again while its previous
 * firing is still working should wait rather than start a second run of the same pipeline. Runs are
 * idempotent by key, but two firings a second apart would otherwise produce two runs.
 */
@DisallowConcurrentExecution
public class StartRunJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(StartRunJob.class);

    /** Key under which {@link ScheduleRegistrar} stores the schedule id on the job detail. */
    static final String SCHEDULE_ID = "scheduleId";

    /**
     * Field injection, uniquely in this codebase, because Quartz owns this object's construction.
     * Its one collaborator is the bean holding the actual behaviour.
     */
    @Autowired
    private ScheduledRunStarter starter;

    @Override
    public void execute(JobExecutionContext context) {
        String rawId = context.getMergedJobDataMap().getString(SCHEDULE_ID);
        if (rawId == null) {
            log.error("A scheduled job fired with no schedule id attached; ignoring it");
            return;
        }

        // The instant the trigger was due, not the instant it ran. That difference is what makes
        // the run's idempotency key stable across replicas and across a late firing.
        Instant scheduledFor = context.getScheduledFireTime() == null
                ? Instant.now()
                : context.getScheduledFireTime().toInstant();

        starter.start(ScheduleId.parse(rawId), scheduledFor);
    }
}
