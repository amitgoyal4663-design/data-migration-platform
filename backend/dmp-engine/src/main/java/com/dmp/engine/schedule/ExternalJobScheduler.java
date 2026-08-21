package com.dmp.engine.schedule;

import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitId;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Date;

/**
 * Arms a one-shot Quartz trigger to ask a destination whether a parked chunk's job has finished.
 *
 * <p>This is the piece that replaced a worker sleeping in a loop. The old shape held a virtual
 * thread, a concurrency permit and one of the run's slots for the entire life of a Salesforce bulk
 * job — minutes, for a job the worker contributed nothing to — and it did all that on a single pod,
 * so the pod dying took the only knowledge of the job with it.
 *
 * <p>Quartz was chosen over a timer or a polling loop for one reason: its JDBC job store is
 * clustered, so the trigger lives in Postgres rather than in the memory of whichever pod happened
 * to submit the job. The node that arms a trigger need not be the node that fires it, and if that
 * node is gone when the moment comes, another one fires it instead. That is the whole requirement.
 *
 * <p>{@code requestRecovery} covers the narrower case of a node dying <em>during</em> the poll:
 * Quartz re-fires the job elsewhere once the cluster notices, rather than losing the firing.
 *
 * <p>None of this is trusted blindly. {@code RunReaper} sweeps for parked chunks whose poll is
 * overdue, because the failure mode here is silent — a chunk nobody ever asks about waits for ever,
 * holds its run open for ever, and looks exactly like a chunk that is merely taking a while.
 */
@Component
public class ExternalJobScheduler {

    private static final Logger log = LoggerFactory.getLogger(ExternalJobScheduler.class);

    static final String GROUP = "dmp-external-jobs";
    static final String TENANT_ID = "tenantId";
    static final String SPLIT_ID = "splitId";

    private final Scheduler scheduler;

    public ExternalJobScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Schedules the next status check for a parked chunk, replacing any check already scheduled.
     *
     * <p>Never throws. A trigger that cannot be armed is a degraded poll, not a lost chunk — the
     * reaper's sweep finds it by its overdue {@code dueAt} and polls it anyway. Failing the caller
     * here would instead fail the chunk, which is a far worse answer to "Quartz is briefly unwell".
     */
    public void pollAt(Split split, Instant when) {
        JobKey key = keyOf(split.id());
        try {
            JobDetail job = JobBuilder.newJob(CheckExternalJobJob.class)
                    .withIdentity(key)
                    .withDescription("Chunk " + split.index() + " of run " + split.runId())
                    .usingJobData(TENANT_ID, split.tenantId().toString())
                    .usingJobData(SPLIT_ID, split.id().toString())
                    // Re-fire on another node if the one running this poll dies mid-call.
                    .requestRecovery(true)
                    .build();

            Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity(key.getName(), GROUP)
                    .forJob(key)
                    .startAt(Date.from(when))
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                            // Fire late rather than not at all. The opposite instruction is right
                            // for a nightly schedule — a missed 03:00 load should not stampede on
                            // recovery — but a missed poll here means a chunk waits for the reaper,
                            // so a late one is strictly better than a skipped one.
                            .withMisfireHandlingInstructionFireNow())
                    .build();

            scheduler.scheduleJob(job, java.util.Set.of(trigger), true);

        } catch (SchedulerException e) {
            log.warn("Could not schedule the status check for chunk {} of run {}; the reaper will "
                            + "pick it up once its poll is overdue",
                    split.index(), split.runId(), e);
        }
    }

    /**
     * Drops a chunk's pending status check.
     *
     * <p>Called once the chunk stops being parked. Left behind, the trigger would fire against a
     * chunk that has already moved on — harmless, because the poller re-reads the split and finds
     * it is no longer waiting, but it is one more thing firing for no reason.
     */
    public void cancel(SplitId splitId) {
        try {
            scheduler.deleteJob(keyOf(splitId));
        } catch (SchedulerException e) {
            log.debug("Could not cancel the status check for split {}", splitId, e);
        }
    }

    private static JobKey keyOf(SplitId splitId) {
        return new JobKey(splitId.toString(), GROUP);
    }
}
