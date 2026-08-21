package com.dmp.engine;

import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitState;
import com.dmp.engine.schedule.ExternalJobPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The periodic sweep that stops a run from stalling forever.
 *
 * <p>{@link WorkerLoop} only ever reacts to something it is doing: a chunk finishing triggers the
 * check for whether the run is done. That covers the happy path and nothing else. Three situations
 * leave a run with nothing in flight and therefore nothing to trigger it again:
 *
 * <ul>
 *   <li>A pod died mid-chunk. The chunk stays RUNNING behind a lease nobody renews, and no other
 *       pod may claim it because it is not PENDING.</li>
 *   <li>A run was stopped. Only RUNNING runs are offered work, so its unclaimed chunks will never
 *       be picked up — and treating them as outstanding leaves the run in STOPPING forever.</li>
 *   <li>The slot counter drifted. Each leak permanently reduces a run's concurrency; on a strictly
 *       sequential run a single leaked slot means no pod can ever reserve again.</li>
 * </ul>
 *
 * <p>Every one of those is invisible from inside a worker's own control flow, because the worker
 * that would have noticed is precisely the one that is gone. It takes an outside observer on a
 * timer, which is this.
 *
 * <p>Runs on every worker pod rather than on an elected leader. Each operation below is a
 * conditional transition, so two pods sweeping the same run at the same instant produce one winner
 * and one no-op — the same primitive that distributes chunks. Leader election would add a failure
 * mode to remove a race that atomic updates already remove.
 */
@Component
@Profile({"worker", "all", "default"})
public class RunReaper {

    private static final Logger log = LoggerFactory.getLogger(RunReaper.class);

    /** Bounded so one sweep cannot monopolise the scheduler after a large-scale pod failure. */
    private static final int BATCH_SIZE = 200;

    /**
     * How many consecutive sweeps must agree before the slot counter is rewritten.
     *
     * <p>A worker reserves a slot and then claims a chunk, so for a few milliseconds the counter
     * legitimately exceeds the number of RUNNING chunks. Reconciling on a single observation would
     * mistake that window for a leak and hand out a slot that is already taken. Requiring the same
     * disagreement across two sweeps means the discrepancy must persist for the whole sweep
     * interval, which the reserve-then-claim window cannot.
     */
    private static final int CONSECUTIVE_MISMATCHES_BEFORE_RECONCILE = 2;

    /** States in which a run can still be waiting on something. */
    private static final Set<RunState> UNSETTLED =
            Set.of(RunState.RUNNING, RunState.STOPPING, RunState.PAUSED, RunState.PREPARING);

    private final RunRepository runs;
    private final SplitRepository splits;
    private final RunPlanner planner;
    private final RunOrchestrator orchestrator;
    private final ExternalJobPoller externalJobs;
    private final Duration externalWaitGrace;
    private final Clock clock;

    /** Per-run count of consecutive sweeps that saw the slot counter disagree with reality. */
    private final Map<RunId, Integer> slotMismatches = new ConcurrentHashMap<>();

    public RunReaper(RunRepository runs,
                     SplitRepository splits,
                     RunPlanner planner,
                     RunOrchestrator orchestrator,
                     ExternalJobPoller externalJobs,
                     @Value("${dmp.engine.reaper.external-wait-grace:PT2M}") Duration externalWaitGrace,
                     Clock clock) {
        this.runs = runs;
        this.splits = splits;
        this.planner = planner;
        this.orchestrator = orchestrator;
        this.externalJobs = externalJobs;
        this.externalWaitGrace = externalWaitGrace;
        this.clock = clock;
    }

    /**
     * One sweep.
     *
     * <p>{@code fixedDelay}, not {@code fixedRate}: a slow sweep must not overlap itself. The
     * interval is a floor on how long a stall can last, so it wants to be short relative to a chunk
     * lease and long relative to a claim.
     */
    @Scheduled(fixedDelayString = "${dmp.engine.reaper.interval:PT30S}",
            initialDelayString = "${dmp.engine.reaper.initial-delay:PT20S}")
    public void sweep() {
        try {
            reclaimExpiredLeases();
        } catch (Exception e) {
            log.error("Lease reclaim failed; will retry next sweep", e);
        }
        try {
            pollOverdueExternalJobs();
        } catch (Exception e) {
            log.error("The overdue external-job sweep failed; will retry next sweep", e);
        }
        try {
            settleRuns();
        } catch (Exception e) {
            log.error("Run settlement failed; will retry next sweep", e);
        }
    }

    /**
     * Polls parked chunks whose status check is overdue.
     *
     * <p>The fallback the user asked for behind Quartz, and it earns its place by the shape of the
     * failure it covers. Quartz's clustered store means a trigger survives the node that armed it,
     * so this should never find anything — but "should never" is doing a lot of work there, and if
     * it is wrong the symptom is silence: a chunk nobody asks about waits for ever, its run stays
     * open for ever, and the console shows a migration that looks like it is simply taking a while.
     * A sweep that costs one indexed query every thirty seconds is a cheap second opinion.
     *
     * <p>The grace period keeps this out of Quartz's way. A trigger due two seconds ago is a
     * trigger about to fire, not a lost one, and polling it here would spend an org's API quota
     * racing a mechanism that was going to work.
     */
    private void pollOverdueExternalJobs() {
        Instant cutoff = clock.instant().minus(externalWaitGrace);
        List<Split> overdue = splits.findDueExternalWaits(cutoff, BATCH_SIZE);
        if (overdue.isEmpty()) {
            return;
        }

        log.warn("{} parked chunk(s) are past due for a status check; their scheduled checks did "
                + "not fire. Polling them here.", overdue.size());

        for (Split split : overdue) {
            try {
                externalJobs.poll(split);
            } catch (Exception e) {
                log.error("Could not poll the remote job for chunk {} of run {}",
                        split.index(), split.runId(), e);
            }
        }
    }

    /**
     * Returns chunks whose worker stopped renewing its lease.
     *
     * <p>The lease is what makes a pod crash recoverable without pods talking to each other. It is
     * only worth anything if something acts when it lapses — otherwise a chunk sits RUNNING behind
     * a dead worker and the run never finishes.
     *
     * <p>Reclaiming goes through FAILED rather than straight back to PENDING so the attempt counter
     * advances. Without that, a chunk that reliably wedges its worker would be retried forever.
     */
    private void reclaimExpiredLeases() {
        Instant now = clock.instant();
        List<Split> expired = splits.findExpiredLeases(now, BATCH_SIZE);
        if (expired.isEmpty()) {
            return;
        }

        for (Split split : expired) {
            Run run = runs.findById(split.tenantId(), split.runId()).orElse(null);
            if (run == null || run.state().isTerminal()) {
                continue;
            }

            // Conditional on the split still being RUNNING. The original worker may have been
            // slow rather than dead and finished between the query and here; it wins, and this
            // becomes a no-op rather than corrupting its result.
            Split failed = splits.transitionState(split.tenantId(), split.id(), SplitState.RUNNING,
                            split.fail("LEASE_EXPIRED",
                                    "Worker " + split.worker().orElse("unknown")
                                            + " stopped renewing its lease on chunk " + split.index(),
                                    now))
                    .orElse(null);
            if (failed == null) {
                continue;
            }

            // The dead worker still holds its slot. Nothing else will ever release it.
            runs.releaseSlot(split.tenantId(), split.runId());

            int maxAttempts = maxAttempts(run);
            if (failed.hasExhaustedAttempts(maxAttempts)) {
                splits.transitionState(split.tenantId(), split.id(), SplitState.FAILED,
                        failed.abandon(now));
                log.warn("Chunk {} of run {} abandoned after {} attempt(s); last worker was {}",
                        split.index(), split.runId(), failed.attemptsMade(),
                        split.worker().orElse("unknown"));
            } else {
                splits.transitionState(split.tenantId(), split.id(), SplitState.FAILED,
                        failed.scheduleRetry(now));
                log.info("Reclaimed chunk {} of run {} from {} after lease expiry; attempt {} of {}",
                        split.index(), split.runId(), split.worker().orElse("unknown"),
                        failed.attemptsMade(), maxAttempts);
            }
        }
    }

    /**
     * Gives every unsettled run a chance to finish, and repairs its slot counter.
     *
     * <p>The chunk-completion path already calls {@code completeIfFinished}. This exists for runs
     * where that path will never fire again — every chunk already terminal, or every remaining one
     * cancelled by a stop.
     */
    private void settleRuns() {
        List<Run> unsettled = runs.findByStates(UNSETTLED, BATCH_SIZE);
        if (unsettled.isEmpty()) {
            slotMismatches.clear();
            return;
        }

        for (Run run : unsettled) {
            try {
                reconcileSlotsIfPersistentlyWrong(run);
                orchestrator.completeIfFinished(run);
            } catch (Exception e) {
                log.error("Could not settle run {}", run.id(), e);
            }
        }

        slotMismatches.keySet().retainAll(unsettled.stream().map(Run::id).toList());
    }

    private void reconcileSlotsIfPersistentlyWrong(Run run) {
        int actual = (int) splits.countRunning(run.tenantId(), run.id());
        if (run.activeSlots() == actual) {
            slotMismatches.remove(run.id());
            return;
        }

        int seen = slotMismatches.merge(run.id(), 1, Integer::sum);
        if (seen < CONSECUTIVE_MISMATCHES_BEFORE_RECONCILE) {
            return;
        }

        runs.reconcileSlots(run.tenantId(), run.id(), actual);
        slotMismatches.remove(run.id());
        log.warn("Run {} slot counter said {} but {} chunk(s) are running; corrected",
                run.id(), run.activeSlots(), actual);
    }

    private int maxAttempts(Run run) {
        try {
            return planner.resolve(run).execution().maxAttemptsPerChunk();
        } catch (Exception e) {
            // The pipeline version is unreadable — deleted connector, bad config. One attempt is
            // the safe reading: retrying work whose definition cannot be loaded will not succeed.
            log.warn("Could not resolve retry budget for run {}; treating the chunk as final", run.id());
            return 1;
        }
    }
}
