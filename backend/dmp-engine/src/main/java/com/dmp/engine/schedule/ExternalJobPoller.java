package com.dmp.engine.schedule;

import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.Sink;
import com.dmp.connector.runtime.ConnectorContexts;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.run.SplitState;
import com.dmp.domain.tenant.TenantId;
import com.dmp.engine.ChunkExecutor;
import com.dmp.engine.ChunkParkedException;
import com.dmp.engine.ResolvedPipeline;
import com.dmp.engine.RunPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Asks a destination whether a parked chunk's job has finished, and acts on the answer.
 *
 * <p><b>It polls; it does not settle.</b> When the job is done this hands the chunk back to the
 * pool as PENDING and stops. A worker then claims it and runs the settle path — harvest the
 * per-record rejections, release the remote job, roll the counts into the run, generate the
 * successor if the run is lazily chunked, publish the event, release the run slot. That is a dozen
 * steps of bookkeeping that already exist in exactly one place, and putting a second copy of them
 * behind a Quartz trigger would guarantee the two drift.
 *
 * <p>So the division is: Quartz owns <em>when to ask</em>, the worker owns <em>what a finished
 * chunk means</em>. This class is the join between them and does one HTTP call.
 *
 * <p>A failed job is handed back the same way, rather than being failed here. The worker's settle
 * path asks the destination again and gets the same answer, which costs one extra call and buys
 * something worth more: the retry budget, the abandon decision and the stop-the-run policy stay in
 * the one place that already implements them.
 */
@Component
public class ExternalJobPoller {

    private static final Logger log = LoggerFactory.getLogger(ExternalJobPoller.class);

    /**
     * How long to wait before trying again after the poll itself failed.
     *
     * <p>Longer than an ordinary poll interval. A failing poll usually means the org is unreachable
     * or throttling, and asking a struggling system more often is the wrong response — the job is
     * still running and will still be there in a minute.
     */
    private static final Duration RETRY_AFTER_POLL_ERROR = Duration.ofSeconds(30);

    private final SplitRepository splits;
    private final RunRepository runs;
    private final RunPlanner planner;
    private final ConnectorRegistry connectors;
    private final ConnectorContexts contexts;
    private final ExternalJobScheduler scheduler;
    private final Clock clock;

    public ExternalJobPoller(SplitRepository splits,
                             RunRepository runs,
                             RunPlanner planner,
                             ConnectorRegistry connectors,
                             ConnectorContexts contexts,
                             ExternalJobScheduler scheduler,
                             Clock clock) {
        this.splits = splits;
        this.runs = runs;
        this.planner = planner;
        this.connectors = connectors;
        this.contexts = contexts;
        this.scheduler = scheduler;
        this.clock = clock;
    }

    /** Convenience for the reaper, which already holds the split it wants polled. */
    public void poll(Split split) {
        poll(split.tenantId(), split.id());
    }

    /**
     * One status check.
     *
     * <p>Re-reads the split rather than trusting whatever the caller held. A trigger armed four
     * minutes ago describes a chunk as it was four minutes ago, and in between the run may have
     * been stopped, the chunk cancelled, or another node's firing may have already moved it on.
     */
    public void poll(TenantId tenantId, SplitId splitId) {
        Split split = splits.findById(tenantId, splitId).orElse(null);

        if (split == null || split.state() != SplitState.WAITING_EXTERNAL) {
            // Ordinary. The chunk has already moved on — settled by a worker, cancelled with its
            // run, or polled by the reaper a moment before this trigger fired.
            scheduler.cancel(splitId);
            return;
        }

        Run run = runs.findById(tenantId, split.runId()).orElse(null);
        if (run == null || run.state().isTerminal()) {
            // The run is over and nothing will settle this chunk. Stop asking; the remote job is
            // left to age out of the destination, which is a tidiness problem rather than a data
            // one — nothing here can write it down anywhere that anyone would read.
            log.warn("Chunk {} of run {} is parked on a remote job but its run has ended; "
                            + "no longer polling it", split.index(), split.runId());
            scheduler.cancel(splitId);
            return;
        }

        try {
            check(run, split);
        } catch (RuntimeException e) {
            // The destination could not be reached or answered something unusable. The job is still
            // running regardless, so this backs off and asks again rather than failing the chunk on
            // the strength of one bad call.
            log.warn("Could not check the remote job for chunk {} of run {}: {}",
                    split.index(), split.runId(), e.getMessage());
            rearm(split, RETRY_AFTER_POLL_ERROR);
        }
    }

    private void check(Run run, Split split) {
        ResolvedPipeline pipeline = planner.resolve(run);
        Preparation job = ChunkParkedException.sinkJobOf(split.externalJob());

        ConnectorContext context = contexts.forChunk(pipeline.sinkInstance(),
                split.runId().toString(), "poller", split.index(), true);

        Preparation.Status status;
        try (Sink.SinkSession session =
                     connectors.sink(pipeline.sinkInstance().connectorType()).openSink(context)) {
            status = session.checkCommit(job);
        }

        if (!status.isReady() && !status.isFailed()) {
            rearm(split, ChunkExecutor.pollInterval(status));
            return;
        }

        Instant now = clock.instant();
        Split released = splits.transitionState(split.tenantId(), split.id(),
                SplitState.WAITING_EXTERNAL, split.externalJobFinished(now)).orElse(null);

        if (released == null) {
            // Another node's firing won, or the run was stopped between the two reads. Its
            // transition stands; this one is a no-op.
            return;
        }

        scheduler.cancel(split.id());
        log.info("The destination has finished chunk {} of run {} ({}); it is back in the pool "
                        + "to be settled",
                split.index(), split.runId(), status.isFailed() ? "failed" : "complete");
    }

    /**
     * Records the next poll time on the split and arms the trigger for it.
     *
     * <p>Both, in that order, and not just the trigger. {@code dueAt} is what the reaper's fallback
     * sweep looks at, so a chunk whose trigger is lost still has a written record of when it should
     * have been asked — which is the difference between a delayed chunk and a stranded one.
     */
    private void rearm(Split split, Duration after) {
        Instant now = clock.instant();
        Instant next = now.plus(after);

        Split rescheduled = splits.transitionState(split.tenantId(), split.id(),
                SplitState.WAITING_EXTERNAL, split.pollAgainAt(next, now)).orElse(null);

        if (rescheduled != null) {
            scheduler.pollAt(rescheduled, next);
        }
    }
}
