package com.dmp.engine;

import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.application.port.out.RunEventPublisher;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitState;
import com.dmp.domain.tenant.TenantId;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The pull loop every worker pod runs.
 *
 * <p>Work is never assigned to a pod. Each pod asks for a chunk only when it has a free slot, which
 * makes distribution self-balancing: a pod finishing a fast chunk immediately asks for another,
 * while a pod grinding through a slow one asks for nothing. Chunks are never equal in duration and
 * nobody has to predict which will be slow.
 *
 * <p>Adding a pod mid-run needs no coordination — it starts asking. Removing one costs a lease
 * interval, after which its chunks return to the pool and resume from their checkpoints.
 */
@Component
@Profile({"worker", "all", "default"})
public class WorkerLoop {

    private static final Logger log = LoggerFactory.getLogger(WorkerLoop.class);

    /** How many runs one poll pass considers. Bounded so a large backlog cannot stall the loop. */
    private static final int BATCH_SIZE = 50;

    /**
     * Ceiling on how far an idle worker backs off.
     *
     * <p>Fifteen seconds, arrived at by measurement rather than by taste. A one-minute cap was
     * tried first and cost fifty-three seconds between pressing Run and the migration starting on
     * an idle cluster — every pod had backed off to a minute and none of them knew the run existed.
     * Saving eleven queries a minute is not worth making someone watch a blank screen for most of
     * one.
     *
     * <p>Fifteen still cuts idle chatter by two thirds and bounds the worst start delay at
     * something a person will tolerate. The real fix is to stop waiting for a poll at all: the
     * platform already publishes RUN_CREATED, and a worker that woke on that message could back
     * off for minutes without costing any latency. Until then this is the honest compromise.
     */
    private static final Duration MAX_IDLE_POLL = Duration.ofSeconds(15);

    private final RunRepository runs;
    private final SplitRepository splits;
    private final CheckpointRepository checkpoints;
    private final RunPlanner planner;
    private final RunOrchestrator orchestrator;
    private final ChunkExecutor executor;
    private final RunEventPublisher events;
    private final com.dmp.engine.schedule.ExternalJobScheduler externalJobs;
    private final RateLimitGate rateLimits;
    private final Clock clock;

    private final String workerId;
    private final int maxConcurrentChunks;
    private final Duration idlePollInterval;
    private final Duration busyPollInterval;

    /**
     * When the soonest chunk this pod deferred becomes claimable again.
     *
     * <p>Without it, the idle backoff and a rate limit work against each other. The backoff doubles
     * its sleep while nothing is claimable — up to fifteen seconds — and a chunk waiting six
     * seconds for its budget is not claimable, so the pod is asleep when the budget arrives. The
     * result is a run that goes at less than half the rate the client actually allowed, with the
     * limiter behaving perfectly and the engine sitting on its hands.
     *
     * <p>Only what this pod deferred, which is enough: another pod polls on its own schedule, and
     * the value is a hint that shortens a sleep, never a substitute for the claim itself.
     */
    private final java.util.concurrent.atomic.AtomicReference<Instant> nextDeferredDueAt =
            new java.util.concurrent.atomic.AtomicReference<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger inFlight = new AtomicInteger();

    /**
     * Free chunk slots on this pod.
     *
     * <p>Created in the constructor rather than at application-ready, so the field is never null.
     * It was, for the window between the two, and anything that reached a poll in that window —
     * a test, or an eager caller — hit a null semaphore rather than an empty one.
     */
    private final Semaphore slots;
    private ExecutorService chunkPool;
    private Thread pollerThread;

    public WorkerLoop(RunRepository runs,
                      SplitRepository splits,
                      CheckpointRepository checkpoints,
                      RunPlanner planner,
                      RunOrchestrator orchestrator,
                      ChunkExecutor executor,
                      RunEventPublisher events,
                      com.dmp.engine.schedule.ExternalJobScheduler externalJobs,
                      com.dmp.application.port.out.RateLimiter rateLimiter,
                      com.dmp.connector.runtime.ConnectorRegistry connectors,
                      Clock clock,
                      @Value("${dmp.worker.id:}") String configuredWorkerId,
                      @Value("${dmp.worker.max-concurrent-chunks:4}") int maxConcurrentChunks,
                      @Value("${dmp.worker.idle-poll-interval:PT5S}") Duration idlePollInterval,
                      @Value("${dmp.worker.busy-poll-interval:PT0.2S}") Duration busyPollInterval) {
        this.runs = runs;
        this.splits = splits;
        this.checkpoints = checkpoints;
        this.planner = planner;
        this.orchestrator = orchestrator;
        this.executor = executor;
        this.events = events;
        this.externalJobs = externalJobs;
        this.rateLimits = new RateLimitGate(rateLimiter, connectors);
        this.clock = clock;
        this.workerId = configuredWorkerId == null || configuredWorkerId.isBlank()
                ? defaultWorkerId() : configuredWorkerId;
        this.maxConcurrentChunks = maxConcurrentChunks;
        this.idlePollInterval = idlePollInterval;
        this.busyPollInterval = busyPollInterval;
        this.slots = new Semaphore(maxConcurrentChunks);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        ThreadFactory factory = Thread.ofVirtual().name("dmp-chunk-", 0).factory();
        this.chunkPool = Executors.newThreadPerTaskExecutor(factory);

        this.pollerThread = Thread.ofPlatform()
                .name("dmp-worker-poller")
                .daemon(true)
                .start(this::poll);

        log.info("Worker {} started with {} concurrent chunk slot(s)", workerId, maxConcurrentChunks);
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        if (pollerThread != null) {
            pollerThread.interrupt();
        }
        if (chunkPool != null) {
            // Chunks are given time to reach a checkpoint boundary. Killing them immediately would
            // discard up to one batch of work per chunk, which then has to be redone on resume.
            chunkPool.shutdown();
            try {
                if (!chunkPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("Worker {} still had chunks in flight at shutdown; they will be "
                            + "reclaimed by lease expiry", workerId);
                    chunkPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                chunkPool.shutdownNow();
            }
        }
        log.info("Worker {} stopped", workerId);
    }

    /**
     * The pull loop, paced by whether there is anything to pull.
     *
     * <p>An idle pod used to ask every five seconds for ever. With nothing running, twenty pods
     * spent four queries a second establishing that nothing was running — a cost paid continuously
     * for information that never changed.
     *
     * <p>So an idle worker backs off: five seconds, then ten, then {@link #MAX_IDLE_POLL} — the
     * doubling stops at the cap, so the sequence is 5, 10, 15 and no further. A quiet cluster
     * settles to four questions per pod per minute instead of twelve. The moment a chunk is
     * claimed it snaps back to the busy interval, so responsiveness under load is exactly what it
     * was — the backoff only ever grows while there is provably nothing to do.
     *
     * <p>The cost is latency on the first chunk of a run after a quiet spell: up to
     * {@link #MAX_IDLE_POLL} before a pod notices. That is why {@link RunOrchestrator} advances new
     * runs on the same pass — a run created now is picked up by whichever pod wakes first, and the
     * rest join as they wake.
     */
    private void poll() {
        long idleMillis = idlePollInterval.toMillis();

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            boolean didWork = false;
            try {
                didWork = pollOnce();
            } catch (Exception e) {
                // A poller that dies takes the pod's capacity with it, so nothing is allowed to
                // escape this loop. Errors are logged and the next tick tries again.
                log.error("Worker {} poll failed; continuing", workerId, e);
            }

            long wait;
            if (didWork) {
                wait = busyPollInterval.toMillis();
                idleMillis = idlePollInterval.toMillis();   // busy again: forget the backoff
            } else {
                wait = idleMillis;
                idleMillis = Math.min(idleMillis * 2, MAX_IDLE_POLL.toMillis());
            }

            wait = Math.min(wait, millisUntilDeferredWorkIsDue());

            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * One pass: advance any run that needs it, then claim what capacity allows.
     *
     * <p>Runs are visited in order and each gets at most one chunk per pass. That is deliberate
     * fairness: draining one run before looking at the next would let a ten-thousand-chunk
     * migration starve a ten-chunk one queued behind it.
     */
    /**
     * One pass: advance runs that need starting, then claim what work this pod has room for.
     *
     * <p>Package-private rather than private so a test can drive a single pass. The loop around it
     * is a thread and a sleep, and neither is what anybody needs to assert on.
     */
    boolean pollOnce() {
        boolean didWork = false;

        for (Run run : runs.findByStates(Set.of(RunState.CREATED), BATCH_SIZE)) {
            didWork |= advance(run);
        }

        didWork |= claimAndExecute();
        return didWork;
    }

    private boolean advance(Run run) {
        try {
            orchestrator.advanceToRunning(run, workerId);
            return true;
        } catch (Exception e) {
            log.error("Could not advance run {}", run.id(), e);
            failRun(run, e);
            return false;
        }
    }

    /** Claims one chunk per runnable run, as far as this pod's free slots allow. */
    private boolean claimAndExecute() {
        if (slots.availablePermits() == 0) {
            return false;
        }

        boolean claimedAnything = false;
        for (Run run : runnableRuns()) {
            if (!slots.tryAcquire()) {
                break;
            }

            Optional<Split> claimed = tryClaim(run);
            if (claimed.isEmpty()) {
                slots.release();
                continue;
            }

            claimedAnything = true;
            Split split = claimed.get();
            inFlight.incrementAndGet();

            chunkPool.submit(() -> {
                try {
                    runChunk(run, split);
                } finally {
                    inFlight.decrementAndGet();
                    slots.release();
                }
            });
        }
        return claimedAnything;
    }

    private List<Run> runnableRuns() {
        return runs.findByStates(Set.of(RunState.RUNNING), BATCH_SIZE);
    }

    /**
     * Reserves a concurrency slot, then claims a chunk.
     *
     * <p>In that order, and never the reverse. Claiming first and then discovering the run is at
     * its limit would leave a chunk marked RUNNING with nobody executing it, stalling until the
     * lease expired.
     */
    private Optional<Split> tryClaim(Run run) {
        ResolvedPipeline pipeline;
        try {
            pipeline = planner.resolve(run);
        } catch (Exception e) {
            log.error("Cannot resolve pipeline for run {}", run.id(), e);
            return Optional.empty();
        }

        int limit = pipeline.execution().maxConcurrentChunks();
        if (!runs.tryReserveSlot(run.tenantId(), run.id(), limit)) {
            return Optional.empty();
        }

        Optional<Split> claimed = splits.claimNextPending(
                run.tenantId(), run.id(), workerId, clock.instant(), pipeline.execution().chunkLease());

        if (claimed.isEmpty()) {
            // Reserved a slot and found no work. Ordinary — another pod took the last chunk between
            // the reservation and the claim.
            runs.releaseSlot(run.tenantId(), run.id());
        }
        return claimed;
    }

    private void runChunk(Run run, Split split) {
        // Everything logged from here down — by the engine, by a transform, by a connector talking
        // to a warehouse — carries this chunk's id. Without it a connector's "job 750bm… refused
        // 200 records" was attributable to no chunk at all, and with several chunks in flight the
        // log could not be read.
        try (ChunkLogContext ignored = ChunkLogContext.of(split)) {
            runChunkLogged(run, split);
        }
    }

    private void runChunkLogged(Run run, Split split) {
        ResolvedPipeline pipeline = planner.resolve(run);
        Instant now = clock.instant();

        try {
            // Before anything is opened, read or sent: may this chunk spend what a client agreed
            // to? A chunk that cannot is put back with a time on it, having done nothing, and this
            // worker goes and finds another run rather than sitting on a slot waiting.
            Optional<Duration> holdFor = rateLimits.reserve(pipeline, split.hasExternalJob());
            if (holdFor.isPresent()) {
                defer(run, split, holdFor.get());
                return;
            }

            ChunkResult result = executor.execute(pipeline, split, workerId);

            // Before this chunk is marked complete, not after. Completion is what triggers the
            // run-finished check, and a run whose only chunk had just completed would be declared
            // finished in the gap before its successor appeared.
            if (!result.sourceExhausted() && !stopRequested()) {
                generateNextChunk(run, split);
            }

            // What the reservation at the door assumed, minus what the chunk turned out to need.
            // Before completion, so the budget is available to whichever chunk is claimed next
            // rather than a moment after it has already been refused.
            rateLimits.settle(pipeline, result);

            splits.transitionState(split.tenantId(), split.id(), SplitState.RUNNING,
                    split.complete(clock.instant()));

            runs.incrementMetrics(run.tenantId(), run.id(), new com.dmp.domain.run.RunMetrics(
                    result.recordsRead(), result.recordsProduced(), result.recordsWritten(),
                    result.recordsFailed(), result.recordsFiltered(), result.bytesRead(), 0, 1, 0));

            // One message per chunk — the finest granularity published. An event per record would
            // reintroduce exactly the volume this architecture avoids.
            publish(RunEventPublisher.Type.CHUNK_COMPLETED, run, split,
                    java.util.Map.of(
                            "chunkIndex", split.index(),
                            "recordsRead", result.recordsRead(),
                            "recordsWritten", result.recordsWritten(),
                            "recordsFailed", result.recordsFailed(),
                            "workerId", workerId));

        } catch (ChunkParkedException parked) {
            // Not a failure and not a completion. The chunk's records are with the destination,
            // which is still deciding, so it is written down and put aside — and this worker's slot
            // is freed by the finally block below to go and do something useful.
            park(run, split, parked);

        } catch (ChunkExecutor.LeaseLostException e) {
            // Another worker owns this chunk now. Do not touch its state — that worker is
            // responsible for it, and writing here would corrupt its progress.
            log.warn("{}", e.getMessage());
            return;

        } catch (Exception e) {
            handleChunkFailure(run, split, pipeline, e, now);

        } finally {
            runs.releaseSlot(run.tenantId(), run.id());
            runs.findById(run.tenantId(), run.id()).ifPresent(orchestrator::completeIfFinished);
        }
    }

    /**
     * How long until the soonest chunk this pod deferred can run, or the sleep it was going to take.
     *
     * <p>Clears the hint once it has passed, so a stale time cannot hold the loop at a busy poll
     * for ever. A lost hint costs one ordinary poll interval, which is why it is allowed to be
     * approximate.
     */
    private long millisUntilDeferredWorkIsDue() {
        Instant due = nextDeferredDueAt.get();
        if (due == null) {
            return Long.MAX_VALUE;
        }
        long remaining = java.time.Duration.between(clock.instant(), due).toMillis();
        if (remaining <= 0) {
            nextDeferredDueAt.compareAndSet(due, null);
            return busyPollInterval.toMillis();
        }
        return remaining;
    }

    /**
     * Puts a chunk back, unrun, to be claimed no earlier than the given wait.
     *
     * <p>Distinct from {@link #park} in what it is waiting for and in what it has already done.
     * A parked chunk has handed records to a destination that is still deciding; a deferred chunk
     * has not started. Distinct from {@link Split#scheduleRetry} in that nothing went wrong — so
     * the attempt counter does not move, and a chunk that waits ten times for a budget is not ten
     * failures away from being abandoned.
     */
    private void defer(Run run, Split split, Duration holdFor) {
        Instant now = clock.instant();
        Instant until = now.plus(holdFor);

        Split deferred = splits.transitionState(split.tenantId(), split.id(), SplitState.RUNNING,
                split.deferUntil(until, now)).orElse(null);

        if (deferred == null) {
            // Another pod holds it now. Nothing was read or written, so there is nothing to undo
            // and nothing to warn about: that pod will ask for the same budget and get the same
            // answer.
            log.debug("Chunk {} of run {} was reclaimed before it could be held back",
                    split.index(), run.id());
            return;
        }

        // So the poller does not doze past the moment this becomes claimable. Earliest wins: two
        // chunks deferred by different amounts should wake the loop for the nearer one.
        nextDeferredDueAt.accumulateAndGet(until,
                (existing, candidate) -> existing == null || candidate.isBefore(existing)
                        ? candidate : existing);

        log.info("Chunk {} of run {} is waiting for the rate limit; claimable again at {}",
                split.index(), run.id(), until);
    }

    /**
     * Writes a chunk's remote job down and hands responsibility for it to the scheduler.
     *
     * <p>Until this existed, a chunk waiting on a Salesforce bulk job sat in a sleep loop. It held
     * a concurrency permit and one of the run's slots for the whole life of the job — minutes, on a
     * job the worker was contributing nothing to — and, worse, the job's id lived only in that
     * worker's memory. A pod restart during the wait lost it completely: the org carried on
     * processing records that nobody was watching, its per-record rejections were never fetched,
     * and every one of them was reported as written.
     *
     * <p>Both problems have the same fix. The handle goes on the split, where it survives the pod;
     * the worker lets go; and a Quartz trigger does the asking.
     */
    private void park(Run run, Split split, ChunkParkedException parked) {
        Instant now = clock.instant();
        Instant dueAt = now.plus(parked.retryAfter());

        Split waiting = splits.transitionState(split.tenantId(), split.id(), SplitState.RUNNING,
                split.parkOnExternalJob(parked.parkedState(), dueAt, now)).orElse(null);

        if (waiting == null) {
            // The chunk was reclaimed between the upload and this write. Loud, because the remote
            // job now exists with nothing pointing at it: whoever retries the chunk will submit a
            // second one, and under a non-idempotent operation that is duplicated data.
            log.error("Chunk {} of run {} submitted work to the destination but was no longer "
                            + "held by worker {} when it came to record the job; the remote job is "
                            + "orphaned and the chunk will be retried from its checkpoint",
                    split.index(), run.id(), workerId);
            return;
        }

        externalJobs.pollAt(waiting, dueAt);

        log.info("Chunk {} of run {} handed its records to the destination and is parked; "
                        + "the next status check is at {}",
                split.index(), run.id(), dueAt);
    }

    /**
     * Adds the chunk that continues where this one stopped.
     *
     * <p>Only reached for a lazily chunked run, and only when the source still had rows. The new
     * chunk carries no range of its own — it inherits its starting position from a checkpoint
     * seeded with the cursor this chunk finished on, which is the same mechanism a resumed chunk
     * already uses rather than a second path to keep correct.
     *
     * <p>Because each chunk asks the source what is there <em>now</em>, rows written after the run
     * started are read rather than falling beyond a boundary frozen at planning time.
     */
    private void generateNextChunk(Run run, Split finished) {
        Instant now = clock.instant();

        checkpoints.findBySplit(finished.tenantId(), finished.id()).ifPresent(previous -> {
            Split next = Split.plan(run.id(), run.tenantId(), finished.index() + 1,
                    finished.spec(), now);
            splits.saveAll(List.of(next));
            checkpoints.save(Checkpoint.initial(next.id(), run.id(), run.tenantId(), now)
                    .startingFrom(previous.sourceCursor()));

            // The total grows as the run proceeds rather than being known at the start, so the
            // console reports chunks finished instead of a percentage. A denominator would have to
            // be invented, and an invented one is worse than none.
            runs.incrementMetrics(run.tenantId(), run.id(),
                    com.dmp.domain.run.RunMetrics.ZERO.withSplitsTotal(1));

            log.debug("Run {} continues into chunk {}", run.id(), next.index());
        });
    }

    /**
     * Whether a chunk that failed this way is worth another attempt.
     *
     * <p>Unrecognised failures are assumed retryable. An unclassified error is more often a blip
     * than a certainty, and one wasted attempt costs far less than abandoning a chunk that would
     * have succeeded on the next try. The two named cases are where we know better.
     *
     * <p>A rejection-threshold failure is never retried. The rejections that reach a threshold are
     * systematic — a schema that changed, a key that already exists everywhere — and re-sending the
     * same records produces the same rejections, at whatever the target charges for the attempt.
     *
     * <p>A {@link DmpException} carries the answer in its own error code and is asked for it. That
     * it was not is how a chunk needing 167 calls against a limit of 5 was attempted five times: a
     * configuration that cannot work does not become workable by being tried again, and each
     * attempt buried the message explaining what to change under four more copies of itself.
     */
    static boolean isRetryable(Throwable cause) {
        return switch (cause) {
            case com.dmp.connector.api.ConnectorException e -> e.isRetryable();
            case com.dmp.common.error.DmpException e -> e.errorCode().isRetryable();
            case RejectionThresholdExceededException ignored -> false;
            case null -> true;
            default -> true;
        };
    }

    /** The code recorded on the chunk and published with the failure event. */
    static String errorCodeFor(Throwable cause) {
        return switch (cause) {
            case com.dmp.connector.api.ConnectorException e -> e.kind().name();
            case com.dmp.common.error.DmpException e -> e.errorCode().name();
            case RejectionThresholdExceededException ignored -> "REJECTION_THRESHOLD_EXCEEDED";
            case null -> "CHUNK_FAILED";
            default -> "CHUNK_FAILED";
        };
    }

    /**
     * Records a chunk failure and decides whether to retry it.
     *
     * <p>Retry is bounded by the pipeline's budget. A chunk that exhausts it is abandoned rather
     * than retried forever, so a genuinely broken range fails the run visibly instead of consuming
     * capacity indefinitely. A non-retryable failure — bad SQL, a missing table, wrong credentials —
     * skips the budget entirely, because attempting it four more times only delays the error the
     * user needs to see.
     */
    private void handleChunkFailure(Run run, Split split, ResolvedPipeline pipeline,
                                    Exception cause, Instant now) {
        String code = errorCodeFor(cause);
        boolean retryable = isRetryable(cause);

        Split failed = split.fail(code, cause.getMessage(), now);
        splits.transitionState(split.tenantId(), split.id(), SplitState.RUNNING, failed);

        publish(RunEventPublisher.Type.CHUNK_FAILED, run, split,
                java.util.Map.of("chunkIndex", split.index(), "attempt", failed.attempt(),
                        "errorCode", code, "workerId", workerId,
                        "retryable", String.valueOf(retryable)));

        if (retryable && !failed.hasExhaustedAttempts(pipeline.execution().maxAttemptsPerChunk())) {
            splits.transitionState(split.tenantId(), split.id(), SplitState.FAILED,
                    failed.scheduleRetry(now));
            log.warn("Chunk {} of run {} failed ({}), attempt {} of {}; returning it to the pool",
                    split.index(), run.id(), code, failed.attemptsMade(),
                    pipeline.execution().maxAttemptsPerChunk());
        } else {
            // The successor is created before this chunk is marked abandoned, for the same reason
            // it is on the success path: abandoning is what triggers the run-finished check, and a
            // run whose only outstanding chunk had just been abandoned would be declared finished
            // in the gap before its successor appeared.
            boolean continued = continueAfterFailure(run, split, pipeline);

            splits.transitionState(split.tenantId(), split.id(), SplitState.FAILED,
                    failed.abandon(now));
            log.error("Chunk {} of run {} abandoned after {} attempt(s): {}",
                    split.index(), run.id(), failed.attemptsMade(), cause.getMessage(), cause);

            rollUpAbandonedChunk(run, split);

            if (continued) {
                log.info("Chunk {} of run {} was abandoned, but the run carries on from where it "
                                + "stopped because this pipeline does not stop on a chunk failure",
                        split.index(), run.id());
            }

            if (pipeline.execution().stopRunOnChunkFailure()) {
                stopRunAfterFailure(run, split, code, now);
            }
        }
    }

    /**
     * Keeps a lazily chunked run going after one of its chunks gives up.
     *
     * <p>Without this, {@code stopRunOnChunkFailure = false} promised something the execution model
     * could not deliver. A lazily chunked run creates chunk N+1 only when chunk N <em>completes</em>,
     * so one failure ended the run whatever the setting said: a twenty-thousand-record migration
     * stopped after five thousand and reported "carry on with the other chunks" as its policy while
     * there were no other chunks to carry on with.
     *
     * <p>The successor starts from the failed chunk's cursor, so nothing is skipped — an open-ended
     * chunk has no range of its own, and whatever the failed one did not consume simply becomes the
     * next one's territory. Records it read and had rejected are already in the dead-letter queue,
     * where replay is the recovery path for a record-level failure anyway.
     *
     * <p>Returns whether a successor was created, because that is precisely what makes retrying the
     * failed chunk unsafe — see {@code RunOrchestrator.splitsToRetry}.
     */
    private boolean continueAfterFailure(Run run, Split split, ResolvedPipeline pipeline) {
        if (pipeline.execution().stopRunOnChunkFailure()
                || !OpenEnded.isOpenEnded(split.spec())
                || stopRequested()) {
            return false;
        }
        generateNextChunk(run, split);
        return true;
    }

    /**
     * Adds an abandoned chunk's work to the run's totals.
     *
     * <p>A chunk that gave up still did something, and until this existed the run did not say so. A
     * run whose twenty chunks each read a hundred records and had every one rejected reported zero
     * read, zero written and zero failed — while the dead-letter queue held two thousand entries and
     * the chunks themselves each reported a hundred. The failure was captured correctly at every
     * level except the one people look at first, which made a working platform look like one that
     * had done nothing at all.
     *
     * <p>Counted here and not on the earlier retryable branch, because that branch returns the chunk
     * to the pool: it will be claimed again and will eventually either complete or be abandoned, and
     * both of those already count. Adding it here as well would count the same records twice.
     *
     * <p>Read from the checkpoint rather than from the executor, because there is no result to read
     * — the exception left {@code execute} without one. The checkpoint is the better source anyway:
     * it holds what was durably done across every attempt this chunk made, which is precisely what
     * the run should be crediting.
     */
    private void rollUpAbandonedChunk(Run run, Split split) {
        checkpoints.findBySplit(split.tenantId(), split.id()).ifPresent(checkpoint ->
                runs.incrementMetrics(run.tenantId(), run.id(), new com.dmp.domain.run.RunMetrics(
                        checkpoint.recordsRead(), checkpoint.recordsProduced(),
                        checkpoint.recordsWritten(), checkpoint.recordsFailed(),
                        checkpoint.recordsFiltered(), checkpoint.bytesRead(), 0, 0, 1)));
    }

    /**
     * Ends the whole run because one of its chunks gave up.
     *
     * <p>Off by default, because "one bad range out of four hundred" is a partial result worth
     * having and the pull loop is perfectly capable of finishing the rest. It earns its place when
     * the failure is a property of the target rather than of the data: a schema that changed, a
     * credential that expired, a required field nobody populated. Then every remaining chunk is
     * about to discover the same thing at its own expense, and against a system that meters bulk
     * jobs that discovery is billed four hundred times over.
     *
     * <p>Requests a stop rather than failing the run outright. Chunks already executing drain to
     * their next checkpoint instead of being torn in half, chunks never claimed are cancelled by
     * {@code settleStop}, and the run reaches a resumable boundary — which is what makes the retry
     * button useful afterwards rather than a fresh start.
     */
    private void stopRunAfterFailure(Run run, Split split, String code, Instant now) {
        Run current = runs.findById(run.tenantId(), run.id()).orElse(null);
        if (current == null || current.state() != RunState.RUNNING) {
            // Already stopping, already failed, or another pod got here first with its own failed
            // chunk. Either way the decision has been taken and taking it twice changes nothing.
            return;
        }

        runs.transitionState(run.tenantId(), run.id(), RunState.RUNNING, current.requestStop(now))
                .ifPresent(stopping -> {
                    log.error("Stopping run {}: chunk {} was abandoned ({}) and this pipeline is "
                                    + "configured to stop the run on the first chunk failure",
                            run.id(), split.index(), code);
                    publish(RunEventPublisher.Type.RUN_STOP_REQUESTED, stopping, split,
                            java.util.Map.of("reason", "CHUNK_ABANDONED",
                                    "chunkIndex", split.index(), "errorCode", code));
                });
    }

    /**
     * Marks a run failed, from whatever state it actually reached.
     *
     * <p>Re-read rather than trusting the copy this was handed, and that is the whole point.
     * {@code advanceToRunning} moves a run through VALIDATED and PREPARING <em>before</em> the work
     * that can fail — planning, the source's own preparation — so by the time anything throws, the
     * caller's copy still says CREATED. Using it as the precondition for the conditional update
     * matched nothing, silently, and left the run stranded in PREPARING with no error recorded and
     * nothing to say why it stopped.
     *
     * <p>It looked correct for as long as failures happened before the first transition.
     */
    private void failRun(Run run, Exception cause) {
        try {
            Run current = runs.findById(run.tenantId(), run.id()).orElse(run);

            runs.transitionState(current.tenantId(), current.id(), current.state(),
                            current.fail("RUN_FAILED", cause.getMessage(), clock.instant()))
                    .ifPresentOrElse(
                            failed -> log.info("Run {} marked failed: {}",
                                    failed.id(), cause.getMessage()),
                            () -> log.error("Run {} could not be marked failed from {}; it will be "
                                    + "left for the reaper", current.id(), current.state()));
        } catch (Exception e) {
            log.error("Could not mark run {} failed", run.id(), e);
        }
    }

    private void publish(RunEventPublisher.Type type, Run run, Split split,
                         java.util.Map<String, Object> details) {
        if (!events.isEnabled()) {
            return;
        }
        try {
            events.publish(new RunEventPublisher.RunEvent(
                    type, run.tenantId(), run.id(), run.pipelineId().toString(),
                    null, run.versionNumber(), clock.instant(), details));
        } catch (Exception e) {
            log.debug("Could not publish {} for chunk {}", type, split.index(), e);
        }
    }

    private boolean stopRequested() {
        return !running.get();
    }

    public String workerId() {
        return workerId;
    }

    public int inFlightChunks() {
        return inFlight.get();
    }

    /**
     * A stable-enough identity for this process.
     *
     * <p>Uses the hostname, which under Kubernetes is the pod name — so an orphaned chunk in the
     * console names a pod an operator can actually go and look at.
     */
    private static String defaultWorkerId() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isBlank()) {
            return hostname;
        }
        return "worker-" + ProcessHandle.current().pid();
    }
}
