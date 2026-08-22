package com.dmp.application.port.out;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.RunMetrics;
import com.dmp.domain.tenant.TenantId;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Persistence port for runs. Implemented by the MongoDB adapter (ADR-0005).
 *
 * <p>The interface is shaped around MongoDB's strengths without naming it. In particular
 * {@link #transitionState} exists because a run is a single document, which makes a state change
 * an atomic compare-and-swap in one round trip — a stronger primitive than read-modify-write with
 * a version column, and the reason execution data moved out of PostgreSQL.
 */
public interface RunRepository {

    /**
     * Inserts a new run.
     *
     * @throws com.dmp.common.error.DmpException {@code DUPLICATE} if the idempotency key is already
     *         present for the tenant. This is the guard against a delay-queue resume-token replay
     *         starting the same migration twice — an at-least-once redelivery that ADR-0002
     *         explicitly accepts, so the uniqueness constraint is load-bearing, not defensive.
     */
    Run create(Run run);

    Optional<Run> findById(TenantId tenantId, RunId id);

    Optional<Run> findByIdempotencyKey(TenantId tenantId, String idempotencyKey);

    /**
     * Atomically moves a run from an expected state to a new one.
     *
     * <p>Applies the update only if the stored state still matches {@code expectedState}, so two
     * workers racing to pause the same run cannot both succeed.
     *
     * @return the updated run, or empty if the run was no longer in the expected state
     */
    Optional<Run> transitionState(TenantId tenantId, RunId id, RunState expectedState, Run updated);

    /** Overwrites the whole document. Use {@link #transitionState} for state changes. */
    Run save(Run run);

    /**
     * Atomically reserves one concurrency slot, if the run is below its limit.
     *
     * <p>This is how {@code ExecutionPolicy.maxConcurrentChunks} is enforced across a fleet of
     * pods that never talk to each other. The check and the increment happen in one operation, so
     * two workers cannot both observe "3 of 4 in use" and both proceed. With a limit of 1 —
     * strictly sequential execution — exactly one worker in the cluster holds the slot.
     *
     * <p>A worker must reserve before claiming a chunk, and release when the chunk finishes or
     * fails. Reserving and then finding no pending chunk is an ordinary outcome; the caller
     * releases and moves on.
     *
     * @return true if a slot was taken; false if the run is already at its limit
     */
    boolean tryReserveSlot(TenantId tenantId, RunId id, int maxConcurrentChunks);

    /** Releases a previously reserved slot. Never drops below zero. */
    void releaseSlot(TenantId tenantId, RunId id);

    /**
     * Resets the slot counter to the true number of running chunks.
     *
     * <p>A counter that only workers decrement leaks whenever a worker dies holding a slot. The
     * lease sweep releases what it can see, but a periodic reconciliation against the actual
     * RUNNING count is what stops slow drift from eventually deadlocking a sequential run — the
     * failure mode being: counter says 1, nothing is actually running, no worker can ever reserve
     * again.
     */
    void reconcileSlots(TenantId tenantId, RunId id, int actualRunningChunks);

    /**
     * Applies metric deltas without reading the document first.
     *
     * <p>Counters only increase, so concurrent workers can each apply a relative increment with no
     * ordering requirement between them and no lost updates. Reading-then-writing would need a
     * lock per split completion, on the hottest write path in the platform.
     */
    void incrementMetrics(TenantId tenantId, RunId id, RunMetrics delta);

    Page<Run> search(TenantId tenantId, RunSearch criteria, PageQuery pageQuery);

    /**
     * The attempts belonging to the given migrations — every run that resumed or retried one of
     * them, and every run that resumed those, to the end of each chain.
     *
     * <p>Fetched for a page of migrations in one go rather than a query per row, because a list of
     * twenty-five would otherwise be twenty-five round trips to display something most of them do
     * not have.
     */
    List<Run> findAttemptsOf(TenantId tenantId, java.util.Collection<RunId> roots);

    /** Runs currently occupying worker capacity. Used for concurrency limits and the dashboard. */
    List<Run> findActive(TenantId tenantId);

    /**
     * Runs in any of the given states, across every tenant.
     *
     * <p>Deliberately not tenant-scoped: a worker pod serves the whole platform and has no request
     * context to take a tenant from. Ordered oldest-first so a long-waiting run is picked up before
     * a newly created one, which stops a steady stream of new work from starving something already
     * queued.
     */
    List<Run> findByStates(java.util.Set<RunState> states, int limit);

    /**
     * Runs in a terminal state that still hold unreleased external resources.
     *
     * <p>The reaper's query (ADR-0012). Covers the case where a worker died between failing and
     * releasing a Salesforce bulk job or a Databricks statement — which the happy-path FINALIZING
     * transition structurally cannot.
     */
    List<Run> findWithUnreleasedResources(Instant olderThan, int limit);

    /**
     * Runs stuck in PREPARING whose next poll is due.
     *
     * <p>A safety net beneath the delay queue. ADR-0002 accepts that a resume-token gap can lose
     * timers silently; without this sweep, a lost timer means a run waits on Salesforce forever
     * with nothing indicating anything is wrong.
     */
    List<Run> findPreparingDueForCheck(Instant dueBefore, int limit);

    record RunSearch(PipelineId pipelineId, Set<RunState> states, Instant startedAfter,
                     Instant startedBefore, String triggeredBy, boolean rootsOnly) {

        public RunSearch {
            states = Set.copyOf(states == null ? Set.of() : states);
        }

        /** The four-argument form, from before a run could be an attempt within a migration. */
        public RunSearch(PipelineId pipelineId, Set<RunState> states, Instant startedAfter,
                         Instant startedBefore, String triggeredBy) {
            this(pipelineId, states, startedAfter, startedBefore, triggeredBy, false);
        }

        /**
         * Only runs that started a migration, excluding the resumes and retries within one.
         *
         * <p>What makes a page size mean what it says. A run stopped and resumed three times is one
         * migration and four rows in the store, so a page of twenty-five runs was drawing eight
         * groups — the number somebody chose bore no relation to what they saw. Pages are counted
         * in migrations; the attempts inside one travel with it.
         */
        public RunSearch onlyRoots() {
            return new RunSearch(pipelineId, states, startedAfter, startedBefore, triggeredBy, true);
        }

        public static RunSearch none() {
            return new RunSearch(null, Set.of(), null, null, null);
        }

        public static RunSearch forPipeline(PipelineId pipelineId) {
            return new RunSearch(pipelineId, Set.of(), null, null, null);
        }
    }
}
