package com.dmp.application.port.out;

import com.dmp.domain.run.RunId;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.run.SplitState;
import com.dmp.domain.tenant.TenantId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Persistence port for splits. Implemented by the MongoDB adapter (ADR-0005). */
public interface SplitRepository {

    /**
     * Persists a whole split plan in one operation.
     *
     * <p>Bulk rather than per-split because a large migration plans tens of thousands of splits at
     * once, and a round trip each would make planning slower than reading the data.
     */
    void saveAll(List<Split> splits);

    Optional<Split> findById(TenantId tenantId, SplitId id);

    List<Split> findByRun(TenantId tenantId, RunId runId);

    List<Split> findByRunAndState(TenantId tenantId, RunId runId, SplitState state);

    /**
     * Atomically claims the lowest-indexed pending split for a worker.
     *
     * <p>Find and mark happen in one operation, so two workers racing cannot both win the same
     * split — one gets it, the other gets the next one, or empty. That single property is the
     * entire work-distribution mechanism: there is no coordinator, no assignment, and no rebalance.
     *
     * <p>Because pods only ask when they have a free slot, distribution self-balances. A pod that
     * finishes a fast chunk immediately asks for another; a pod stuck on a slow chunk asks for
     * nothing. Chunks are never equal in duration, and this corrects for that without anyone
     * having to predict it.
     *
     * <p>Empty is an ordinary outcome, not an error.
     *
     * @param lease how long the claim survives without a heartbeat
     */
    Optional<Split> claimNextPending(TenantId tenantId, RunId runId, String workerId,
                                     Instant now, Duration lease);

    /**
     * Extends a worker's lease on a split it is still processing.
     *
     * <p>Conditional on the split still being assigned to this worker. A worker whose lease lapsed
     * and whose split was reclaimed must not be able to extend it back — that would produce two
     * pods processing the same chunk, each believing it holds the claim.
     *
     * @return the updated split, or empty if the worker no longer holds it
     */
    Optional<Split> heartbeat(TenantId tenantId, SplitId id, String workerId,
                              Instant now, Duration lease);

    /**
     * Splits whose lease has lapsed and which may be reclaimed.
     *
     * <p>Not tenant-scoped: this is a platform sweep. Covers the worker that still holds its claim
     * but has stopped making progress — a network partition, a long stop-the-world pause, a wedged
     * connector. Without it those splits stay RUNNING forever, the run never completes, and on a
     * sequential run the single slot is never released.
     */
    List<Split> findExpiredLeases(Instant now, int limit);

    /**
     * Parked chunks whose external job is due to be asked about again.
     *
     * <p>The fallback path. Each parked chunk gets a Quartz one-shot trigger, and Quartz's clustered
     * job store survives the node that armed it dying — but "survives" is a claim worth having a
     * second opinion on, because the failure mode is silent: a chunk that is never polled waits
     * for ever, holds its run open for ever, and looks in the console exactly like a chunk that is
     * simply taking a while.
     *
     * <p>Not tenant-scoped: this is a platform sweep, like the lease reclaim beside it.
     */
    List<Split> findDueExternalWaits(Instant now, int limit);

    /** True count of splits in flight for a run, used to reconcile the slot counter. */
    long countRunning(TenantId tenantId, RunId runId);

    /** Conditional state transition. Empty when the split was no longer in the expected state. */
    Optional<Split> transitionState(TenantId tenantId, SplitId id, SplitState expectedState, Split updated);

    /** Count by state, for run progress without loading every split document. */
    long countByState(TenantId tenantId, RunId runId, SplitState state);

    /** Removes every split for a run. Used when a run is deleted rather than archived. */
    void deleteByRun(TenantId tenantId, RunId runId);
}
