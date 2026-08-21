package com.dmp.domain.run;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/** Lifecycle of a single unit of parallel work. */
public enum SplitState {

    /** Planned, not yet claimed by a worker. */
    PENDING,

    /** Claimed by a worker and executing. */
    RUNNING,

    /**
     * Handed to an external system that has not finished deciding, and parked until it has.
     *
     * <p>A Salesforce bulk job, a Databricks statement, an Athena query: the records have been
     * uploaded and the destination is working through them, which can take minutes. Holding a
     * worker for that is waste — the chunk occupies a concurrency slot and a run slot while doing
     * nothing but sleeping — and, far worse, the handle on the remote job would exist only in that
     * worker's memory. A pod restart during the wait lost it entirely: the job carried on in the
     * org unobserved, its per-record rejections were never fetched, and the run reported every one
     * of those records as written.
     *
     * <p>So the chunk lets go instead. Its lease and its worker are cleared, the handle is written
     * to the split, and {@code dueAt} says when to ask again. Any pod may pick it up, and it is
     * durable across every pod dying at once.
     */
    WAITING_EXTERNAL,

    /** Finished successfully; its checkpoint is final. */
    COMPLETED,

    /** Failed and may be retried within the run's retry budget. */
    FAILED,

    /** Failed beyond the retry budget. The run cannot complete successfully. */
    ABANDONED,

    /** Cancelled because the run was stopped before this split started or finished. */
    CANCELLED;

    private static final Map<SplitState, Set<SplitState>> TRANSITIONS = Map.of(
            PENDING, EnumSet.of(RUNNING, CANCELLED),
            RUNNING, EnumSet.of(COMPLETED, FAILED, CANCELLED, WAITING_EXTERNAL),
            // Back to PENDING once the destination has finished, so an ordinary claim picks it up
            // and the chunk is settled — harvested, released, completed — by the one code path that
            // knows how to do that. Never straight to COMPLETED: the rejections the destination
            // decided on are not known until something fetches them.
            WAITING_EXTERNAL, EnumSet.of(PENDING, FAILED, CANCELLED),
            // A failed split returns to PENDING for reassignment; the retry budget is enforced by
            // the run, not by this state machine, so that policy lives in one place.
            FAILED, EnumSet.of(PENDING, ABANDONED, CANCELLED),
            COMPLETED, EnumSet.noneOf(SplitState.class),
            ABANDONED, EnumSet.noneOf(SplitState.class),
            CANCELLED, EnumSet.noneOf(SplitState.class));

    public boolean canTransitionTo(SplitState target) {
        return TRANSITIONS.getOrDefault(this, Set.of()).contains(target);
    }

    public void requireTransitionTo(SplitState target) {
        if (!canTransitionTo(target)) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "A split cannot move from " + this + " to " + target,
                    Map.of("from", name(), "to", target.name(),
                            "allowed", TRANSITIONS.getOrDefault(this, Set.of()).toString()));
        }
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == ABANDONED || this == CANCELLED;
    }

    /**
     * Whether this split still needs to be executed for the run to finish.
     *
     * <p>A parked chunk counts. Its records are with the destination and nobody has yet asked what
     * became of them — declaring the run complete while a bulk job is still processing would report
     * a migration as finished before the target had finished accepting it.
     */
    public boolean isOutstanding() {
        return this == PENDING || this == RUNNING || this == FAILED || this == WAITING_EXTERNAL;
    }
}
