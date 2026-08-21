package com.dmp.domain.run;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The run lifecycle, enforced as an explicit state machine.
 *
 * <p>Encoding the transitions here rather than scattering {@code if} statements across services
 * means an illegal transition is impossible to express, not merely unlikely. In a distributed
 * data plane where the control plane, a scheduler and any number of workers all mutate run state
 * concurrently, that guarantee is worth the enum.
 *
 * <p>{@link #STOPPING} is not in the original lifecycle sketch but is unavoidable in practice:
 * stopping a run with forty in-flight splits is a request, not an instant. Without it the UI
 * cannot distinguish "asked to stop" from "stopped", and users press the button repeatedly.
 */
public enum RunState {

    /** Persisted, not yet checked. */
    CREATED,

    /** Definition, connector references and quotas verified. No external calls made yet. */
    VALIDATED,

    /**
     * External jobs submitted; the engine is polling for readiness (ADR-0012).
     *
     * <p>Distinct from RUNNING because it can last hours for a Salesforce Bulk, Athena or BigQuery
     * job, during which no data is moving. Collapsing the two would make every throughput metric
     * and duration chart in the platform misleading — a run "running" for three hours that read
     * nothing is a very different situation from one that is reading slowly.
     */
    PREPARING,

    /** Splits are assigned and executing. */
    RUNNING,

    /** Execution suspended, assignments retained. Resumable without replanning. */
    PAUSED,

    /** Stop requested; in-flight splits are draining to their next checkpoint. */
    STOPPING,

    /**
     * Final sink commits, and release of any external resources held by connectors.
     *
     * <p>Distinct from COMPLETED because releasing an external resource is a network call that can
     * fail and must be retried. Folding it into a state transition would make it unobservable and
     * unretryable — and leaked Salesforce bulk jobs exhaust an org-wide quota of 10,000 per rolling
     * 24 hours, affecting integrations this platform does not own.
     */
    FINALIZING,

    /** Stopped before completing. Terminal. */
    STOPPED,

    /** Ended in failure. Terminal. */
    FAILED,

    /** All splits completed successfully. Terminal. */
    COMPLETED,

    /** Retained for history, excluded from active views. Terminal. */
    ARCHIVED;

    private static final Map<RunState, Set<RunState>> TRANSITIONS = Map.ofEntries(
            Map.entry(CREATED, EnumSet.of(VALIDATED, FAILED, STOPPED)),
            Map.entry(VALIDATED, EnumSet.of(PREPARING, FAILED, STOPPED)),
            // PREPARING may be re-entered: each delay-queue poll that finds the external job still
            // pending re-enters the same state rather than inventing a distinct one per attempt.
            Map.entry(PREPARING, EnumSet.of(PREPARING, RUNNING, FINALIZING, STOPPING, FAILED)),
            Map.entry(RUNNING, EnumSet.of(PAUSED, STOPPING, FINALIZING, FAILED)),
            Map.entry(PAUSED, EnumSet.of(RUNNING, STOPPING, FAILED)),
            // A stopped run still routes through FINALIZING. Stopping a migration must release the
            // external resources it acquired; skipping cleanup on the cancel path is how quotas leak.
            Map.entry(STOPPING, EnumSet.of(FINALIZING, STOPPED, FAILED)),
            Map.entry(FINALIZING, EnumSet.of(COMPLETED, STOPPED, FAILED)),
            Map.entry(STOPPED, EnumSet.of(ARCHIVED)),
            Map.entry(FAILED, EnumSet.of(ARCHIVED)),
            Map.entry(COMPLETED, EnumSet.of(ARCHIVED)),
            Map.entry(ARCHIVED, EnumSet.noneOf(RunState.class)));

    private static final Set<RunState> TERMINAL = EnumSet.of(STOPPED, FAILED, COMPLETED, ARCHIVED);
    private static final Set<RunState> ACTIVE = EnumSet.of(PREPARING, RUNNING, PAUSED, STOPPING, FINALIZING);

    public boolean canTransitionTo(RunState target) {
        return TRANSITIONS.getOrDefault(this, Set.of()).contains(target);
    }

    public void requireTransitionTo(RunState target) {
        if (!canTransitionTo(target)) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "A run cannot move from " + this + " to " + target,
                    Map.of("from", name(), "to", target.name(),
                            "allowed", TRANSITIONS.getOrDefault(this, Set.of()).toString()));
        }
    }

    public Set<RunState> allowedTransitions() {
        return Set.copyOf(TRANSITIONS.getOrDefault(this, Set.of()));
    }

    /** No further progress is possible without creating a new run. */
    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }

    /** The run occupies worker capacity and counts against concurrency limits. */
    public boolean isActive() {
        return ACTIVE.contains(this);
    }

    /**
     * Whether a failure here can be retried by restarting from checkpoints.
     *
     * <p>Only a run that got as far as executing has checkpoints to resume from. One that failed
     * during validation has nothing to resume and must be recreated.
     */
    public boolean isResumable() {
        return this == PAUSED || this == STOPPED;
    }

    /**
     * Whether the engine should be polling an external system rather than moving data.
     *
     * <p>Drives the delay-queue re-check loop and lets the UI show "waiting on Salesforce" instead
     * of a run that appears stalled.
     */
    public boolean isAwaitingExternalSystem() {
        return this == PREPARING;
    }

    /**
     * Whether a run in this state may still hold unreleased external resources.
     *
     * <p>The reaper (ADR-0012) uses this to decide what to sweep. Anything past VALIDATED may have
     * submitted an external job, and a run that failed mid-preparation is exactly the case the
     * happy-path cleanup cannot cover.
     */
    public boolean mayHoldExternalResources() {
        return this != CREATED && this != VALIDATED;
    }
}
