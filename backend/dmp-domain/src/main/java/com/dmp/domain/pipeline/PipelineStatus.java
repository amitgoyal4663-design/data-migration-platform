package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/** Lifecycle of a pipeline, independent of the lifecycle of its versions. */
public enum PipelineStatus {

    /** Created but never published. No version is runnable. */
    DRAFT,

    /** Has a published version and may be run. */
    ACTIVE,

    /** Retained for audit and history. Cannot be run; may be restored. */
    ARCHIVED;

    private static final Map<PipelineStatus, Set<PipelineStatus>> TRANSITIONS = Map.of(
            DRAFT, EnumSet.of(ACTIVE, ARCHIVED),
            ACTIVE, EnumSet.of(ARCHIVED),
            ARCHIVED, EnumSet.of(ACTIVE));

    public boolean canTransitionTo(PipelineStatus target) {
        return TRANSITIONS.getOrDefault(this, Set.of()).contains(target);
    }

    public void requireTransitionTo(PipelineStatus target) {
        if (this == target) {
            return;
        }
        if (!canTransitionTo(target)) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "A pipeline cannot move from " + this + " to " + target,
                    Map.of("from", name(), "to", target.name(),
                            "allowed", TRANSITIONS.getOrDefault(this, Set.of()).toString()));
        }
    }

    public boolean isRunnable() {
        return this == ACTIVE;
    }
}
