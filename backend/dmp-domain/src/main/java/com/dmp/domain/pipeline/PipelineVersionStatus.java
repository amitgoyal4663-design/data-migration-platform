package com.dmp.domain.pipeline;

/** Lifecycle of a single pipeline version. */
public enum PipelineVersionStatus {

    /** Editable work in progress. May be structurally invalid. */
    DRAFT,

    /** Passed structural validation. Still editable; validation is re-run on each change. */
    VALIDATED,

    /**
     * Frozen and runnable. Immutable from this point.
     *
     * <p>Immutability is not a policy preference: a run records the version it executed, and an
     * audit trail that can be rewritten after the fact is not an audit trail. Editing a published
     * version creates a new one.
     */
    PUBLISHED;

    public boolean isMutable() {
        return this != PUBLISHED;
    }
}
