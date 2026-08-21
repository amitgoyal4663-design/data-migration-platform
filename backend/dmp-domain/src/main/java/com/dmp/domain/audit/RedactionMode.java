package com.dmp.domain.audit;

/** How a field marked sensitive is handled before an audit record is written. */
public enum RedactionMode {

    /**
     * Replace with a fixed mask, preserving that a value was present.
     *
     * <p>Suitable when the investigator needs to know the field was populated but never its value.
     */
    MASK,

    /**
     * Replace with a salted digest.
     *
     * <p>The default for identifiers. Preserves the ability to correlate the same value across
     * runs and records — usually the actual investigative need — without retaining the value
     * itself. The salt is per-tenant, so digests cannot be compared across tenants or attacked
     * with a precomputed table.
     */
    HASH,

    /** Remove the field entirely. The strongest option, and the least useful for debugging. */
    DROP
}
