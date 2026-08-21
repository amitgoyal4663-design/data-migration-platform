package com.dmp.common.error;

/**
 * The platform's error taxonomy.
 *
 * <p>These codes are the stable contract between the domain and every outbound surface: the
 * REST layer maps them to HTTP status, the engine maps them to retry decisions, and the UI maps
 * them to messages. Deliberately free of transport concepts — the domain must never know that
 * HTTP exists.
 *
 * <p>The {@code retryable} flag is consumed by the engine from Phase 3 onward. It answers a
 * single question: would attempting this again, unchanged, plausibly succeed? A connection
 * timeout would; a malformed pipeline definition would not.
 */
public enum ErrorCode {

    /** Input failed structural or semantic validation. */
    VALIDATION_FAILED(false),

    /** The referenced entity does not exist, or is not visible to the current tenant. */
    NOT_FOUND(false),

    /** A uniqueness constraint was violated — typically a duplicate name within a tenant. */
    DUPLICATE(false),

    /** Concurrent modification detected via optimistic locking. The caller should re-read and retry. */
    CONCURRENT_MODIFICATION(true),

    /** The requested transition is not legal from the entity's current state. */
    ILLEGAL_STATE_TRANSITION(false),

    /** An attempt to mutate something immutable, such as a published pipeline version. */
    IMMUTABLE(false),

    /** A referenced entity exists but is not usable in the requested role. */
    INVALID_REFERENCE(false),

    /** A downstream system was unreachable or timed out. */
    UPSTREAM_UNAVAILABLE(true),

    /** A configured rate limit or quota was exceeded. */
    RATE_LIMITED(true),

    /** Unexpected failure. Anything reaching the user with this code is a defect. */
    INTERNAL(false);

    private final boolean retryable;

    ErrorCode(boolean retryable) {
        this.retryable = retryable;
    }

    /** Whether an identical retry could plausibly succeed. Drives engine retry policy. */
    public boolean isRetryable() {
        return retryable;
    }
}
