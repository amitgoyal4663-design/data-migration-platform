package com.dmp.connector.api;

/**
 * The failure type connectors throw.
 *
 * <p>{@link Kind} is the important part: it tells the engine whether to retry. A connector that
 * reports a permanent configuration error as retryable will have the platform attempt the same
 * doomed operation five times with backoff; one that reports a transient network blip as fatal
 * will abandon a chunk that would have succeeded on the next attempt. Getting this classification
 * right is the single most consequential thing a connector author does for operability.
 */
public class ConnectorException extends RuntimeException {

    private final Kind kind;

    public ConnectorException(Kind kind, String message) {
        this(kind, message, null);
    }

    public ConnectorException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }

    public boolean isRetryable() {
        return kind.retryable;
    }

    public enum Kind {

        /** Bad configuration, a missing table, an unparseable query. Retrying changes nothing. */
        CONFIGURATION(false),

        /** Wrong credentials or insufficient permissions. Retrying changes nothing. */
        AUTHENTICATION(false),

        /** The remote system was unreachable, timed out, or returned a 5xx. Worth retrying. */
        UNAVAILABLE(true),

        /** A quota or rate limit was hit. Worth retrying, after a delay. */
        RATE_LIMITED(true),

        /**
         * A single record was rejected while the rest of the batch was fine.
         *
         * <p>Not a chunk failure. The record goes to the dead-letter queue with its reason and the
         * chunk carries on — one malformed row out of a million must not fail a migration.
         */
        RECORD_REJECTED(false),

        /** Anything unclassified. Treated as non-retryable, because guessing wrong wastes attempts. */
        UNKNOWN(false);

        private final boolean retryable;

        Kind(boolean retryable) {
            this.retryable = retryable;
        }
    }
}
