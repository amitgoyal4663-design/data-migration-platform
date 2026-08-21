package com.dmp.domain.run;

import java.util.Objects;

/**
 * What a retry should re-do, and how far back it should start.
 *
 * <p>Two questions the platform genuinely cannot answer on the user's behalf. Whether to resume a
 * chunk or start it over depends on why it failed, and whether the resulting duplicates matter
 * depends on the target — so both are asked rather than assumed, with defaults chosen from what the
 * sink says about itself.
 */
public record RetryOptions(From from, Scope scope, boolean acknowledgeDuplicates) {

    public RetryOptions {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(scope, "scope");
    }

    /** Where a re-attempted chunk picks up. */
    public enum From {

        /**
         * Continue from the last saved position.
         *
         * <p>The only safe choice when repeating a write duplicates: the engine saves the position
         * after every batch for such sinks, so the window of re-written records is one batch rather
         * than everything the chunk had done.
         */
        CHECKPOINT,

        /**
         * Discard the saved position and run the chunk from its beginning.
         *
         * <p>What "retry" usually means to the person pressing it, and the right answer when the
         * pipeline was changed or the earlier partial result is not trusted. Against a sink where
         * repeating a write duplicates, it re-writes everything the chunk already wrote — which is
         * why it needs acknowledging there.
         */
        CHUNK_START
    }

    /** Which of the original run's chunks to re-attempt. */
    public enum Scope {

        /** Chunks that exhausted their attempts. Chunks that succeeded are never re-run. */
        FAILED,

        /**
         * Also the chunks that never started because the run was stopped.
         *
         * <p>How a stopped run is resumed. There is no separate mechanism for it: a chunk cancelled
         * mid-run and a chunk abandoned mid-run both need running, and inventing a second verb for
         * the same act would only mean two code paths to keep correct.
         */
        FAILED_AND_CANCELLED
    }

    /** Resume the failed chunks — the sensible default for a sink that cannot absorb a repeat. */
    public static RetryOptions resumingFailed() {
        return new RetryOptions(From.CHECKPOINT, Scope.FAILED, false);
    }

    /** Re-run the failed chunks from the start — the default when repeating a write is harmless. */
    public static RetryOptions restartingFailed() {
        return new RetryOptions(From.CHUNK_START, Scope.FAILED, false);
    }

    /** Whether this retry re-writes records the original run had already written. */
    public boolean mayDuplicateCompletedWork() {
        return from == From.CHUNK_START;
    }

    public boolean includesCancelled() {
        return scope == Scope.FAILED_AND_CANCELLED;
    }
}
