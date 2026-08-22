package com.dmp.engine;

import com.dmp.connector.api.ConnectorException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which failures are worth attempting again.
 *
 * <p>Getting this wrong is expensive in both directions: retrying a wrong password four times only
 * delays the message the user needs, and abandoning a chunk after one network blip fails a run that
 * would have finished.
 */
class ChunkFailureClassificationTest {

    @Test
    @DisplayName("a rejection threshold is never retried")
    void thresholdBreachIsNotRetryable() {
        // The records that tripped it will be rejected identically next time — nothing about
        // re-sending twenty thousand rows to the same target changes their fate, and against a
        // system that meters bulk jobs it burns quota to learn nothing.
        RejectionThresholdExceededException breach =
                new RejectionThresholdExceededException(7, 20_000, 20_000, "everything was rejected");

        assertThat(WorkerLoop.isRetryable(breach)).isFalse();
        assertThat(WorkerLoop.errorCodeFor(breach)).isEqualTo("REJECTION_THRESHOLD_EXCEEDED");
    }

    @Test
    @DisplayName("a threshold failure names the chunk and the counts that caused it")
    void breachCarriesItsEvidence() {
        RejectionThresholdExceededException breach = new RejectionThresholdExceededException(
                7, 20_000, 19_998, "19998 of 20000 record(s) were rejected (99%)");

        assertThat(breach.chunkIndex()).isEqualTo(7);
        assertThat(breach.produced()).isEqualTo(20_000);
        assertThat(breach.failed()).isEqualTo(19_998);
        assertThat(breach.getMessage()).contains("Chunk 7").contains("99%").contains("not retried");
    }

    @Test
    @DisplayName("an unreachable system is retried; a bad configuration is not")
    void connectorFailuresFollowTheirOwnKind() {
        assertThat(WorkerLoop.isRetryable(
                new ConnectorException(ConnectorException.Kind.UNAVAILABLE, "timed out"))).isTrue();
        assertThat(WorkerLoop.isRetryable(
                new ConnectorException(ConnectorException.Kind.RATE_LIMITED, "429"))).isTrue();
        assertThat(WorkerLoop.isRetryable(
                new ConnectorException(ConnectorException.Kind.AUTHENTICATION, "bad password")))
                .isFalse();
        assertThat(WorkerLoop.isRetryable(
                new ConnectorException(ConnectorException.Kind.CONFIGURATION, "no such table")))
                .isFalse();
    }

    @Test
    @DisplayName("a platform failure is asked for its own verdict rather than assumed transient")
    void platformFailuresFollowTheirErrorCode() {
        // Found by running one: a chunk needing 167 calls against a limit of 5 was attempted five
        // times before being abandoned. No amount of retrying makes 167 fit into 5, and each
        // attempt pushed the message that says exactly what to change further up the log.
        var impossible = new com.dmp.common.error.DmpException(
                com.dmp.common.error.ErrorCode.VALIDATION_FAILED,
                "A chunk of 167 call(s) cannot be sent under this connector's rate limit");

        assertThat(WorkerLoop.isRetryable(impossible)).isFalse();
        assertThat(WorkerLoop.errorCodeFor(impossible)).isEqualTo("VALIDATION_FAILED");

        // The flag is the error code's to decide, not this method's. A contended row really is
        // worth another attempt, and the same switch arm has to reach the opposite conclusion.
        var contended = new com.dmp.common.error.DmpException(
                com.dmp.common.error.ErrorCode.CONCURRENT_MODIFICATION, "someone else got there first");

        assertThat(WorkerLoop.isRetryable(contended)).isTrue();
    }

    @Test
    @DisplayName("an unclassified failure gets another attempt")
    void unknownFailuresAreRetried() {
        // The safer default of the two: a wasted attempt costs one chunk's work, while wrongly
        // abandoning costs the run.
        assertThat(WorkerLoop.isRetryable(new IllegalStateException("something odd"))).isTrue();
        assertThat(WorkerLoop.errorCodeFor(new IllegalStateException("odd")))
                .isEqualTo("CHUNK_FAILED");
    }
}
