package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for fleet-wide concurrency control. */
class ExecutionPolicyTest {

    @Test
    @DisplayName("treats zero as unlimited and skips slot reservation entirely")
    void unlimitedSkipsReservation() {
        // The common case must not pay for a feature it does not use: an unlimited run performs
        // no reservation round trip before claiming a chunk.
        assertThat(ExecutionPolicy.DEFAULT.isUnlimited()).isTrue();
        assertThat(ExecutionPolicy.DEFAULT.requiresSlotReservation()).isFalse();
    }

    @Test
    @DisplayName("recognises a limit of one as sequential")
    void oneIsSequential() {
        var policy = ExecutionPolicy.sequential();

        assertThat(policy.isSequential()).isTrue();
        assertThat(policy.maxConcurrentChunks()).isEqualTo(1);
        assertThat(policy.requiresSlotReservation()).isTrue();
    }

    @Test
    @DisplayName("caps per-pod concurrency at the fleet limit when that is lower")
    void perPodNeverExceedsFleetLimit() {
        // A pod allowed 8 concurrent chunks on a run limited to 2 would waste reservation attempts
        // it can never win.
        var policy = ExecutionPolicy.limitedTo(2);

        assertThat(policy.maxConcurrentChunks()).isEqualTo(2);
        assertThat(policy.maxChunksPerPod()).isEqualTo(2);
    }

    @Test
    @DisplayName("leaves per-pod concurrency alone when the fleet limit is higher")
    void perPodStaysAtDefaultForLargeLimits() {
        var policy = ExecutionPolicy.limitedTo(100);

        assertThat(policy.maxChunksPerPod()).isEqualTo(ExecutionPolicy.DEFAULT.maxChunksPerPod());
    }

    @Test
    @DisplayName("heartbeats at a third of the lease, so two missed beats are survivable")
    void heartbeatIsAThirdOfTheLease() {
        // A worker that misses one heartbeat to a garbage-collection pause must not lose a chunk
        // it is actively processing.
        var policy = new ExecutionPolicy(0, 8, Duration.ofMinutes(6), 5, ExecutionPolicy.ROWS_PER_CHUNK_AUTO, null, null, false);

        assertThat(policy.heartbeatInterval()).isEqualTo(Duration.ofMinutes(2));
    }

    @Test
    @DisplayName("rejects a lease too short to heartbeat against")
    void rejectsTooShortLease() {
        assertThatThrownBy(() -> new ExecutionPolicy(0, 8, Duration.ofSeconds(5), 5, ExecutionPolicy.ROWS_PER_CHUNK_AUTO, null, null, false))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("chunkLease");
    }

    @Test
    @DisplayName("rejects a negative concurrency limit")
    void rejectsNegativeLimit() {
        assertThatThrownBy(() -> new ExecutionPolicy(-1, 8, Duration.ofMinutes(5), 5,
                ExecutionPolicy.ROWS_PER_CHUNK_AUTO, null, null, false))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("maxConcurrentChunks");
    }

    @Test
    @DisplayName("rejects a per-pod limit of zero, which would let a pod claim nothing")
    void rejectsZeroPerPod() {
        assertThatThrownBy(() -> new ExecutionPolicy(0, 0, Duration.ofMinutes(5), 5, ExecutionPolicy.ROWS_PER_CHUNK_AUTO, null, null, false))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("maxChunksPerPod");
    }

    @Test
    @DisplayName("rejects a retry budget of zero, which would abandon on first failure")
    void rejectsZeroAttempts() {
        assertThatThrownBy(() -> new ExecutionPolicy(0, 8, Duration.ofMinutes(5), 0, ExecutionPolicy.ROWS_PER_CHUNK_AUTO, null, null, false))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("maxAttemptsPerChunk");
    }

    @Test
    @DisplayName("gives a sequential run a long lease, because ordered work is often slow work")
    void sequentialUsesALongLease() {
        // A sequential pipeline is typically sequential because each chunk is heavy — a bulk load
        // against a lock-contended object. A short lease would have the sweep reclaim chunks that
        // are progressing perfectly well.
        assertThat(ExecutionPolicy.sequential().chunkLease()).isEqualTo(Duration.ofMinutes(30));
    }
}
