package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for flow-control sizing (ADR-0009). */
class ChunkingPolicyTest {

    @Test
    @DisplayName("derives a memory ceiling an operator can size a worker against")
    void peakHeapIsCalculable() {
        // Record count alone gives no memory bound — a thousand records may be 1 KB or 10 MB.
        // maxBatchBytes is what makes this number exist at all.
        var policy = new ChunkingPolicy(500, 8L * 1024 * 1024, Duration.ofSeconds(5), 4, ChunkingPolicy.CHECKPOINT_AUTO);

        assertThat(policy.estimatedPeakHeapBytes()).isEqualTo(32L * 1024 * 1024);
    }

    @Test
    @DisplayName("a sink's protocol limit lowers the batch, and nothing else does")
    void sinkLimitIsTheOnlyCeilingAboveBytes() {
        var sizes = ChunkingPolicy.DEFAULT.resolved(50_000, 10_000, 0);

        assertThat(sizes.writeBatchSize())
                .as("a bulk API that refuses more than 10,000 is not handed 50,000")
                .isEqualTo(10_000);
        assertThat(ChunkingPolicy.DEFAULT.resolved(50_000, 0, 0).writeBatchSize())
                .as("and with no declared limit the chunk stands")
                .isEqualTo(50_000);
    }

    @Test
    @DisplayName("rejects a zero flush interval")
    void rejectsZeroFlushInterval() {
        // Without a linger, a low-volume stream would hold records indefinitely waiting for the
        // chunk to fill, and would never checkpoint.
        assertThatThrownBy(() -> new ChunkingPolicy(500, 8L * 1024 * 1024, Duration.ZERO, 2,
                ChunkingPolicy.CHECKPOINT_AUTO))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("flushInterval");
    }

    @Test
    @DisplayName("rejects sizes outside safe bounds")
    void rejectsOutOfRangeValues() {
        // Zero is not out of range on the read size: it means "take the platform default".
        // Negative still is.
        assertThatThrownBy(() -> new ChunkingPolicy(-1, 8L * 1024 * 1024, Duration.ofSeconds(5), 2, ChunkingPolicy.CHECKPOINT_AUTO))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("readFetchSize");

        assertThatThrownBy(() -> new ChunkingPolicy(500, 1_024, Duration.ofSeconds(5), 2, ChunkingPolicy.CHECKPOINT_AUTO))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("maxBatchBytes");

        assertThatThrownBy(() -> new ChunkingPolicy(500, 8L * 1024 * 1024, Duration.ofSeconds(5), 0, ChunkingPolicy.CHECKPOINT_AUTO))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("maxInFlightBatches");
    }

    @Test
    @DisplayName("bounds the default's memory footprint to something a worker can hold")
    void defaultIsModest() {
        assertThat(ChunkingPolicy.DEFAULT.estimatedPeakHeapBytes())
                .isLessThanOrEqualTo(32L * 1024 * 1024);
    }
}
