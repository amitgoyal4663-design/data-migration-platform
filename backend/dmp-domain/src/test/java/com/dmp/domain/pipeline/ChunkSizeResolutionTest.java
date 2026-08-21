package com.dmp.domain.pipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How big a batch and a fetch actually end up.
 *
 * <p>The batch <b>is</b> the chunk. There is no separate write size, and there used to be: a
 * pipeline could be configured with a chunk of 100 and a batch of 1,000, whereupon it wrote one
 * batch of 100 and the batch size meant nothing. Two numbers describing one nested thing can only
 * agree by accident, and the one the user could see was the one that did not count.
 *
 * <p>What remains are ceilings rather than preferences — the sink's protocol limit and the byte
 * cap — plus one genuinely separate number, the read fetch, which is constrained by the source's
 * round trips and not by anything the destination cares about.
 *
 * <p>Where a destination needs smaller calls than a chunk, that is delivery's job: one call per
 * record, fixed groups, or a split script. These tests pin the sizes; {@code BatchDeliveryTest}
 * pins the division.
 */
class ChunkSizeResolutionTest {

    private static ChunkingPolicy policy(int readFetchSize) {
        return new ChunkingPolicy(readFetchSize, 8L * 1024 * 1024, Duration.ofSeconds(5), 2,
                ChunkingPolicy.CHECKPOINT_AUTO);
    }

    @Nested
    @DisplayName("the chunk is the batch")
    class ChunkIsTheBatch {

        @Test
        @DisplayName("a chunk of 500 writes batches of 500")
        void batchTakesTheChunk() {
            var sizes = policy(100).resolved(500, 0, 0);

            assertThat(sizes.writeBatchSize())
                    .as("nothing else to derive it from, and nothing else to disagree with")
                    .isEqualTo(500);
        }

        @Test
        @DisplayName("a chunk of one writes one record at a time")
        void aChunkOfOne() {
            var sizes = policy(100).resolved(1, 0, 0);

            assertThat(sizes.writeBatchSize()).isEqualTo(1);
            assertThat(sizes.readFetchSize())
                    .as("no point pulling a hundred rows over the network to use one")
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("a huge chunk still yields a usable batch, because bytes bound it later")
        void aHugeChunk() {
            var sizes = policy(1_000).resolved(10_000_000, 0, 0);

            assertThat(sizes.writeBatchSize())
                    .as("the record count is honoured here; maxBatchBytes stops it in the builder")
                    .isEqualTo(10_000_000);
            assertThat(sizes.maxBatchBytes())
                    .as("which is the number that actually bounds memory")
                    .isEqualTo(8L * 1024 * 1024);
        }
    }

    @Nested
    @DisplayName("what the sink says")
    class SinkLimits {

        @Test
        @DisplayName("a protocol limit lowers the batch below the chunk")
        void protocolLimitWins() {
            // Salesforce Bulk v2 refuses more than 10,000 records in one request. A chunk of
            // 50,000 must not be handed over whole and rejected mid-migration.
            var sizes = policy(1_000).resolved(50_000, 10_000, 0);

            assertThat(sizes.writeBatchSize()).isEqualTo(10_000);
        }

        @Test
        @DisplayName("a preference does not override a chunk the user sized")
        void preferenceDoesNotOverrideTheChunk() {
            var sizes = policy(100).resolved(200, 0, 5_000);

            assertThat(sizes.writeBatchSize())
                    .as("the chunk is the instruction; a preference is not a reason to ignore it")
                    .isEqualTo(200);
        }

        @Test
        @DisplayName("a preference is used when the chunk is a key range with no row count")
        void preferenceFillsTheGap() {
            // A planned chunk is a key range: it has no row count, so there is nothing to take the
            // batch from. This is the one place the sink's preference still decides.
            var sizes = policy(100).resolved(0, 0, 2_500);

            assertThat(sizes.writeBatchSize()).isEqualTo(2_500);
        }

        @Test
        @DisplayName("a key range with no preference falls back rather than guessing")
        void keyRangeWithoutPreference() {
            var sizes = policy(100).resolved(0, 0, 0);

            assertThat(sizes.writeBatchSize()).isEqualTo(1_000);
        }

        @Test
        @DisplayName("a protocol limit applies to a key range too")
        void protocolLimitOnAKeyRange() {
            var sizes = policy(100).resolved(0, 500, 5_000);

            assertThat(sizes.writeBatchSize()).isEqualTo(500);
        }
    }

    @Nested
    @DisplayName("the read fetch")
    class ReadFetch {

        @Test
        @DisplayName("stays independent of the batch, because a source constrains it")
        void independentOfTheBatch() {
            // The motivating case: 100 rows per JDBC round trip into a chunk of 10,000. The two
            // numbers answer to different systems and neither should follow the other.
            var sizes = policy(100).resolved(10_000, 0, 0);

            assertThat(sizes.readFetchSize()).isEqualTo(100);
            assertThat(sizes.writeBatchSize()).isEqualTo(10_000);
        }

        @Test
        @DisplayName("is clamped to the chunk, so no row is fetched to be thrown away")
        void clampedToTheChunk() {
            var sizes = policy(500).resolved(100, 0, 0);

            assertThat(sizes.readFetchSize()).isEqualTo(100);
        }

        @Test
        @DisplayName("automatic takes the platform default")
        void automaticTakesTheDefault() {
            var sizes = policy(ChunkingPolicy.READ_FETCH_AUTO).resolved(10_000, 0, 0);

            assertThat(sizes.readFetchSize()).isEqualTo(1_000);
        }

        @Test
        @DisplayName("the two automatic defaults do not refer to each other in a circle")
        void noCircularity() {
            // A chunk left automatic is sized from the read size, while a read size is capped by
            // the chunk. readFetchSizeOrDefault breaks the cycle by resolving one side alone.
            var policy = policy(ChunkingPolicy.READ_FETCH_AUTO);

            assertThat(policy.readFetchSizeOrDefault())
                    .as("resolvable without knowing the chunk")
                    .isEqualTo(1_000);
        }
    }

    @Nested
    @DisplayName("nothing ever resolves to zero")
    class NeverZero {

        @Test
        @DisplayName("a chunk of zero rows still produces workable sizes")
        void zeroChunk() {
            var sizes = policy(0).resolved(0, 0, 0);

            assertThat(sizes.writeBatchSize()).isPositive();
            assertThat(sizes.readFetchSize()).isPositive();
        }

        @Test
        @DisplayName("a sink limit of zero means no limit, not a batch of nothing")
        void zeroSinkLimitMeansNoLimit() {
            var sizes = policy(100).resolved(700, 0, 0);

            assertThat(sizes.writeBatchSize())
                    .as("a batch of zero would be an infinite loop writing nothing")
                    .isEqualTo(700);
        }
    }
}
