package com.dmp.domain.pipeline;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Zero means zero.
 *
 * <p>The proportional threshold has a sample floor so a small opening prefix cannot produce a
 * misleading ratio. That floor was applied to a zero limit as well, which made the strictest
 * setting the platform offers tolerate up to ninety-nine rejected records before reacting — while
 * the console described it as failing on a single one.
 */
class RejectionZeroTest {

    private static final ExecutionPolicy ANY_REJECTION_FAILS =
            ExecutionPolicy.DEFAULT.failingAbove(0, null);

    @Test
    void firstRejectedRecordFailsTheChunk() {
        assertThat(ANY_REJECTION_FAILS.rejectionBreach(1, 1, false))
                .as("a single rejection in the first batch must fail the chunk immediately")
                .isPresent();
    }

    @Test
    void doesNotWaitForTheSampleFloor() {
        assertThat(ANY_REJECTION_FAILS.rejectionBreach(50, 50, false))
                .as("fifty of fifty rejected must not be tolerated while the sample floor is unmet")
                .isPresent();
    }

    @Test
    void aCleanChunkIsUntouched() {
        assertThat(ANY_REJECTION_FAILS.rejectionBreach(1_000, 0, true)).isEmpty();
    }

    @Test
    void aProportionalLimitStillWaitsForEnoughRecords() {
        ExecutionPolicy halfMayFail = ExecutionPolicy.DEFAULT.failingAbove(50, null);

        assertThat(halfMayFail.rejectionBreach(5, 3, false))
                .as("three of the first five records must not kill a chunk of a million")
                .isEmpty();
        assertThat(halfMayFail.rejectionBreach(200, 120, false))
                .as("past the floor, the ratio is believed")
                .isPresent();
    }
}
