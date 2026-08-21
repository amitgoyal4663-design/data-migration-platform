package com.dmp.domain.pipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The rule that separates one bad row from a broken pipeline.
 *
 * <p>Both arrive as rejected records. Without a threshold both are treated as the first: the chunk
 * completes, the run completes, and a migration that wrote nothing reports success. A green run
 * that moved no data is worse than a red one, because nobody investigates it.
 */
class RejectionThresholdTest {

    private static ExecutionPolicy failingAbove(Integer percent, Long records) {
        return ExecutionPolicy.DEFAULT.failingAbove(percent, records);
    }

    @Test
    @DisplayName("a pipeline stored before this setting existed keeps its old behaviour")
    void storedPipelinesAreUnaffected() {
        // Such a version deserialises with zero here, because a JSON document without the field
        // yields zero. If zero meant anything other than "no limit", upgrading the platform would
        // start failing runs that had been succeeding for months.
        ExecutionPolicy stored = failingAbove(null, null);

        assertThat(stored.hasRejectionLimit()).isFalse();
        assertThat(stored.rejectionBreach(20_000, 20_000, true)).isEmpty();
    }

    @Test
    @DisplayName("a pipeline created now catches a total failure without being configured")
    void newPipelinesCatchTotalFailure() {
        // The case this whole mechanism exists for: everything rejected, nothing written, and the
        // run reporting COMPLETED. Leaving that as the out-of-the-box behaviour would mean the
        // protection only ever helped people who already knew to go and switch it on.
        assertThat(ExecutionPolicy.DEFAULT.rejectionBreach(20_000, 20_000, true)).isPresent();

        // And it cannot misfire: no legitimate pipeline rejects literally every record.
        assertThat(ExecutionPolicy.DEFAULT.rejectionBreach(20_000, 19_999, true)).isEmpty();
    }

    @Test
    @DisplayName("zero means what it says: any rejection at all fails the chunk")
    void zeroPercentIsStrictestNotAbsent() {
        // The distinction that made this worth changing. Zero once meant "no limit", so the user
        // who typed the strictest value they could think of got no protection whatsoever.
        assertThat(failingAbove(0, null).rejectionBreach(1_000_000, 1, true)).isPresent();
        assertThat(failingAbove(0, null).rejectionBreach(1_000_000, 0, true)).isEmpty();
    }

    @Test
    @DisplayName("one bad row in a million does not fail the chunk")
    void toleratesIsolatedRejections() {
        assertThat(failingAbove(50, null).rejectionBreach(1_000_000, 1, true)).isEmpty();
    }

    @Test
    @DisplayName("a chunk rejecting everything fails, and says so in records rather than ratios")
    void systematicFailureIsCaught() {
        assertThat(failingAbove(50, null).rejectionBreach(20_000, 20_000, false))
                .hasValueSatisfying(reason -> assertThat(reason)
                        .contains("20000 of 20000")
                        .contains("100%")
                        .contains("50%"));
    }

    @Test
    @DisplayName("the percentage is not believed until enough records have been processed")
    void smallSamplesAreNotJudgedMidFlight() {
        // Three rejections out of the first five records is sixty percent. Acting on it would kill
        // a chunk of a million good rows on its opening sample.
        assertThat(failingAbove(50, null).rejectionBreach(5, 3, false)).isEmpty();
    }

    @Test
    @DisplayName("but a small chunk is judged once it finishes")
    void smallChunksAreJudgedAtTheEnd() {
        // Otherwise a forty-record chunk that rejected all forty slips past the limit purely for
        // being small — the sample floor would become an exemption instead of a delay.
        assertThat(failingAbove(50, null).rejectionBreach(5, 3, true)).isPresent();
    }

    @Test
    @DisplayName("the absolute limit applies before the sample floor is reached")
    void absoluteLimitNeedsNoSample() {
        // The floor exists because small denominators make percentages unreliable. A count has no
        // denominator, so nothing about it is unreliable at small numbers.
        assertThat(failingAbove(null, 3L).rejectionBreach(5, 3, false))
                .hasValueSatisfying(reason -> assertThat(reason).contains("limit of 3"));
    }

    @Test
    @DisplayName("no rejections is never a breach, whatever the limits say")
    void zeroFailuresNeverBreaches() {
        assertThat(failingAbove(1, 1L).rejectionBreach(1_000, 0, true)).isEmpty();
    }

    @Test
    @DisplayName("a hundred percent means only a total failure trips it")
    void hundredPercentCatchesOnlyTotalFailure() {
        // Worth keeping expressible: "let anything through unless literally nothing landed" is the
        // setting most pipelines actually want, and it is not the same as switching the check off.
        assertThat(failingAbove(100, null).rejectionBreach(20_000, 19_999, true)).isEmpty();
        assertThat(failingAbove(100, null).rejectionBreach(20_000, 20_000, true)).isPresent();
    }

    @Test
    @DisplayName("a resumed chunk is judged on everything it has done, not this attempt alone")
    void cumulativeAcrossResumes() {
        // The counts come from the checkpoint, which survives the crash. A chunk that rejected
        // 9,000 before dying and 1,000 after has rejected 10,000, and judging only the second
        // attempt would let it retry its way past any limit.
        assertThat(failingAbove(50, null).rejectionBreach(20_000, 10_000, false)).isPresent();
    }
}
