package com.dmp.connector.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What one chunk costs the far end, for the purpose of a limit somebody agreed to.
 *
 * <p>Two answers, because there turned out to be exactly two kinds of destination: one where a call
 * is a request, and one where a call is a job.
 */
class CallCostTest {

    @Test
    @DisplayName("an ordinary destination takes the delivery policy's word for it")
    void perRequestFollowsDelivery() {
        // 500 records in groups of three is the 167 a real run charged, and the number this exists
        // to keep correct.
        assertThat(CallCost.PER_REQUEST.callsFor(167)).isEqualTo(167);
        assertThat(CallCost.PER_REQUEST.callsFor(1)).isEqualTo(1);
        assertThat(CallCost.PER_REQUEST.callsFor(500)).isEqualTo(500);
    }

    @Test
    @DisplayName("a destination whose chunk is a job costs one, however many requests that takes")
    void perChunkIsAlwaysOne() {
        // Salesforce: create, upload, upload-complete, poll until done, fetch counts. The poll
        // count depends on how busy the org is, so billing per request would make the identical
        // migration cost different amounts on different mornings.
        assertThat(CallCost.PER_CHUNK.callsFor(1)).isEqualTo(1);
        assertThat(CallCost.PER_CHUNK.callsFor(10_000))
                .as("delivery cannot subdivide a job either")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("saying nothing means one call per request")
    void theDefaultIsTheOrdinaryCase() {
        assertThat(new ConnectorSpec("some-connector", "Some connector", "…",
                ConnectorSpec.Direction.SINK, null, null, "1.0.0").callCost())
                .isEqualTo(CallCost.PER_REQUEST);
    }
}
