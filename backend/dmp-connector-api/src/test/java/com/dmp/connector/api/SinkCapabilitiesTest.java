package com.dmp.connector.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Idempotency is the one property the engine needs from a sink, and the only one it may reason
 * about generically.
 *
 * <p>These tests exist because the alternative — describing the property in the vocabulary of
 * whichever sink was written first — produced an engine that asked databases about upserts and had
 * nothing coherent to ask an object store, a queue or a file.
 */
class SinkCapabilitiesTest {

    private static Sink.Capabilities notIdempotent(String advice) {
        return new Sink.Capabilities(false, advice, false, false, false, false, 0, 500);
    }

    @Test
    @DisplayName("a sink that is not idempotent must say how to fix it, or why it cannot be")
    void adviceIsMandatoryWhenNotIdempotent() {
        // The engine knows only the boolean. If the connector declines to word the remedy, the
        // user gets a warning that names a problem and no way to act on it — and the engine
        // cannot fill the gap, because "use upsert mode" is nonsense for an append-only file.
        assertThatThrownBy(() -> notIdempotent(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("how to make them so");

        assertThatThrownBy(() -> notIdempotent("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("an idempotent sink needs no advice")
    void adviceIsNotRequiredWhenIdempotent() {
        assertThatCode(() -> Sink.Capabilities.idempotent(500)).doesNotThrowAnyException();
        assertThat(Sink.Capabilities.idempotent(500).advice()).isEmpty();
    }

    @Test
    @DisplayName("advice on an idempotent sink is dropped rather than carried")
    void adviceIsClearedWhenIdempotent() {
        // Otherwise a connector that flips to upsert mode but keeps building the same string leaves
        // a remedy hanging on a sink that has nothing to remedy, and the console displays it.
        Sink.Capabilities capabilities =
                new Sink.Capabilities(true, "switch to upsert", false, false, false, false, 0, 500);

        assertThat(capabilities.advice()).isEmpty();
    }

    @Test
    @DisplayName("transactional is not idempotent, and does not stand in for it")
    void transactionalIsNotIdempotent() {
        // The distinction that cost twenty-five thousand rows: a batch of inserts that commits
        // atomically, and is re-sent because the chunk resumed behind it, duplicates exactly as a
        // non-atomic batch would. Atomicity bounds a batch; it says nothing about repeating one.
        Sink.Capabilities atomicInserts = new Sink.Capabilities(
                false, "set write mode to UPSERT", true, false, false, false, 0, 500);

        assertThat(atomicInserts.writeIsIdempotent()).isFalse();
        assertThat(atomicInserts.deliveryGuarantee()).contains("may duplicate records");
    }

    @Test
    @DisplayName("the delivery guarantee is stated without naming any sink's mechanism")
    void guaranteeIsVocabularyNeutral() {
        // Read by the console against every sink there is. A guarantee phrased as "duplicates
        // collapse on the natural key" is unreadable for an S3 bucket or a compacted topic.
        String exactlyOnce = new Sink.Capabilities(true, null, true, false, false, false, 0, 500)
                .deliveryGuarantee();
        String effectivelyOnce = Sink.Capabilities.idempotent(500).deliveryGuarantee();
        String atLeastOnce = notIdempotent("append-only, nothing to overwrite").deliveryGuarantee();

        assertThat(exactlyOnce).contains("Exactly-once");
        assertThat(effectivelyOnce).contains("once");
        assertThat(atLeastOnce).contains("At-least-once");

        for (String guarantee : new String[] {exactlyOnce, effectivelyOnce, atLeastOnce}) {
            assertThat(guarantee.toLowerCase())
                    .doesNotContain("upsert")
                    .doesNotContain("insert")
                    .doesNotContain("topic")
                    .doesNotContain("row");
        }
    }
}
