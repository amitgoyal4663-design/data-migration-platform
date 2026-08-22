package com.dmp.engine;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Engine metrics")
class EngineMetricsTest {

    @Test
    @DisplayName("carries no identifier as a tag, whatever it is given")
    void tagsAreBounded() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EngineMetrics metrics = new EngineMetrics(registry);

        metrics.records("read", "databricks", 1000);
        metrics.sinkCall("salesforce", Duration.ofMillis(120), true);
        metrics.chunk("completed", Duration.ofSeconds(4));

        // The rule this exists to protect: a run, chunk or pipeline id as a tag is a time series
        // per chunk, which is millions of series to express what the stage log already holds
        // properly. If a future meter adds one, this fails.
        assertThat(registry.getMeters())
                .flatExtracting(meter -> meter.getId().getTags())
                .extracting(io.micrometer.core.instrument.Tag::getKey)
                .containsOnly("stage", "connector", "outcome");
    }

    @Test
    @DisplayName("separates written from refused, so the gap between them is graphable")
    void countsRecordsByStage() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EngineMetrics metrics = new EngineMetrics(registry);

        metrics.records("read", "databricks", 1000);
        metrics.records("written", "rest", 940);
        metrics.records("refused", "rest", 60);

        assertThat(registry.get("dmp.records").tag("stage", "read").counter().count())
                .isEqualTo(1000);
        assertThat(registry.get("dmp.records").tag("stage", "written").counter().count())
                .isEqualTo(940);
        // A pipeline reading at full speed and writing nothing looks healthy on every other
        // measure, which is why these are separate series rather than one net number.
        assertThat(registry.get("dmp.records").tag("stage", "refused").counter().count())
                .isEqualTo(60);
    }

    @Test
    @DisplayName("does not record a count of nothing")
    void ignoresZero() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new EngineMetrics(registry).records("written", "rest", 0);

        // A meter that exists with a value of zero and one that was never touched mean different
        // things on a dashboard, and only the second is honest about a stage that did not run.
        assertThat(registry.find("dmp.records").counter()).isNull();
    }

    @Test
    @DisplayName("times a failed destination call as well as a successful one")
    void timesBothOutcomes() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EngineMetrics metrics = new EngineMetrics(registry);

        metrics.sinkCall("rest", Duration.ofMillis(50), true);
        metrics.sinkCall("rest", Duration.ofSeconds(30), false);

        // How long a failure took to fail is the shape of an outage, and averaging it into the
        // successes would hide both.
        assertThat(registry.get("dmp.sink.call").tag("outcome", "ok").timer().count()).isEqualTo(1);
        assertThat(registry.get("dmp.sink.call").tag("outcome", "failed").timer()
                .totalTime(java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(30);
    }

    @Test
    @DisplayName("tracks chunks in flight up and back down again")
    void gaugesInFlight() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EngineMetrics metrics = new EngineMetrics(registry);

        try (AutoCloseable first = metrics.inFlight()) {
            try (AutoCloseable second = metrics.inFlight()) {
                assertThat(registry.get("dmp.chunks.inflight").gauge().value()).isEqualTo(2);
            }
            assertThat(registry.get("dmp.chunks.inflight").gauge().value()).isEqualTo(1);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertThat(registry.get("dmp.chunks.inflight").gauge().value()).isZero();
    }

    @Test
    @DisplayName("does nothing at all when nothing is measuring")
    void theNoOpIsSafe() {
        // Every engine constructor falls back to this, so it has to be callable everywhere without
        // a registry. Instrumentation must never be the reason a component is harder to construct.
        EngineMetrics none = EngineMetrics.NONE;

        none.records("read", "databricks", 10);
        none.sinkCall("rest", Duration.ofMillis(1), false);
        none.sourceFetch("databricks", Duration.ofMillis(1), true);
        none.chunk("completed", Duration.ofSeconds(1));
        none.leaseLost();
        none.deferred("rate_limit");

        try (AutoCloseable inFlight = none.inFlight()) {
            assertThat(inFlight).isNotNull();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
