package com.dmp.engine;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What the engine is doing, as numbers a dashboard can graph.
 *
 * <p>The platform already writes three stores — what happened to each record, what the platform
 * did, and the narration. All three answer questions about <em>one</em> run, asked after the fact by
 * somebody who has a run id. None of them answer "is the fleet keeping up", "is that destination
 * slower than it was last week", or "are we losing leases", which are the questions asked without a
 * run id and usually before anybody knows there is a problem.
 *
 * <p><b>No run, chunk, pipeline or record identifiers as tags.</b> Every one of them is unbounded,
 * and a time series per chunk would put millions of series into Prometheus to express something the
 * stage log already holds properly. The tags here are connector type and outcome — both small,
 * fixed sets — which is what makes these safe to leave on in production. Anything needing an id
 * belongs in OpenSearch, where an id is a field rather than a dimension.
 */
@Component
public class EngineMetrics {

    /**
     * Stands in when nothing is measuring.
     *
     * <p>Exists so the engine's constructors are unchanged for the tests and paths that build one
     * directly. Instrumentation must never be the reason a component becomes harder to construct,
     * because the next person will skip it.
     */
    static final EngineMetrics NONE = new EngineMetrics(null);

    private final MeterRegistry registry;
    private final AtomicInteger chunksInFlight = new AtomicInteger();

    public EngineMetrics(MeterRegistry registry) {
        this.registry = registry;
        if (registry != null) {
            // A gauge rather than a counter: the question is how many are running now, and the
            // answer has to be able to go down.
            registry.gauge("dmp.chunks.inflight", chunksInFlight, AtomicInteger::get);
        }
    }

    /**
     * Records moving, by the stage that saw them.
     *
     * <p>{@code read} against {@code written} over time is the throughput graph; the gap between
     * them is the one worth alerting on, because a pipeline reading at full speed and writing
     * nothing looks healthy on every other measure.
     */
    void records(String stage, String connectorType, long count) {
        if (registry == null || count <= 0) {
            return;
        }
        Counter.builder("dmp.records")
                .tag("stage", stage)
                .tag("connector", connectorType == null ? "unknown" : connectorType)
                .register(registry)
                .increment(count);
    }

    /**
     * One call on a destination, and how it went.
     *
     * <p>Timed rather than counted, because the number that matters is how long somebody else's
     * system took. A destination degrading is the commonest cause of a migration missing its
     * window, and it is invisible in a per-record store.
     */
    void sinkCall(String connectorType, Duration took, boolean succeeded) {
        if (registry == null) {
            return;
        }
        Timer.builder("dmp.sink.call")
                .tag("connector", connectorType == null ? "unknown" : connectorType)
                .tag("outcome", succeeded ? "ok" : "failed")
                .register(registry)
                .record(took);
    }

    /** One real call a source made, as the connector reported it. */
    void sourceFetch(String connectorType, Duration took, boolean succeeded) {
        if (registry == null) {
            return;
        }
        Timer.builder("dmp.source.fetch")
                .tag("connector", connectorType == null ? "unknown" : connectorType)
                .tag("outcome", succeeded ? "ok" : "failed")
                .register(registry)
                .record(took);
    }

    /** A whole chunk, from claim to terminal state. */
    void chunk(String outcome, Duration took) {
        if (registry == null) {
            return;
        }
        Timer.builder("dmp.chunk")
                .tag("outcome", outcome)
                .register(registry)
                .record(took);
    }

    /**
     * A worker discovered another worker had taken its chunk.
     *
     * <p>Deserves its own meter because it is silent everywhere else and it means the lease is too
     * short for the work — which, left alone, produces duplicate writes rather than an error. A
     * non-zero rate here is a configuration problem, not a transient.
     */
    void leaseLost() {
        if (registry != null) {
            registry.counter("dmp.lease.lost").increment();
        }
    }

    /** A chunk turned away at the rate-limit gate, before it read anything. */
    void deferred(String reason) {
        if (registry != null) {
            registry.counter("dmp.chunks.deferred", "reason", reason).increment();
        }
    }

    AutoCloseable inFlight() {
        chunksInFlight.incrementAndGet();
        return chunksInFlight::decrementAndGet;
    }
}
