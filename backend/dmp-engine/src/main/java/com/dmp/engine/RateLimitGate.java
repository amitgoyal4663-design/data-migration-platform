package com.dmp.engine;

import com.dmp.application.port.out.RateLimiter;
import com.dmp.connector.api.CallCost;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.pipeline.DeliveryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * Asks, once, whether a chunk may run — before it has read a row.
 *
 * <p>Everything a chunk will spend at both ends is reserved here, at the door. A chunk that gets
 * past this runs straight through and never pauses for budget; a chunk that does not has opened no
 * cursor, buffered no records and holds no lease, so being told to come back later costs nothing.
 *
 * <p>The alternative — checking as records flow — was considered and is worse in every direction.
 * It puts a Redis round trip on the hottest path in the engine, it leaves a source cursor open
 * across a wait long enough for the server to close it, and it strands a half-read batch that has
 * to be discarded and re-read. Reserving once trades a small over-estimate for none of that.
 *
 * <p><b>The reservation is an over-estimate on the last chunk of a run,</b> which asks for a full
 * chunk's worth and may find fewer rows. The unused tokens are not returned. That is the safe
 * direction — under-sending, once per run, by less than one chunk — and the alternative is a refund
 * path that cannot tell "we sent less than planned" from "we sent it and lost the response".
 */
final class RateLimitGate {

    private static final Logger log = LoggerFactory.getLogger(RateLimitGate.class);

    private final RateLimiter limiter;
    private final ConnectorRegistry connectors;

    RateLimitGate(RateLimiter limiter, ConnectorRegistry connectors) {
        this.limiter = limiter;
        this.connectors = connectors;
    }

    /**
     * Reserves both ends of the pipeline for one chunk.
     *
     * <p>Ends are taken one at a time, and when both are limited the first may be spent on a chunk
     * the second then refuses. That costs one chunk's budget at one end, it is self-correcting — the
     * chunk comes back and the tokens have refilled — and it is far cheaper than the alternative,
     * which is a two-endpoint distributed transaction to move two counters that belong to two
     * different clients. In practice one end is limited and the other is not, so only one is asked.
     *
     * @param settlingRemoteJob whether this pass is picking up a destination's verdict on records
     *                          already delivered, rather than moving records. Such a pass reads
     *                          nothing and sends nothing; it costs one request and no records.
     * @return empty when the chunk may proceed, or how long until it could
     */
    Optional<Duration> reserve(ResolvedPipeline pipeline, boolean settlingRemoteJob) {
        ConnectorInstance source = pipeline.sourceInstance();
        ConnectorInstance sink = pipeline.sinkInstance();

        if (source.rateLimit().isUnlimited() && sink.rateLimit().isUnlimited()) {
            return Optional.empty();
        }

        long records = settlingRemoteJob ? 0 : recordsFor(pipeline);
        long sinkCalls = settlingRemoteJob ? 0 : sinkCallsFor(pipeline, sink, records);

        if (settlingRemoteJob) {
            // Picking up a verdict on records delivered earlier. The chunk that delivered them was
            // charged for the whole job, and charging again here would bill the same work twice —
            // once for doing it and once for asking how it went.
            return Optional.empty();
        }

        Optional<Duration> sinkWait =
                limiter.tryAcquire(sink.id(), sink.rateLimit(), records, sinkCalls);
        if (sinkWait.isPresent()) {
            log.info("Chunk held back {}s: {} allows {} and its budget is spent",
                    sinkWait.get().toSeconds(), describe(sink), sink.rateLimit().describe());
            return sinkWait;
        }

        Optional<Duration> sourceWait = limiter.tryAcquire(source.id(), source.rateLimit(),
                records, sourceCallsFor(pipeline, records));
        if (sourceWait.isPresent()) {
            log.info("Chunk held back {}s: {} allows {} and its budget is spent{}",
                    sourceWait.get().toSeconds(), describe(source), source.rateLimit().describe(),
                    sink.rateLimit().isUnlimited() ? ""
                            : " (the destination's budget for this chunk was already taken and "
                                    + "will not be used until the chunk runs)");
        }
        return sourceWait;
    }

    /**
     * Gives back the budget a completed chunk reserved and did not use.
     *
     * <p>The reservation is recomputed rather than remembered, because it is pure arithmetic over
     * the pipeline and produces the same numbers it produced at the door. Carrying it on the chunk
     * would mean persisting it, and persisting a number that can be derived is a second copy to
     * keep in step.
     *
     * <p>Only for a chunk that finished. A failed chunk's boundary between sent and not-sent is
     * unknown, and the safe reading of unknown is that all of it was spent.
     */
    void settle(ResolvedPipeline pipeline, ChunkResult result) {
        ConnectorInstance source = pipeline.sourceInstance();
        ConnectorInstance sink = pipeline.sinkInstance();

        if (source.rateLimit().isUnlimited() && sink.rateLimit().isUnlimited()) {
            return;
        }

        long reservedRecords = recordsFor(pipeline);

        // What the destination was actually handed — after a filter dropped records or a fan-out
        // multiplied them — rather than what was read. It is the sink's budget being settled.
        long sentRecords = Math.min(reservedRecords, result.recordsProduced());

        limiter.returnUnused(sink.id(), sink.rateLimit(),
                reservedRecords, sinkCallsFor(pipeline, sink, reservedRecords),
                sentRecords, result.sinkCalls());

        limiter.returnUnused(source.id(), source.rateLimit(),
                reservedRecords, sourceCallsFor(pipeline, reservedRecords),
                Math.min(reservedRecords, result.recordsRead()),
                sourceCallsFor(pipeline, result.recordsRead()));
    }

    /**
     * How many records this chunk expects to move.
     *
     * <p>The configured chunk size, not the actual row count, because the row count is not known
     * until the chunk has been read — and reading it is the thing being asked permission for.
     */
    static long recordsFor(ResolvedPipeline pipeline) {
        return pipeline.execution().effectiveRowsPerChunk(pipeline.chunking().readFetchSizeOrDefault());
    }

    /**
     * How many requests this chunk will make of its destination.
     *
     * <p>One, normally, because the chunk <em>is</em> the batch. Delivery is the exception: a
     * pipeline that calls the destination once per record, or in fixed groups, makes that many
     * requests out of the same records — which is precisely the case a records-only limit cannot
     * see and a calls limit exists for.
     *
     * <p>A split script is counted as one call per record, the worst case, since only the script
     * knows how many groups it will produce and guessing low would overspend the client's quota.
     */
    long sinkCallsFor(ResolvedPipeline pipeline, ConnectorInstance sink, long records) {
        return costOf(sink).callsFor(deliveryCallsFor(pipeline, records));
    }

    /**
     * What the connector says a chunk costs it, or one call per delivery group if it says nothing.
     *
     * <p>Asked of the connector rather than decided here. A destination where one chunk is one
     * <em>job</em> — created, uploaded, polled an unpredictable number of times, then harvested —
     * is charged once, because the number of polls describes how busy the far end is rather than
     * how much work the migration asked of it. The engine does not know that and should not be
     * edited to learn it.
     */
    private CallCost costOf(ConnectorInstance sink) {
        return connectors.spec(sink.connectorType())
                .map(ConnectorSpec::callCost)
                .orElse(CallCost.PER_REQUEST);
    }

    /** How many groups the pipeline's delivery policy would make of a chunk this size. */
    private static long deliveryCallsFor(ResolvedPipeline pipeline, long records) {
        DeliveryPolicy delivery = pipeline.delivery();
        if (delivery.splitScript() != null) {
            return records;
        }
        int groupSize = delivery.groupSize();
        if (groupSize <= DeliveryPolicy.WHOLE_BATCH) {
            return 1;
        }
        return (records + groupSize - 1) / groupSize;
    }

    /**
     * How many requests this chunk will make of its source.
     *
     * <p>A source is pulled a fetch at a time, so the request count follows the read fetch size
     * rather than anything the destination does.
     */
    static long sourceCallsFor(ResolvedPipeline pipeline, long records) {
        int fetch = Math.max(1, pipeline.chunking().readFetchSizeOrDefault());
        return Math.max(1, (records + fetch - 1) / fetch);
    }

    private static String describe(ConnectorInstance instance) {
        return instance.name() + " (" + instance.connectorType() + ")";
    }
}
