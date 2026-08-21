package com.dmp.domain.pipeline;

/**
 * The transport carrying records between pipeline stages, per ADR-0001.
 *
 * <p>This is the architecture's central decision expressed in the domain. Connectors and
 * transformations are written against a single streaming abstraction and never observe which of
 * these they were given; only the engine binds an edge to a concrete transport.
 *
 * <p>Kafka is always the control bus regardless of the value here. This enum describes the
 * <em>data</em> path only.
 */
public enum ChannelType {

    /**
     * Bounded in-process buffer with zero-copy handoff.
     *
     * <p>Back-pressure is a full buffer blocking the reader. Chosen for batch work, where routing
     * every record through a broker would amplify cost by one to two orders of magnitude for no
     * benefit a checkpoint does not already provide.
     */
    IN_PROCESS(false),

    /**
     * One durable Kafka topic per DAG edge.
     *
     * <p>Back-pressure is {@code consumer.pause()} on the partition. Chosen for streaming and CDC,
     * where replay, consumer-group scaling and cross-team fan-out are the point.
     */
    KAFKA(true);

    private final boolean durable;

    ChannelType(boolean durable) {
        this.durable = durable;
    }

    /** Whether in-flight records survive a worker crash without replaying from the last checkpoint. */
    public boolean isDurable() {
        return durable;
    }
}
