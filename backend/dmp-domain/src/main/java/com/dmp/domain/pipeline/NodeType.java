package com.dmp.domain.pipeline;

/**
 * The node palette offered by the pipeline designer.
 *
 * <p>Each constant carries the structural rules the validator enforces, so that adding a node
 * type is a matter of adding a constant rather than editing validation logic in several places.
 */
public enum NodeType {

    /** Reads from an external system. Requires a connector instance; may not have inbound edges. */
    SOURCE(true, false, true, false),

    /** Writes to an external system. Requires a connector instance; may not have outbound edges. */
    SINK(true, true, false, false),

    /** Sandboxed JavaScript run once per record, before batching (ADR-0008). */
    TRANSFORM(false, true, true, false),

    /**
     * Sandboxed JavaScript run once per batch, shaping the payload the sink sends.
     *
     * <p>A separate type rather than a flag on {@link #TRANSFORM} because the difference is not a
     * setting — it decides what the script can do. Only a per-record node can drop or multiply
     * records; only a per-batch node can see the group. Making the user pick from the palette puts
     * that choice where they can see it.
     */
    BATCH_TRANSFORM(false, true, true, false),

    /** Drops records failing a predicate. */
    FILTER(false, true, true, false),

    /** Declarative field mapping, evaluated by JSONata. */
    MAPPER(false, true, true, false),

    /** Expands one record into many, for example by exploding an array field. */
    SPLITTER(false, true, true, false),

    /** Combines multiple inbound branches into one stream. */
    MERGER(false, true, true, false),

    /** Rejects records violating declared rules, routing them to the error path. */
    VALIDATION(false, true, true, false),

    /**
     * Terminal handler for rejected records. Exempt from the "must reach a sink" rule, because
     * routing to a dead-letter destination is itself a legitimate terminus.
     */
    ERROR_HANDLER(false, true, true, true),

    /** Defers records via the delay queue (ADR-0002). Subject to its ~60 second floor. */
    DELAY(false, true, true, false),

    /** Re-attempts the downstream branch according to a retry policy. */
    RETRY(false, true, true, false);

    private final boolean requiresConnector;
    private final boolean allowsInbound;
    private final boolean allowsOutbound;
    private final boolean terminal;

    NodeType(boolean requiresConnector, boolean allowsInbound, boolean allowsOutbound, boolean terminal) {
        this.requiresConnector = requiresConnector;
        this.allowsInbound = allowsInbound;
        this.allowsOutbound = allowsOutbound;
        this.terminal = terminal;
    }

    /** Whether this node must reference a connector instance. */
    public boolean requiresConnector() {
        return requiresConnector;
    }

    public boolean allowsInbound() {
        return allowsInbound;
    }

    public boolean allowsOutbound() {
        return allowsOutbound;
    }

    /** Whether this node is a legitimate end of a branch without reaching a sink. */
    public boolean isTerminal() {
        return terminal;
    }
}
