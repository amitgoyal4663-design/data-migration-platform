package com.dmp.engine;

import com.dmp.common.json.Json;
import com.dmp.connector.api.Preparation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;

/**
 * Not a failure: the chunk's records are with the destination, which has not finished deciding.
 *
 * <p>Thrown by {@link ChunkExecutor} instead of sleeping. Everything needed to pick the chunk up
 * again is carried here, written to the split, and asked about on a timer — so the worker is free
 * within milliseconds of the upload finishing rather than held for however long the org takes.
 *
 * <p>An exception rather than a return value because it has to unwind the connector sessions. The
 * source stream, the transform and the sink session are all held in a try-with-resources, and
 * returning normally from the middle of that block would mean threading a "did we park?" flag
 * through every line after it. Throwing closes them in the right order and lands in one place.
 */
public class ChunkParkedException extends RuntimeException {

    /** Key under which the connector's own handle is nested inside {@link #parkedState()}. */
    private static final String SINK = "sink";

    /** Key under which the engine records whether the source had more to give. */
    private static final String SOURCE_EXHAUSTED = "sourceExhausted";

    private final transient Preparation commit;
    private final transient Duration retryAfter;
    private final boolean sourceExhausted;

    public ChunkParkedException(int chunkIndex, Preparation commit, Duration retryAfter,
                                boolean sourceExhausted) {
        super("Chunk " + chunkIndex + " is waiting for the destination to finish processing it");
        this.commit = commit;
        this.retryAfter = retryAfter;
        this.sourceExhausted = sourceExhausted;
    }

    /**
     * What gets written to the split.
     *
     * <p>The connector's handle plus one fact the engine cannot recover on its own. {@code
     * sourceExhausted} is decided by the read loop and consumed after the chunk settles, to decide
     * whether a lazily chunked run needs a successor — and by then the read loop is long gone. Left
     * out, every parked chunk would look like the end of its run, and a lazily chunked migration
     * against Salesforce would stop at its first chunk having read a fraction of the data.
     */
    public JsonNode parkedState() {
        ObjectNode state = Json.newObject();
        state.set(SINK, commit.state());
        state.put(SOURCE_EXHAUSTED, sourceExhausted);
        return state;
    }

    /** The connector's handle, read back out of what {@link #parkedState()} wrote. */
    public static Preparation sinkJobOf(JsonNode parkedState) {
        return Preparation.of(parkedState.path(SINK));
    }

    /** Whether the source had run out when this chunk parked. */
    public static boolean sourceWasExhausted(JsonNode parkedState) {
        return parkedState.path(SOURCE_EXHAUSTED).asBoolean(true);
    }

    /** How long the connector asked us to wait before asking again. */
    public Duration retryAfter() {
        return retryAfter;
    }

    /**
     * No stack trace.
     *
     * <p>This is ordinary control flow on every asynchronous sink, several times per chunk. Filling
     * in a stack trace for it is pure cost, and a stack trace in the logs would suggest something
     * went wrong when nothing did.
     */
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
