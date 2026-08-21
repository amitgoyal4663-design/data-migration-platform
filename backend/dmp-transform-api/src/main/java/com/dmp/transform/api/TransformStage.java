package com.dmp.transform.api;

/**
 * Where in the record's journey a script runs.
 *
 * <p>The two stages exist because they can do things the other cannot, not as a convenience. A
 * per-record script is the only place a record can be dropped or multiplied, because batches are
 * assembled afterwards and their sizing depends on the final count. A per-batch script is the only
 * place the outgoing group can be shaped, because a single record does not know it belongs to one.
 */
public enum TransformStage {

    /**
     * Runs once per record, before batching.
     *
     * <p>May return null to drop the record, an object to replace it, or an array to fan it out
     * into several.
     */
    RECORD,

    /**
     * Runs once per batch, on the list about to be written, and produces the payload the sink
     * sends.
     *
     * <p>Does not change which records exist — the engine's accounting is driven by the record
     * list, and a script that appeared to remove records would make them look lost. Only sinks
     * that send a batch as one payload can use the result; a sink writing rows individually has
     * nothing to apply it to.
     *
     * <p>Attaching one makes {@code writeBatchSize} semantic rather than a tuning knob: the script
     * sees exactly that many records, so changing it for throughput changes what the script
     * computes.
     */
    BATCH,

    /**
     * Runs once per batch, before {@link #BATCH}, and decides how the batch is divided into calls
     * on the sink.
     *
     * <p>Returns one group label per record, in the same order. Records sharing a label are written
     * together; each distinct label is one call. Labels rather than groups of records because the
     * engine pairs script output back to records by position — see
     * {@code RecordTransform.split}.
     *
     * <p>Sees the whole batch, which is the point: a running total, a comparison with the previous
     * record, a size-based cut are all expressible, and none of them are from a per-record
     * function. What it cannot do is change which records exist, because it never returns records.
     *
     * <p><b>A group never crosses a batch.</b> The engine holds a batch, never a chunk, so two
     * records sharing a label in different batches are two separate calls.
     */
    SPLIT
}
