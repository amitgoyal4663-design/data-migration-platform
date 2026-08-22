package com.dmp.engine;

/**
 * The outcome of executing one chunk.
 *
 * @param recordsRead      records pulled from the source
 * @param recordsProduced  records the transform stage handed to the sink, which differs from
 *                         {@code recordsRead} whenever a script filters or fans out
 * @param recordsWritten   records the sink accepted
 * @param recordsFailed    records the sink rejected or a transform threw on, already sent to the
 *                         dead-letter queue
 * @param recordsFiltered  records a transform deliberately dropped
 * @param bytesRead        approximate, for throughput metrics
 * @param sinkCalls        requests actually made of the destination. Known only once the batches
 *                         have been grouped and written, which is why a rate limit reserves a
 *                         pessimistic number up front and reconciles against this afterwards
 * @param batchesCommitted number of checkpoint advances
 * @param sourceExhausted  whether the source ran out, as opposed to the chunk stopping because it
 *                         had read its allotted rows. Only meaningful for an open-ended chunk,
 *                         where it is the signal that no further chunk needs to be generated —
 *                         and therefore the only thing that ends a lazily chunked run
 */
public record ChunkResult(
        long recordsRead,
        long recordsProduced,
        long recordsWritten,
        long recordsFailed,
        long recordsFiltered,
        long bytesRead,
        long sinkCalls,
        int batchesCommitted,
        boolean sourceExhausted) {

    /**
     * The shape before call counting, for the many places that build a result without making calls.
     */
    public ChunkResult(long recordsRead, long recordsProduced, long recordsWritten,
                       long recordsFailed, long recordsFiltered, long bytesRead,
                       int batchesCommitted, boolean sourceExhausted) {
        this(recordsRead, recordsProduced, recordsWritten, recordsFailed, recordsFiltered,
                bytesRead, 0, batchesCommitted, sourceExhausted);
    }

    public static ChunkResult empty() {
        return new ChunkResult(0, 0, 0, 0, 0, 0, 0, 0, true);
    }

    /**
     * Records handed to the sink stage that were neither written nor rejected.
     *
     * <p>Must be zero when a chunk completes. Measured against {@code recordsProduced} rather than
     * {@code recordsRead}, because a transform is entitled to change how many records exist — a
     * filter drops them, a splitter multiplies them. What nothing here may do is take a record and
     * lose it, which is the one outcome a migration platform can never be relaxed about, so the
     * executor asserts it rather than reporting a success that is not one.
     */
    public long unaccounted() {
        return recordsProduced - recordsWritten - recordsFailed;
    }

    /**
     * Source records that were neither filtered out nor turned into at least one output.
     *
     * <p>Every record read must either be dropped on purpose or produce something. A positive
     * value means records vanished inside the transform stage without anyone counting them.
     */
    public long unexplainedReads() {
        return Math.max(0, (recordsRead - recordsFiltered) - recordsProduced);
    }
}
