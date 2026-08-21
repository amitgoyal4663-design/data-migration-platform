package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Flow control for a pipeline, per ADR-0009.
 *
 * <p><b>The chunk is the batch.</b> There is no separate write size, and removing it removed a
 * question nobody could answer well. A chunk is already the unit of work, of retry and of
 * checkpointing; making it the unit of writing too means "how much is redone after a crash?" has
 * one answer — a chunk — rather than a product of two numbers a user had to multiply in their head.
 *
 * <p>Two things still lower it, and neither is a preference a user expresses:
 *
 * <ul>
 *   <li>{@code maxBatchBytes}, the real memory guarantee. A record count bounds nothing: a
 *       thousand records may be 1 MB or 1 GB.</li>
 *   <li>the sink's own protocol ceiling — Salesforce Bulk caps at 10,000 records a request, and
 *       the connector's word on that is final.</li>
 * </ul>
 *
 * <p>Where a destination needs <em>smaller</em> calls than a chunk, that is
 * {@link DeliveryPolicy}: one call per record, fixed groups, or a split script. Dividing a batch
 * into calls and deciding how much work a crash repeats are different questions, and one number
 * could only ever answer them together.
 *
 * <p>Read size stays, because it is constrained by something else entirely: a JDBC cursor is
 * efficient at 100–1000 rows per round trip regardless of how the destination likes to be written
 * to.
 *
 * @param readFetchSize      records per source round trip; {@code 0} takes the platform default
 * @param maxBatchBytes      byte ceiling per batch — the actual memory guarantee
 * @param flushInterval      linger, so a low-volume stream neither stalls nor stops checkpointing
 * @param maxInFlightBatches buffer depth between reader and writer
 * @param checkpointEveryNBatches how many batches to write before persisting the resume position;
 *                                {@code 0} lets the engine decide from the sink's own capabilities
 */
public record ChunkingPolicy(
        int readFetchSize,
        long maxBatchBytes,
        Duration flushInterval,
        int maxInFlightBatches,
        int checkpointEveryNBatches) {

    private static final int MAX_READ_FETCH_SIZE = 100_000;
    private static final long MIN_BATCH_BYTES = 64L * 1024;
    private static final long MAX_BATCH_BYTES = 2L * 1024 * 1024 * 1024;
    private static final int MAX_IN_FLIGHT_BATCHES = 64;
    private static final Duration MAX_FLUSH_INTERVAL = Duration.ofMinutes(15);

    /** Let the engine choose the checkpoint frequency from what the sink declares. */
    public static final int CHECKPOINT_AUTO = 0;

    /** Take the read fetch size from the platform default rather than a number on the pipeline. */
    public static final int READ_FETCH_AUTO = 0;

    /** Used when neither the pipeline nor the sink expresses a preference. */
    private static final int FALLBACK_READ_FETCH = 1_000;

    private static final int MAX_CHECKPOINT_INTERVAL = 1_000;

    /**
     * Used when the sink can absorb a repeated write — an upsert, or a transactional sink.
     *
     * <p>Fifty is a deliberate middle: it removes the per-record bookkeeping cost on a
     * one-record-per-call pipeline, while capping the redone work after a crash at something a
     * remote API will not notice.
     */
    private static final int IDEMPOTENT_SINK_CHECKPOINT_INTERVAL = 50;

    /** Platform defaults. Connectors override these; users override connectors. */
    public static final ChunkingPolicy DEFAULT = new ChunkingPolicy(
            500,
            8L * 1024 * 1024,
            Duration.ofSeconds(5),
            2,
            CHECKPOINT_AUTO);

    public ChunkingPolicy {
        Objects.requireNonNull(flushInterval, "flushInterval");

        require(readFetchSize >= 0 && readFetchSize <= MAX_READ_FETCH_SIZE,
                "readFetchSize must be between 0 (automatic) and " + MAX_READ_FETCH_SIZE,
                "readFetchSize", readFetchSize);
        require(maxBatchBytes >= MIN_BATCH_BYTES && maxBatchBytes <= MAX_BATCH_BYTES,
                "maxBatchBytes must be between 64 KiB and 2 GiB", "maxBatchBytes", maxBatchBytes);
        require(maxInFlightBatches > 0 && maxInFlightBatches <= MAX_IN_FLIGHT_BATCHES,
                "maxInFlightBatches must be between 1 and " + MAX_IN_FLIGHT_BATCHES,
                "maxInFlightBatches", maxInFlightBatches);
        require(!flushInterval.isNegative() && !flushInterval.isZero()
                        && flushInterval.compareTo(MAX_FLUSH_INTERVAL) <= 0,
                "flushInterval must be positive and at most 15 minutes", "flushInterval", flushInterval);
        require(checkpointEveryNBatches >= 0 && checkpointEveryNBatches <= MAX_CHECKPOINT_INTERVAL,
                "checkpointEveryNBatches must be between 0 (automatic) and " + MAX_CHECKPOINT_INTERVAL,
                "checkpointEveryNBatches", checkpointEveryNBatches);
    }

    private static void require(boolean condition, String message, String field, Object value) {
        if (!condition) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, message,
                    Map.of("field", field, "value", String.valueOf(value)));
        }
    }

    /**
     * Worst-case heap attributable to one running split's buffering.
     *
     * <p>This is the number an operator sizes a worker against: total worker heap must exceed
     * the sum of this figure across concurrently executing splits, plus the JVM's own overhead.
     * It is calculable only because {@code maxBatchBytes} exists — record count alone provides no
     * memory bound whatsoever, since a thousand records may be 1 KB or 10 MB.
     */
    public long estimatedPeakHeapBytes() {
        return (long) maxInFlightBatches * maxBatchBytes;
    }

    /**
     * The read size before the chunk has had its say.
     *
     * <p>Needed because the two defaults refer to each other: a chunk left automatic is sized as a
     * multiple of the read size, while a read size is capped by the chunk. Resolving the read size
     * on its own first breaks the cycle, and does so in the right direction — the chunk is the
     * container, so it is the one that should be derived.
     */
    public int readFetchSizeOrDefault() {
        return readFetchSize == READ_FETCH_AUTO ? FALLBACK_READ_FETCH : readFetchSize;
    }

    /**
     * The sizes this chunk will actually run with, once the sink and the chunk have had their say.
     *
     * <p>The batch <b>is</b> the chunk. Nothing on the pipeline sets it, because a second number
     * could only ever disagree with the first: a chunk of 100 with a batch of 1,000 wrote one batch
     * of 100 and the batch size was decoration.
     *
     * <p>Two ceilings apply, and neither is a preference:
     *
     * <ol>
     *   <li>the sink's protocol limit — the connector's word is final, and a bulk API that refuses
     *       more than 10,000 a request must not be handed 50,000;</li>
     *   <li>{@code maxBatchBytes}, applied by the batch builder while records accumulate rather
     *       than here, because only the records themselves know what they weigh.</li>
     * </ol>
     *
     * <p>A chunk that is a key range has no row count of its own. There is nothing to derive a
     * batch from, so the sink's preference is used — the one place it still has a say — and the
     * byte ceiling does the rest.
     *
     * @param rowsPerChunk           the chunk's row budget, or {@code 0} for a key range
     * @param sinkMaxBatchSize       protocol ceiling; {@code 0} means the sink has none
     * @param sinkPreferredBatchSize what the sink performs best with, used only when the chunk has
     *                               no row count; {@code 0} means no opinion
     */
    public EffectiveSizes resolved(long rowsPerChunk, int sinkMaxBatchSize,
                                   int sinkPreferredBatchSize) {
        int fetch = readFetchSizeOrDefault();

        long batch = rowsPerChunk > 0
                ? rowsPerChunk
                : (sinkPreferredBatchSize > 0 ? sinkPreferredBatchSize : FALLBACK_READ_FETCH);

        if (sinkMaxBatchSize > 0) {
            batch = Math.min(batch, sinkMaxBatchSize);
        }
        if (rowsPerChunk > 0) {
            fetch = (int) Math.min(fetch, rowsPerChunk);
        }

        return new EffectiveSizes(
                Math.max(1, fetch),
                (int) Math.max(1, Math.min(batch, Integer.MAX_VALUE)),
                maxBatchBytes,
                flushInterval,
                maxInFlightBatches);
    }

    /**
     * What one chunk runs with, after the chunk and the sink have both had their say.
     *
     * <p>A separate type from the policy, and deliberately so. The policy is what a user configured
     * and what is stored; this is what a particular chunk of a particular run will do, and it
     * carries a number — {@code writeBatchSize} — that nobody configured. Returning a
     * {@code ChunkingPolicy} from resolution meant a value the user never set could be read back as
     * if they had.
     */
    public record EffectiveSizes(
            int readFetchSize,
            int writeBatchSize,
            long maxBatchBytes,
            Duration flushInterval,
            int maxInFlightBatches) {
    }

    /**
     * How many batches to write between saving the resume position.
     *
     * <p>The resume position is a bookmark, and saving it is a database write. With a large batch
     * that cost is trivial — one bookmark per thousand records. With {@code writeBatchSize = 1},
     * which some APIs require, it becomes one bookmark per record: the bookkeeping then costs as
     * much as the work.
     *
     * <p>Saving it less often means a crash redoes the batches since the last save. Whether that is
     * acceptable is not the user's judgement to make — it depends on whether the destination can
     * absorb a repeated write, which the sink already declares. So the engine decides:
     *
     * <ul>
     *   <li><b>Idempotent sink</b> (upsert, or transactional) — every 50 batches. A redone record
     *       lands on the same key and changes nothing.</li>
     *   <li><b>Append-only sink</b> — every batch. Redoing a write here creates duplicates, so the
     *       bookkeeping cost is the correct price.</li>
     * </ul>
     *
     * <p>An explicit setting overrides this, for the case where a user knows something about their
     * destination that its connector does not.
     */
    public int effectiveCheckpointInterval(boolean sinkIsIdempotent) {
        if (checkpointEveryNBatches != CHECKPOINT_AUTO) {
            return checkpointEveryNBatches;
        }
        return sinkIsIdempotent ? IDEMPOTENT_SINK_CHECKPOINT_INTERVAL : 1;
    }
}
