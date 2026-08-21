package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * How much of a run may execute at once, and for how long a claim on a chunk stays valid.
 *
 * <p>Distinct from {@link ChunkingPolicy}, which sizes an individual batch. This sizes the
 * <em>fleet's</em> attack on a run: how many chunks are in flight across every worker pod
 * simultaneously.
 *
 * <p>Sequential execution is a real requirement, not a degraded mode. Concurrent Salesforce bulk
 * jobs against the same object produce {@code UNABLE_TO_LOCK_ROW} failures on rows that are
 * otherwise perfectly valid; parent records must land before the children referencing them; and a
 * modest read replica will not survive sixteen simultaneous range scans. A platform that can only
 * go fast is unusable for those cases.
 *
 * <p>One thing this cannot do, and no design could: strict sequencing and simultaneous work across
 * many pods are contradictory. With {@code maxConcurrentChunks = 1} exactly one chunk of that run
 * is executing at any instant. What is preserved is that the <em>pod</em> rotates — any worker may
 * take the next chunk — and that the rest of the fleet stays busy on other runs. The limit throttles
 * a run, never the cluster.
 *
 * @param maxConcurrentChunks chunks in flight across all pods; {@code 0} means unlimited,
 *                            {@code 1} means strictly sequential
 * @param maxChunksPerPod     ceiling for a single worker, so one pod cannot claim an entire run
 * @param chunkLease          how long a claim survives without a heartbeat before another pod may
 *                            reclaim the chunk
 * @param maxAttemptsPerChunk retry budget before a chunk is abandoned
 * @param rowsPerChunk        how many source rows one chunk covers; {@code 0} lets the engine
 *                            derive it from the read size
 * @param maxFailedPercent    share of a chunk's records that may be rejected before the chunk is
 *                            treated as failed rather than merely lossy. {@code null} means no
 *                            limit; {@code 0} means any rejection at all fails the chunk
 * @param maxFailedRecords    absolute count of rejected records with the same effect. {@code null}
 *                            means no limit; {@code 0} means any rejection at all fails the chunk
 * @param stopRunOnChunkFailure whether the first abandoned chunk ends the run, instead of the run
 *                            carrying on and failing once every remaining chunk has also been tried
 */
public record ExecutionPolicy(
        int maxConcurrentChunks,
        int maxChunksPerPod,
        Duration chunkLease,
        int maxAttemptsPerChunk,
        int rowsPerChunk,
        Integer maxFailedPercent,
        Long maxFailedRecords,
        boolean stopRunOnChunkFailure) {

    public static final int UNLIMITED = 0;

    /**
     * No limit at all — rejections never fail a chunk, however many there are.
     *
     * <p>Absence rather than zero, because zero has an obvious meaning of its own: fail on any
     * rejection whatsoever. Using it as the "switched off" sentinel made the field read as the
     * exact opposite of what it did, which is the worst way a setting can be wrong — the user who
     * types the strictest value they can think of gets no protection at all.
     *
     * <p>A JSON document written before this setting existed has no such field, so it deserialises
     * to null and keeps its old behaviour. That is what the sentinel was bought for, and a boxed
     * type buys it without the ambiguity.
     */
    public static final Integer NO_LIMIT = null;

    /**
     * Records a chunk must have processed before a <em>proportional</em> threshold is believed.
     *
     * <p>Without a floor, the first batch decides everything: three rejections out of the first
     * five records is sixty percent, and a chunk of a million perfectly good rows dies on its
     * opening sample. The absolute limit still applies below this, and the percentage is evaluated
     * unconditionally once the chunk finishes, so a genuinely small chunk is not exempt — only a
     * small <em>prefix</em> of a large one is.
     *
     * <p>Deliberately not applied when the limit is zero. Zero expresses "no rejection is
     * acceptable" rather than a ratio, so there is no sample to be unrepresentative of, and making
     * the strictest setting wait for a hundred records was simply wrong.
     *
     * <p>Not configurable, and it is a judgement rather than a tuned number. Anyone needing an
     * exact cut-off should set the absolute limit, which has no floor and no arithmetic.
     */
    private static final long MIN_RECORDS_FOR_PERCENT = 100;

    private static final int MAX_CONCURRENT_CEILING = 10_000;
    private static final int MAX_PER_POD_CEILING = 256;
    private static final int MAX_ATTEMPTS_CEILING = 100;

    /** Derive the chunk size from the pipeline's read size rather than being told. */
    public static final int ROWS_PER_CHUNK_AUTO = 0;

    private static final int MAX_ROWS_PER_CHUNK = 100_000_000;

    /**
     * Default chunk size, as a multiple of the read size.
     *
     * <p>Chunk size, not chunk count, because a count has to be guessed and guesses ignore skew:
     * five equal key ranges over unevenly distributed data leave one pod grinding while the others
     * idle. A size produces however many chunks the data actually warrants.
     *
     * <p>Ten reads per chunk is a compromise. Each chunk pays setup — claiming it, opening a source
     * connection, opening a sink connection, reading its checkpoint — of roughly 25 to 100
     * milliseconds. Against a fast bulk database write that overhead matters at one read per chunk;
     * against a slow per-record API it is invisible, and smaller chunks balance far better. Lower
     * this for slow sinks; raise it for fast ones.
     */
    private static final int DEFAULT_READS_PER_CHUNK = 10;
    private static final Duration MIN_LEASE = Duration.ofSeconds(30);
    private static final Duration MAX_LEASE = Duration.ofHours(6);

    /**
     * Fail a chunk only when every single record was rejected.
     *
     * <p>The default for a pipeline created from now on, and chosen because the alternative is
     * indefensible: with no limit at all, a run whose destination rejects all twenty thousand
     * records reads everything, writes nothing, and reports COMPLETED. A green run that moved no
     * data is worse than a red one, because nobody investigates it.
     *
     * <p>A hundred rather than fifty, because this is a default rather than a decision. It cannot
     * misfire — no legitimate pipeline rejects literally everything — while still catching the case
     * that actually matters. Anyone who wants to be told sooner lowers it.
     */
    public static final int CATCH_TOTAL_FAILURE = 100;

    /** Unlimited parallelism, eight chunks per pod, five-minute lease, five attempts. */
    public static final ExecutionPolicy DEFAULT =
            new ExecutionPolicy(UNLIMITED, 8, Duration.ofMinutes(5), 5, ROWS_PER_CHUNK_AUTO,
                    CATCH_TOTAL_FAILURE, null, false);

    public ExecutionPolicy {
        Objects.requireNonNull(chunkLease, "chunkLease");

        require(maxConcurrentChunks >= 0 && maxConcurrentChunks <= MAX_CONCURRENT_CEILING,
                "maxConcurrentChunks must be between 0 (unlimited) and " + MAX_CONCURRENT_CEILING,
                "maxConcurrentChunks", maxConcurrentChunks);
        require(maxChunksPerPod > 0 && maxChunksPerPod <= MAX_PER_POD_CEILING,
                "maxChunksPerPod must be between 1 and " + MAX_PER_POD_CEILING,
                "maxChunksPerPod", maxChunksPerPod);
        require(maxAttemptsPerChunk > 0 && maxAttemptsPerChunk <= MAX_ATTEMPTS_CEILING,
                "maxAttemptsPerChunk must be between 1 and " + MAX_ATTEMPTS_CEILING,
                "maxAttemptsPerChunk", maxAttemptsPerChunk);
        require(rowsPerChunk >= 0 && rowsPerChunk <= MAX_ROWS_PER_CHUNK,
                "rowsPerChunk must be between 0 (automatic) and " + MAX_ROWS_PER_CHUNK,
                "rowsPerChunk", rowsPerChunk);
        require(maxFailedPercent == null || (maxFailedPercent >= 0 && maxFailedPercent <= 100),
                "maxFailedPercent must be between 0 (any rejection fails) and 100, "
                        + "or absent for no limit",
                "maxFailedPercent", maxFailedPercent);
        require(maxFailedRecords == null || maxFailedRecords >= 0,
                "maxFailedRecords must not be negative; omit it for no limit",
                "maxFailedRecords", maxFailedRecords);
        // A lease shorter than the heartbeat interval would let a healthy worker lose its own
        // chunk to the reclaim sweep, producing duplicate work that looks like a phantom bug.
        require(chunkLease.compareTo(MIN_LEASE) >= 0 && chunkLease.compareTo(MAX_LEASE) <= 0,
                "chunkLease must be between 30 seconds and 6 hours",
                "chunkLease", chunkLease);
    }

    private static void require(boolean condition, String message, String field, Object value) {
        if (!condition) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, message,
                    Map.of("field", field, "value", String.valueOf(value)));
        }
    }

    /** One chunk at a time, in index order. For ordering-sensitive or lock-contended targets. */
    public static ExecutionPolicy sequential() {
        return new ExecutionPolicy(1, 1, Duration.ofMinutes(30), 5, ROWS_PER_CHUNK_AUTO,
                null, null, false);
    }

    /** A fixed number of chunks in flight across the whole fleet. */
    public static ExecutionPolicy limitedTo(int concurrentChunks) {
        return new ExecutionPolicy(concurrentChunks,
                Math.min(concurrentChunks, DEFAULT.maxChunksPerPod()),
                DEFAULT.chunkLease(), DEFAULT.maxAttemptsPerChunk(), ROWS_PER_CHUNK_AUTO,
                null, null, false);
    }

    /** This policy, with a ceiling on how much of a chunk may be rejected before it is a failure. */
    public ExecutionPolicy failingAbove(Integer percent, Long records) {
        return new ExecutionPolicy(maxConcurrentChunks, maxChunksPerPod, chunkLease,
                maxAttemptsPerChunk, rowsPerChunk, percent, records, stopRunOnChunkFailure);
    }

    /**
     * One chunk per source read.
     *
     * <p>The finest useful granularity, and the best balancing available: a pod that finishes a
     * read immediately claims another, so a slow range holds up nobody but the pod on it. Worth it
     * when each record costs a great deal to write — a per-record API call, for instance — because
     * the per-chunk setup is then a rounding error against the work.
     *
     * <p>Wasteful against a fast bulk database write, where opening two connections per five
     * hundred rows is real overhead.
     */
    public static ExecutionPolicy oneChunkPerRead(int readFetchSize) {
        return new ExecutionPolicy(UNLIMITED, DEFAULT.maxChunksPerPod(), DEFAULT.chunkLease(),
                DEFAULT.maxAttemptsPerChunk(), readFetchSize,
                null, null, false);
    }

    /**
     * Whether so much of a chunk has been rejected that it should be called a failure.
     *
     * <p>The rule that separates "one malformed row in a million" from "the target schema changed
     * and nothing is landing". Both arrive here as rejected records, and without a threshold both
     * are treated as the first: the chunk completes, the run completes, and a migration that wrote
     * nothing at all reports success. That is the worst outcome the platform can produce — worse
     * than failing, because nobody investigates a green run.
     *
     * <p>Evaluated against records that reached the sink rather than records read, so filtering a
     * record deliberately in a transform never counts against the pipeline that filtered it.
     *
     * @param produced      records this chunk has sent to the sink so far, cumulative across resumes
     * @param failed        how many of those were rejected
     * @param chunkFinished true once the source is exhausted, which is when a percentage may be
     *                      applied to a chunk too small to have satisfied the sample floor
     * @return why the chunk should fail, or empty if it should carry on
     */
    public Optional<String> rejectionBreach(long produced, long failed, boolean chunkFinished) {
        if (failed <= 0) {
            return Optional.empty();
        }
        if (maxFailedRecords != null && failed >= maxFailedRecords) {
            return Optional.of(failed + " record(s) were rejected, reaching the limit of "
                    + maxFailedRecords + " set for this pipeline");
        }
        if (maxFailedPercent == null || produced <= 0) {
            return Optional.empty();
        }

        // Zero is not a percentage. It is a statement that no rejection is acceptable, and it is
        // the strictest thing this setting can say, so it must fire on the first rejected record —
        // not on the hundredth.
        //
        // The sample floor below exists so that a small opening prefix cannot produce a misleading
        // ratio: three rejections in the first five records is sixty percent, and a chunk of a
        // million good rows should not die on that. There is nothing to mislead when the rule is
        // "any rejection at all", and applying the floor here made the strictest available setting
        // silently tolerate up to ninety-nine rejected records — the opposite of what it says, and
        // what a user configuring zero is explicitly trying to prevent.
        if (maxFailedPercent == 0) {
            return Optional.of(failed + " record(s) were rejected, and this pipeline is set to fail "
                    + "a chunk on any rejection at all");
        }

        if (produced < MIN_RECORDS_FOR_PERCENT && !chunkFinished) {
            return Optional.empty();
        }

        // Integer arithmetic throughout: a percentage of a record count is exact, and floating
        // point here would make the boundary depend on rounding rather than on the setting.
        long percent = failed * 100 / produced;
        if (percent >= maxFailedPercent) {
            return Optional.of(failed + " of " + produced + " record(s) were rejected (" + percent
                    + "%), reaching the limit of " + maxFailedPercent + "% set for this pipeline");
        }
        return Optional.empty();
    }

    /** Whether any rejection limit is configured at all. */
    public boolean hasRejectionLimit() {
        return maxFailedPercent != null || maxFailedRecords != null;
    }

    public boolean isUnlimited() {
        return maxConcurrentChunks == UNLIMITED;
    }

    public boolean isSequential() {
        return maxConcurrentChunks == 1;
    }

    /**
     * Whether a slot must be reserved before claiming a chunk.
     *
     * <p>Unlimited runs skip reservation entirely, so the common case costs no extra round trip.
     */
    public boolean requiresSlotReservation() {
        return !isUnlimited();
    }

    /**
     * How often a worker should heartbeat to retain its claim.
     *
     * <p>A third of the lease, so two consecutive missed heartbeats — a garbage-collection pause, a
     * brief network blip — do not cost a worker a chunk it is actively processing.
     */
    public Duration heartbeatInterval() {
        return chunkLease.dividedBy(3);
    }

    /**
     * Rows one chunk should cover, given the pipeline's read size.
     *
     * <p>Chunk size rather than chunk count, deliberately. A count must be guessed before anything
     * is known about the distribution, and an unlucky guess produces chunks that differ by an order
     * of magnitude — leaving one pod working long after the rest have finished. A size produces
     * however many chunks the data warrants, and the pull loop then balances them by itself.
     */
    public int effectiveRowsPerChunk(int readFetchSize) {
        if (rowsPerChunk != ROWS_PER_CHUNK_AUTO) {
            return rowsPerChunk;
        }
        return Math.max(1, readFetchSize * DEFAULT_READS_PER_CHUNK);
    }
}
