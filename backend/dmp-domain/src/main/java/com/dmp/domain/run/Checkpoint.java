package com.dmp.domain.run;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * The resumption point for a split.
 *
 * <p>Exactly one checkpoint exists per split, overwritten in place. Per ADR-0009 a checkpoint is
 * written <em>after</em> a successful sink flush and records the source cursor as of the last
 * record in that batch, which is what makes resumption land on a batch boundary. Checkpointing per
 * record would be prohibitively expensive; checkpointing on a wall-clock timer would allow the
 * cursor to advance past records the sink never accepted, silently losing them.
 *
 * <p>{@code sourceCursor} is connector-defined and opaque — a primary key value, an oplog
 * timestamp, a Kafka offset, a byte position, an API page token. The engine stores and returns it
 * without interpretation.
 *
 * <p>The ordering of the write matters and is not negotiable:
 * <pre>
 *   1. sink.write(batch)      succeeds
 *   2. checkpoint.advance(...)  is persisted
 * </pre>
 * Reversing these turns an at-least-once pipeline into a lossy one, because a crash between the
 * two would resume past records that were never written.
 */
public record Checkpoint(
        SplitId splitId,
        RunId runId,
        TenantId tenantId,
        JsonNode sourceCursor,
        long lastSeq,
        long recordsRead,
        long recordsProduced,
        long recordsWritten,
        long recordsFailed,
        long recordsFiltered,
        long bytesRead,
        int batchesCommitted,
        Instant createdAt,
        Instant updatedAt) {

    public Checkpoint {
        Objects.requireNonNull(splitId, "splitId");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");

        sourceCursor = Json.orEmpty(sourceCursor);
        if (lastSeq < 0 || recordsRead < 0 || recordsProduced < 0 || recordsWritten < 0
                || recordsFailed < 0 || recordsFiltered < 0 || bytesRead < 0
                || batchesCommitted < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "Checkpoint counters must not be negative");
        }
    }

    /** The starting checkpoint for a split that has never run. */
    public static Checkpoint initial(SplitId splitId, RunId runId, TenantId tenantId, Instant now) {
        return new Checkpoint(splitId, runId, tenantId, Json.emptyObject(),
                0, 0, 0, 0, 0, 0, 0, 0, now, now);
    }

    /**
     * Advances the checkpoint after a batch has been committed to the sink.
     *
     * <p>The cursor may only move forward. A connector handing back a cursor that regresses would
     * cause silent re-processing on resume, so it is rejected here rather than tolerated — this is
     * the single invariant protecting the resume guarantee, and it is worth failing loudly for.
     */
    public Checkpoint advance(JsonNode newCursor, long newLastSeq, long read, long produced,
                              long written, long failed, long filtered, long bytes, Instant now) {
        if (newLastSeq < lastSeq) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Checkpoint sequence must advance monotonically; refusing to move from "
                            + lastSeq + " back to " + newLastSeq,
                    Map.of("splitId", splitId.toString(), "current", lastSeq, "proposed", newLastSeq));
        }
        return new Checkpoint(splitId, runId, tenantId, newCursor, newLastSeq,
                recordsRead + read,
                recordsProduced + produced,
                recordsWritten + written,
                recordsFailed + failed,
                recordsFiltered + filtered,
                bytesRead + bytes,
                batchesCommitted + 1,
                createdAt, now);
    }

    /**
     * A fresh checkpoint positioned where another chunk stopped.
     *
     * <p>For a chunk generated mid-run: it has read nothing itself, so every counter stays at zero,
     * but it must begin after the last record its predecessor wrote rather than at the start of the
     * source. Distinct from {@link #copiedTo} for exactly that reason — a retry inherits the
     * counters because it is continuing the same chunk, this does not because it is a new one.
     *
     * <p>{@code lastSeq} deliberately stays at zero. A sequence number counts position <em>within a
     * chunk</em>, not within a run, so carrying the predecessor's forward left the new chunk
     * claiming to be a thousand records in before it had read one — and its first real advance then
     * looked like the sequence going backwards, which {@link #advance} correctly refuses.
     */
    /**
     * Moves records from written to failed after the destination rejected them late.
     *
     * <p>An asynchronous sink accepts a batch and decides afterwards. Those records were counted as
     * written when they were handed over, because at that moment nothing said otherwise — and when
     * the destination comes back and refuses some of them, the count has to be corrected rather
     * than left flattering. A run that says it wrote a hundred records into an org holding none is
     * the failure this platform exists to prevent.
     */
    public Checkpoint recordingLateFailures(long lateFailures) {
        if (lateFailures <= 0) {
            return this;
        }
        long corrected = Math.max(0, recordsWritten - lateFailures);
        return new Checkpoint(splitId, runId, tenantId, sourceCursor, lastSeq,
                recordsRead, recordsProduced, corrected, recordsFailed + lateFailures,
                recordsFiltered, bytesRead, batchesCommitted, createdAt, updatedAt);
    }

    public Checkpoint startingFrom(JsonNode cursor) {
        return new Checkpoint(splitId, runId, tenantId, Json.orEmpty(cursor), 0,
                0, 0, 0, 0, 0, 0, 0, createdAt, updatedAt);
    }

    /** Whether any progress has been made, and therefore whether resuming differs from restarting. */
    public boolean hasProgress() {
        return batchesCommitted > 0;
    }

    /**
     * Whether this checkpoint says anything about where its chunk should start.
     *
     * <p>Wider than {@link #hasProgress()} and the distinction cost a migration its correctness. A
     * chunk of a lazily chunked source is created with <em>no range of its own</em>: its start is a
     * cursor seeded here from where the previous chunk finished, and until it runs it has committed
     * nothing. Judged on progress alone such a chunk looks empty and disposable — and discarding it
     * does not lose progress, it loses the only record of where the chunk begins. The replacement
     * then starts from the beginning of the source and reads everything again.
     *
     * <p>So: progress, or a position, or both. Either makes this worth carrying to a retry.
     */
    public boolean hasResumePosition() {
        return hasProgress() || (sourceCursor != null && !sourceCursor.isEmpty());
    }

    /**
     * This position, transplanted onto a chunk of a retrying run.
     *
     * <p>A checkpoint is keyed by chunk, and a retry's chunks are new, so resuming a retry means
     * moving the position across rather than pointing at the old one. Sharing it would let a
     * running retry advance a finished run's record — the one thing the design refuses to do
     * anywhere, because it is what makes a completed run's account trustworthy.
     *
     * <p>The counters travel with the cursor. They have to: {@code advance} adds to them, and a
     * chunk resuming at row nine thousand with its counters reset to zero would report having read
     * only the remainder, quietly understating the run by everything the first attempt had done.
     */
    public Checkpoint copiedTo(RunId newRunId, SplitId newSplitId) {
        return new Checkpoint(newSplitId, newRunId, tenantId, sourceCursor, lastSeq,
                recordsRead, recordsProduced, recordsWritten, recordsFailed, recordsFiltered,
                bytesRead, batchesCommitted, createdAt, updatedAt);
    }

    /**
     * Records the transform produced that the sink neither accepted nor rejected.
     *
     * <p>Compares against {@code recordsProduced} rather than {@code recordsRead} because a
     * transform may legitimately change the count: a filter drops records and a splitter multiplies
     * them. What must never happen is a record entering the sink stage and vanishing, and that is
     * what this measures. Non-zero after a clean split completion is a defect.
     */
    public long unaccountedRecords() {
        return recordsProduced - recordsWritten - recordsFailed;
    }

    /**
     * Source records that were neither dropped by a transform nor turned into anything.
     *
     * <p>Every record read is either filtered out or produces at least one output, so a positive
     * value here means records disappeared inside the transform stage without being counted.
     */
    public long unexplainedReads() {
        long survived = recordsRead - recordsFiltered;
        return Math.max(0, survived - recordsProduced);
    }
}
