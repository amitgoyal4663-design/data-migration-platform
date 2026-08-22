package com.dmp.domain.run;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A unit of parallel work within a run, and simultaneously the unit of resumption.
 *
 * <p>Those two roles are the same object on purpose. Parallelism without resumability means a
 * worker crash at 90% costs the whole run; resumability without parallelism means a 500-million-row
 * migration runs on one thread. Binding them makes "kill a worker, another resumes from the last
 * checkpoint" a structural property rather than a feature.
 *
 * <p>{@code spec} is connector-defined and opaque: a primary-key range for JDBC, an {@code _id}
 * range for MongoDB, a topic-partition for Kafka, a file path for object storage. The engine
 * never interprets it — only the connector that produced it does.
 *
 * <p>{@code attempt} increments on each reassignment after failure. It is the input to the retry
 * ladder, whose delays are served by the delay queue and are therefore subject to its ~60 second
 * floor (ADR-0002).
 *
 * <p>{@code externalJob} is the other opaque field, and it is the one that makes a chunk survive
 * being handed to a system that answers later. See {@link SplitState#WAITING_EXTERNAL}.
 *
 * @param externalJob a remote job's handle — a Salesforce bulk job id, a Databricks statement id —
 *                    written here so the worker that submitted it need not be the one that observes
 *                    it finishing. Null for the overwhelming majority of chunks, whose sinks answer
 *                    immediately.
 * @param dueAt       when this chunk's remote job should next be asked whether it has finished.
 *                    Only meaningful while {@code WAITING_EXTERNAL}.
 * @param plannedRows how many rows this chunk covers, as the connector counted them at planning
 *                    time, or 0 when it could not say. Unlike {@code spec} this is not opaque —
 *                    it is the one thing about a chunk's contents the engine needs in its own
 *                    right, because it decides the batch. Without it a chunk of a known thousand
 *                    rows was written in whatever size the destination happened to prefer, and
 *                    the log then showed the source query once per batch: two reads, two writes,
 *                    and no way to tell that from the query having genuinely run twice.
 */
public record Split(
        SplitId id,
        RunId runId,
        TenantId tenantId,
        int index,
        SplitState state,
        JsonNode spec,
        String assignedTo,
        Instant leaseExpiresAt,
        int attempt,
        String errorCode,
        String errorMessage,
        Instant createdAt,
        Instant startedAt,
        Instant endedAt,
        Instant updatedAt,
        JsonNode externalJob,
        Instant dueAt,
        long plannedRows) {

    private static final int MAX_ERROR_MESSAGE_LENGTH = 4_000;

    public Split {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");

        spec = Json.orEmpty(spec);

        // Absent and empty mean the same thing — no remote job — and collapsing them here means
        // every caller can ask hasExternalJob() rather than each inventing its own emptiness check.
        if (externalJob != null && externalJob.isEmpty()) {
            externalJob = null;
        }
        if (index < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "Split index must not be negative",
                    Map.of("index", index));
        }
        if (attempt < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "Split attempt must not be negative",
                    Map.of("attempt", attempt));
        }
        if (errorMessage != null && errorMessage.length() > MAX_ERROR_MESSAGE_LENGTH) {
            errorMessage = errorMessage.substring(0, MAX_ERROR_MESSAGE_LENGTH);
        }
    }

    /** A chunk whose size the connector knows, which is what lets its batch be the whole chunk. */
    public static Split plan(RunId runId, TenantId tenantId, int index, JsonNode spec,
                             long plannedRows, Instant now) {
        return new Split(SplitId.newId(), runId, tenantId, index, SplitState.PENDING, spec,
                null, null, 0, null, null, now, null, null, now, null, null, plannedRows);
    }

    /** A chunk of unknown size — a key range, an open-ended cursor, a replay. */
    public static Split plan(RunId runId, TenantId tenantId, int index, JsonNode spec, Instant now) {
        return plan(runId, tenantId, index, spec, 0, now);
    }

    /**
     * Claims this split for a worker, holding it until the lease expires.
     *
     * <p>The lease is what makes a crashed worker recoverable without a heartbeat protocol between
     * pods. A worker retains its claim by extending the lease; one that stops doing so — because it
     * died, was partitioned, or is wedged in a stop-the-world pause — loses the split to another
     * pod once the lease lapses. {@code workerId} is recorded so the orphan is attributable.
     */
    public Split claim(String workerId, Instant now, Duration lease) {
        state.requireTransitionTo(SplitState.RUNNING);
        // externalJob is carried through deliberately. A chunk claimed while holding one is being
        // picked up to be settled, not re-executed, and the handle is how the executor knows that.
        return new Split(id, runId, tenantId, index, SplitState.RUNNING, spec, workerId, now.plus(lease), attempt,
                null, null, createdAt, startedAt == null ? now : startedAt, null, now, externalJob, null,
                plannedRows);
    }

    /**
     * Parks this chunk on a remote job and lets go of the worker.
     *
     * <p>The claim is surrendered — no worker, no lease — because the wait may be minutes and the
     * pod holding it may not survive them. Everything needed to resume is now in the split itself.
     *
     * @param job    the connector's handle on the remote work, opaque to the engine
     * @param nextAt when to ask the destination again
     */
    public Split parkOnExternalJob(JsonNode job, Instant nextAt, Instant now) {
        state.requireTransitionTo(SplitState.WAITING_EXTERNAL);
        return new Split(id, runId, tenantId, index, SplitState.WAITING_EXTERNAL, spec, null, null, attempt,
                null, null, createdAt, startedAt, null, now, job, nextAt, plannedRows);
    }

    /** Moves the next poll without disturbing anything else. The destination is still working. */
    public Split pollAgainAt(Instant nextAt, Instant now) {
        if (state != SplitState.WAITING_EXTERNAL) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "Only a parked split has a poll time; this one is " + state,
                    Map.of("splitId", id.toString(), "state", state.name()));
        }
        return new Split(id, runId, tenantId, index, state, spec, assignedTo, leaseExpiresAt, attempt,
                errorCode, errorMessage, createdAt, startedAt, endedAt, now, externalJob, nextAt, plannedRows);
    }

    /**
     * The destination has finished; return this chunk to the pool to be settled.
     *
     * <p>PENDING rather than COMPLETED, and the attempt counter does not move. Somebody still has
     * to fetch the per-record rejections the destination decided on, release the remote job and
     * roll the counts into the run — and that is the worker's completion path, reached by an
     * ordinary claim. The handle rides along so the worker settles this job rather than starting a
     * second one.
     */
    public Split externalJobFinished(Instant now) {
        state.requireTransitionTo(SplitState.PENDING);
        return new Split(id, runId, tenantId, index, SplitState.PENDING, spec, null, null, attempt,
                null, null, createdAt, startedAt, null, now, externalJob, null, plannedRows);
    }

    /**
     * Marks this chunk done.
     *
     * <p>The remote job handle is <b>kept</b>, as it is on a failure. It used to be cleared here on
     * the reasoning that a finished chunk has nothing left to settle — true, and beside the point.
     * The handle is also the only way back to what the destination recorded about the work, and a
     * chunk that succeeded has results worth reading too: which records the org confirmed, and the
     * job id to quote when somebody asks about them. Clearing it meant a download was offered for
     * the chunks that failed and refused for the ones that worked.
     *
     * <p>It costs one short string on a document that already exists. What it buys is that "show me
     * what the destination did with chunk 7" has the same answer whether or not chunk 7 went well.
     */
    public Split complete(Instant now) {
        state.requireTransitionTo(SplitState.COMPLETED);
        return new Split(id, runId, tenantId, index, SplitState.COMPLETED, spec, assignedTo, null, attempt,
                null, null, createdAt, startedAt, now, now, externalJob, null, plannedRows);
    }

    public Split fail(String code, String message, Instant now) {
        state.requireTransitionTo(SplitState.FAILED);
        // The handle is kept on a failure so an operator can see which remote job it was, and so
        // the reaper can tell a chunk that failed holding one from a chunk that never had one.
        return new Split(id, runId, tenantId, index, SplitState.FAILED, spec, assignedTo, null, attempt,
                code, message, createdAt, startedAt, now, now, externalJob, null, plannedRows);
    }

    /**
     * Returns this split to the pending pool for another attempt.
     *
     * <p>The worker assignment is cleared deliberately. Retrying on the same worker that just
     * failed is the least likely assignment to succeed if the cause was that worker rather than
     * the data.
     */
    public Split scheduleRetry(Instant now) {
        state.requireTransitionTo(SplitState.PENDING);
        // The handle is dropped here, and that is the whole difference between this and
        // externalJobFinished. A retry is a fresh attempt from the checkpoint: it re-reads and
        // submits new work. Carrying the old handle into it would send the executor down the
        // settle path instead, where it would harvest a job that has nothing to do with the
        // records this attempt is about to move.
        return new Split(id, runId, tenantId, index, SplitState.PENDING, spec, null, null, attempt + 1,
                errorCode, errorMessage, createdAt, null, null, now, null, null, plannedRows);
    }

    /**
     * Returns this chunk to the pool without having run it, to be picked up no earlier than
     * {@code notBefore}.
     *
     * <p>For work that cannot start yet through no fault of its own — today, a destination whose
     * agreed rate has been spent. The chunk read nothing, wrote nothing and holds nothing, so there
     * is nothing to undo and nothing to resume from; it simply becomes claimable later.
     *
     * <p><b>The attempt counter does not move, and that is the important line.</b> A chunk that
     * waited politely five times is not a chunk that failed five times, and counting it as one would
     * abandon a perfectly healthy migration for honouring the limit it was told to honour. Attempts
     * exist to stop something broken from being retried for ever; waiting for a budget is neither
     * broken nor a retry.
     *
     * <p>The worker assignment is cleared, so whichever pod is free when the time comes takes it.
     * Holding it for the pod that happened to look first would idle that pod's slot for the wait.
     */
    public Split deferUntil(Instant notBefore, Instant now) {
        state.requireTransitionTo(SplitState.PENDING);
        return new Split(id, runId, tenantId, index, SplitState.PENDING, spec, null, null, attempt,
                errorCode, errorMessage, createdAt, null, null, now, externalJob, notBefore, plannedRows);
    }

    /** When this chunk may next be claimed, if something asked for it to be held back. */
    public Optional<Instant> notBefore() {
        return Optional.ofNullable(dueAt);
    }

    public Split abandon(Instant now) {
        state.requireTransitionTo(SplitState.ABANDONED);
        return new Split(id, runId, tenantId, index, SplitState.ABANDONED, spec, assignedTo, null, attempt,
                errorCode, errorMessage, createdAt, startedAt, now, now, externalJob, null, plannedRows);
    }

    public Split cancel(Instant now) {
        state.requireTransitionTo(SplitState.CANCELLED);
        return new Split(id, runId, tenantId, index, SplitState.CANCELLED, spec, assignedTo, null, attempt,
                errorCode, errorMessage, createdAt, startedAt, now, now, externalJob, null, plannedRows);
    }

    /**
     * Extends the lease while the worker is still making progress.
     *
     * <p>Called on the interval given by {@code ExecutionPolicy.heartbeatInterval()}, which is a
     * third of the lease — so two consecutive missed heartbeats do not cost a worker a split it is
     * actively processing.
     */
    public Split heartbeat(Instant now, Duration lease) {
        if (state != SplitState.RUNNING) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "Only a RUNNING split holds a lease; this one is " + state,
                    Map.of("splitId", id.toString(), "state", state.name()));
        }
        return new Split(id, runId, tenantId, index, state, spec, assignedTo, now.plus(lease), attempt,
                errorCode, errorMessage, createdAt, startedAt, endedAt, now, externalJob, null, plannedRows);
    }

    /**
     * Whether this split's claim has lapsed and another worker may take it.
     *
     * <p>Only meaningful while RUNNING. A split with no lease has never been claimed.
     */
    public boolean isLeaseExpired(Instant now) {
        return state == SplitState.RUNNING
                && leaseExpiresAt != null
                && leaseExpiresAt.isBefore(now);
    }

    /**
     * Whether the retry budget has been exhausted and this split should be abandoned.
     *
     * <p>Called on a split that has just failed, so the attempt it was making counts. {@code
     * attempt} is zero during the first one, which makes {@link #attemptsMade()} the number to
     * compare — using {@code attempt} directly gives every chunk one more try than configured, and
     * "attempts per chunk: 1" would run it twice.
     */
    public boolean hasExhaustedAttempts(int maxAttempts) {
        return attemptsMade() >= maxAttempts;
    }

    /** How many times this split has been executed, counting the attempt in progress. */
    public int attemptsMade() {
        return attempt + 1;
    }

    /** Whether this split carries a handle on work already submitted to an external system. */
    public boolean hasExternalJob() {
        return externalJob != null && !externalJob.isEmpty();
    }

    /** Whether a parked chunk is due to be asked again. */
    public boolean isPollDue(Instant now) {
        return state == SplitState.WAITING_EXTERNAL
                && (dueAt == null || !dueAt.isAfter(now));
    }

    public Optional<String> worker() {
        return Optional.ofNullable(assignedTo);
    }

    public Optional<String> failureMessage() {
        return Optional.ofNullable(errorMessage);
    }
}
