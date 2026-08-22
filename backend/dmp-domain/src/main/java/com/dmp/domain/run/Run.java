package com.dmp.domain.run;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One execution of a specific pipeline version.
 *
 * <p>A run pins {@code pipelineVersionId}, not just {@code pipelineId}. Because published versions
 * are immutable, that reference remains a truthful account of what executed no matter how the
 * pipeline is edited afterwards.
 *
 * <p>{@code idempotencyKey} makes run creation safe to retry. A scheduler that fires twice because
 * of a delay-queue resume-token replay — an at-least-once guarantee this platform explicitly
 * accepts, per ADR-0002 — must not start the same migration twice. The key is enforced by a
 * unique constraint, so the second attempt loses at the database rather than at a race-prone check.
 */
public record Run(
        RunId id,
        TenantId tenantId,
        PipelineId pipelineId,
        PipelineVersionId pipelineVersionId,
        int versionNumber,
        PipelineMode mode,
        RunTrigger trigger,
        /**
         * The run this one re-attempts, or null if it stands alone.
         *
         * <p>A retry is a new run rather than a reopening of the old one. The original's duration,
         * metrics and published events are a finished account of what happened, and rewriting them
         * would make "how long did that migration take" unanswerable and break the terminal states
         * every other component relies on. The link is what keeps the two legible as one effort.
         */
        RunId retryOf,
        RunState state,
        String idempotencyKey,
        RunMetrics metrics,
        /**
         * Chunks currently in flight across the whole fleet, for concurrency limiting.
         *
         * <p>Read-only from the domain's point of view. It is incremented and decremented by
         * atomic database operations, because the invariant it enforces — never exceed
         * {@code ExecutionPolicy.maxConcurrentChunks} — spans every worker pod and cannot be
         * upheld by any single process reading then writing.
         */
        int activeSlots,
        JsonNode preparationState,
        /**
         * Values bound into the source's query for this run — typically a {@code from} and a
         * {@code to}.
         *
         * <p>Stored on the run rather than recomputed when it executes, and that is the whole
         * point. A scheduled run's window is decided once, at the moment it is created; a retry
         * three hours later must cover the same window rather than a fresh one shifted by however
         * long the failure took. Keeping them here also makes a run's coverage a matter of record —
         * the run list becomes a log of which ranges were actually processed, and a gap in it is
         * visible rather than inferred.
         *
         * <p>Never interpreted by the engine. It carries them from wherever they were decided —
         * a person typing them, a schedule computing them — to the connector that binds them.
         */
        JsonNode parameters,
        /**
         * Read and transform everything; write nothing.
         *
         * <p>What you run the day before the real one. The source is read in full, every script
         * runs against every record, and the counts and rejections that come out are the ones the
         * real run would produce — but no destination is opened, so nothing is created, no bulk job
         * is submitted and no quota is spent.
         *
         * <p>On the run rather than on the pipeline version, and that is the whole point: the
         * rehearsal and the real thing must execute the <em>same</em> published version. A flag on
         * the version would mean publishing a second version to do the real run, and then what was
         * rehearsed is not what runs.
         *
         * <p>Fixed for the life of the run. A retry of a dry run is a dry run; there is no way to
         * turn a rehearsal into a delivery halfway through, which is the kind of thing that would
         * otherwise be discovered afterwards.
         */
        boolean dryRun,
        String errorCode,
        String errorMessage,
        String triggeredBy,
        Instant createdAt,
        Instant startedAt,
        Instant endedAt,
        Instant updatedAt,
        long rowVersion) {

    private static final int MAX_ERROR_MESSAGE_LENGTH = 8_000;

    public Run {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(pipelineVersionId, "pipelineVersionId");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(trigger, "trigger");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");

        metrics = metrics == null ? RunMetrics.ZERO : metrics;
        preparationState = Json.orEmpty(preparationState);
        parameters = Json.orEmpty(parameters);
        if (activeSlots < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "activeSlots must not be negative", Map.of("activeSlots", activeSlots));
        }
        if (errorMessage != null && errorMessage.length() > MAX_ERROR_MESSAGE_LENGTH) {
            errorMessage = errorMessage.substring(0, MAX_ERROR_MESSAGE_LENGTH);
        }
    }

    public static Run create(TenantId tenantId, PipelineId pipelineId, PipelineVersionId versionId,
                             int versionNumber, PipelineMode mode, RunTrigger trigger,
                             String idempotencyKey, String triggeredBy, Instant now) {
        return create(tenantId, pipelineId, versionId, versionNumber, mode, trigger,
                idempotencyKey, triggeredBy, null, now);
    }

    /** Creates a run that re-attempts {@code retryOf}. */
    public static Run create(TenantId tenantId, PipelineId pipelineId, PipelineVersionId versionId,
                             int versionNumber, PipelineMode mode, RunTrigger trigger,
                             String idempotencyKey, String triggeredBy, RunId retryOf, Instant now) {
        return create(tenantId, pipelineId, versionId, versionNumber, mode, trigger,
                idempotencyKey, triggeredBy, retryOf, Json.emptyObject(), now);
    }

    /**
     * Creates a run with the parameters its source query will be given.
     *
     * <p>A retry passes forward the parameters of the run it re-attempts, which is what makes a
     * retry cover the same window rather than a newly computed one.
     */
    public static Run create(TenantId tenantId, PipelineId pipelineId, PipelineVersionId versionId,
                             int versionNumber, PipelineMode mode, RunTrigger trigger,
                             String idempotencyKey, String triggeredBy, RunId retryOf,
                             JsonNode parameters, Instant now) {
        return create(tenantId, pipelineId, versionId, versionNumber, mode, trigger,
                idempotencyKey, triggeredBy, retryOf, parameters, false, now);
    }

    /** Creates a run that reads and transforms but writes nothing. See {@link #dryRun()}. */
    public static Run create(TenantId tenantId, PipelineId pipelineId, PipelineVersionId versionId,
                             int versionNumber, PipelineMode mode, RunTrigger trigger,
                             String idempotencyKey, String triggeredBy, RunId retryOf,
                             JsonNode parameters, boolean dryRun, Instant now) {
        return new Run(RunId.newId(), tenantId, pipelineId, versionId, versionNumber, mode, trigger, retryOf,
                RunState.CREATED, idempotencyKey, RunMetrics.ZERO, 0, Json.emptyObject(), parameters,
                dryRun, null, null, triggeredBy, now, null, null, now, 0L);
    }

    public Run markValidated(Instant now) {
        return transition(RunState.VALIDATED, now);
    }

    /**
     * Enters or re-enters preparation, recording the handle a connector returned.
     *
     * <p>Re-entry is legal: each delay-queue poll finding the external job still pending returns
     * here rather than inventing a state per attempt. Handles are keyed by node id, because a
     * pipeline may have several sources each holding its own external job.
     *
     * <p>The handle is persisted rather than held by the worker, so that a Salesforce job id
     * submitted by one worker survives that worker's death and is polled — and released — by
     * another. See ADR-0012.
     */
    public Run recordPreparation(String nodeId, JsonNode handle, Instant now) {
        state.requireTransitionTo(RunState.PREPARING);
        ObjectNode merged = preparationState.deepCopy();
        merged.set(nodeId, handle == null ? Json.emptyObject() : handle);
        return new Run(id, tenantId, pipelineId, pipelineVersionId, versionNumber, mode, trigger, retryOf,
                RunState.PREPARING, idempotencyKey, metrics, activeSlots, merged, parameters, dryRun, errorCode, errorMessage,
                triggeredBy, createdAt, startedAt, endedAt, now, rowVersion);
    }

    /** Marks one node's external resource released, so the reaper stops considering it. */
    public Run releasePreparation(String nodeId, Instant now) {
        ObjectNode remaining = preparationState.deepCopy();
        remaining.remove(nodeId);
        return new Run(id, tenantId, pipelineId, pipelineVersionId, versionNumber, mode, trigger, retryOf,
                state, idempotencyKey, metrics, activeSlots, remaining, parameters, dryRun, errorCode, errorMessage,
                triggeredBy, createdAt, startedAt, endedAt, now, rowVersion);
    }

    /** Records the planned split count once preparation has produced a plan. */
    public Run withSplitPlan(int splitCount) {
        return withMetrics(metrics.withSplitsTotal(splitCount));
    }

    public Run start(Instant now) {
        state.requireTransitionTo(RunState.RUNNING);
        return new Run(id, tenantId, pipelineId, pipelineVersionId, versionNumber, mode, trigger, retryOf,
                RunState.RUNNING, idempotencyKey, metrics, activeSlots, preparationState, parameters, dryRun, null, null, triggeredBy,
                createdAt, startedAt == null ? now : startedAt, null, now, rowVersion);
    }

    /** Enters cleanup: final sink commits, then {@code release()} for every held handle. */
    public Run finalizing(Instant now) {
        return transition(RunState.FINALIZING, now);
    }

    public Run pause(Instant now) {
        return transition(RunState.PAUSED, now);
    }

    public Run resume(Instant now) {
        return transition(RunState.RUNNING, now);
    }

    public Run requestStop(Instant now) {
        return transition(RunState.STOPPING, now);
    }

    public Run stopped(Instant now) {
        return terminate(RunState.STOPPED, null, null, now);
    }

    /**
     * Marks the run complete.
     *
     * <p>A continuous run cannot complete on its own — a streaming pipeline reaching COMPLETED
     * would mean its unbounded source ended, which is a stop, not a success. Rejecting it here
     * prevents a whole class of misleading run history.
     */
    public Run complete(Instant now) {
        if (mode.isContinuous()) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "A " + mode + " run does not complete on its own; stop it instead",
                    Map.of("runId", id.toString(), "mode", mode.name()));
        }
        return terminate(RunState.COMPLETED, null, null, now);
    }

    public Run fail(String code, String message, Instant now) {
        return terminate(RunState.FAILED, code, message, now);
    }

    public Run archive(Instant now) {
        return transition(RunState.ARCHIVED, now);
    }

    public Run withMetrics(RunMetrics newMetrics) {
        return new Run(id, tenantId, pipelineId, pipelineVersionId, versionNumber, mode, trigger, retryOf,
                state, idempotencyKey, newMetrics, activeSlots, preparationState, parameters, dryRun, errorCode, errorMessage,
                triggeredBy, createdAt, startedAt, endedAt, updatedAt, rowVersion);
    }

    private Run transition(RunState target, Instant now) {
        state.requireTransitionTo(target);
        return new Run(id, tenantId, pipelineId, pipelineVersionId, versionNumber, mode, trigger, retryOf,
                target, idempotencyKey, metrics, activeSlots, preparationState, parameters, dryRun, errorCode, errorMessage,
                triggeredBy, createdAt, startedAt, endedAt, now, rowVersion);
    }

    private Run terminate(RunState target, String code, String message, Instant now) {
        state.requireTransitionTo(target);
        return new Run(id, tenantId, pipelineId, pipelineVersionId, versionNumber, mode, trigger, retryOf,
                target, idempotencyKey, metrics, activeSlots, preparationState, parameters, dryRun, code, message, triggeredBy,
                createdAt, startedAt, now, now, rowVersion);
    }

    /** Wall-clock duration: elapsed so far while running, final duration once ended. */
    public Optional<Duration> duration(Instant now) {
        if (startedAt == null) {
            return Optional.empty();
        }
        return Optional.of(Duration.between(startedAt, endedAt == null ? now : endedAt));
    }

    public boolean isTerminal() {
        return state.isTerminal();
    }

    public boolean isActive() {
        return state.isActive();
    }

    public Optional<String> failureMessage() {
        return Optional.ofNullable(errorMessage);
    }

    /**
     * Whether this run still holds external resources that were never released.
     *
     * <p>The reaper's sweep predicate (ADR-0012). A terminal run with a non-empty preparation
     * state means a worker died between failing and cleaning up — the exact case the happy-path
     * FINALIZING transition structurally cannot cover, and the one that leaks Salesforce bulk job
     * quota until someone notices.
     */
    public boolean hasUnreleasedExternalResources() {
        return state.mayHoldExternalResources() && !preparationState.isEmpty();
    }

    /** The persisted handle a connector returned from {@code prepare()}, if any. */
    public Optional<JsonNode> preparationHandle(String nodeId) {
        JsonNode handle = preparationState.get(nodeId);
        return Optional.ofNullable(handle);
    }

    /**
     * Whether the fleet may start another chunk of this run.
     *
     * <p>Advisory only. A worker must still win the atomic slot reservation, because between this
     * check and the claim another pod may have taken the last slot. This exists so a worker can
     * skip a run it obviously cannot help with, rather than issuing a reservation it will lose.
     */
    public boolean hasCapacityFor(com.dmp.domain.pipeline.ExecutionPolicy policy) {
        return policy.isUnlimited() || activeSlots < policy.maxConcurrentChunks();
    }
}
