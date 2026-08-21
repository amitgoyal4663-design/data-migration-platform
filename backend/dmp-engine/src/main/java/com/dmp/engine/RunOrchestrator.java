package com.dmp.engine;

import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.AuditLogPort;
import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.application.port.out.RecordErrorPort;
import com.dmp.application.port.out.RunEventPublisher;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.audit.AuditAction;
import com.dmp.domain.audit.AuditEntry;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.ReplayOptions;
import com.dmp.domain.run.RetryOptions;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.RunMetrics;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.RunTrigger;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.run.SplitState;
import com.dmp.domain.tenant.TenantId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Starts, pauses, stops and completes runs.
 *
 * <p>Everything here manipulates run state; nothing here moves data. Data movement happens in
 * workers, driven by {@link WorkerLoop}. Keeping the two apart is what stops a stop request from
 * having to interrupt a thread mid-batch — it changes state, and workers notice at their next
 * checkpoint boundary.
 */
@Service
public class RunOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(RunOrchestrator.class);

    /**
     * Records per replay chunk when the pipeline states no preference.
     *
     * <p>Smaller than a normal chunk on purpose. These records already failed once, so a replay is
     * likelier than an ordinary run to fail again, and a smaller chunk means less to repeat when it
     * does.
     */
    private static final int DEFAULT_REPLAY_CHUNK = 500;

    /** Ceiling, so a pipeline configured with a tiny chunk size cannot plan a runaway replay. */
    private static final int MAX_REPLAY_CHUNKS = 10_000;

    private final PipelineRepository pipelines;
    private final PipelineVersionRepository versions;
    private final RunRepository runs;
    private final SplitRepository splits;
    private final CheckpointRepository checkpoints;
    private final RecordErrorPort recordErrors;
    private final RunPlanner planner;
    private final RunEventPublisher events;
    private final AuditLogPort auditLog;
    private final TenantContext tenantContext;
    private final com.dmp.engine.schedule.ExternalJobScheduler externalJobs;
    private final Clock clock;

    public RunOrchestrator(PipelineRepository pipelines,
                           PipelineVersionRepository versions,
                           RunRepository runs,
                           SplitRepository splits,
                           CheckpointRepository checkpoints,
                           RecordErrorPort recordErrors,
                           RunPlanner planner,
                           RunEventPublisher events,
                           AuditLogPort auditLog,
                           TenantContext tenantContext,
                           com.dmp.engine.schedule.ExternalJobScheduler externalJobs,
                           Clock clock) {
        this.pipelines = pipelines;
        this.versions = versions;
        this.runs = runs;
        this.splits = splits;
        this.checkpoints = checkpoints;
        this.recordErrors = recordErrors;
        this.planner = planner;
        this.events = events;
        this.auditLog = auditLog;
        this.tenantContext = tenantContext;
        this.externalJobs = externalJobs;
        this.clock = clock;
    }

    /**
     * Creates a run for a pipeline's published version.
     *
     * <p>An {@code idempotencyKey} makes this safe to call twice. A scheduler firing again after a
     * redelivery, or a user double-clicking Run, must not start the same migration twice — and the
     * guard is a unique index rather than a check, so it holds under a genuine race.
     */
    public Run start(PipelineId pipelineId, RunTrigger trigger, String idempotencyKey) {
        return start(pipelineId, trigger, idempotencyKey, com.dmp.common.json.Json.emptyObject());
    }

    /**
     * The values this pipeline's published version expects when a run is started.
     *
     * <p>Asked before the Run dialog is drawn, so it shows exactly the boxes this pipeline needs
     * and nothing else. An unpublished pipeline, or one whose source takes no parameters, answers
     * empty — which is every pipeline that existed before this feature.
     */
    public java.util.Set<String> runParameterNames(PipelineId pipelineId) {
        TenantId tenantId = tenantContext.currentTenant();

        return pipelines.findById(tenantId, pipelineId)
                .flatMap(Pipeline::publishedVersionNumber)
                .flatMap(number -> versions.findByNumber(tenantId, pipelineId, number))
                .map(version -> planner.parameterNames(tenantId, version))
                .orElseGet(java.util.Set::of);
    }

    /**
     * Creates a run with the parameters its source query will be given.
     *
     * <p>The parameters are decided here, once, and stored on the run. Whatever produced them — a
     * person typing a range, a schedule computing a window — has finished its job by this point,
     * and everything downstream reads the stored values. That is what makes a retry hours later
     * cover the window the original was asked for rather than a freshly computed one.
     */
    public Run start(PipelineId pipelineId, RunTrigger trigger, String idempotencyKey,
                     com.fasterxml.jackson.databind.JsonNode parameters) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();

        Pipeline pipeline = pipelines.findById(tenantId, pipelineId)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Pipeline not found", Map.of("pipelineId", pipelineId.toString())));

        if (!pipeline.isRunnable()) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "Pipeline '" + pipeline.name() + "' has no published version to run. "
                            + "Publish a version first.",
                    Map.of("pipelineId", pipelineId.toString(), "status", pipeline.status().name()));
        }

        if (idempotencyKey != null) {
            var existing = runs.findByIdempotencyKey(tenantId, idempotencyKey);
            if (existing.isPresent()) {
                log.info("Run for idempotency key '{}' already exists as {}",
                        idempotencyKey, existing.get().id());
                return existing.get();
            }
        }

        int versionNumber = pipeline.publishedVersionNumber().orElseThrow();
        PipelineVersion version = versions.findByNumber(tenantId, pipelineId, versionNumber)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Published version " + versionNumber + " not found",
                        Map.of("pipelineId", pipelineId.toString())));

        Run run = runs.create(Run.create(tenantId, pipelineId, version.id(), versionNumber,
                version.mode(), trigger, idempotencyKey, tenantContext.currentActor(), null,
                parameters, now));

        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(), AuditAction.RUN_START,
                "run", run.id().toString(),
                "Started run of '" + pipeline.name() + "' version " + versionNumber,
                null, null, now));

        publish(RunEventPublisher.Type.RUN_CREATED, run, pipeline.name(),
                Map.of("trigger", trigger.name()));

        log.info("Created run {} for pipeline '{}' version {}", run.id(), pipeline.name(), versionNumber);
        return run;
    }

    /**
     * Re-attempts the chunks of a finished run that did not succeed.
     *
     * <p>Creates a <em>new</em> run rather than reopening the old one. The original's duration,
     * metrics and published events are a finished account of what happened; rewriting them would
     * make "how long did that migration take" unanswerable and would break the terminal states the
     * reaper, the scheduler and the console all rely on. The two are linked by {@code retryOf}, so
     * they stay legible as one effort.
     *
     * <p>The retry runs the <em>same pinned version</em> as the original, not whatever is published
     * now. Re-attempting against a definition somebody edited in the meantime is not a retry of the
     * failed run; it is a different migration wearing its name, and half its chunks would carry the
     * old logic while half carried the new.
     *
     * <p>Chunks that completed are not re-run. That is the entire value of the feature: a run that
     * failed on two of forty chunks costs two chunks to finish, not forty.
     */
    public Run retry(RunId originalRunId, RetryOptions options) {
        return retryWith(originalRunId, options, null);
    }

    /**
     * Re-attempts a single chunk of a finished run.
     *
     * <p>Shares every mechanic with the whole-run path — lineage, checkpoints, the duplicate guard
     * — so the two cannot drift apart on the parts that are easy to get wrong.
     */
    public Run retryChunk(RunId originalRunId, SplitId chunkId, RetryOptions.From from,
                          boolean acknowledgeDuplicates) {

        TenantId tenantId = tenantContext.currentTenant();
        Split chunk = splits.findById(tenantId, chunkId)
                .filter(candidate -> candidate.runId().equals(originalRunId))
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Chunk not found on this run",
                        Map.of("runId", originalRunId.toString(), "chunkId", chunkId.toString())));

        if (chunk.state() == SplitState.COMPLETED) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Chunk " + chunk.index() + " completed successfully. Re-running it would "
                            + "rewrite records that are already correct.",
                    Map.of("chunkId", chunkId.toString(), "chunkIndex", chunk.index()));
        }

        return retryWith(originalRunId,
                new RetryOptions(from, RetryOptions.Scope.FAILED_AND_CANCELLED, acknowledgeDuplicates),
                List.of(chunk));
    }

    /**
     * Re-delivers the records a finished run rejected, without reading the source again.
     *
     * <p>Not the same operation as {@link #retry}, and the difference is the state of the chunk. A
     * retry re-runs chunks that <em>failed</em>: nothing of theirs reached the target, so their
     * records are read from the source afresh. A replay covers records rejected inside chunks that
     * <em>succeeded</em> — the other rows in those chunks are already written, so re-reading the
     * source would push every one of them a second time to recover a handful. The rejected records
     * were kept precisely so that they alone can be sent again.
     *
     * <p>Everything after the read is the pipeline's own: the same transforms, the same sink, the
     * same batching, the same rejection capture. A record that fails again lands in this run's
     * dead-letter queue and can be replayed from there once its cause is fixed too — which is also
     * how a run with two distinct faults is worked through when only one has been dealt with.
     */
    public Run replay(RunId originalRunId, ReplayOptions options) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();

        Run original = runs.findById(tenantId, originalRunId)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Run not found", Map.of("runId", originalRunId.toString())));

        if (!original.state().isTerminal() || original.state() == RunState.ARCHIVED) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "Run " + originalRunId + " is " + original.state() + ". Its rejected records "
                            + "can be replayed once it has finished — more may still arrive.",
                    Map.of("runId", originalRunId.toString(), "state", original.state().name()));
        }

        long waiting = recordErrors.countByRun(tenantId, originalRunId);
        if (waiting <= 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Run " + originalRunId + " has no rejected records to replay. Either it "
                            + "rejected none, or the audit retention has already expired them.",
                    Map.of("runId", originalRunId.toString()));
        }

        PipelineVersion version = versionForReplay(tenantId, original, options);
        requireRedactionAcknowledged(version, options, waiting);

        Run replay = runs.create(Run.create(tenantId, original.pipelineId(),
                version.id(), version.versionNumber(), original.mode(),
                RunTrigger.REPLAY, null, tenantContext.currentActor(), original.id(),
                original.parameters(), now));

        List<Split> planned = planReplayChunks(tenantId, replay.id(), originalRunId,
                waiting, version.executionPolicy(), options.throughLatestVersion(), now);
        // The total is not set here: advanceToRunning counts the saved chunks and sets it, the same
        // way it does for a retry. Setting it twice is how the two accounts drift apart.
        splits.saveAll(planned);

        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(), AuditAction.RUN_START,
                "run", replay.id().toString(),
                "Replay of " + waiting + " record(s) rejected by run " + originalRunId
                        + ", through version " + version.versionNumber(),
                null, null, now));

        publish(RunEventPublisher.Type.RUN_CREATED, replay, null,
                Map.of("trigger", RunTrigger.REPLAY.name(),
                        "replayOf", originalRunId.toString(),
                        "records", waiting,
                        "chunks", planned.size(),
                        "versionNumber", version.versionNumber()));

        log.info("Created run {} replaying {} record(s) rejected by run {} through version {}",
                replay.id(), waiting, originalRunId, version.versionNumber());
        return replay;
    }

    /**
     * The version a replay executes.
     *
     * <p>The original's pinned version by default, for the same reason a retry uses it: the records
     * should meet the definition that produced their siblings. Overridden when the fix was in the
     * pipeline rather than at the destination, which is a deliberate choice and never inferred.
     */
    private PipelineVersion versionForReplay(TenantId tenantId, Run original, ReplayOptions options) {
        if (!options.throughLatestVersion()) {
            return versions.findById(tenantId, original.pipelineVersionId())
                    .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                            "The version run " + original.id() + " executed no longer exists. "
                                    + "Replay through the published version instead.",
                            Map.of("runId", original.id().toString())));
        }

        Pipeline pipeline = pipelines.findById(tenantId, original.pipelineId())
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Pipeline not found",
                        Map.of("pipelineId", original.pipelineId().toString())));

        int published = pipeline.publishedVersionNumber().orElseThrow(() -> new DmpException(
                ErrorCode.ILLEGAL_STATE_TRANSITION,
                "Pipeline '" + pipeline.name() + "' has no published version to replay through.",
                Map.of("pipelineId", pipeline.id().toString())));

        return versions.findByNumber(tenantId, original.pipelineId(), published)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Published version " + published + " not found",
                        Map.of("pipelineId", original.pipelineId().toString())));
    }

    /**
     * Refuses a replay that would send redacted placeholders to the target.
     *
     * <p>Payloads are redacted before they reach the dead-letter queue, which is the correct order
     * — an unredacted value must never be written down. The consequence is that the stored record
     * is not the record: a masked field holds {@code ***} and a hashed one holds a digest, and the
     * original value was never kept anywhere. Replaying loads those placeholders into the target as
     * though they were data.
     *
     * <p>Refused rather than prevented. A pipeline may redact a field the destination ignores, and
     * the owner is the one who knows. But it must be a decision, taken with the count in view.
     */
    private void requireRedactionAcknowledged(PipelineVersion version, ReplayOptions options,
                                              long waiting) {
        Set<String> redacted = version.auditPolicy().redactedFields();
        if (redacted.isEmpty() || options.acknowledgeRedaction()) {
            return;
        }

        throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                "This pipeline redacts " + String.join(", ", redacted) + " before storing a "
                        + "rejected record, so the stored copies hold placeholders rather than the "
                        + "original values — which were never kept. Replaying will write those "
                        + "placeholders into the target for all " + waiting + " record(s). "
                        + "Confirm with acknowledgeRedaction if the destination does not need "
                        + "those fields.",
                Map.of("redactedFields", redacted, "records", waiting));
    }

    /**
     * Divides the waiting rejections into chunks by offset over a stable ordering.
     *
     * <p>Planned up front rather than lazily, because unlike a live source the dead-letter queue is
     * finite, already counted, and cannot grow while the replay runs — the run that would have
     * added to it is over. There is nothing for lazy chunking to protect against here.
     */
    private List<Split> planReplayChunks(TenantId tenantId, RunId replayId, RunId originalRunId,
                                         long waiting, ExecutionPolicy execution,
                                         boolean applyTransforms, Instant now) {
        int perChunk = execution.effectiveRowsPerChunk(DEFAULT_REPLAY_CHUNK);
        int chunks = (int) Math.min(MAX_REPLAY_CHUNKS,
                Math.max(1, (waiting + perChunk - 1) / perChunk));

        List<Split> planned = new ArrayList<>(chunks);
        for (int index = 0; index < chunks; index++) {
            planned.add(Split.plan(replayId, tenantId, index,
                    Replay.spec(originalRunId, index * perChunk, perChunk, applyTransforms), now));
        }
        return planned;
    }

    private Run retryWith(RunId originalRunId, RetryOptions options, List<Split> preselected) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();

        Run original = runs.findById(tenantId, originalRunId)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Run not found", Map.of("runId", originalRunId.toString())));

        if (!original.state().isTerminal() || original.state() == RunState.ARCHIVED) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "Run " + originalRunId + " is " + original.state() + ". Only a run that has "
                            + "finished can be retried — stop it first if it is still going.",
                    Map.of("runId", originalRunId.toString(), "state", original.state().name()));
        }

        List<Split> retryable = preselected != null
                ? preselected : splitsToRetry(tenantId, original, options);

        if (retryable.isEmpty()) {
            // Distinguished, because "nothing to retry" reads as wrong when the run plainly shows
            // abandoned chunks. They are not retryable for a specific reason, and the reason is
            // also the answer: their records are in the dead-letter queue, so replay is the way
            // back rather than re-running ranges a successor already covered.
            long superseded = splits.findByRun(tenantId, original.id()).stream()
                    .filter(split -> split.state() == SplitState.ABANDONED
                            || split.state() == SplitState.FAILED)
                    .count();

            String reason = superseded > 0
                    ? "Run " + originalRunId + " has " + superseded + " failed chunk(s), but this "
                            + "run carried on past them and later chunks already covered the "
                            + "records they had not reached. Re-running them would write those "
                            + "records twice. The records they rejected are in the dead-letter "
                            + "queue — replay the run to send those again."
                    : "Run " + originalRunId + " has no chunks to retry. Every chunk either "
                            + "completed or falls outside the chosen scope.";

            throw new DmpException(ErrorCode.VALIDATION_FAILED, reason,
                    Map.of("runId", originalRunId.toString(), "scope", options.scope().name(),
                            "supersededChunks", superseded));
        }

        refuseChunkStartOnOpenEndedChunks(retryable, options);
        requireDuplicatesAcknowledged(original, retryable, options);

        // The original's parameters travel with the retry. A retry exists to finish the work the
        // first run was asked for, so it must read the same window — recomputing one here would
        // quietly move the boundaries by however long the failure took to notice.
        Run retry = runs.create(Run.create(tenantId, original.pipelineId(),
                original.pipelineVersionId(), original.versionNumber(), original.mode(),
                RunTrigger.RETRY, null, tenantContext.currentActor(), original.id(),
                original.parameters(), now));

        // Fresh chunks carrying the originals' specs. New identities rather than reused ones,
        // because the old chunk belongs to a finished run's record: its lease, its attempt count
        // and its failure message describe that run, not this one.
        List<Split> planned = new ArrayList<>(retryable.size());
        for (Split source : retryable) {
            planned.add(Split.plan(retry.id(), tenantId, source.index(), source.spec(), now));
        }
        splits.saveAll(planned);

        if (options.from() == RetryOptions.From.CHECKPOINT) {
            carryCheckpointsForward(tenantId, retryable, planned);
        }

        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(), AuditAction.RUN_START,
                "run", retry.id().toString(),
                "Retry of run " + originalRunId + ": " + planned.size() + " chunk(s), from "
                        + options.from() + ", scope " + options.scope(),
                null, null, now));

        publish(RunEventPublisher.Type.RUN_CREATED, retry, null,
                Map.of("trigger", RunTrigger.RETRY.name(),
                        "retryOf", originalRunId.toString(),
                        "chunks", planned.size(),
                        "from", options.from().name(),
                        "scope", options.scope().name()));

        log.info("Created run {} retrying {} chunk(s) of run {} from {}",
                retry.id(), planned.size(), originalRunId, options.from());
        return retry;
    }

    /**
     * Which of the original's chunks this retry covers.
     *
     * <p>Completed chunks are never included, whatever the scope. A chunk that succeeded wrote its
     * records correctly; re-running it is wasted work at best and a second copy at worst.
     */
    private List<Split> splitsToRetry(TenantId tenantId, Run original, RetryOptions options) {
        List<Split> all = splits.findByRun(tenantId, original.id());
        int highestIndex = all.stream().mapToInt(Split::index).max().orElse(-1);

        return all.stream()
                .filter(split -> switch (split.state()) {
                    case ABANDONED, FAILED -> true;
                    // A run does not reach a terminal state with chunks still pending or running
                    // unless it was stopped, so these belong with the cancelled ones.
                    // A chunk cancelled while parked on a remote job belongs here too, and it is
                    // the one case where a retry re-sends records the destination may already have
                    // accepted: the job was abandoned mid-flight, so what it did is unknown. That
                    // is precisely what the duplicate acknowledgement on a retry exists to warn
                    // about, and it is why upsert is the operation to migrate with.
                    case CANCELLED, PENDING, RUNNING, WAITING_EXTERNAL -> options.includesCancelled();
                    case COMPLETED -> false;
                })
                .filter(split -> !supersededLazily(split, highestIndex))
                .toList();
    }

    /**
     * Whether a lazily generated chunk's remaining work was taken over by its successor.
     *
     * <p>When a run is configured to carry on past a failed chunk, the next chunk is created from
     * the failed one's cursor — so everything it had not consumed becomes the successor's territory
     * rather than being lost. Re-running the failed chunk afterwards would cover exactly the ground
     * its successor already covered, which is the same collision that made CHUNK_START unsafe on
     * these chunks.
     *
     * <p>Nothing is stranded by this. Records the chunk read and had rejected are in the
     * dead-letter queue, where replay is the recovery path for a record-level failure; records it
     * never reached were read by the successor.
     *
     * <p>Only applies to open-ended chunks. A planned chunk owns a fixed range that no successor
     * can absorb, so it stays retryable however the run continued around it.
     */
    private static boolean supersededLazily(Split split, int highestIndex) {
        return OpenEnded.isOpenEnded(split.spec()) && split.index() < highestIndex;
    }

    /**
     * Refuses to restart a chunk that has no start of its own.
     *
     * <p>A lazily chunked run creates each chunk as the previous one finishes, and those chunks
     * carry no range at all — their position comes entirely from the checkpoint cursor. Discarding
     * that cursor does not rewind the chunk to its beginning; it rewinds it to <em>the beginning of
     * the source</em>, because with no boundaries and no cursor there is nothing else for the
     * reader to start from.
     *
     * <p>Which is how this was found: retrying chunk five of a lazily chunked run from CHUNK_START
     * re-read the collection from record one and collided with the five thousand records earlier
     * chunks had already written. The duplicate guard did not catch it, and could not have — that
     * guard asks how much the retried chunk itself had written, which was nothing. The damage came
     * from re-reading rows that belonged to other chunks entirely.
     *
     * <p>Refused rather than reinterpreted. Resuming from the checkpoint is what CHUNK_START would
     * have to mean here, and silently substituting one option for the other leaves the user
     * believing they restarted something they did not.
     */
    private void refuseChunkStartOnOpenEndedChunks(List<Split> retryable, RetryOptions options) {
        if (options.from() != RetryOptions.From.CHUNK_START) {
            return;
        }

        List<Integer> openEnded = retryable.stream()
                .filter(split -> OpenEnded.isOpenEnded(split.spec()))
                .map(Split::index)
                .toList();

        if (openEnded.isEmpty()) {
            return;
        }

        throw new DmpException(ErrorCode.VALIDATION_FAILED,
                "Chunk(s) " + openEnded + " were generated as this run proceeded rather than "
                        + "planned in advance, so they have no start of their own — their position "
                        + "is the saved cursor. Starting them over would re-read the source from "
                        + "the very beginning and re-send records other chunks already wrote. "
                        + "Retry from the checkpoint instead, which is what starting over would "
                        + "have to mean here anyway.",
                Map.of("chunks", openEnded, "from", options.from().name()));
    }

    /**
     * Refuses a restart that would silently re-write records the original run already wrote.
     *
     * <p>Judged on the checkpoints rather than by asking the sink. Opening a connection to a target
     * from the API thread, merely to read a capability, turns a retry request into something that
     * can fail on the target's credentials or availability — and this process is not guaranteed to
     * have connector plugins loaded at all.
     *
     * <p>Refused, never prevented. There are real reasons to want the duplicates — a target that
     * will be de-duplicated afterwards, a table about to be truncated — but not by accident, and
     * not without being shown the number first.
     */
    private void requireDuplicatesAcknowledged(Run original, List<Split> retryable,
                                               RetryOptions options) {
        if (!options.mayDuplicateCompletedWork() || options.acknowledgeDuplicates()) {
            return;
        }

        Set<SplitId> inScope = retryable.stream().map(Split::id).collect(Collectors.toSet());
        long alreadyWritten = checkpoints.findByRun(original.tenantId(), original.id()).stream()
                .filter(checkpoint -> inScope.contains(checkpoint.splitId()))
                .mapToLong(Checkpoint::recordsWritten)
                .sum();

        if (alreadyWritten <= 0) {
            // Nothing was written, so there is nothing to duplicate. Starting over is exactly the
            // same as resuming, and asking the user to confirm it would be noise.
            return;
        }

        throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                "Starting these chunks over will send " + alreadyWritten + " record(s) they had "
                        + "already written a second time. If this sink overwrites by key that is "
                        + "harmless; if it appends or inserts, it produces " + alreadyWritten
                        + " duplicate(s). Resume from the checkpoint instead, or set "
                        + "acknowledgeDuplicates to proceed.",
                Map.of("runId", original.id().toString(),
                        "recordsAtRisk", alreadyWritten,
                        "chunks", retryable.size()));
    }

    /**
     * Copies each original chunk's resume position onto its replacement.
     *
     * <p>Checkpoints are keyed by chunk and the retry's chunks are new, so resuming means moving
     * the position across rather than sharing it. Sharing would let a running retry advance a
     * finished run's record, which is the one thing this design refuses to do anywhere.
     */
    private void carryCheckpointsForward(TenantId tenantId, List<Split> originals,
                                         List<Split> replacements) {
        Map<Integer, Split> byIndex = new HashMap<>();
        for (Split replacement : replacements) {
            byIndex.put(replacement.index(), replacement);
        }

        for (Split original : originals) {
            Split replacement = byIndex.get(original.index());
            if (replacement == null) {
                continue;
            }
            checkpoints.findBySplit(tenantId, original.id())
                    .filter(Checkpoint::hasProgress)
                    .ifPresent(previous -> checkpoints.save(
                            previous.copiedTo(replacement.runId(), replacement.id())));
        }
    }

    /**
     * Moves a newly created run through validation, preparation and planning into RUNNING.
     *
     * <p>Called by a worker rather than by the API thread. Preparation can take hours for an
     * asynchronous source, and planning a large table is not something an HTTP request should wait
     * on — the run is created immediately and advanced in the background.
     */
    public void advanceToRunning(Run run, String workerId) {
        TenantId tenantId = run.tenantId();
        Instant now = clock.instant();

        ResolvedPipeline pipeline = planner.resolve(run);

        Run validated = runs.transitionState(tenantId, run.id(), RunState.CREATED,
                        run.markValidated(now))
                .orElseThrow(() -> concurrentlyAdvanced(run));

        // Asynchronous sources return a handle here and are polled; synchronous ones return an
        // empty preparation that is immediately ready, so the common case passes straight through.
        Run preparing = runs.transitionState(tenantId, run.id(), RunState.VALIDATED,
                        validated.recordPreparation(pipeline.sourceNode().id(),
                                com.dmp.common.json.Json.emptyObject(), now))
                .orElseThrow(() -> concurrentlyAdvanced(run));

        // A run derived from another arrives with its chunks already decided, so there is nothing
        // to plan. For a retry they are seeded from the run being re-attempted; for a replay they
        // are windows over its dead-letter queue. Asking the source to plan again would be worse
        // than redundant: a table that has grown since would divide into different ranges, and the
        // carefully preserved checkpoints would then describe positions inside chunks that no
        // longer exist.
        int chunks;
        if (run.retryOf() != null) {
            chunks = splits.findByRun(tenantId, run.id()).size();
            log.info("Run {} derives from {}; using its {} existing chunk(s) rather than planning",
                    run.id(), run.retryOf(), chunks);
        } else {
            planner.rejectRePlan(preparing, tenantId);
            chunks = planner.planChunks(preparing, pipeline,
                    com.dmp.connector.api.Preparation.none(), workerId);
        }

        if (chunks == 0) {
            // Nothing to read is a successful run, not a failure. A nightly incremental load that
            // finds no new rows must not page anyone.
            Run empty = runs.transitionState(tenantId, run.id(), RunState.PREPARING,
                            preparing.finalizing(now))
                    .orElseThrow(() -> concurrentlyAdvanced(run));
            runs.transitionState(tenantId, run.id(), RunState.FINALIZING, empty.complete(now));
            log.info("Run {} completed with nothing to read", run.id());
            return;
        }

        Run planned = preparing.withSplitPlan(chunks);
        runs.transitionState(tenantId, run.id(), RunState.PREPARING, planned.start(now))
                .orElseThrow(() -> concurrentlyAdvanced(run));

        publish(RunEventPublisher.Type.RUN_STARTED, run, pipeline.version().pipelineId().toString(),
                Map.of("chunks", chunks,
                        "rowsPerChunk", pipeline.execution()
                                .effectiveRowsPerChunk(pipeline.chunking().readFetchSizeOrDefault()),
                        "sequential", pipeline.execution().isSequential()));

        log.info("Run {} is now RUNNING with {} chunk(s)", run.id(), chunks);
    }

    /**
     * Completes a run once every chunk has reached a terminal state.
     *
     * <p>Called after each chunk finishes, and periodically by {@link RunReaper} for runs with
     * nothing in flight to trigger it. Deriving completion from the chunks rather than from a
     * counter means a miscounted increment cannot leave a finished run stuck at 99%.
     */
    public void completeIfFinished(Run run) {
        TenantId tenantId = run.tenantId();
        List<Split> all = splits.findByRun(tenantId, run.id());
        if (all.isEmpty()) {
            return;
        }

        Instant now = clock.instant();
        Run current = runs.findById(tenantId, run.id()).orElse(run);
        if (current.state().isTerminal() || current.state() == RunState.FINALIZING) {
            return;
        }

        if (current.state() == RunState.STOPPING) {
            settleStop(current, all, now);
            return;
        }

        boolean outstanding = all.stream().anyMatch(split -> split.state().isOutstanding());
        if (outstanding) {
            return;
        }

        long abandoned = all.stream().filter(s -> s.state() == SplitState.ABANDONED).count();

        Run finalizing = runs.transitionState(tenantId, run.id(), current.state(),
                current.finalizing(now)).orElse(null);
        if (finalizing == null) {
            return;
        }

        if (abandoned > 0) {
            // Some chunks exhausted their retries. The run failed, and the message says how much
            // of it succeeded so the operator can decide whether to re-run everything or just the
            // failed ranges.
            runs.transitionState(tenantId, run.id(), RunState.FINALIZING,
                    finalizing.fail("CHUNKS_ABANDONED",
                            abandoned + " of " + all.size() + " chunk(s) failed after exhausting "
                                    + "their retry budget", now));
            publish(RunEventPublisher.Type.RUN_FAILED, current, null,
                    Map.of("abandonedChunks", abandoned, "totalChunks", all.size()));
            log.error("Run {} failed: {} of {} chunk(s) abandoned", run.id(), abandoned, all.size());
            return;
        }

        runs.transitionState(tenantId, run.id(), RunState.FINALIZING, finalizing.complete(now));

        publish(RunEventPublisher.Type.RUN_COMPLETED, current, null,
                Map.of("chunks", all.size(),
                        "recordsRead", current.metrics().recordsRead(),
                        "recordsWritten", current.metrics().recordsWritten(),
                        "recordsFailed", current.metrics().recordsFailed()));

        log.info("Run {} completed: {} chunk(s)", run.id(), all.size());
    }

    /**
     * Brings a stopping run to rest.
     *
     * <p>A stopped run must not wait for chunks that were never claimed. Only RUNNING runs are
     * offered work, so the moment a stop is requested no pod will ever claim the remainder — and
     * treating a never-started chunk as outstanding leaves the run in STOPPING forever. They are
     * cancelled instead, which is also the honest record: they did not run.
     *
     * <p>Chunks already executing are left alone. They drain to their next checkpoint and finish
     * or fail on their own, and this runs again when they do.
     *
     * <p>Chunks parked on a remote job are cancelled rather than waited for. A Salesforce bulk job
     * can take many minutes, and holding a run in STOPPING for one is not what somebody pressing
     * Stop asked for. The records that job already accepted are counted — they were written, and
     * the checkpoint says so — but the per-record verdicts it had yet to hand back are not fetched,
     * and the job itself is left to age out of the destination. The log names it so that anybody
     * who needs to go and look at it can.
     */
    private void settleStop(Run current, List<Split> all, Instant now) {
        TenantId tenantId = current.tenantId();

        for (Split split : all) {
            if (split.state() == SplitState.PENDING || split.state() == SplitState.FAILED) {
                splits.transitionState(tenantId, split.id(), split.state(), split.cancel(now));

            } else if (split.state() == SplitState.WAITING_EXTERNAL) {
                if (splits.transitionState(tenantId, split.id(), SplitState.WAITING_EXTERNAL,
                        split.cancel(now)).isPresent()) {
                    externalJobs.cancel(split.id());
                    log.warn("Run {} was stopped while chunk {} was waiting on the destination. "
                                    + "The remote job {} is left running and will age out; its "
                                    + "per-record outcomes were not collected.",
                            current.id(), split.index(), split.externalJob());
                }
            }
        }

        long stillRunning = all.stream().filter(s -> s.state() == SplitState.RUNNING).count();
        if (stillRunning > 0) {
            log.debug("Run {} is stopping; {} chunk(s) still draining", current.id(), stillRunning);
            return;
        }

        Run finalizing = runs.transitionState(tenantId, current.id(), RunState.STOPPING,
                current.finalizing(now)).orElse(null);
        if (finalizing == null) {
            return;
        }

        long completed = all.stream().filter(s -> s.state() == SplitState.COMPLETED).count();
        long abandoned = all.stream().filter(s -> s.state() == SplitState.ABANDONED).count();

        // A run that stopped because a chunk gave up did not "stop" in the sense anybody reads that
        // word — it failed, and stopping was how it failed. Reporting STOPPED made a failed
        // migration indistinguishable in the run list from one somebody halted on purpose, and
        // nobody investigates a deliberate stop.
        //
        // Without this the same event produced two different outcomes depending on a setting about
        // how eagerly to stop: with stopRunOnChunkFailure on, an abandoned chunk ended as STOPPED;
        // with it off, the ordinary completion path called the identical situation FAILED.
        if (abandoned > 0) {
            runs.transitionState(tenantId, current.id(), RunState.FINALIZING,
                    finalizing.fail("CHUNKS_ABANDONED",
                            abandoned + " of " + all.size() + " chunk(s) failed after exhausting "
                                    + "their retry budget, and the run was stopped rather than "
                                    + "letting the remaining chunks meet the same failure", now));

            publish(RunEventPublisher.Type.RUN_FAILED, current, null,
                    Map.of("abandonedChunks", abandoned,
                            "completedChunks", completed,
                            "totalChunks", all.size()));

            log.error("Run {} failed: {} of {} chunk(s) abandoned, the rest cancelled by the stop",
                    current.id(), abandoned, all.size());
            return;
        }

        runs.transitionState(tenantId, current.id(), RunState.FINALIZING, finalizing.stopped(now));

        publish(RunEventPublisher.Type.RUN_STOPPED, current, null,
                Map.of("completedChunks", completed,
                        "cancelledChunks", all.size() - completed,
                        "totalChunks", all.size(),
                        "recordsWritten", current.metrics().recordsWritten()));

        log.info("Run {} stopped: {} of {} chunk(s) completed, the rest cancelled",
                current.id(), completed, all.size());
    }

    public Run pause(RunId runId) {
        return transition(runId, Run::pause, AuditAction.RUN_PAUSE, "Paused run",
                RunEventPublisher.Type.RUN_PAUSED);
    }

    public Run resume(RunId runId) {
        return transition(runId, Run::resume, AuditAction.RUN_RESUME, "Resumed run",
                RunEventPublisher.Type.RUN_RESUMED);
    }

    /**
     * Requests a stop.
     *
     * <p>A request, not an instant. Chunks in flight drain to their next checkpoint so the run
     * stops at a resumable boundary rather than tearing a batch in half. The distinct STOPPING
     * state is what lets the console show that difference instead of appearing unresponsive.
     */
    public Run stop(RunId runId) {
        Run stopping = transition(runId, Run::requestStop, AuditAction.RUN_STOP,
                "Requested stop of run", RunEventPublisher.Type.RUN_STOP_REQUESTED);

        // Settle immediately rather than waiting for the reaper. A run whose chunks had all
        // finished before the stop request has nothing in flight to trigger completion, so
        // without this it would sit in STOPPING until the next sweep for no reason.
        completeIfFinished(stopping);
        return stopping;
    }

    private Run transition(RunId runId, java.util.function.BiFunction<Run, Instant, Run> change,
                           AuditAction action, String summary, RunEventPublisher.Type eventType) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();

        Run run = runs.findById(tenantId, runId)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Run not found", Map.of("runId", runId.toString())));

        Run updated = runs.transitionState(tenantId, runId, run.state(), change.apply(run, now))
                .orElseThrow(() -> new DmpException(ErrorCode.CONCURRENT_MODIFICATION,
                        "The run changed state while this request was being handled. Reload it.",
                        Map.of("runId", runId.toString(), "state", run.state().name())));

        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(), action,
                "run", runId.toString(), summary, null, null, now));

        // Map.of rejects a null value, and the actor is absent until SSO lands.
        String actor = tenantContext.currentActor() == null ? "unknown" : tenantContext.currentActor();
        publish(eventType, updated, null,
                Map.of("previousState", run.state().name(), "state", updated.state().name(),
                        "requestedBy", actor));
        return updated;
    }

    /**
     * Announces something that happened, if a bus is configured.
     *
     * <p>Guarded on {@code isEnabled()} so a deployment without an event bus does not pay to build
     * events nobody receives. Never throws: an event bus being unavailable is not a reason for a
     * migration to fail.
     */
    private void publish(RunEventPublisher.Type type, Run run, String pipelineName,
                         Map<String, Object> details) {
        if (!events.isEnabled()) {
            return;
        }
        try {
            events.publish(new RunEventPublisher.RunEvent(
                    type, run.tenantId(), run.id(), run.pipelineId().toString(),
                    pipelineName, run.versionNumber(), clock.instant(), details));
        } catch (Exception e) {
            log.debug("Could not publish {} for run {}", type, run.id(), e);
        }
    }

    private DmpException concurrentlyAdvanced(Run run) {
        return new DmpException(ErrorCode.CONCURRENT_MODIFICATION,
                "Another worker advanced run " + run.id() + " first",
                Map.of("runId", run.id().toString()));
    }
}
