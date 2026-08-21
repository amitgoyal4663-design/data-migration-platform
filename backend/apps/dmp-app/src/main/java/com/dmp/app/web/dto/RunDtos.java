package com.dmp.app.web.dto;

import com.dmp.application.port.out.RecordErrorPort;
import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.ReplayOptions;
import com.dmp.domain.run.RetryOptions;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.Split;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/** Web contract for runs, chunks and rejected records. */
public final class RunDtos {

    private RunDtos() {
    }

    @Schema(name = "RunResponse")
    /** The placeholders a pipeline's source query expects, in the order they appear. */
    public record ParameterNames(
            @Schema(description = "e.g. [\"from\", \"to\"]. Empty when the query takes none.")
            java.util.List<String> names) {
    }

    /**
     * The body of a start request.
     *
     * <p>Optional in every sense: a pipeline whose query carries no placeholders sends nothing, and
     * behaves exactly as it did before parameters existed.
     */
    public record StartRequest(
            @Schema(description = "Values bound into the source's query, e.g. "
                    + "{\"from\": 5000} or {\"from\": \"2026-08-01T00:00:00Z\", "
                    + "\"to\": \"2026-08-02T00:00:00Z\"}")
            JsonNode parameters) {

        public StartRequest {
            parameters = com.dmp.common.json.Json.orEmpty(parameters);
        }
    }

    public record Response(
            String id,
            String pipelineId,
            String pipelineVersionId,
            int versionNumber,
            String mode,
            String trigger,
            @Schema(description = "The run this one re-attempts, if it is a retry. The two are "
                    + "separate runs on purpose: the original's duration and metrics stay a "
                    + "truthful record of what happened.")
            String retryOf,
            @Schema(description = "CREATED, VALIDATED, PREPARING, RUNNING, PAUSED, STOPPING, "
                    + "FINALIZING, COMPLETED, FAILED, STOPPED or ARCHIVED")
            String state,
            @Schema(description = "True while the run occupies worker capacity")
            boolean active,
            @Schema(description = "True when nothing further will happen without a new run")
            boolean terminal,
            @Schema(description = "Set while waiting on an external system rather than moving data")
            boolean waitingOnExternalSystem,
            Metrics metrics,
            @Schema(description = "Fraction of chunks finished, 0 to 1; null before planning")
            Double progress,
            @Schema(description = "Seconds elapsed, or total duration once ended")
            Long durationSeconds,
            @Schema(description = "Values bound into the source's query for this run — typically "
                    + "a from and a to. Recorded so the run says which range it actually covered, "
                    + "and so a retry repeats that range rather than a freshly computed one.")
            JsonNode parameters,
            String errorCode,
            String errorMessage,
            String triggeredBy,
            Instant createdAt,
            Instant startedAt,
            Instant endedAt) {

        public static Response from(Run run, Instant now) {
            var metrics = run.metrics();
            return new Response(
                    run.id().toString(),
                    run.pipelineId().toString(),
                    run.pipelineVersionId().toString(),
                    run.versionNumber(),
                    run.mode().name(),
                    run.trigger().name(),
                    run.retryOf() == null ? null : run.retryOf().toString(),
                    run.state().name(),
                    run.isActive(),
                    run.isTerminal(),
                    run.state().isAwaitingExternalSystem(),
                    Metrics.from(run),
                    metrics.progress().isPresent() ? metrics.progress().getAsDouble() : null,
                    run.duration(now).map(java.time.Duration::toSeconds).orElse(null),
                    run.parameters(),
                    run.errorCode(),
                    run.errorMessage(),
                    run.triggeredBy(),
                    run.createdAt(),
                    run.startedAt(),
                    run.endedAt());
        }
    }

    @Schema(name = "RunMetrics")
    public record Metrics(
            long recordsRead,
            @Schema(description = "Records the transform stage handed to the sink. Differs from "
                    + "recordsRead whenever a script drops or multiplies records.")
            long recordsProduced,
            long recordsWritten,
            long recordsFailed,
            @Schema(description = "Records a transform deliberately dropped")
            long recordsFiltered,
            long bytesRead,
            int chunksTotal,
            int chunksCompleted,
            int chunksFailed,
            @Schema(description = "Records that reached the sink and were neither written nor "
                    + "rejected. Measured against recordsProduced, because a transform may "
                    + "legitimately change the count. Anything other than zero on a completed run "
                    + "means records went missing, which is a defect.")
            long unaccountedRecords,
            @Schema(description = "Records written per second so far")
            Double throughputPerSecond) {

        static Metrics from(Run run) {
            var m = run.metrics();
            var elapsed = run.duration(Instant.now()).orElse(java.time.Duration.ZERO);
            var throughput = m.throughputPerSecond(elapsed);

            return new Metrics(
                    m.recordsRead(), m.recordsProduced(), m.recordsWritten(), m.recordsFailed(),
                    m.recordsFiltered(), m.bytesRead(),
                    m.splitsTotal(), m.splitsCompleted(), m.splitsFailed(),
                    m.unaccountedRecords(),
                    throughput.isPresent() ? throughput.getAsDouble() : null);
        }
    }

    @Schema(name = "ChunkResponse")
    public record ChunkResponse(
            String id,
            int index,
            String state,
            @Schema(description = "Connector-defined boundaries, e.g. a primary-key range")
            JsonNode spec,
            @Schema(description = "Worker currently holding this chunk")
            String assignedTo,
            @Schema(description = "When this worker's claim lapses if it stops reporting")
            Instant leaseExpiresAt,
            int attempt,
            String errorCode,
            String errorMessage,
            @Schema(description = "Records this chunk has written, from its checkpoint. Survives a "
                    + "failure, so it also says how much a restart-from-the-beginning would re-send.")
            long recordsWritten,
            @Schema(description = "Records this chunk's sink rejected")
            long recordsFailed,
            @Schema(description = "Share of this chunk's records that were rejected, 0 to 100. "
                    + "Makes the one bad chunk in forty visible without opening each in turn.")
            Integer rejectionPercent,
            @Schema(description = "True when this chunk has a saved position, so resuming it "
                    + "differs from starting it over")
            boolean resumable,
            @Schema(description = "True when the destination accepted this chunk as a job of its "
                    + "own and still holds a result file for it — a Salesforce bulk job. The "
                    + "console shows a download only for these; every other sink decides "
                    + "synchronously and has no file to offer.")
            boolean hasDestinationResults,
            @Schema(description = "The destination's own id for that job, so it can be found in "
                    + "the target system", nullable = true)
            String destinationJobId,
            Instant startedAt,
            Instant endedAt) {

        public static ChunkResponse from(Split split) {
            return from(split, null);
        }

        public static ChunkResponse from(Split split, Checkpoint checkpoint) {
            long produced = checkpoint == null ? 0 : checkpoint.recordsProduced();
            long failed = checkpoint == null ? 0 : checkpoint.recordsFailed();

            return new ChunkResponse(
                    split.id().toString(),
                    split.index(),
                    split.state().name(),
                    split.spec(),
                    split.assignedTo(),
                    split.leaseExpiresAt(),
                    split.attempt(),
                    split.errorCode(),
                    split.errorMessage(),
                    checkpoint == null ? 0 : checkpoint.recordsWritten(),
                    failed,
                    produced <= 0 ? null : (int) (failed * 100 / produced),
                    checkpoint != null && checkpoint.hasProgress(),
                    split.hasExternalJob(),
                    // The destination's own id, read from the handle the engine parked. Shown so
                    // somebody can open the job in the target system rather than only download
                    // through here — and it is the thing a support ticket quotes.
                    split.hasExternalJob()
                            ? split.externalJob().path("sink").path("jobId").asText(null)
                            : null,
                    split.startedAt(),
                    split.endedAt());
        }
    }

    @Schema(name = "RecordErrorResponse", description = "One record the sink rejected")
    public record RecordErrorResponse(
            String chunkId,
            String nodeId,
            long seq,
            String key,
            @Schema(description = "The external system's own error code, verbatim")
            String code,
            String message,
            @Schema(description = "The record, redacted per the pipeline's audit policy")
            JsonNode payload,
            Instant occurredAt) {

        public static RecordErrorResponse from(RecordErrorPort.RecordErrorEntry entry) {
            return new RecordErrorResponse(
                    entry.splitId().toString(),
                    entry.nodeId(),
                    entry.seq(),
                    entry.key(),
                    entry.code(),
                    entry.message(),
                    entry.payload(),
                    entry.occurredAt());
        }
    }

    @Schema(name = "RetryRequest",
            description = "How to re-attempt a run's unsuccessful chunks. Every field is optional; "
                    + "omitting the body resumes the failed chunks.")
    public record RetryRequest(
            @Schema(description = "CHECKPOINT resumes each chunk where it stopped. CHUNK_START "
                    + "discards the saved position and runs the chunk from its beginning, which "
                    + "re-sends whatever it had already written. Defaults to CHECKPOINT.",
                    allowableValues = {"CHECKPOINT", "CHUNK_START"})
            RetryOptions.From from,

            @Schema(description = "FAILED re-runs only the chunks that gave up. "
                    + "FAILED_AND_CANCELLED also picks up chunks that never started because the "
                    + "run was stopped — this is how a stopped run is resumed. Defaults to FAILED.",
                    allowableValues = {"FAILED", "FAILED_AND_CANCELLED"})
            RetryOptions.Scope scope,

            @Schema(description = "Required to proceed when starting chunks over would re-send "
                    + "records they had already written. The request is refused without it, and "
                    + "the refusal states how many records are at stake.")
            boolean acknowledgeDuplicates) {

        public RetryOptions toOptions() {
            return new RetryOptions(from(), scope(), acknowledgeDuplicates);
        }

        /** Defaults to resuming, which is the choice that cannot duplicate anything. */
        @Override
        public RetryOptions.From from() {
            return from == null ? RetryOptions.From.CHECKPOINT : from;
        }

        /** Defaults to the chunks that failed, leaving a stopped run's untouched ranges alone. */
        @Override
        public RetryOptions.Scope scope() {
            return scope == null ? RetryOptions.Scope.FAILED : scope;
        }
    }

    @Schema(name = "ReplayRequest",
            description = "How to re-deliver the records a run rejected. Both fields are optional; "
                    + "omitting the body sends them again unchanged through the version that "
                    + "rejected them.")
    public record ReplayRequest(
            @Schema(description = "Send the records through the pipeline's currently published "
                    + "version instead of the one the original run executed. Choose this when the "
                    + "fix was in the pipeline — a transform that now maps the value the target "
                    + "refused. Leave it false when the fix was at the destination.")
            boolean throughLatestVersion,

            @Schema(description = "Required to proceed when the pipeline redacts fields. Rejected "
                    + "records are stored redacted, so replaying writes the placeholder into the "
                    + "target rather than the original value, which was never kept. The request is "
                    + "refused without it, and the refusal names the fields.")
            boolean acknowledgeRedaction) {

        public ReplayOptions toOptions() {
            return new ReplayOptions(throughLatestVersion, acknowledgeRedaction);
        }
    }

    @Schema(name = "ErrorGroupResponse",
            description = "One distinct fault in a run, with an exact count of how many records "
                    + "hit it. Twenty thousand records failing one rule are one row here, not "
                    + "twenty thousand.")
    public record ErrorGroupResponse(
            String signature,
            @Schema(description = "The external system's own error code, verbatim")
            String code,
            @Schema(description = "A representative message, with per-record identifiers replaced")
            String message,
            String nodeId,
            @Schema(description = "Records that hit this fault. Exact, regardless of how many "
                    + "payloads were kept.")
            long count,
            @Schema(description = "Payloads available to inspect. Capped by the pipeline's audit "
                    + "policy, so this is normally far smaller than the count.")
            long samplesStored,
            Instant firstSeenAt,
            Instant lastSeenAt) {

        public static ErrorGroupResponse from(RecordErrorPort.SignatureSummary summary) {
            return new ErrorGroupResponse(
                    summary.signature(),
                    summary.code(),
                    summary.message(),
                    summary.nodeId(),
                    summary.count(),
                    summary.samplesStored(),
                    summary.firstSeenAt(),
                    summary.lastSeenAt());
        }
    }
}
