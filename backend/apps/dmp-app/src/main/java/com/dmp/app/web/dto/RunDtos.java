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
import java.util.List;

/** Web contract for runs, chunks and rejected records. */
public final class RunDtos {

    private RunDtos() {
    }

    @Schema(name = "RunResponse")
    /** The placeholders a pipeline's source query expects, in the order they appear. */
    public record ParameterNames(
            @Schema(description = "e.g. [\"from\", \"to\"]. Empty when the query takes none.")
            java.util.List<String> names,
            @Schema(description = "Those of the above that take several values, because the query "
                    + "uses them inside IN (…) or $in. Sent as an array rather than a string.")
            java.util.List<String> lists) {

        public ParameterNames(java.util.List<String> names) {
            this(names, java.util.List.of());
        }
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
            JsonNode parameters,
            @Schema(description = "Read and transform everything, write nothing. The destination "
                    + "is never opened, so nothing is created and no quota is spent — which also "
                    + "means a dry run cannot tell you the destination would have accepted the "
                    + "records, only what would have been sent and which never got that far.")
            boolean dryRun,
            @Schema(description = "Which of the source's named queries to select records with, "
                    + "for example \"By policy number\". Omit for the connector's own query.")
            String query) {

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

            @Schema(description = "Resumes and retries of this run, oldest first. Empty for the "
                    + "overwhelming majority of runs, which were never resumed.")
            List<Response> attempts,
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
            @Schema(description = "True when this run rehearsed rather than delivered. Its record "
                    + "index entries are deliberately absent, because \"would have been "
                    + "transferred\" must never be searchable as \"was transferred\".")
            boolean dryRun,
            @Schema(description = "The named query this run selected records with. Null for the "
                    + "connector's own — which is what every run before this feature used.")
            String query,
            String errorCode,
            String errorMessage,
            String triggeredBy,
            Instant createdAt,
            Instant startedAt,
            Instant endedAt) {

        /**
         * This run with its whole chain of attempts nested underneath, oldest first.
         *
         * <p>Depth first, because an attempt can itself be resumed. Flattened rather than nested at
         * each level: a chain is read as a sequence of attempts, and a tree of one-child nodes is a
         * sequence drawn awkwardly.
         */
        public static Response withAttempts(Run run, java.util.Map<String, List<Run>> byParent,
                                            Instant now) {
            List<Run> children = byParent.getOrDefault(run.id().toString(), List.of()).stream()
                    .sorted(java.util.Comparator.comparing(
                            Run::createdAt, java.util.Comparator.nullsLast(Instant::compareTo)))
                    .toList();

            List<Response> attempts = children.stream()
                    .<Response>mapMulti((child, collect) -> {
                        Response nested = withAttempts(child, byParent, now);
                        collect.accept(nested);
                        nested.attempts().forEach(collect);
                    })
                    .toList();

            return from(run, now).carrying(attempts);
        }

        /** This response with its chain attached; everything else is left exactly as it is. */
        public Response carrying(List<Response> chain) {
            return new Response(id, pipelineId, pipelineVersionId, versionNumber, mode, trigger,
                    retryOf, chain, state, active, terminal, waitingOnExternalSystem, metrics,
                    progress, durationSeconds, parameters, dryRun, query, errorCode, errorMessage,
                    triggeredBy, createdAt, startedAt, endedAt);
        }

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
                    List.of(),
                    run.state().name(),
                    run.isActive(),
                    run.isTerminal(),
                    run.state().isAwaitingExternalSystem(),
                    Metrics.from(run),
                    metrics.progress().isPresent() ? metrics.progress().getAsDouble() : null,
                    run.duration(now).map(java.time.Duration::toSeconds).orElse(null),
                    run.parameters(),
                    run.dryRun(),
                    run.queryName(),
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

    /**
     * A run's chunks counted by state, and what a retry would re-send.
     *
     * <p>Exists so a console can answer "can this be retried, and at what cost" without holding the
     * chunks themselves.
     */
    @Schema(name = "ChunkSummaryResponse")
    public record ChunkSummaryResponse(
            @Schema(description = "Chunk count for each state that has any")
            java.util.Map<String, Long> byState,
            @Schema(description = "Chunks that failed or were abandoned")
            long failed,
            @Schema(description = "Chunks that never finished - cancelled, still pending, still "
                    + "running, or parked on a destination that answers later")
            long unfinished,
            @Schema(description = "Records already written by chunks that did not finish, and so "
                    + "the number a retry would deliver a second time")
            long recordsAtRisk) {

        public static ChunkSummaryResponse from(
                java.util.Map<com.dmp.domain.run.SplitState, Long> byState, long recordsAtRisk) {

            long failed = count(byState, com.dmp.domain.run.SplitState.FAILED)
                    + count(byState, com.dmp.domain.run.SplitState.ABANDONED);
            // WAITING_EXTERNAL belongs here, matching the engine's own retry scope: a run stopped
            // while a chunk was parked on a bulk job cancels it without collecting the
            // destination's per-record verdicts, so that chunk did not finish.
            long unfinished = count(byState, com.dmp.domain.run.SplitState.CANCELLED)
                    + count(byState, com.dmp.domain.run.SplitState.PENDING)
                    + count(byState, com.dmp.domain.run.SplitState.RUNNING)
                    + count(byState, com.dmp.domain.run.SplitState.WAITING_EXTERNAL);

            java.util.Map<String, Long> named = new java.util.LinkedHashMap<>();
            byState.forEach((state, n) -> named.put(state.name(), n));
            return new ChunkSummaryResponse(named, failed, unfinished, recordsAtRisk);
        }

        private static long count(java.util.Map<com.dmp.domain.run.SplitState, Long> byState,
                                  com.dmp.domain.run.SplitState state) {
            return byState.getOrDefault(state, 0L);
        }
    }

    /**
     * A run's balance sheet, and the verdict on it.
     *
     * <p>The artifact a migration is signed off with. Deliberately flat and label-carrying: the
     * console renders it without knowing what any particular line means, and the CSV export is the
     * same rows written out, so the printed page and the screen can never disagree.
     */
    @Schema(name = "ReconciliationResponse")
    public record ReconciliationResponse(
            @Schema(description = "BALANCED, DISCREPANCY, or INCOMPLETE while the run is still "
                    + "going. The only part most people read.")
            String verdict,
            @Schema(description = "The balance, in the order it should be read")
            List<ReconciliationLine> sheet,
            @Schema(description = "Comparisons between the run's own counters and the record "
                    + "index, which are written by different code paths to different stores. "
                    + "Empty when the pipeline does not index records.")
            List<ReconciliationCheck> checks,
            @Schema(description = "The record index's own tally, by outcome. Outcomes that did "
                    + "not occur are absent rather than zero.")
            java.util.Map<String, Long> byOutcome,
            long indexedTotal,
            @Schema(description = "Whether there is an index to compare against at all")
            boolean indexed,
            @Schema(description = "Whether the run has finished. A mid-run balance is arithmetic "
                    + "about a moving target.")
            boolean complete,
            @Schema(description = "The run this reconciles, for a report saved away from the tool")
            String runId,
            String pipelineName,
            @Schema(description = "True when these numbers describe a rehearsal. Carried on the "
                    + "report rather than only on the run, because the report is downloaded and "
                    + "read away from the screen that would otherwise have said so.")
            boolean dryRun,
            Instant generatedAt) {

        public static ReconciliationResponse from(com.dmp.domain.run.Reconciliation source,
                                                  Run run, String pipelineName, Instant now) {
            return new ReconciliationResponse(
                    source.verdict().name(),
                    source.sheet().stream().map(ReconciliationLine::from).toList(),
                    source.checks().stream().map(ReconciliationCheck::from).toList(),
                    source.byOutcome(),
                    source.indexedTotal(),
                    source.indexed(),
                    source.complete(),
                    run.id().toString(),
                    pipelineName,
                    run.dryRun(),
                    now);
        }
    }

    @Schema(name = "ReconciliationLine")
    public record ReconciliationLine(
            String label,
            long count,
            @Schema(description = "TOTAL, DEDUCTION, SUBTOTAL, RESULT, PENDING or BALANCE — so a "
                    + "console can style the row without parsing its label")
            String kind,
            @Schema(description = "Why this line is here, in the language of the person reading it")
            String note) {

        static ReconciliationLine from(com.dmp.domain.run.Reconciliation.Line line) {
            return new ReconciliationLine(line.label(), line.count(), line.kind().name(),
                    line.note());
        }
    }

    @Schema(name = "ReconciliationCheck")
    public record ReconciliationCheck(
            String label,
            @Schema(description = "What the run's own counters say")
            long expected,
            @Schema(description = "What the record index says")
            long actual,
            long difference,
            boolean passed,
            String note) {

        static ReconciliationCheck from(com.dmp.domain.run.Reconciliation.Check check) {
            return new ReconciliationCheck(check.label(), check.expected(), check.actual(),
                    check.difference(), check.passed(), check.note());
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
            @Schema(description = "Records this chunk did not deliver and did not mean to drop: "
                    + "a transform threw on them, or the destination refused them")
            long recordsFailed,
            @Schema(description = "Records a transform dropped on purpose. Not a failure, and "
                    + "shown beside one so the difference is visible per chunk rather than only "
                    + "in the run's totals — a chunk filtering far more than its neighbours is a "
                    + "question worth asking, and nothing else on this row would raise it.")
            long recordsFiltered,
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
            long filtered = checkpoint == null ? 0 : checkpoint.recordsFiltered();

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
                    filtered,
                    // Over what the chunk attempted to deliver, which is produced — records a
                    // filter dropped are deliberately not in it. Including them would let a
                    // pipeline that filters ninety-nine percent of its input report a near-perfect
                    // delivery rate however badly the destination behaved.
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
            @Schema(description = "The step's name from the canvas, which is what somebody named "
                    + "it and therefore what they will look for. Falls back to the node id for a "
                    + "step that has since been removed from the pipeline.")
            String node,
            @Schema(description = "Records that hit this fault. Exact, regardless of how many "
                    + "payloads were kept.")
            long count,
            @Schema(description = "Payloads available to inspect. Capped by the pipeline's audit "
                    + "policy, so this is normally far smaller than the count.")
            long samplesStored,
            Instant firstSeenAt,
            Instant lastSeenAt) {

        public static ErrorGroupResponse from(RecordErrorPort.SignatureSummary summary) {
            return from(summary, java.util.Map.of());
        }

        /**
         * @param nodeNames canvas id to the name the user gave it, from the version this run
         *                  executed — resolved by the caller, because the group knows which node
         *                  rejected the record but not what anybody called it
         */
        public static ErrorGroupResponse from(RecordErrorPort.SignatureSummary summary,
                                              java.util.Map<String, String> nodeNames) {
            return new ErrorGroupResponse(
                    summary.signature(),
                    summary.code(),
                    summary.message(),
                    summary.nodeId(),
                    nodeNames.getOrDefault(summary.nodeId(), summary.nodeId()),
                    summary.count(),
                    summary.samplesStored(),
                    summary.firstSeenAt(),
                    summary.lastSeenAt());
        }
    }
}
