package com.dmp.app.web;

import com.dmp.app.web.dto.PageResponse;
import com.dmp.app.web.dto.RunDtos;
import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.application.port.out.RecordErrorPort;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.RetryOptions;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.RunTrigger;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitId;
import com.dmp.engine.RunOrchestrator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Starting, monitoring and controlling runs. */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Runs", description = "Start, monitor, pause and stop pipeline executions")
public class RunController {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(RunController.class);

    private final RunOrchestrator orchestrator;
    private final RunRepository runs;
    private final SplitRepository splits;
    private final CheckpointRepository checkpoints;
    private final RecordErrorPort recordErrors;
    private final com.dmp.application.port.out.RecordIndexPort recordIndex;
    private final com.dmp.application.port.out.PipelineRepository pipelines;
    private final com.dmp.application.port.out.PipelineVersionRepository versions;
    private final TenantContext tenantContext;
    private final Clock clock;

    public RunController(RunOrchestrator orchestrator,
                         RunRepository runs,
                         SplitRepository splits,
                         CheckpointRepository checkpoints,
                         RecordErrorPort recordErrors,
                         com.dmp.application.port.out.RecordIndexPort recordIndex,
                         com.dmp.application.port.out.PipelineRepository pipelines,
                         com.dmp.application.port.out.PipelineVersionRepository versions,
                         TenantContext tenantContext,
                         Clock clock) {
        this.orchestrator = orchestrator;
        this.runs = runs;
        this.splits = splits;
        this.checkpoints = checkpoints;
        this.recordErrors = recordErrors;
        this.recordIndex = recordIndex;
        this.pipelines = pipelines;
        this.versions = versions;
        this.tenantContext = tenantContext;
        this.clock = clock;
    }

    @GetMapping("/pipelines/{pipelineId}/run-parameters")
    @Operation(summary = "The values this pipeline needs when a run is started",
            description = """
                    Names the placeholders the published version's source query uses, so the Run
                    dialog can ask for exactly those and nothing else. Empty for a pipeline whose
                    query takes no parameters, which is most of them.
                    """)
    public RunDtos.ParameterNames runParameters(
            @PathVariable String pipelineId,
            @Parameter(description = "Which of the source's named queries. Omitted for its own.")
            @RequestParam(required = false) String query) {

        PipelineId id = PipelineId.parse(pipelineId);
        return new RunDtos.ParameterNames(
                java.util.List.copyOf(orchestrator.runParameterNames(id, query)),
                java.util.List.copyOf(orchestrator.runListParameterNames(id, query)));
    }

    @GetMapping("/pipelines/{pipelineId}/queries")
    @Operation(summary = "The named ways this pipeline's source can find records",
            description = """
                    A connector may declare several — "By date range" for the nightly load, "By
                    policy number" for a support desk holding one policy and no idea when it was
                    last touched. Each declares its own placeholders, so choosing one changes which
                    boxes the run dialog asks for.

                    Empty for a source that declares none, which is every one written before this
                    existed: no variants means the configuration is already the query.
                    """)
    public RunDtos.ParameterNames runQueries(@PathVariable String pipelineId) {
        return new RunDtos.ParameterNames(orchestrator.runQueryNames(PipelineId.parse(pipelineId)));
    }

    @PostMapping("/pipelines/{pipelineId}/runs")
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Operation(summary = "Start a run",
            description = """
                    Creates a run of the pipeline's published version and returns immediately with
                    202. The run is planned and executed by workers in the background, because
                    planning a large table — or waiting on an asynchronous source — is not something
                    an HTTP request should hold a thread for.

                    Send `Idempotency-Key` to make this safe to retry. A repeated call with the same
                    key returns the existing run rather than starting a second migration.
                    """)
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Run created and queued"),
            @ApiResponse(responseCode = "409", description = "The pipeline has no published version")
    })
    public RunDtos.Response start(
            @PathVariable String pipelineId,
            @Parameter(description = "Makes a retry safe. A repeat returns the existing run.")
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(description =
                    "Values bound into the source's query, such as {\"from\": 5000}. Optional: a "
                            + "pipeline whose query has no placeholders needs none.")
            @RequestBody(required = false) RunDtos.StartRequest request) {

        Run run = orchestrator.start(PipelineId.parse(pipelineId), RunTrigger.API, idempotencyKey,
                request == null ? com.dmp.common.json.Json.emptyObject() : request.parameters(),
                request != null && request.dryRun(),
                request == null ? null : request.query());
        return RunDtos.Response.from(run, clock.instant());
    }

    @GetMapping("/runs/{runId}")
    @Operation(summary = "Fetch a run with its progress and counters")
    public RunDtos.Response get(@PathVariable String runId) {
        return RunDtos.Response.from(require(runId), clock.instant());
    }

    @GetMapping("/runs")
    @Operation(summary = "Search runs, newest first",
            description = "One entry per migration by default, with any resumes or retries of it "
                    + "nested in `attempts`. A page size therefore counts migrations, which is "
                    + "what somebody choosing \"25 per page\" means: a run stopped and resumed "
                    + "three times is one migration and four rows in the store, and paging by "
                    + "rows made the chosen number describe nothing anybody could see. Pass "
                    + "grouped=false for the flat list, where every attempt is its own entry.")
    public PageResponse<RunDtos.Response> search(
            @RequestParam(required = false) String pipelineId,
            @Parameter(description = "Repeatable, for example ?state=RUNNING&state=FAILED")
            @RequestParam(required = false) Set<RunState> state,
            @RequestParam(defaultValue = "true") boolean grouped,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size) {

        var criteria = new RunRepository.RunSearch(
                pipelineId == null ? null : PipelineId.parse(pipelineId),
                state == null ? Set.of() : state,
                null, null, null);

        var tenantId = tenantContext.currentTenant();
        var result = runs.search(tenantId, grouped ? criteria.onlyRoots() : criteria,
                PageQuery.of(page, size));

        if (!grouped) {
            return PageResponse.from(result, run -> RunDtos.Response.from(run, clock.instant()));
        }

        // One query for the whole page's attempts rather than one per row: a list of twenty-five
        // would otherwise be twenty-five round trips to show something most rows do not have.
        var byParent = runs
                .findAttemptsOf(tenantId, result.content().stream()
                        .map(com.dmp.domain.run.Run::id).toList())
                .stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        run -> run.retryOf().toString()));

        return PageResponse.from(result,
                run -> RunDtos.Response.withAttempts(run, byParent, clock.instant()));
    }

    @GetMapping("/runs/{runId}/chunks")
    @Operation(summary = "List a run's chunks",
            description = "Shows which worker holds each chunk, how many attempts it has taken, "
                    + "and why any of them failed.")
    public PageResponse<RunDtos.ChunkResponse> chunks(
            @PathVariable String runId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {

        Run run = require(runId);
        // A page of chunks, not all of them. This answered a screen showing fifty rows with every
        // chunk of the run — a hundred thousand documents and a hundred thousand DTOs for a run
        // that size, every time somebody opened it or it refreshed.
        Page<Split> chunks = splits.findByRun(run.tenantId(), run.id(), PageQuery.of(page, size));

        // Counters live on the checkpoints, not the chunks. Joined here rather than duplicated
        // onto the chunk on every batch: a chunk is written once per state change, a checkpoint
        // many times per chunk, and keeping a second copy in step would be a write per batch.
        //
        // Fetched for this page's chunks only — loading the run's whole checkpoint collection to
        // annotate fifty rows was the second half of the same problem.
        Map<SplitId, Checkpoint> progress = checkpoints
                .findBySplits(run.tenantId(), chunks.content().stream().map(Split::id).toList())
                .stream().collect(java.util.stream.Collectors.toMap(Checkpoint::splitId, c -> c));

        // PageResponse rather than the application's own Page, so this endpoint answers with the
        // hasNext every other paged endpoint here does — and the console's infinite scroll knows
        // when to stop asking.
        return PageResponse.from(chunks,
                split -> RunDtos.ChunkResponse.from(split, progress.get(split.id())));
    }

    @GetMapping("/runs/{runId}/chunk-summary")
    @Operation(summary = "How many of a run's chunks are in each state",
            description = "The numbers a screen needs to decide whether a run can be retried, and "
                    + "how much would be re-sent if it were. A few counts rather than the chunks "
                    + "themselves: working them out in the browser meant fetching every chunk of "
                    + "the run to render a banner and a button.")
    public RunDtos.ChunkSummaryResponse chunkSummary(@PathVariable String runId) {
        Run run = require(runId);
        Map<com.dmp.domain.run.SplitState, Long> byState =
                splits.countByState(run.tenantId(), run.id());

        // Records already delivered by chunks that did not finish, which is what a retry would send
        // a second time. Only the failed chunks are loaded, and a run with many of those has bigger
        // problems than this query.
        List<Split> failed = new java.util.ArrayList<>();
        failed.addAll(splits.findByRunAndState(run.tenantId(), run.id(),
                com.dmp.domain.run.SplitState.FAILED));
        failed.addAll(splits.findByRunAndState(run.tenantId(), run.id(),
                com.dmp.domain.run.SplitState.ABANDONED));

        long recordsAtRisk = checkpoints
                .findBySplits(run.tenantId(), failed.stream().map(Split::id).toList())
                .stream().mapToLong(Checkpoint::recordsWritten).sum();

        return RunDtos.ChunkSummaryResponse.from(byState, recordsAtRisk);
    }

    @GetMapping("/runs/{runId}/reconciliation")
    @Operation(summary = "The run's balance sheet, and the verdict on it",
            description = """
                    What a migration is signed off with: what the source handed over, what was
                    deliberately dropped, what the destination took, what it refused, and whether
                    anything is left over. Anything other than zero on the closing line is data
                    loss.

                    Where the pipeline indexes records, the sheet is checked against that index —
                    a second count, written by a different code path to a different store. That
                    comparison is the only check here capable of catching a defect in the counting
                    itself, which is why it is reported separately rather than folded in.
                    """)
    public RunDtos.ReconciliationResponse reconciliation(@PathVariable String runId) {
        return reconcile(require(runId));
    }

    @GetMapping(value = "/runs/{runId}/reconciliation.csv", produces = "text/csv")
    @Operation(summary = "The same balance sheet as a file",
            description = "For attaching to whatever the sign-off actually happens in. The same "
                    + "rows as the JSON, so the printed page and the screen cannot disagree.")
    public org.springframework.http.ResponseEntity<String> reconciliationCsv(
            @PathVariable String runId) {

        RunDtos.ReconciliationResponse report = reconcile(require(runId));

        StringBuilder csv = new StringBuilder();
        csv.append("section,label,value,detail,note\n");
        row(csv, "verdict", "Verdict", report.verdict(), "", "");
        row(csv, "verdict", "Run", report.runId(), "", "");
        row(csv, "verdict", "Pipeline", report.pipelineName(), "", "");
        row(csv, "verdict", "Generated", report.generatedAt().toString(), "", "");
        if (report.dryRun()) {
            row(csv, "verdict", "Dry run", "true", "",
                    "Nothing was written. These are the records that would have been sent");
        }
        for (RunDtos.ReconciliationLine line : report.sheet()) {
            row(csv, "sheet", line.label(), String.valueOf(line.count()), line.kind(), line.note());
        }
        for (RunDtos.ReconciliationCheck check : report.checks()) {
            row(csv, "check", check.label(), String.valueOf(check.expected()),
                    check.actual() + (check.passed() ? " (ok)" : " (differs by "
                            + check.difference() + ")"), check.note());
        }

        return org.springframework.http.ResponseEntity.ok()
                .header("Content-Disposition",
                        "attachment; filename=\"reconciliation-" + report.runId() + ".csv\"")
                .body(csv.toString());
    }

    /**
     * Builds the report, for both representations of it.
     *
     * <p>The index is asked for its tally and a failure to answer is not allowed to fail the
     * report. A deployment with no search backend, or one whose cluster is briefly unreachable,
     * still has a run's own counters — and the sheet built from those alone is the larger part of
     * the answer. Reporting nothing because the corroborating source is down would withhold what
     * is known in order to avoid admitting what is not.
     */
    private RunDtos.ReconciliationResponse reconcile(Run run) {
        Map<String, Long> byOutcome;
        try {
            byOutcome = recordIndex.countByOutcome(run.tenantId(), run.id());
        } catch (RuntimeException e) {
            log.warn("Could not read the record index while reconciling run {}: {}",
                    run.id(), e.getMessage());
            byOutcome = Map.of();
        }

        String pipelineName = pipelines.findById(run.tenantId(), run.pipelineId())
                .map(com.dmp.domain.pipeline.Pipeline::name)
                .orElse(run.pipelineId().toString());

        return RunDtos.ReconciliationResponse.from(
                com.dmp.domain.run.Reconciliation.of(run.state(), run.metrics(), byOutcome),
                run, pipelineName, clock.instant());
    }

    /** One CSV row, with the quoting a note containing a comma requires. */
    private static void row(StringBuilder csv, String section, String label, String value,
                            String detail, String note) {
        csv.append(quote(section)).append(',').append(quote(label)).append(',')
                .append(quote(value)).append(',').append(quote(detail)).append(',')
                .append(quote(note)).append('\n');
    }

    private static String quote(String value) {
        String text = value == null ? "" : value;
        return '"' + text.replace("\"", "\"\"") + '"';
    }

    @GetMapping("/runs/{runId}/errors")
    @Operation(summary = "List records this run rejected",
            description = """
                    The dead-letter queue. Each entry carries the record and the external system's
                    own error message, because a count of failures tells a user they have a problem
                    and nothing about how to fix it.

                    Payloads are redacted according to the pipeline's audit policy.
                    """)
    public List<RunDtos.RecordErrorResponse> errors(
            @PathVariable String runId,
            @RequestParam(defaultValue = "100") int limit) {

        Run run = require(runId);
        return recordErrors.findByRun(run.tenantId(), run.id(), Math.min(limit, 1000)).stream()
                .map(RunDtos.RecordErrorResponse::from)
                .toList();
    }

    @GetMapping("/runs/{runId}/error-groups")
    @Operation(summary = "Summarise a run's rejections by cause",
            description = """
                    The distinct faults behind a run's rejected records, costliest first.

                    A run that rejected twenty thousand records on one rule returns a single row
                    here with a count of twenty thousand, rather than twenty thousand rows saying
                    the same sentence. The counts are exact; the stored payloads behind them are
                    sampled per the pipeline's audit policy, which is why samplesStored is normally
                    far smaller than count.
                    """)
    public List<RunDtos.ErrorGroupResponse> errorGroups(
            @PathVariable String runId,
            @RequestParam(defaultValue = "50") int limit) {

        Run run = require(runId);

        // Resolved from the version this run executed, not the pipeline's current one. A run is a
        // truthful account of what happened, and naming a step after something published later
        // would attribute a rejection to a node that did not exist when it happened.
        Map<String, String> nodeNames = versions
                .findById(run.tenantId(), run.pipelineVersionId())
                .map(version -> version.definition().nodes().stream()
                        .collect(java.util.stream.Collectors.toMap(
                                com.dmp.domain.pipeline.NodeDefinition::id,
                                com.dmp.domain.pipeline.NodeDefinition::name,
                                (first, second) -> first)))
                .orElse(Map.of());

        return recordErrors.summariseByRun(run.tenantId(), run.id(), Math.min(limit, 500)).stream()
                .map(summary -> RunDtos.ErrorGroupResponse.from(summary, nodeNames))
                .toList();
    }

    @PostMapping("/runs/{runId}/retry")
    @Operation(summary = "Retry a finished run's unsuccessful chunks",
            description = """
                    Creates a new run that re-attempts only the chunks that did not succeed, linked
                    to the original by retryOf. Chunks that completed are never re-run, which is the
                    point: a run that failed on two of forty chunks costs two chunks to finish.

                    It executes the same pinned pipeline version as the original, not whatever is
                    published now — otherwise half the chunks would carry the old logic and half
                    the new.

                    Set scope to FAILED_AND_CANCELLED to also pick up chunks that never started
                    because the run was stopped. That is how a stopped run is resumed.

                    from=CHUNK_START discards each chunk's saved position and runs it from the
                    beginning. Against a sink that cannot absorb a repeated write, that re-sends
                    everything those chunks had already written, so it is refused unless
                    acknowledgeDuplicates is set.
                    """)
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Retry run created"),
            @ApiResponse(responseCode = "409",
                    description = "Run has not finished, has nothing to retry, or the restart "
                            + "would duplicate records without acknowledgeDuplicates")
    })
    public RunDtos.Response retry(@PathVariable String runId,
                                  @RequestBody(required = false) RunDtos.RetryRequest request) {

        RunDtos.RetryRequest effective = request == null ? new RunDtos.RetryRequest(null, null, false) : request;
        return RunDtos.Response.from(
                orchestrator.retry(RunId.parse(runId), effective.toOptions()), clock.instant());
    }

    @PostMapping("/runs/{runId}/replay")
    @Operation(summary = "Re-deliver the records a run rejected",
            description = """
                    Creates a new run that sends this run's rejected records through the pipeline
                    again — the same transforms, the same sink — without reading the source.

                    Distinct from retry, and the difference is which chunk failed. Retry re-runs
                    chunks that failed, reading their rows from the source afresh. Replay covers
                    records rejected inside chunks that succeeded: the other rows in those chunks
                    are already written, so re-reading the source would deliver every one of them a
                    second time to recover a handful.

                    Records that fail again land in the new run's dead-letter queue and can be
                    replayed from there once their cause is fixed too. That is also how a run with
                    two distinct faults is worked through when only one has been dealt with.

                    Only what is still stored can be replayed: entries the pipeline's audit
                    retention has expired are gone.
                    """)
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Replay run created"),
            @ApiResponse(responseCode = "409",
                    description = "Run has not finished, has no rejected records left, or the "
                            + "pipeline redacts fields and acknowledgeRedaction was not set")
    })
    public RunDtos.Response replay(@PathVariable String runId,
                                   @RequestBody(required = false) RunDtos.ReplayRequest request) {

        RunDtos.ReplayRequest effective =
                request == null ? new RunDtos.ReplayRequest(false, false) : request;
        return RunDtos.Response.from(
                orchestrator.replay(RunId.parse(runId), effective.toOptions()), clock.instant());
    }

    @PostMapping("/runs/{runId}/chunks/{chunkId}/retry")
    @Operation(summary = "Retry one chunk of a finished run",
            description = "For the single range that needed a closer look. Same mechanics as "
                    + "retrying the run, scoped to one chunk. A chunk that completed successfully "
                    + "is refused.")
    public RunDtos.Response retryChunk(@PathVariable String runId,
                                       @PathVariable String chunkId,
                                       @RequestBody(required = false) RunDtos.RetryRequest request) {

        RunDtos.RetryRequest effective = request == null ? new RunDtos.RetryRequest(null, null, false) : request;
        return RunDtos.Response.from(
                orchestrator.retryChunk(RunId.parse(runId), SplitId.parse(chunkId),
                        effective.from(), effective.acknowledgeDuplicates()),
                clock.instant());
    }

    @PostMapping("/runs/{runId}/pause")
    @Operation(summary = "Pause a run",
            description = "Chunks in flight finish; no new chunks are claimed. Resume continues "
                    + "from where each chunk left off.")
    public RunDtos.Response pause(@PathVariable String runId) {
        return RunDtos.Response.from(orchestrator.pause(RunId.parse(runId)), clock.instant());
    }

    @PostMapping("/runs/{runId}/resume")
    @Operation(summary = "Resume a paused run")
    public RunDtos.Response resume(@PathVariable String runId) {
        return RunDtos.Response.from(orchestrator.resume(RunId.parse(runId)), clock.instant());
    }

    @PostMapping("/runs/{runId}/stop")
    @Operation(summary = "Request a stop",
            description = "A request, not an instant. Chunks in flight drain to their next "
                    + "checkpoint so the run stops at a resumable boundary rather than tearing a "
                    + "batch in half. The run reports STOPPING until they finish.")
    public RunDtos.Response stop(@PathVariable String runId) {
        return RunDtos.Response.from(orchestrator.stop(RunId.parse(runId)), clock.instant());
    }

    private Run require(String runId) {
        return runs.findById(tenantContext.currentTenant(), RunId.parse(runId))
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Run not found", Map.of("runId", runId)));
    }
}
