package com.dmp.engine;

import com.dmp.application.port.out.ConnectorInstanceRepository;
import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.Source;
import com.dmp.connector.runtime.ConnectorContexts;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.Split;
import com.dmp.domain.tenant.TenantId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Turns a run into chunks.
 *
 * <p>Planning is the one moment the platform asks the source how its data should be divided. The
 * source's answer must be deterministic — planning the same table twice has to yield the same
 * boundaries — because a re-planned run would otherwise re-read ranges it finished and skip ranges
 * it never started.
 */
@Component
public class RunPlanner {

    private static final Logger log = LoggerFactory.getLogger(RunPlanner.class);

    private final PipelineVersionRepository versions;
    private final ConnectorInstanceRepository connectorInstances;
    private final ConnectorRegistry connectors;
    private final ConnectorContexts contexts;
    private final RunRepository runs;
    private final SplitRepository splits;
    private final Clock clock;
    private final int maxChunksPerRun;

    public RunPlanner(PipelineVersionRepository versions,
                      ConnectorInstanceRepository connectorInstances,
                      ConnectorRegistry connectors,
                      ConnectorContexts contexts,
                      RunRepository runs,
                      SplitRepository splits,
                      Clock clock,
                      @Value("${dmp.engine.max-chunks-per-run:100000}") int maxChunksPerRun) {
        this.versions = versions;
        this.connectorInstances = connectorInstances;
        this.connectors = connectors;
        this.contexts = contexts;
        this.runs = runs;
        this.splits = splits;
        this.clock = clock;
        this.maxChunksPerRun = maxChunksPerRun;
    }

    /**
     * The values a version's source expects to be supplied when a run is started.
     *
     * <p>Asked of the connector rather than worked out here, so the console can put the right boxes
     * on the Run dialog without the platform — or the frontend — knowing that a Databricks query
     * writes its placeholders as {@code :from}. A source with no parameters answers empty and the
     * dialog shows nothing extra, which is every pipeline that exists today.
     *
     * <p>Never throws. This answers a question a page asked while somebody was looking at it, and a
     * connector that cannot be loaded should grey out a box rather than break the page — the run
     * itself will fail with a far better message.
     */
    public Set<String> parameterNames(TenantId tenantId, PipelineVersion version) {
        return parameterNames(tenantId, version, null);
    }

    /**
     * The placeholders one of the source's named queries expects.
     *
     * <p>Per query, because that is the point of them: "by date range" wants a from and a to, "by
     * policy number" wants a list of policy numbers, and a dialog that asked for all three would be
     * asking for values two of which cannot be used.
     */
    public Set<String> parameterNames(TenantId tenantId, PipelineVersion version, String query) {
        try {
            NodeDefinition source = version.definition().nodesOfType(NodeType.SOURCE).stream()
                    .findFirst().orElse(null);
            if (source == null || source.connectorInstanceId() == null) {
                return Set.of();
            }
            ConnectorInstance instance = connectorInstances.findById(tenantId,
                    ConnectorInstanceId.of(source.connectorInstanceId())).orElse(null);
            if (instance == null) {
                return Set.of();
            }
            // The variant is merged first, so the connector reads placeholders out of the query
            // that will actually run — it never learns that others were on offer.
            return connectors.source(instance.connectorType()).parameterNames(
                    com.dmp.connector.api.QueryVariants.apply(instance.config(), query));
        } catch (RuntimeException e) {
            log.debug("Could not determine run parameters for version {}: {}",
                    version.id(), e.getMessage());
            return Set.of();
        }
    }

    /** Which of those placeholders take a list, so the dialog offers a list rather than a box. */
    public Set<String> listParameterNames(TenantId tenantId, PipelineVersion version, String query) {
        try {
            NodeDefinition source = version.definition().nodesOfType(NodeType.SOURCE).stream()
                    .findFirst().orElse(null);
            if (source == null || source.connectorInstanceId() == null) {
                return Set.of();
            }
            ConnectorInstance instance = connectorInstances.findById(tenantId,
                    ConnectorInstanceId.of(source.connectorInstanceId())).orElse(null);
            if (instance == null) {
                return Set.of();
            }
            return connectors.source(instance.connectorType()).listParameterNames(
                    com.dmp.connector.api.QueryVariants.apply(instance.config(), query));
        } catch (RuntimeException e) {
            log.debug("Could not determine run parameters for version {}: {}",
                    version.id(), e.getMessage());
            return Set.of();
        }
    }

    /**
     * The named queries this pipeline's source offers, in declaration order.
     *
     * <p>Empty for a source that declares none, which is every one written before they existed —
     * and the run dialog then shows no picker, exactly as it did.
     */
    public java.util.List<String> queryNames(TenantId tenantId, PipelineVersion version) {
        try {
            NodeDefinition source = version.definition().nodesOfType(NodeType.SOURCE).stream()
                    .findFirst().orElse(null);
            if (source == null || source.connectorInstanceId() == null) {
                return java.util.List.of();
            }
            return connectorInstances
                    .findById(tenantId, ConnectorInstanceId.of(source.connectorInstanceId()))
                    .map(instance -> com.dmp.connector.api.QueryVariants.names(instance.config()))
                    .orElseGet(java.util.List::of);
        } catch (RuntimeException e) {
            log.debug("Could not read the query names for version {}: {}",
                    version.id(), e.getMessage());
            return java.util.List.of();
        }
    }

    /** Loads the frozen version and its connector instances, and checks the pipeline is executable. */
    public ResolvedPipeline resolve(Run run) {
        PipelineVersion version = versions
                .findById(run.tenantId(), run.pipelineVersionId())
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "The pipeline version this run executes no longer exists",
                        Map.of("runId", run.id().toString(),
                                "versionId", run.pipelineVersionId().toString())));

        Set<ConnectorInstanceId> referenced = version.definition().nodes().stream()
                .filter(node -> node.connectorInstanceId() != null)
                .map(node -> ConnectorInstanceId.of(node.connectorInstanceId()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<String, ConnectorInstance> instances = connectorInstances
                .findAllById(run.tenantId(), referenced).stream()
                .collect(Collectors.toMap(instance -> instance.id().toString(), Function.identity()));

        return ResolvedPipeline.resolve(version, instances, run.parameters(), run.dryRun(),
                run.queryName());
    }

    /**
     * Asks the source to divide its work, and persists the resulting chunks.
     *
     * <p>Every chunk starts PENDING; no chunk is assigned here. Distribution happens when workers
     * ask for work, which is what lets a fast pod take more chunks than a slow one without anyone
     * predicting how long a chunk will take.
     *
     * @return the number of chunks planned
     */
    /**
     * Submits the source's remote work and waits for it to become readable.
     *
     * <p>Polled rather than blocked on inside the connector, so the interval belongs to whoever
     * knows what the remote system tolerates. A failed preparation stops the run with the remote
     * system's own message: a query that will not compile fails identically on every attempt, and
     * retrying it only delays telling somebody what to fix.
     */
    private Preparation awaitSourcePreparation(Run run, Source.SourceSession session) {
        Preparation prepared = session.prepare();
        if (prepared.isEmpty()) {
            return prepared;
        }

        log.info("Run {} is waiting for its source to prepare", run.id());

        while (true) {
            Preparation.Status status = session.checkPreparation(prepared);

            if (status.isReady()) {
                return prepared;
            }
            if (status.isFailed()) {
                throw new com.dmp.connector.api.ConnectorException(
                        com.dmp.connector.api.ConnectorException.Kind.CONFIGURATION,
                        "The source could not prepare this run: " + status.message());
            }

            java.time.Duration wait = status.retryAfter() == null || status.retryAfter().isZero()
                    ? java.time.Duration.ofSeconds(2) : status.retryAfter();
            try {
                Thread.sleep(wait.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new com.dmp.connector.api.ConnectorException(
                        com.dmp.connector.api.ConnectorException.Kind.UNAVAILABLE,
                        "Interrupted while waiting for the source to prepare run " + run.id(), e);
            }
        }
    }

    public int planChunks(Run run, ResolvedPipeline pipeline, Preparation preparation, String workerId) {
        ConnectorContext context = contexts.forInstance(
                pipeline.sourceInstance(), run.id().toString(), workerId,
                pipeline.runParameters(), pipeline.queryName());
        Source source = connectors.source(pipeline.sourceInstance().connectorType());

        // Chunk size, derived from the pipeline's read size unless set explicitly. Size rather
        // than count, because a count must be guessed before the data's distribution is known.
        int rowsPerChunk = pipeline.execution()
                .effectiveRowsPerChunk(pipeline.chunking().readFetchSizeOrDefault());

        try (Source.SourceSession session = source.openSource(context)) {

            // A sequential run over a cursor-pageable source needs no plan at all. Only one chunk
            // executes at a time, so the wait that makes lazy chunking impossible in parallel — pod
            // B cannot know where pod A's range ended until A has read it — is the mode already
            // chosen. What that buys is worth more than the plan: nothing is counted, chunks come
            // out exactly one budget long instead of however many rows an arithmetic slice of the
            // key range happened to contain, and rows arriving mid-run are read rather than falling
            // beyond a maximum frozen at planning time.
            if (pipeline.execution().isSequential() && session.supportsCursorPagination()) {
                splits.saveAll(List.of(Split.plan(run.id(), run.tenantId(), 0,
                        OpenEnded.spec(), clock.instant())));
                log.info("Run {} will generate chunks as it goes: sequential over a cursor-pageable "
                        + "source, so nothing is counted and no boundaries are guessed", run.id());
                return 1;
            }

            // The source's own preparation, and it must happen here rather than being assumed
            // away. A source with remote work to do — a Salesforce query job, a warehouse export —
            // submits it in prepare() and hands back the handle that plan() and read() need. This
            // used to be called with Preparation.none(), so those connectors planned a chunk with
            // no job in it and every read asked for results that did not exist.
            //
            // A source with nothing to prepare inherits a default that returns none(), so an
            // ordinary database pays one method call.
            Preparation prepared = preparation != null && !preparation.isEmpty()
                    ? preparation
                    : awaitSourcePreparation(run, session);

            List<Source.SplitSpec> specs = session.plan(prepared,
                    new Source.PlanRequest(rowsPerChunk, maxChunksPerRun));

            if (specs.isEmpty()) {
                log.info("Run {} has nothing to read", run.id());
                return 0;
            }

            List<Split> planned = new ArrayList<>(specs.size());
            for (Source.SplitSpec spec : specs) {
                // The size travels with the chunk. A connector that counted its rows at planning
                // time is the only thing that ever knows them, and by read time the manifest that
                // said so is long gone.
                planned.add(Split.plan(run.id(), run.tenantId(), spec.id(), spec.spec(),
                        spec.rows(), clock.instant()));
            }
            splits.saveAll(planned);

            log.info("Run {} planned into {} chunk(s)", run.id(), planned.size());
            return planned.size();
        }
    }

    /**
     * Re-plans a run that was previously planned.
     *
     * <p>Refuses rather than replacing. Chunks carry checkpoints, and discarding them would either
     * re-read completed ranges or lose the resume position for ranges in flight. A run needing a
     * different plan is a new run.
     */
    public void rejectRePlan(Run run, TenantId tenantId) {
        if (!splits.findByRun(tenantId, run.id()).isEmpty()) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "Run " + run.id() + " already has chunks and cannot be re-planned. "
                            + "Start a new run instead.",
                    Map.of("runId", run.id().toString(), "state", run.state().name()));
        }
    }

    /** Whether this run is in a state where planning is meaningful. */
    public boolean isPlannable(Run run) {
        return run.state() == RunState.VALIDATED || run.state() == RunState.PREPARING;
    }
}
