package com.dmp.app.web;

import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.ConnectorInstanceRepository;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.Sink;
import com.dmp.connector.runtime.ConnectorContexts;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.application.port.out.PipelineVersionRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;

/**
 * The destination's own record of what it did with a chunk.
 *
 * <p>For a destination that decides asynchronously and keeps its own results file — Salesforce Bulk
 * being the case this exists for. The platform stores the <em>counts</em>, which are permanent, and
 * fetches the <em>file</em> from the org on demand, which is not.
 *
 * <p>That split is deliberate. Copying every rejected row into the platform would mean a second
 * store of customer data with its own redaction, retention and erasure story, paid for on every run
 * by every pipeline, to answer a question asked about a handful of them. Fetching on request costs
 * nothing until somebody asks, and what comes back is the org's own file rather than the platform's
 * reading of it.
 *
 * <p><b>An absent file is a normal answer.</b> Salesforce keeps job results for about a week. After
 * that this returns 404 and the console says the org no longer holds it — which is true, and is not
 * the same sentence as "this chunk had no failures".
 */
@RestController
@RequestMapping("/api/v1/runs/{runId}/chunks/{chunkId}")
@Tag(name = "Chunk results", description = "Result files the destination is still holding")
public class ChunkResultsController {

    private final RunRepository runs;
    private final SplitRepository splits;
    private final PipelineVersionRepository versions;
    private final ConnectorInstanceRepository connectorInstances;
    private final ConnectorRegistry connectors;
    private final ConnectorContexts contexts;
    private final TenantContext tenantContext;

    public ChunkResultsController(RunRepository runs, SplitRepository splits,
                                  PipelineVersionRepository versions,
                                  ConnectorInstanceRepository connectorInstances,
                                  ConnectorRegistry connectors, ConnectorContexts contexts,
                                  TenantContext tenantContext) {
        this.runs = runs;
        this.splits = splits;
        this.versions = versions;
        this.connectorInstances = connectorInstances;
        this.connectors = connectors;
        this.contexts = contexts;
        this.tenantContext = tenantContext;
    }

    @GetMapping("/results")
    @Operation(summary = "Download the destination's result file for this chunk",
            description = """
                    Streams the file the destination is holding for this chunk's job — for
                    Salesforce Bulk, the failedResults or successfulResults CSV, exactly as the org
                    serves it.

                    The platform stores none of it. What it stores is the count, which is on the
                    chunk and is permanent; this is the detail behind that count, and it lives for
                    as long as the destination keeps it — about a week for Salesforce.

                    404 means the destination no longer has the file, or never had one. That is a
                    different statement from "this chunk had no failures", and the console shows it
                    as one.

                    kind=failed (default) or kind=successful.
                    """)
    public ResponseEntity<Resource> results(@PathVariable String runId,
                                            @PathVariable String chunkId,
                                            @RequestParam(defaultValue = "failed") String kind) {

        TenantId tenantId = tenantContext.currentTenant();
        Split chunk = splits.findById(tenantId, SplitId.parse(chunkId))
                .orElseThrow(() -> notFound("Chunk not found", chunkId));

        // The handle the engine kept when it parked the chunk. Without it there is nothing to ask
        // the destination about — this chunk was written synchronously and has no remote job.
        if (!chunk.hasExternalJob()) {
            throw new DmpException(ErrorCode.NOT_FOUND,
                    "This chunk has no destination job. Result files exist only for destinations "
                            + "that accept work and decide on it later, such as Salesforce Bulk.",
                    Map.of("chunkId", chunkId));
        }

        Run run = runs.findById(tenantId, chunk.runId())
                .orElseThrow(() -> notFound("Run not found", runId));
        PipelineVersion version = versions.findById(tenantId, run.pipelineVersionId())
                .orElseThrow(() -> notFound("Pipeline version not found", runId));

        ConnectorInstance sinkInstance = sinkInstanceOf(tenantId, version);
        Sink sink = connectors.sink(sinkInstance.connectorType());

        Preparation job = com.dmp.engine.ChunkParkedException.sinkJobOf(chunk.externalJob());
        Optional<Sink.ResultFile> file = sink.fetchResults(
                contexts.forChunk(sinkInstance, run.id().toString(), "api", chunk.index(), true),
                job, kind);

        return file.map(found -> ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_DISPOSITION,
                                "attachment; filename=\"" + found.filename() + "\"")
                        .contentType(MediaType.parseMediaType(found.mediaType()))
                        .body((Resource) new ByteArrayResource(found.content())))
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "The destination no longer holds a '" + kind + "' file for this chunk. "
                                + "Salesforce keeps job results for about a week; the counts on "
                                + "the chunk are the permanent record.",
                        Map.of("chunkId", chunkId, "kind", kind)));
    }

    private ConnectorInstance sinkInstanceOf(TenantId tenantId, PipelineVersion version) {
        return version.definition().nodes().stream()
                .filter(node -> node.type() == com.dmp.domain.pipeline.NodeType.SINK)
                .findFirst()
                .flatMap(node -> connectorInstances.findById(tenantId,
                        com.dmp.domain.connector.ConnectorInstanceId.of(node.connectorInstanceId())))
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "This pipeline's destination connector no longer exists",
                        Map.of("versionId", version.id().toString())));
    }

    private static DmpException notFound(String message, String id) {
        return new DmpException(ErrorCode.NOT_FOUND, message, Map.of("id", id));
    }
}
