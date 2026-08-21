package com.dmp.engine;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.audit.RecordAuditLevel;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.pipeline.PipelineVersion;
import com.fasterxml.jackson.databind.JsonNode;
import com.dmp.transform.api.TransformSpec;
import com.dmp.transform.api.TransformStage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A published pipeline version with its connector instances resolved and its shape checked.
 *
 * <p>Everything the engine needs to run a chunk, gathered once when the run starts rather than
 * re-read per chunk. A run executes against a frozen version, so nothing here can change underneath
 * it — which is what lets the resolution be done once and trusted.
 *
 * <p>Phase 3 executes single-source, single-sink pipelines. Transformation nodes are accepted in
 * the definition and pass records through untouched; the sandbox that gives them behaviour arrives
 * in Phase 5. Multi-source and multi-sink graphs are rejected here with a clear message rather than
 * silently executing one branch, which would be far worse than refusing.
 */
public record ResolvedPipeline(
        PipelineVersion version,
        NodeDefinition sourceNode,
        ConnectorInstance sourceInstance,
        NodeDefinition sinkNode,
        ConnectorInstance sinkInstance,
        List<TransformSpec> transforms,
        ChunkingPolicy chunking,
        ExecutionPolicy execution,
        AuditPolicy audit,
        /**
         * The parameters the run was started with, on their way to a parameterised source query.
         *
         * <p>Carried here rather than threaded through every method that needs them, because this
         * record already exists to be "everything resolved once when the run starts" and a run's
         * parameters are exactly that: fixed for its whole life, identical for every chunk.
         */
        JsonNode runParameters) {

    /**
     * Ceiling on how long one invocation of a user's script may run.
     *
     * <p>Generous, because it bounds a runaway loop rather than paces normal work: a transform
     * doing real work on one record finishes in microseconds, so anything near this is a defect.
     */
    private static final Duration SCRIPT_TIMEOUT = Duration.ofSeconds(5);

    public ResolvedPipeline {
        transforms = List.copyOf(transforms == null ? List.of() : transforms);
    }

    public static ResolvedPipeline resolve(PipelineVersion version,
                                           Map<String, ConnectorInstance> instancesById) {
        return resolve(version, instancesById, com.dmp.common.json.Json.emptyObject());
    }

    public static ResolvedPipeline resolve(PipelineVersion version,
                                           Map<String, ConnectorInstance> instancesById,
                                           JsonNode runParameters) {

        List<NodeDefinition> sources = version.definition().nodesOfType(NodeType.SOURCE);
        List<NodeDefinition> sinks = version.definition().nodesOfType(NodeType.SINK);

        if (sources.size() != 1 || sinks.size() != 1) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "This engine version executes pipelines with exactly one source and one sink. "
                            + "This pipeline has " + sources.size() + " source(s) and "
                            + sinks.size() + " sink(s).",
                    Map.of("sources", sources.size(), "sinks", sinks.size()));
        }

        NodeDefinition sourceNode = sources.get(0);
        NodeDefinition sinkNode = sinks.get(0);
        ConnectorInstance sinkInstance = requireInstance(instancesById, sinkNode);

        return new ResolvedPipeline(
                version,
                sourceNode, requireInstance(instancesById, sourceNode),
                sinkNode, sinkInstance,
                transformChain(version, sourceNode, sinkNode),
                version.chunkingPolicy(),
                version.executionPolicy(),
                auditPolicyFor(version.auditPolicy(), sinkInstance),
                com.dmp.common.json.Json.orEmpty(runParameters));
    }

    /**
     * Connector types whose sinks cannot name the records they refuse.
     *
     * <p>Salesforce reports how many records a bulk job rejected in the status the engine already
     * polls, and names <em>which</em> ones only in a separate results file holding every rejected
     * row — hundreds of kilobytes for a chunk that fails wholesale, to produce a number we already
     * had. The connector therefore reports the count and not the list, which means a dead-letter
     * queue for its rejections would always be empty.
     */
    private static final Set<String> SINKS_WITHOUT_RECORD_LEVEL_REJECTIONS = Set.of("salesforce");

    /**
     * Applies the audit policy the engine will actually obey.
     *
     * <p>Enforced here, at resolution, rather than only at save time. A policy stored before this
     * rule existed, or written straight to the API, still cannot make the engine promise a
     * dead-letter queue its destination cannot fill — there is no field to set wrongly, because the
     * value is recomputed every time a run resolves.
     *
     * <p><b>This switches the queue off entirely, not only for the sink's rejections.</b> Records a
     * transform threw on never reached the destination and their payloads are ours to keep, so
     * there is a narrower rule available that would preserve those. The blunt one is chosen
     * deliberately: a queue holding twelve transform failures beside five thousand uncounted sink
     * rejections invites exactly the wrong reading of a run, and "this pipeline keeps no rejected
     * records" is a sentence an operator can act on without first learning which stage refused
     * what. If replaying transform failures against a Salesforce sink is wanted, this is the rule
     * to revisit.
     */
    private static AuditPolicy auditPolicyFor(AuditPolicy declared, ConnectorInstance sink) {
        if (!SINKS_WITHOUT_RECORD_LEVEL_REJECTIONS.contains(sink.connectorType())) {
            return declared;
        }
        // The same rule twice, because the two settings make the same promise. A destination that
        // cannot name what it did to each record cannot fill a dead-letter queue, and cannot
        // complete a per-record index either: entries are written as SENT when the batch is handed
        // over, and the only thing that could ever resolve them — a per-record verdict — is the
        // thing this destination does not report. They would sit at SENT for the life of the
        // index, which reads as an answer and is not one.
        //
        // What replaces it is chunk-level truth: the org's own processed and failed counts on the
        // chunk, and its results file on demand for as long as the org keeps it.
        return declared.withoutRejectedPayloads().indexing(RecordAuditLevel.ERRORS, false);
    }

    /**
     * Walks source to sink, collecting transform nodes in the order the user wired them.
     *
     * <p>Order is the whole point: a script that computes a total and a script that filters on
     * that total produce different results depending on which runs first, and the user expressed
     * their intent by drawing the arrows. Reading nodes in declaration order instead would make
     * the result depend on the order the canvas happened to serialise them.
     *
     * <p>Branches are refused rather than guessed at. This engine version executes one path, and
     * silently running one of two branches is a worse outcome than declining to start.
     */
    /**
     * How the sink is called with each batch.
     *
     * <p>Derived rather than a component: this record is already constructed in several tests and
     * two production paths, and every added component is another arity to keep in step. The
     * version is the authority anyway — nothing here would ever hold a different answer.
     */
    public com.dmp.domain.pipeline.DeliveryPolicy delivery() {
        return version == null
                ? com.dmp.domain.pipeline.DeliveryPolicy.DEFAULT
                : version.deliveryPolicy();
    }

    private static List<TransformSpec> transformChain(PipelineVersion version,
                                                      NodeDefinition sourceNode,
                                                      NodeDefinition sinkNode) {
        Map<String, Set<String>> outbound = version.definition().outbound();
        Map<String, NodeDefinition> byId = version.definition().nodesById();

        List<TransformSpec> chain = new ArrayList<>();
        Set<String> visited = new LinkedHashSet<>();
        String currentId = sourceNode.id();

        while (!currentId.equals(sinkNode.id())) {
            if (!visited.add(currentId)) {
                // The validator rejects cycles at publish time; reaching one here means a version
                // was stored by some other path. Failing beats looping forever.
                throw new DmpException(ErrorCode.VALIDATION_FAILED,
                        "The pipeline graph loops back to node '" + currentId + "'",
                        Map.of("nodeId", currentId));
            }

            Set<String> next = outbound.getOrDefault(currentId, Set.of());
            if (next.size() != 1) {
                throw new DmpException(ErrorCode.VALIDATION_FAILED,
                        "This engine version executes a single path from source to sink. Node '"
                                + currentId + "' has " + next.size() + " outgoing connection(s).",
                        Map.of("nodeId", currentId, "outgoing", next.size()));
            }

            currentId = next.iterator().next();
            NodeDefinition node = byId.get(currentId);
            if (node == null) {
                throw new DmpException(ErrorCode.INVALID_REFERENCE,
                        "The pipeline has a connection to node '" + currentId + "', which does not exist",
                        Map.of("nodeId", currentId));
            }

            transformSpec(node).ifPresent(chain::add);
        }

        // The split script rides with the chain rather than the graph, because it is not a step
        // records pass through — it decides how the batch about to be written is divided into calls
        // on the sink, which is a property of delivery rather than of the DAG. Appended last so it
        // sees records exactly as the sink will.
        String splitScript = version.deliveryPolicy().splitScript();
        if (splitScript != null) {
            chain.add(new TransformSpec("delivery-split", "Split", TransformStage.SPLIT,
                    splitScript, SCRIPT_TIMEOUT));
        }
        return chain;
    }

    /**
     * Turns a node into a script spec, if it is one that carries a script.
     *
     * <p>Node types that pass records through untouched in this engine version return empty, so
     * placing one on the canvas is harmless rather than an error — the designer offers a palette
     * wider than the executor currently implements.
     */
    private static Optional<TransformSpec> transformSpec(NodeDefinition node) {
        TransformStage stage = switch (node.type()) {
            case TRANSFORM -> TransformStage.RECORD;
            case BATCH_TRANSFORM -> TransformStage.BATCH;
            default -> null;
        };
        if (stage == null) {
            return Optional.empty();
        }

        String script = node.config().path("script").asText(null);
        if (script == null || script.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Transform '" + node.name() + "' has no script. Write one, or remove the node.",
                    Map.of("nodeId", node.id()));
        }
        return Optional.of(new TransformSpec(node.id(), node.name(), stage, script, SCRIPT_TIMEOUT));
    }

    private static ConnectorInstance requireInstance(Map<String, ConnectorInstance> instances,
                                                     NodeDefinition node) {
        ConnectorInstance instance = instances.get(String.valueOf(node.connectorInstanceId()));
        if (instance == null) {
            throw new DmpException(ErrorCode.INVALID_REFERENCE,
                    "Node '" + node.id() + "' references connector instance "
                            + node.connectorInstanceId() + ", which no longer exists",
                    Map.of("nodeId", node.id(),
                            "connectorInstanceId", String.valueOf(node.connectorInstanceId())));
        }
        instance.requireUsableAs(node.type());
        return instance;
    }
}
