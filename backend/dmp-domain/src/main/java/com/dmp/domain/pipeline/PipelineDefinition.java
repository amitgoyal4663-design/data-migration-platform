package com.dmp.domain.pipeline;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The pipeline DAG: what the designer canvas serialises to and what the engine executes.
 *
 * <p>Insertion order is preserved throughout so that a round-trip through the API returns the
 * canvas in the order the user built it. Adjacency maps are computed once in the constructor
 * rather than on each access, because the validator and the engine planner both traverse this
 * structure repeatedly.
 */
public record PipelineDefinition(List<NodeDefinition> nodes, List<EdgeDefinition> edges) {

    public PipelineDefinition {
        nodes = List.copyOf(nodes == null ? List.of() : nodes);
        edges = List.copyOf(edges == null ? List.of() : edges);
    }

    public static PipelineDefinition empty() {
        return new PipelineDefinition(List.of(), List.of());
    }

    /** Nodes indexed by id. Duplicate ids collapse here and are reported by the validator. */
    public Map<String, NodeDefinition> nodesById() {
        Map<String, NodeDefinition> index = new LinkedHashMap<>();
        for (NodeDefinition node : nodes) {
            index.putIfAbsent(node.id(), node);
        }
        return index;
    }

    /** Outbound adjacency. Every declared node appears, including those with no successors. */
    public Map<String, Set<String>> outbound() {
        return adjacency(true);
    }

    /** Inbound adjacency. Every declared node appears, including those with no predecessors. */
    public Map<String, Set<String>> inbound() {
        return adjacency(false);
    }

    private Map<String, Set<String>> adjacency(boolean forward) {
        Map<String, Set<String>> adjacency = new LinkedHashMap<>();
        for (NodeDefinition node : nodes) {
            adjacency.putIfAbsent(node.id(), new LinkedHashSet<>());
        }
        for (EdgeDefinition edge : edges) {
            String key = forward ? edge.from() : edge.to();
            String value = forward ? edge.to() : edge.from();
            adjacency.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(value);
        }
        return adjacency;
    }

    public List<NodeDefinition> nodesOfType(NodeType type) {
        return nodes.stream().filter(n -> n.type() == type).toList();
    }

    public Optional<NodeDefinition> node(String nodeId) {
        return nodes.stream().filter(n -> n.id().equals(nodeId)).findFirst();
    }

    public boolean isEmpty() {
        return nodes.isEmpty();
    }
}
