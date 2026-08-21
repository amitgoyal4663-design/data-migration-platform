package com.dmp.transform.api;

import java.time.Duration;
import java.util.Objects;

/**
 * One transformation node, resolved from the pipeline definition.
 *
 * <p>Carries the node's identity as well as its script so that a failure names the node the user
 * placed on the canvas. "Transform failed" is unactionable when a pipeline has four of them.
 *
 * @param nodeId  the canvas node id, used in errors and record-error entries
 * @param name    the user's label for the node
 * @param stage   per-record or per-batch
 * @param script  the user's JavaScript, expected to define the function for its stage
 * @param timeout wall-clock ceiling for one invocation
 */
public record TransformSpec(
        String nodeId,
        String name,
        TransformStage stage,
        String script,
        Duration timeout) {

    /**
     * A script that does nothing measurable still costs a sandbox crossing per record, so an empty
     * one is rejected at construction rather than silently costing throughput for no effect.
     */
    public TransformSpec {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(stage, "stage");
        Objects.requireNonNull(timeout, "timeout");

        if (script == null || script.isBlank()) {
            throw new IllegalArgumentException(
                    "Transform node '" + nodeId + "' has no script. Remove the node or write one.");
        }
        name = (name == null || name.isBlank()) ? nodeId : name;
    }
}
