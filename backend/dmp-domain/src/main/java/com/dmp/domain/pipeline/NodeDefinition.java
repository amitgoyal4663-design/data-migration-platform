package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * A single node in a pipeline DAG.
 *
 * <p>{@code config} is deliberately an opaque {@link JsonNode}. The platform cannot know the
 * shape of a third-party connector's configuration at compile time; it is validated at runtime
 * against the JSON Schema the connector declares in its {@code ConnectorSpec} (ADR-0006). That
 * indirection is what allows a connector jar to arrive with a working configuration UI and no
 * platform change.
 *
 * @param id                  designer-assigned, unique within the pipeline and stable across edits
 * @param type                determines the structural rules applied by the validator
 * @param name                human-readable label shown on the canvas
 * @param connectorInstanceId required for SOURCE and SINK, must be null otherwise
 * @param config              node configuration, validated against the connector or node schema
 */
public record NodeDefinition(
        String id,
        NodeType type,
        String name,
        UUID connectorInstanceId,
        JsonNode config) {

    private static final Pattern NODE_ID = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$");

    public NodeDefinition {
        Objects.requireNonNull(type, "node type");

        if (id == null || !NODE_ID.matcher(id).matches()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Node id must be 1-64 alphanumeric characters, hyphens or underscores, "
                            + "starting with an alphanumeric",
                    Map.of("nodeId", String.valueOf(id)));
        }
        config = Json.orEmpty(config);
        name = (name == null || name.isBlank()) ? type.name() : name;
    }

    public boolean hasConnector() {
        return connectorInstanceId != null;
    }
}
