package com.dmp.app.web;

import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;

/**
 * What connectors this deployment has installed, and how to configure each one.
 *
 * <p>The console builds its connector picker and every configuration form from this endpoint. That
 * is what makes the plugin system real: dropping in a connector jar and restarting a worker
 * produces a complete, validated UI with no frontend change and no release.
 */
@RestController
@RequestMapping("/api/v1/connectors")
@Tag(name = "Connector catalogue", description = "Installed connectors and their configuration schemas")
public class ConnectorCatalogController {

    private final ConnectorRegistry registry;

    public ConnectorCatalogController(ConnectorRegistry registry) {
        this.registry = registry;
    }

    @GetMapping
    @Operation(summary = "List installed connectors",
            description = "Reflects what is actually loaded in this deployment, not a hardcoded "
                    + "list. A connector missing here is a connector no pipeline can use.")
    public List<Response> list() {
        return registry.specs().stream().map(Response::from).toList();
    }

    @GetMapping("/{type}")
    @Operation(summary = "Fetch one connector's specification, including its JSON Schema")
    public Response get(@PathVariable String type) {
        return registry.spec(type).map(Response::from)
                .orElseThrow(() -> new com.dmp.common.error.DmpException(
                        com.dmp.common.error.ErrorCode.NOT_FOUND,
                        "No connector of type '" + type + "' is installed",
                        java.util.Map.of("connectorType", type)));
    }

    @Schema(name = "ConnectorSpecResponse")
    public record Response(
            @Schema(example = "jdbc") String type,
            String displayName,
            String description,
            @Schema(description = "SOURCE, SINK or BOTH") String direction,
            @Schema(description = "JSON Schema the console renders the configuration form from")
            JsonNode configSchema,
            @Schema(description = "Config fields holding secret references rather than values")
            Set<String> secretFields,
            String version,

            @Schema(description = "How one chunk is counted against a rate limit. PER_REQUEST means "
                    + "one call per delivery group; PER_CHUNK means the whole chunk is one unit of "
                    + "work however many requests it takes underneath, as for a bulk job that is "
                    + "created, uploaded and polled to completion.",
                    example = "PER_REQUEST")
            String callCost) {

        static Response from(ConnectorSpec spec) {
            return new Response(
                    spec.type(),
                    spec.displayName(),
                    spec.description(),
                    spec.direction().name(),
                    spec.configSchema(),
                    spec.secretFields(),
                    spec.version(),
                    spec.callCost().name());
        }
    }
}
