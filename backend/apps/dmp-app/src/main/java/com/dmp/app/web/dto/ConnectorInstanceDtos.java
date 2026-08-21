package com.dmp.app.web.dto;

import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstance;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/** Web contract for connector instances. */
public final class ConnectorInstanceDtos {

    private ConnectorInstanceDtos() {
    }

    @Schema(name = "ConnectorInstanceResponse")
    public record Response(
            String id,
            String name,
            @Schema(description = "Plugin identifier", example = "jdbc-postgres")
            String connectorType,
            String direction,
            @Schema(description = "Connector-specific configuration, validated against the plugin's JSON Schema")
            JsonNode config,
            @Schema(description = "References to secrets. Never values — safe to display and log.")
            JsonNode secretRefs,
            String status,
            String description,
            Instant lastTestedAt,
            String lastTestError,
            Instant createdAt,
            Instant updatedAt) {

        public static Response from(ConnectorInstance instance) {
            return new Response(
                    instance.id().toString(),
                    instance.name(),
                    instance.connectorType(),
                    instance.direction().name(),
                    instance.config(),
                    instance.secretRefs(),
                    instance.status().name(),
                    instance.description(),
                    instance.lastTestedAt(),
                    instance.lastTestError(),
                    instance.createdAt(),
                    instance.updatedAt());
        }
    }

    @Schema(name = "CreateConnectorInstanceRequest")
    public record CreateRequest(
            @NotBlank(message = "Name is required")
            @Size(max = 255)
            String name,

            @NotBlank(message = "Connector type is required")
            @Pattern(regexp = "^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$",
                    message = "Connector type must be a lowercase plugin identifier such as 'jdbc-postgres'")
            String connectorType,

            @NotNull(message = "Direction is required")
            ConnectorDirection direction,

            @Schema(description = "Free-form until the plugin runtime can validate it against the connector's schema")
            JsonNode config,

            @Schema(description = "Secret references only. Sending a secret value here would store it in plain text.")
            JsonNode secretRefs,

            String description) {
    }

    @Schema(name = "UpdateConnectorInstanceRequest",
            description = "Any change resets the instance to UNTESTED — a previous successful test "
                    + "says nothing about a configuration that has since been edited")
    public record UpdateRequest(
            @NotBlank(message = "Name is required")
            @Size(max = 255)
            String name,

            JsonNode config,

            JsonNode secretRefs,

            String description,

            @Schema(description = "Which pipeline roles this instance may fill. Omit to leave it "
                    + "unchanged. The connector type itself is not editable — a different type has "
                    + "a different configuration shape, so that is a new connection rather than an "
                    + "edit to this one.")
            ConnectorDirection direction) {
    }
}
