package com.dmp.connector.api;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A connector's self-description.
 *
 * <p>{@code configSchema} is a JSON Schema, and it is the mechanism that makes the plugin system
 * usable rather than merely present. The console renders a connector's configuration form from
 * this schema at runtime, so dropping in a new connector jar produces a complete, validated UI with
 * no frontend change. A plugin system where adding a connector still requires editing the frontend
 * is not extensible in any way that matters.
 *
 * <p>{@code secretFields} names the configuration paths holding credentials. The platform stores
 * <em>references</em> at those paths and resolves them at execution time, so nothing secret is ever
 * written to the definition store, the audit log, or a log line.
 *
 * @param type          stable plugin identifier, for example {@code jdbc-postgres}
 * @param displayName   shown in the console
 * @param description   one or two sentences, shown when choosing a connector
 * @param direction     which pipeline roles this connector can fill
 * @param configSchema  JSON Schema for the configuration object
 * @param secretFields  JSON pointer paths whose values are secret references
 * @param version       connector version, independent of the platform's
 */
public record ConnectorSpec(
        String type,
        String displayName,
        String description,
        Direction direction,
        JsonNode configSchema,
        Set<String> secretFields,
        String version) {

    private static final Pattern TYPE = Pattern.compile("^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$");

    public ConnectorSpec {
        Objects.requireNonNull(direction, "direction");

        if (type == null || !TYPE.matcher(type).matches()) {
            throw new IllegalArgumentException(
                    "Connector type must be a lowercase identifier such as 'jdbc-postgres', but was: " + type);
        }
        displayName = displayName == null || displayName.isBlank() ? type : displayName;
        configSchema = Json.orEmpty(configSchema);
        secretFields = Set.copyOf(secretFields == null ? Set.of() : secretFields);
        version = version == null || version.isBlank() ? "0.0.0" : version;
    }

    /** Which pipeline roles a connector can fill. Most are symmetric; some genuinely are not. */
    public enum Direction {
        SOURCE,
        SINK,
        BOTH;

        public boolean canRead() {
            return this == SOURCE || this == BOTH;
        }

        public boolean canWrite() {
            return this == SINK || this == BOTH;
        }
    }
}
