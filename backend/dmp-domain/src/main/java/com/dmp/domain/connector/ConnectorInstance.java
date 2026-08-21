package com.dmp.domain.connector;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A configured, named instance of a connector type — "the finance replica" rather than "PostgreSQL".
 *
 * <p>{@code config} is opaque here on purpose. The domain cannot know the shape of a third-party
 * connector's configuration, and pretending otherwise would mean every new connector required a
 * domain change, which is exactly what ADR-0006 rules out. Configuration is validated at runtime
 * against the JSON Schema the connector declares.
 *
 * <p>{@code secretRefs} holds <em>references</em>, never values. Nothing in this aggregate is
 * secret, so it is safe to log, serialise and return over the API in full. Resolution happens in
 * the worker at execution time through the {@code SecretsProvider} SPI. Keeping that separation
 * absolute is what will make the eventual SSO and vault integration a substitution rather than an
 * audit.
 */
public record ConnectorInstance(
        ConnectorInstanceId id,
        TenantId tenantId,
        String name,
        String connectorType,
        ConnectorDirection direction,
        JsonNode config,
        JsonNode secretRefs,
        ConnectorInstanceStatus status,
        String description,
        Instant lastTestedAt,
        String lastTestError,
        Instant createdAt,
        Instant updatedAt,
        long rowVersion) {

    private static final int MAX_NAME_LENGTH = 255;
    private static final Pattern CONNECTOR_TYPE = Pattern.compile("^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$");

    public ConnectorInstance {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(direction, "direction");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");

        if (name == null || name.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "Connector instance name must not be blank");
        }
        name = name.strip();
        if (name.length() > MAX_NAME_LENGTH) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Connector instance name exceeds " + MAX_NAME_LENGTH + " characters",
                    Map.of("length", name.length()));
        }
        if (connectorType == null || !CONNECTOR_TYPE.matcher(connectorType).matches()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Connector type must be a lowercase plugin identifier such as 'jdbc-postgres'",
                    Map.of("connectorType", String.valueOf(connectorType)));
        }
        config = Json.orEmpty(config);
        secretRefs = Json.orEmpty(secretRefs);
    }

    /**
     * Reference schemes a credential may be stored as.
     *
     * <p>An allow-list rather than "anything with a colon", because a great many credentials
     * <em>do</em> contain a colon — {@code mongodb://user:password@host} being the one that matters
     * most. Accepting an unknown scheme would let exactly the value this rule exists to keep out
     * pass as a reference to a store that does not exist.
     *
     * <p>Add to this when a provider is added. A scheme listed here with no provider behind it
     * still fails at run time, and says so.
     */
    private static final Set<String> SECRET_REFERENCE_SCHEMES = Set.of("env", "vault");

    /**
     * The shape of a reference: a known scheme, a colon, then a name that is not a URL path.
     *
     * <p>Rejecting a leading slash after the colon is what stops {@code mongodb://…} from being read
     * as scheme {@code mongodb}, and whitespace is rejected because no store's key contains any.
     */
    private static final Pattern SECRET_REFERENCE = Pattern.compile("^([a-z][a-z0-9-]*):([^\\s/][^\\s]*)$");

    public static ConnectorInstance create(TenantId tenantId, String name, String connectorType,
                                           ConnectorDirection direction, JsonNode config,
                                           JsonNode secretRefs, String description, Instant now) {
        return new ConnectorInstance(ConnectorInstanceId.newId(), tenantId, name, connectorType, direction,
                config, requireReferencesNotValues(secretRefs), ConnectorInstanceStatus.UNTESTED,
                description, null, null, now, now, 0L);
    }

    /**
     * Refuses a credential stored as its own value rather than as a reference to one.
     *
     * <p>The whole design rests on this aggregate holding nothing secret: it is returned by the API
     * in full, written to the audit log, and included in every backup of the definition store. One
     * pasted password in this field quietly makes all three of those a place credentials live, and
     * nothing downstream would ever notice — the value would simply fail to resolve, producing an
     * error about a missing configuration rather than about a leak.
     *
     * <p>Applied on the write paths rather than in the canonical constructor, so a row stored before
     * this rule existed still loads and can be corrected. It cannot be re-saved uncorrected, which
     * is what closes the hole without making old data unreadable.
     *
     * <p><b>The offending value is never repeated in the error.</b> It may well be a live credential,
     * and an exception message travels to the API response and the log — the two places this method
     * exists to keep it out of.
     */
    private static JsonNode requireReferencesNotValues(JsonNode secretRefs) {
        if (secretRefs == null || secretRefs.isEmpty()) {
            return secretRefs;
        }
        for (Iterator<Map.Entry<String, JsonNode>> fields = secretRefs.fields(); fields.hasNext(); ) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode value = field.getValue();

            if (value == null || value.isNull() || value.asText().isBlank()) {
                continue;
            }
            if (!value.isTextual()) {
                throw new DmpException(ErrorCode.VALIDATION_FAILED,
                        "Credential '" + field.getKey() + "' must be a reference written as text, "
                                + "such as env:PG_PASSWORD.",
                        Map.of("field", field.getKey()));
            }

            Matcher reference = SECRET_REFERENCE.matcher(value.asText().strip());
            if (!reference.matches() || !SECRET_REFERENCE_SCHEMES.contains(reference.group(1))) {
                throw new DmpException(ErrorCode.VALIDATION_FAILED,
                        "Credential '" + field.getKey() + "' looks like a value rather than a "
                                + "reference to one. Store the name of something this deployment "
                                + "provides — for example env:" + envNameFor(field.getKey())
                                + " — and set that in the environment instead. Credentials are "
                                + "never kept here: this record is returned by the API in full, "
                                + "written to the audit log and included in every backup, so a "
                                + "value stored in it would be a credential in all three. The value "
                                + "sent has not been stored and is deliberately not repeated in "
                                + "this message.",
                        Map.of("field", field.getKey(),
                                "expectedSchemes", String.join(", ", SECRET_REFERENCE_SCHEMES)));
            }
        }
        return secretRefs;
    }

    /** {@code clientSecret} to {@code CLIENT_SECRET}, so the suggested name is a usable one. */
    private static String envNameFor(String field) {
        return field.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toUpperCase(java.util.Locale.ROOT);
    }

    /**
     * Applies a configuration change.
     *
     * <p>Any change resets the instance to UNTESTED. A prior successful connectivity check says
     * nothing about a configuration that has since been edited, and carrying the old ACTIVE status
     * forward would present a stale assurance as a current one.
     */
    public ConnectorInstance updateConfiguration(String newName, JsonNode newConfig, JsonNode newSecretRefs,
                                                 String newDescription, Instant now) {
        return updateConfiguration(newName, newConfig, newSecretRefs, newDescription, direction, now);
    }

    /**
     * Replaces the configuration, including which pipeline roles this instance may fill.
     *
     * <p>The role is editable because it decides which configuration fields even apply — a sink has
     * nothing to say about where a read should start. An instance created as BOTH and only ever
     * used to write should be able to become a sink, rather than being deleted and recreated with
     * every field retyped.
     *
     * <p>The connector type is not editable and never will be: its configuration schema is a
     * different shape, so changing it would leave every stored value meaningless. That is a new
     * connection, not an edit to this one.
     */
    public ConnectorInstance updateConfiguration(String newName, JsonNode newConfig, JsonNode newSecretRefs,
                                                 String newDescription, ConnectorDirection newDirection,
                                                 Instant now) {
        return new ConnectorInstance(id, tenantId, newName, connectorType,
                newDirection == null ? direction : newDirection,
                newConfig, requireReferencesNotValues(newSecretRefs),
                ConnectorInstanceStatus.UNTESTED, newDescription,
                lastTestedAt, lastTestError, createdAt, now, rowVersion);
    }

    public ConnectorInstance recordTestSuccess(Instant now) {
        return new ConnectorInstance(id, tenantId, name, connectorType, direction, config, secretRefs,
                ConnectorInstanceStatus.ACTIVE, description, now, null, createdAt, now, rowVersion);
    }

    public ConnectorInstance recordTestFailure(String error, Instant now) {
        return new ConnectorInstance(id, tenantId, name, connectorType, direction, config, secretRefs,
                ConnectorInstanceStatus.FAILED, description, now, error, createdAt, now, rowVersion);
    }

    public ConnectorInstance disable(Instant now) {
        return new ConnectorInstance(id, tenantId, name, connectorType, direction, config, secretRefs,
                ConnectorInstanceStatus.DISABLED, description, lastTestedAt, lastTestError,
                createdAt, now, rowVersion);
    }

    public ConnectorInstance enable(Instant now) {
        return new ConnectorInstance(id, tenantId, name, connectorType, direction, config, secretRefs,
                ConnectorInstanceStatus.UNTESTED, description, lastTestedAt, lastTestError,
                createdAt, now, rowVersion);
    }

    /**
     * Whether this instance may fill the given pipeline role.
     *
     * <p>Checked when a pipeline version is published, so that a sink-only connector wired into a
     * source node is caught at design time rather than at run start.
     */
    public void requireUsableAs(NodeType nodeType) {
        if (!status.isUsable()) {
            throw new DmpException(ErrorCode.INVALID_REFERENCE,
                    "Connector instance '" + name + "' is " + status + " and cannot be used",
                    Map.of("connectorInstanceId", id.toString(), "status", status.name()));
        }
        if (!direction.supports(nodeType)) {
            throw new DmpException(ErrorCode.INVALID_REFERENCE,
                    "Connector instance '" + name + "' is configured for " + direction
                            + " and cannot be used as a " + nodeType,
                    Map.of("connectorInstanceId", id.toString(),
                            "direction", direction.name(), "nodeType", nodeType.name()));
        }
    }

    public Optional<String> lastTestErrorMessage() {
        return Optional.ofNullable(lastTestError);
    }
}
