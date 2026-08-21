package com.dmp.connector.runtime;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Resolving a connector instance's configuration from the environment.
 *
 * <p>The point of these is that a connector instance should describe <em>which</em> database, not
 * <em>where</em> it lives. Where it lives belongs to whoever runs the cluster and differs in every
 * environment, so a stored literal produces an instance that works in one place and must be rebuilt
 * by hand everywhere else.
 *
 * <p>The negative cases matter more than the positive ones. A connection string that is left alone
 * when it should be, and a missing variable that fails loudly instead of being handed to a driver
 * as its own literal text, are what make this safe to apply to every field of every connector.
 */
class ConfigReferenceResolutionTest {

    private static final TenantId TENANT = TenantId.newId();

    private final ConnectorContexts contexts = new ConnectorContexts(List.of(
            new FakeSecrets(Map.of(
                    "MONGO_URI", "mongodb://user:pw@mongo.prod:27017/orders",
                    "MONGO_HOST", "mongo.prod",
                    "DATABRICKS_HOST", "https://adb-999.5.azuredatabricks.net",
                    "SF_CLIENT_SECRET", "the-secret"))));

    @Test
    void anyConfigurationFieldMayBeAReferenceNotOnlyCredentials() {
        ConnectorContext context = contexts.forInstance(instance(config -> {
            config.put("connectionString", "env:MONGO_URI");
            config.put("database", "orders");
        }), "run", "worker");

        assertThat(context.config().get("connectionString").asText())
                .isEqualTo("mongodb://user:pw@mongo.prod:27017/orders");
        assertThat(context.config().get("database").asText())
                .as("a literal stays a literal; only references are touched")
                .isEqualTo("orders");
    }

    @Test
    void aReferenceMayBeEmbeddedInALargerString() {
        // The common shape: a template with one moving part. Requiring a whole variable for a
        // string that is mostly constant is how configuration ends up hard-coded instead.
        ConnectorContext context = contexts.forInstance(instance(config ->
                config.put("connectionString", "mongodb://${MONGO_HOST}:27017/orders")), "run", "worker");

        assertThat(context.config().get("connectionString").asText())
                .isEqualTo("mongodb://mongo.prod:27017/orders");
    }

    @Test
    void aUrlIsNotMistakenForAReference() {
        // The check that makes this safe to apply everywhere. A value is a reference only when its
        // scheme names a registered provider, so anything with a scheme we cannot answer to —
        // mongodb://, https://, jdbc: — is a literal and is left exactly alone.
        ConnectorContext context = contexts.forInstance(instance(config -> {
            config.put("connectionString", "mongodb://localhost:27017/orders");
            config.put("url", "https://example.com/api");
            config.put("jdbcUrl", "jdbc:postgresql://localhost:5432/finance");
        }), "run", "worker");

        assertThat(context.config().get("connectionString").asText())
                .isEqualTo("mongodb://localhost:27017/orders");
        assertThat(context.config().get("url").asText()).isEqualTo("https://example.com/api");
        assertThat(context.config().get("jdbcUrl").asText())
                .isEqualTo("jdbc:postgresql://localhost:5432/finance");
    }

    @Test
    void aMissingVariableFailsLoudlyAndNamesWhatIsMissing() {
        // Never passed through as its own text. Handing MongoDB the string "env:MONGO_URI" produces
        // a parse error about an invalid connection string, which sends whoever reads it looking at
        // the connector rather than at the deployment.
        assertThatThrownBy(() -> contexts.forInstance(
                instance(config -> config.put("connectionString", "env:MONGO_URI_TYPO")),
                "run", "worker"))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("connectionString")
                .hasMessageContaining("MONGO_URI_TYPO")
                .hasMessageContaining("not set on this pod");
    }

    @Test
    void theFailureNamesTheNestedFieldRatherThanJustSayingSomethingIsMissing() {
        assertThatThrownBy(() -> contexts.forInstance(instance(config -> {
            ObjectNode headers = Json.newObject();
            headers.put("X-Tenant", "env:ABSENT_TENANT");
            config.set("headers", headers);
        }), "run", "worker"))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("headers.X-Tenant");
    }

    @Test
    void nestedObjectsAndArraysAreResolvedToo() {
        ConnectorContext context = contexts.forInstance(instance(config -> {
            ObjectNode nested = Json.newObject();
            nested.put("host", "env:DATABRICKS_HOST");
            config.set("options", nested);
            config.set("hosts", Json.mapper().createArrayNode()
                    .add("env:MONGO_HOST")
                    .add("literal.example.com"));
            config.put("timeoutSeconds", 30);
        }), "run", "worker");

        assertThat(context.config().path("options").path("host").asText())
                .isEqualTo("https://adb-999.5.azuredatabricks.net");
        assertThat(context.config().path("hosts").get(0).asText()).isEqualTo("mongo.prod");
        assertThat(context.config().path("hosts").get(1).asText()).isEqualTo("literal.example.com");
        assertThat(context.config().path("timeoutSeconds").asInt())
                .as("non-string values pass through untouched")
                .isEqualTo(30);
    }

    @Test
    void credentialsStillResolveThroughTheSecretPathAsBefore() {
        ConnectorContext context = contexts.forInstance(instanceWithSecrets(), "run", "worker");

        assertThat(context.secret("clientSecret")).contains("the-secret");
        assertThat(context.secret("notConfigured")).isEmpty();
    }

    // ------------------------------------------------------------------ helpers

    private static ConnectorInstance instance(java.util.function.Consumer<ObjectNode> customise) {
        ObjectNode config = Json.newObject();
        customise.accept(config);
        return ConnectorInstance.create(TENANT, "test", "mongodb",
                ConnectorDirection.BOTH, config, Json.emptyObject(), null, Instant.EPOCH);
    }

    private static ConnectorInstance instanceWithSecrets() {
        ObjectNode secrets = Json.newObject();
        secrets.put("clientSecret", "env:SF_CLIENT_SECRET");
        return ConnectorInstance.create(TENANT, "test", "salesforce",
                ConnectorDirection.BOTH, Json.emptyObject(), secrets, null, Instant.EPOCH);
    }

    /** Stands in for the environment, so these tests do not depend on the machine running them. */
    private record FakeSecrets(Map<String, String> values) implements SecretsProvider {

        @Override
        public Optional<String> resolve(TenantId tenantId, String reference) {
            String name = reference.startsWith("env:") ? reference.substring(4) : reference;
            return Optional.ofNullable(values.get(name));
        }

        @Override
        public String scheme() {
            return "env";
        }
    }
}
