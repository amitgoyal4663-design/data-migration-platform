package com.dmp.connector.databricks;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.api.ConnectorSpec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the configuration accepts, what it refuses, and what it assumes when told nothing.
 *
 * <p>The defaults are the interesting part. Every one of them is a decision that someone will
 * inherit without reading this file, so each is asserted rather than left to be discovered by a
 * migration that behaved differently from the one before it.
 */
class DatabricksConfigTest {

    @Test
    void theDefaultsAreTheOnesLargeMigrationsNeed() {
        DatabricksConfig config = DatabricksConfig.from(context(node -> { }));

        assertThat(config.disposition())
                .as("INLINE caps a result at 25 MiB, which a migration passes on its first real "
                        + "table; the default must not be the one that stops working as data grows")
                .isEqualTo(DatabricksConfig.Disposition.EXTERNAL_LINKS);
        assertThat(config.queryTimeout()).isEqualTo(Duration.ofHours(1));
        assertThat(config.pollInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.auth()).isEqualTo(DatabricksConfig.Auth.TOKEN);
        assertThat(config.typedValues())
                .as("numbers arriving at the destination as strings is a silent corruption, so "
                        + "typing is on unless it is turned off deliberately")
                .isTrue();
        assertThat(config.rowLimit()).isZero();
    }

    @Test
    void aTrailingSlashOnTheHostDoesNotProduceADoubleSlashedUrl() {
        DatabricksConfig config = DatabricksConfig.from(
                context(node -> node.put("host", "https://example.cloud.databricks.com/")));

        assertThat(config.statementsUrl())
                .isEqualTo("https://example.cloud.databricks.com/api/2.0/sql/statements");
    }

    @Test
    void theHostAndWarehouseAreRequiredAndSaySoInTermsOfWhereToFindThem() {
        assertThatThrownBy(() -> DatabricksConfig.from(context(node -> node.remove("host"))))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("'host'")
                .hasMessageContaining("https://");

        assertThatThrownBy(() -> DatabricksConfig.from(context(node -> node.remove("warehouseId"))))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("'warehouseId'")
                .hasMessageContaining("last path segment");
    }

    @Test
    void anUnknownDispositionOrAuthMethodIsRefusedRatherThanQuietlyDefaulted() {
        assertThatThrownBy(() -> DatabricksConfig.from(
                context(node -> node.put("disposition", "STREAMING"))))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("EXTERNAL_LINKS");

        assertThatThrownBy(() -> DatabricksConfig.from(
                context(node -> node.put("authMethod", "KERBEROS"))))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("OAUTH");
    }

    @Test
    void aPollIntervalOfZeroIsRaisedRatherThanBecomingABusyLoop() {
        DatabricksConfig config = DatabricksConfig.from(context(node -> {
            node.put("pollSeconds", 0);
            node.put("queryTimeoutSeconds", 0);
        }));

        assertThat(config.pollInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.queryTimeout()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void theConnectorDeclaresItselfAsReadOnlyAndNamesItsSecrets() {
        ConnectorSpec spec = new DatabricksConnector().spec();

        assertThat(spec.type()).isEqualTo("databricks");
        assertThat(spec.direction())
                .as("writing to a warehouse is a different connector with different idempotency; "
                        + "declaring BOTH and throwing from openSink would not be honest")
                .isEqualTo(ConnectorSpec.Direction.SOURCE);
        assertThat(spec.secretFields()).containsExactlyInAnyOrder("token", "clientId", "clientSecret");

        JsonNode properties = spec.configSchema().path("properties");
        assertThat(properties.has("sql")).isTrue();
        assertThat(properties.has("queryTimeoutSeconds")).isTrue();
        assertThat(properties.path("keyColumn").path("x-dmp-record-key").asBoolean())
                .as("the console warns about indexing a source with no identity, and can only do "
                        + "that if the connector says which field carries one")
                .isTrue();
    }

    private static ConnectorContext context(java.util.function.Consumer<ObjectNode> customise) {
        ObjectNode config = Json.newObject();
        config.put("host", "https://example.cloud.databricks.com");
        config.put("warehouseId", "abc123");
        config.put("sql", "SELECT 1");
        customise.accept(config);

        return new ConnectorContext() {
            @Override
            public JsonNode config() {
                return config;
            }

            @Override
            public Optional<String> secret(String name) {
                return Optional.empty();
            }

            @Override
            public String workerId() {
                return "test-worker";
            }

            @Override
            public String runId() {
                return "test-run";
            }

            @Override
            public org.slf4j.Logger log() {
                return LoggerFactory.getLogger(DatabricksConfigTest.class);
            }
        };
    }
}
