package com.dmp.connector.salesforce;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the connector will and will not accept before it talks to an org.
 *
 * <p>These are the mistakes that would otherwise surface as a Salesforce error halfway through a
 * migration, when a job has already been created and records have already moved.
 */
class SalesforceConfigTest {

    @Test
    void upsertWithoutAnExternalIdIsRefused() {
        assertThatThrownBy(() -> config(node -> {
            node.put("object", "Account");
            node.put("operation", "upsert");
        }))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("externalIdField")
                .as("Salesforce cannot tell an update from an insert without it");
    }

    @Test
    void upsertWithAnExternalIdIsAccepted() {
        SalesforceConfig config = config(node -> {
            node.put("object", "Account");
            node.put("operation", "upsert");
            node.put("externalIdField", "Legacy_Id__c");
        });

        assertThat(config.operation()).isEqualTo(SalesforceConfig.Operation.UPSERT);
        assertThat(config.operation().idempotent)
                .as("upsert is the only operation a repeated write cannot duplicate")
                .isTrue();
    }

    @Test
    void insertIsNotIdempotent() {
        assertThat(SalesforceConfig.Operation.INSERT.idempotent)
                .as("Salesforce assigns its own id, so a re-sent batch creates a second set")
                .isFalse();
    }

    @Test
    void anUnknownOperationIsNamedRatherThanGuessed() {
        assertThatThrownBy(() -> config(node -> {
            node.put("object", "Account");
            node.put("operation", "merge");
        }))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("merge");
    }

    @Test
    void theObjectIsTakenFromTheQueryWhenASourceDoesNotName_it() {
        SalesforceConfig config = config(node ->
                node.put("soql", "SELECT Id, Name FROM Account WHERE Name != null"));

        assertThat(config.queryObject()).isEqualTo("Account");
    }

    @Test
    void aTrailingSlashOnTheLoginUrlDoesNotProduceADoubleSlashedApiPath() {
        SalesforceConfig config = config(node ->
                node.put("loginUrl", "https://example.my.salesforce.com/"));

        assertThat(config.loginUrl()).isEqualTo("https://example.my.salesforce.com");
    }

    @Test
    void pollingCannotBeSetToZeroAndSpin() {
        SalesforceConfig config = config(node -> node.put("pollSeconds", 0));

        assertThat(config.pollSeconds())
                .as("a zero poll interval would ask a busy org as fast as the loop can run")
                .isGreaterThanOrEqualTo(1);
    }

    private static SalesforceConfig config(java.util.function.Consumer<ObjectNode> customise) {
        ObjectNode node = Json.newObject();
        customise.accept(node);

        return SalesforceConfig.from(new ConnectorContext() {
            @Override
            public JsonNode config() {
                return node;
            }

            @Override
            public Optional<String> secret(String name) {
                return Optional.empty();
            }

            @Override
            public String workerId() {
                return "test";
            }

            @Override
            public String runId() {
                return "test-run";
            }

            @Override
            public org.slf4j.Logger log() {
                return LoggerFactory.getLogger(SalesforceConfigTest.class);
            }
        });
    }
}
