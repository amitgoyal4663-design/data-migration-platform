package com.dmp.engine;

import com.dmp.common.json.Json;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether a source can identify the records it reads.
 *
 * <p>The answer decides whether a pipeline set to index its records will actually index anything.
 * Records without a key are skipped on purpose — an index of anonymous rows answers no question —
 * so a source that cannot produce one turns the feature into a silent no-op, and that is what the
 * publish-time warning exists to prevent.
 */
class ConnectorCapabilitiesTest {

    private static ConnectorCapabilities capabilities;

    @BeforeAll
    static void loadConnectors() {
        ConnectorRegistry registry = new ConnectorRegistry("plugins-that-do-not-exist");
        capabilities = new ConnectorCapabilities(registry);

        // Guards against the false pass this test was originally written into: with no connectors
        // on the classpath every lookup answers "not installed", which this class treats as "has an
        // identity" — so four of six assertions held for entirely the wrong reason.
        assertThat(registry.isInstalled("file-csv"))
                .as("the connectors under test must actually be loaded")
                .isTrue();
    }

    @Test
    void mongoIdentifiesRecordsWithNoConfigurationAtAll() {
        // splitField declares _id as its default, which is why the ordinary MongoDB pipeline is
        // searchable without anybody configuring anything.
        assertThat(capabilities.identifiesRecords("mongodb", Json.newObject())).isTrue();
    }

    @Test
    void csvCannotIdentifyRecordsUntilToldWhichColumn() {
        assertThat(capabilities.identifiesRecords("file-csv", Json.newObject()))
                .as("a CSV row has no key of its own, and a row number is a position, not a record")
                .isFalse();
    }

    @Test
    void csvIdentifiesRecordsOnceTheColumnIsNamed() {
        ObjectNode config = Json.newObject();
        config.put("keyColumn", "order_id");

        assertThat(capabilities.identifiesRecords("file-csv", config)).isTrue();
    }

    @Test
    void aBlankValueIsNotAnAnswer() {
        ObjectNode config = Json.newObject();
        config.put("keyColumn", "   ");

        assertThat(capabilities.identifiesRecords("file-csv", config))
                .as("whitespace is an empty field somebody tabbed through, not a column name")
                .isFalse();
    }

    @Test
    void jdbcFallsBackToItsSplitColumn() {
        ObjectNode config = Json.newObject();
        config.put("splitColumn", "id");

        // keyColumns is unset, but the split column is almost always the primary key, and the
        // connector reads it as the identity when nothing better is configured.
        assertThat(capabilities.identifiesRecords("jdbc-postgres", config)).isTrue();
    }

    @Test
    void anUninstalledConnectorIsNotComplainedAboutTwice() {
        assertThat(capabilities.identifiesRecords("not-a-connector", Json.newObject()))
                .as("its absence is already a validation error; a second complaint helps nobody")
                .isTrue();
    }
}
