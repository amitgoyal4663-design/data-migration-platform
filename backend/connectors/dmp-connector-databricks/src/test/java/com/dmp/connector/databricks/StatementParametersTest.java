package com.dmp.connector.databricks;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Binding a run's values into a parameterised query.
 *
 * <p>Two things are being defended here. The first is that a value never becomes part of the SQL
 * text — a date containing a quote must not be able to change what the statement means. The second
 * is that the value arrives with a type: a {@code BIGINT} column compared against the string
 * "5000" is a different query plan from one compared against the number, and which of those you
 * get is not worth finding out in production.
 */
class StatementParametersTest {

    @Test
    void aQueryWithNoPlaceholdersBindsNothing() {
        // The overwhelming majority of pipelines. They must behave exactly as they did before
        // parameters existed, including sending no parameters list at all.
        assertThat(StatementParameters.bind("SELECT * FROM orders", values(v -> { }))).isNull();
    }

    @Test
    void placeholdersAreBoundByName() {
        ArrayNode bound = StatementParameters.bind(
                "SELECT * FROM orders WHERE id > :from AND id <= :to",
                values(v -> {
                    v.put("from", "5000");
                    v.put("to", "6000");
                }));

        assertThat(bound).hasSize(2);
        assertThat(bound.get(0).get("name").asText()).isEqualTo("from");
        assertThat(bound.get(0).get("value").asText()).isEqualTo("5000");
    }

    @Test
    void aNumberIsTypedAsANumberAndATimestampAsATimestamp() {
        ArrayNode bound = StatementParameters.bind(
                "SELECT * FROM orders WHERE id > :id AND updated_at > :since AND d = :day",
                values(v -> {
                    v.put("id", "5000");
                    v.put("since", "2026-08-01T10:00:00Z");
                    v.put("day", "2026-08-01");
                }));

        assertThat(bound.get(0).get("type").asText()).isEqualTo("BIGINT");
        assertThat(bound.get(1).get("type").asText()).isEqualTo("TIMESTAMP");
        assertThat(bound.get(2).get("type").asText()).isEqualTo("DATE");
    }

    @Test
    void anOrdinaryStringGetsNoTypeAndStaysAString() {
        ArrayNode bound = StatementParameters.bind(
                "SELECT * FROM orders WHERE region = :region",
                values(v -> v.put("region", "EMEA")));

        assertThat(bound.get(0).has("type")).isFalse();
        assertThat(bound.get(0).get("value").asText()).isEqualTo("EMEA");
    }

    @Test
    void aMissingValueIsRefusedByNameBeforeTheStatementIsSubmitted() {
        // Databricks would answer with a syntax error mentioning a parameter marker, which sends
        // whoever reads it into the SQL rather than to the run that was started without a value.
        assertThatThrownBy(() -> StatementParameters.bind(
                "SELECT * FROM orders WHERE id > :from AND id <= :to",
                values(v -> v.put("from", "5000"))))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("'to'")
                .hasMessageContaining("started without");
    }

    @Test
    void aColonInsideAStringIsNotAPlaceholder() {
        // Otherwise a run fails demanding a value for something the query never asks to bind.
        assertThat(StatementParameters.referencedBy(
                "SELECT * FROM notes WHERE body = 'call me :from tomorrow'"))
                .isEmpty();
    }

    @Test
    void aCastIsNotAPlaceholder() {
        // Databricks writes a cast as value::STRING. One colon is a parameter, two is a cast, and
        // reading the second as a parameter named STRING would fail every query that uses one.
        assertThat(StatementParameters.referencedBy(
                "SELECT id::STRING FROM orders WHERE id > :from"))
                .containsExactly("from");
    }

    @Test
    void theSameNameUsedTwiceIsBoundOnce() {
        ArrayNode bound = StatementParameters.bind(
                "SELECT * FROM orders WHERE a > :from OR b > :from",
                values(v -> v.put("from", "5000")));

        assertThat(bound).hasSize(1);
    }

    @Test
    void aBlankValueCountsAsMissingRatherThanAsAnEmptyString() {
        // A dialog that submits an untouched box sends "", and reading a whole table because a
        // bound is empty is the wrong way to discover it was never filled in.
        assertThatThrownBy(() -> StatementParameters.bind(
                "SELECT * FROM orders WHERE id > :from",
                values(v -> v.put("from", "  "))))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("'from'");
    }

    private static ObjectNode values(java.util.function.Consumer<ObjectNode> customise) {
        ObjectNode node = Json.newObject();
        customise.accept(node);
        return node;
    }
}
