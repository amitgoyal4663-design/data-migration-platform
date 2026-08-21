package com.dmp.connector.mongodb;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Putting a run's values into a MongoDB filter.
 *
 * <p>MongoDB has no prepared statements, so the substitution happens in the parsed document rather
 * than in its text. The structure is fixed the moment the JSON parses, which is what stops a value
 * changing which fields or operators the filter uses — the same guarantee a prepared statement
 * gives, reached differently.
 *
 * <p>The typing carries the weight here. A date field compared against the <em>string</em> that
 * spells a date matches nothing, silently, and is indistinguishable from a window that genuinely
 * had no data in it.
 */
class FilterParametersTest {

    @Test
    void placeholdersAreFoundInsideNestedOperators() {
        assertThat(FilterParameters.referencedBy(
                "{\"updatedAt\": {\"$gt\": \":from\", \"$lte\": \":to\"}}"))
                .containsExactly("from", "to");
    }

    @Test
    void aFilterWithNoPlaceholdersNeedsNothing() {
        assertThat(FilterParameters.referencedBy("{\"status\": \"active\"}")).isEmpty();
        assertThat(FilterParameters.referencedBy(null)).isEmpty();
    }

    @Test
    void anIsoTimestampBecomesADateNotAString() {
        // The one that matters. Mongo compares a date field against a Date; against the string
        // that spells one it matches nothing at all and reports a successful, empty run.
        Document bound = FilterParameters.bind(
                Document.parse("{\"updatedAt\": {\"$gt\": \":from\"}}"),
                values(v -> v.put("from", "2026-08-01T00:00:00Z")));

        Object value = bound.get("updatedAt", Document.class).get("$gt");
        assertThat(value).isInstanceOf(Date.class);
        assertThat((Date) value).isEqualTo(Date.from(java.time.Instant.parse("2026-08-01T00:00:00Z")));
    }

    @Test
    void anOffsetTimestampFromAWindowScriptIsAlsoADate() {
        // What the window script actually produces: ISO-8601 with the schedule's offset, not Z.
        Document bound = FilterParameters.bind(
                Document.parse("{\"updatedAt\": {\"$gt\": \":from\"}}"),
                values(v -> v.put("from", "2026-08-01T00:00:00+05:30")));

        assertThat(bound.get("updatedAt", Document.class).get("$gt")).isInstanceOf(Date.class);
    }

    @Test
    void aNumberBecomesANumber() {
        Document bound = FilterParameters.bind(
                Document.parse("{\"seq\": {\"$gt\": \":from\"}}"),
                values(v -> v.put("from", "5000")));

        assertThat(bound.get("seq", Document.class).get("$gt")).isEqualTo(5000L);
    }

    @Test
    void anOrdinaryStringStaysAString() {
        Document bound = FilterParameters.bind(
                Document.parse("{\"region\": \":region\"}"),
                values(v -> v.put("region", "EMEA")));

        assertThat(bound.getString("region")).isEqualTo("EMEA");
    }

    @Test
    void valuesInsideArraysAreBoundToo() {
        Document bound = FilterParameters.bind(
                Document.parse("{\"$or\": [{\"a\": \":from\"}, {\"b\": \":from\"}]}"),
                values(v -> v.put("from", "7")));

        assertThat(bound.getList("$or", Document.class).get(0).get("a")).isEqualTo(7L);
        assertThat(bound.getList("$or", Document.class).get(1).get("b")).isEqualTo(7L);
    }

    @Test
    void aValueCannotChangeWhatTheFilterAsksFor() {
        // The property that makes this safe. Substitution happens in the parsed document, so a
        // value that looks like an operator is compared against, not executed as one.
        Document bound = FilterParameters.bind(
                Document.parse("{\"name\": \":name\"}"),
                values(v -> v.put("name", "{\"$ne\": null}")));

        assertThat(bound.get("name")).isInstanceOf(String.class);
        assertThat(bound.getString("name")).isEqualTo("{\"$ne\": null}");
    }

    @Test
    void aMissingValueIsRefusedRatherThanLeftInPlace() {
        // A filter still containing ":from" asks for documents whose field equals that literal
        // string. It matches nothing, and the run completes successfully having moved nothing.
        assertThatThrownBy(() -> FilterParameters.bind(
                Document.parse("{\"updatedAt\": {\"$gt\": \":from\", \"$lte\": \":to\"}}"),
                values(v -> v.put("from", "2026-08-01T00:00:00Z"))))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("':to'")
                .hasMessageContaining("started without");
    }

    @Test
    void aLiteralColonInAValueIsNotAPlaceholder() {
        // ":from" is a placeholder; "10:30" is a time somebody meant literally.
        assertThat(FilterParameters.referencedBy("{\"label\": \"10:30 batch\"}")).isEmpty();
    }

    private static ObjectNode values(java.util.function.Consumer<ObjectNode> customise) {
        ObjectNode node = Json.newObject();
        customise.accept(node);
        return node;
    }
}
