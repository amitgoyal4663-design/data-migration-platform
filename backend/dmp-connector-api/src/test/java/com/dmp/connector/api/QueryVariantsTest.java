package com.dmp.connector.api;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QueryVariantsTest {

    private static final String CONFIG = """
            {
              "collection": "policies",
              "connectionString": "mongodb://host:27017/orders",
              "queries": [
                { "name": "By date range",    "filter": "{\\"updatedAt\\": {\\"$gte\\": \\":from\\"}}" },
                { "name": "By policy number", "filter": "{\\"policyNo\\": {\\"$in\\": \\":policyNos\\"}}" },
                { "name": "By region",        "filter": "{\\"region\\": \\":region\\"}" }
              ]
            }
            """;

    private static JsonNode config() {
        return parse(CONFIG);
    }

    private static JsonNode parse(String json) {
        try {
            return Json.mapper().readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalArgumentException(json, e);
        }
    }

    @Test
    @DisplayName("offers the names in the order they were written")
    void namesKeepTheirOrder() {
        // The order is the whole contract of the list: the first is what a run that names no query
        // is given. It was a map, and configuration is stored as jsonb — which orders object keys
        // by length and then by bytes, so these three came back region, date, number and the
        // default silently became the one with the shortest name.
        assertThat(QueryVariants.names(config()))
                .containsExactly("By date range", "By policy number", "By region");
        assertThat(QueryVariants.defaultName(config())).isEqualTo("By date range");
    }

    @Test
    @DisplayName("merges the chosen query's fields and hides the rest")
    void appliesOneVariant() {
        JsonNode applied = QueryVariants.apply(config(), "By policy number");

        assertThat(applied.path("filter").asText()).contains("$in").contains(":policyNos");
        // Everything the variant did not mention comes from the instance, so a query can change
        // what is selected and never what it is selected from.
        assertThat(applied.path("collection").asText()).isEqualTo("policies");
        assertThat(applied.path("connectionString").asText()).contains("mongodb://");
        // A connector sees a configuration in the shape it always saw, including no sign of this.
        assertThat(applied.has("queries")).isFalse();
        assertThat(applied.has("name")).isFalse();
    }

    @Test
    @DisplayName("leaves the configuration alone when no query applies")
    void unknownOrAbsentIsNotAnError() {
        assertThat(QueryVariants.apply(config(), "By something else")).isEqualTo(config());
        assertThat(QueryVariants.apply(config(), null)).isEqualTo(config());
        assertThat(QueryVariants.apply(config(), "  ")).isEqualTo(config());

        JsonNode plain = parse("{\"collection\": \"policies\"}");
        assertThat(QueryVariants.apply(plain, "By date range")).isEqualTo(plain);
        assertThat(QueryVariants.names(plain)).isEmpty();
        assertThat(QueryVariants.defaultName(plain)).isNull();
        assertThat(QueryVariants.any(plain)).isFalse();
    }

    @Test
    @DisplayName("ignores entries that name nothing")
    void skipsUnnamedEntries() {
        JsonNode config = parse("""
                {"queries": [{"sql": "SELECT 1"}, {"name": "", "sql": "SELECT 2"},
                             {"name": "Real", "sql": "SELECT 3"}]}
                """);

        assertThat(QueryVariants.names(config)).isEqualTo(List.of("Real"));
        assertThat(QueryVariants.apply(config, "Real").path("sql").asText()).isEqualTo("SELECT 3");
    }
}
