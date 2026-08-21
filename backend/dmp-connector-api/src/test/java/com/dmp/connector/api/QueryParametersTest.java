package com.dmp.connector.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Finding {@code :name} placeholders in SQL.
 *
 * <p>Shared by every connector that takes a per-run range, so the two ways of getting it wrong are
 * pinned here once. Both are silent: a cast read as a placeholder fails every run of a query that
 * was always valid, and a colon inside a quoted string demands a value nobody can supply.
 */
class QueryParametersTest {

    @Test
    void placeholdersAreFoundInOrderOfFirstAppearance() {
        assertThat(QueryParameters.referencedBy(
                "SELECT * FROM orders WHERE id > :from AND id <= :to"))
                .containsExactly("from", "to");
    }

    @Test
    void aCastIsNotAPlaceholder() {
        // Databricks writes a cast as value::STRING. Reading the second colon as a placeholder
        // called STRING would fail every query that uses one.
        assertThat(QueryParameters.referencedBy("SELECT id::STRING FROM orders WHERE id > :from"))
                .containsExactly("from");
    }

    @Test
    void aColonInsideAStringIsNotAPlaceholder() {
        assertThat(QueryParameters.referencedBy(
                "SELECT * FROM notes WHERE body = 'call me :from tomorrow' AND id > :after"))
                .containsExactly("after");
    }

    @Test
    void aQueryWithNoPlaceholdersFindsNone() {
        assertThat(QueryParameters.referencedBy("SELECT * FROM orders")).isEmpty();
        assertThat(QueryParameters.referencedBy(null)).isEmpty();
    }

    @Test
    void rewritingProducesJdbcMarkersInBindingOrder() {
        QueryParameters.Positional positional =
                QueryParameters.toPositional("id > :from AND id <= :to");

        assertThat(positional.sql()).isEqualTo("id > ? AND id <= ?");
        assertThat(positional.names()).containsExactly("from", "to");
    }

    @Test
    void aNameUsedTwiceProducesTwoMarkers() {
        // A PreparedStatement binds by position, so each marker needs its own value even when both
        // carry the same name. Returning it once would leave the second marker unbound.
        QueryParameters.Positional positional =
                QueryParameters.toPositional("a > :from OR b > :from");

        assertThat(positional.sql()).isEqualTo("a > ? OR b > ?");
        assertThat(positional.names()).containsExactly("from", "from");
    }

    @Test
    void rewritingLeavesQuotedTextIntact() {
        // Quoted text is blanked out to find placeholders, and the query that actually runs must
        // be built from the original — otherwise the rewrite would erase the data it selects on.
        QueryParameters.Positional positional =
                QueryParameters.toPositional("note = 'call me :from' AND id > :after");

        assertThat(positional.sql()).isEqualTo("note = 'call me :from' AND id > ?");
        assertThat(positional.names()).containsExactly("after");
    }
}
