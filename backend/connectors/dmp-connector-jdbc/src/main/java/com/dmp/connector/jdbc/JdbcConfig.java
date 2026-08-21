package com.dmp.connector.jdbc;

import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Typed view over the connector's JSON configuration.
 *
 * <p>Identifiers are validated against a strict pattern rather than escaped. Table and column names
 * cannot be passed as bind parameters — they are part of the SQL text — so the only safe options
 * are a whitelist pattern or quoting, and a pattern is both simpler to reason about and impossible
 * to get subtly wrong. Anything not matching is rejected at session open, where the user sees it,
 * rather than producing a syntax error mid-migration.
 */
record JdbcConfig(
        String url,
        String schema,
        String table,
        String splitColumn,
        List<String> columns,
        String whereClause,
        WriteMode writeMode,
        List<String> keyColumns,
        int queryTimeoutSeconds) {

    /** Unqualified SQL identifier: letters, digits and underscores, not starting with a digit. */
    private static final Pattern IDENTIFIER = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]{0,62}$");

    enum WriteMode {
        /** Plain INSERT. Fastest, and duplicates on retry unless the target tolerates them. */
        INSERT,
        /** INSERT … ON CONFLICT DO UPDATE. Idempotent, so a retried batch cannot duplicate. */
        UPSERT,
        /** INSERT … ON CONFLICT DO NOTHING. Idempotent, and keeps whatever landed first. */
        INSERT_IGNORE
    }

    static JdbcConfig from(ConnectorContext context) {
        JsonNode config = context.config();

        String url = text(config, "url", true);
        if (!url.startsWith("jdbc:")) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "The 'url' must be a JDBC URL such as jdbc:postgresql://host:5432/db");
        }

        String schema = identifier(config, "schema", "public");
        String table = identifier(config, "table", null);
        String splitColumn = config.hasNonNull("splitColumn")
                ? identifier(config, "splitColumn", null) : null;

        List<String> columns = identifiers(config, "columns");
        List<String> keyColumns = identifiers(config, "keyColumns");

        WriteMode writeMode = config.hasNonNull("writeMode")
                ? parseWriteMode(config.get("writeMode").asText())
                : WriteMode.INSERT;

        if (writeMode != WriteMode.INSERT && keyColumns.isEmpty()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Write mode " + writeMode + " needs 'keyColumns' to decide what constitutes "
                            + "a conflict");
        }

        // Passed through verbatim into the WHERE clause, so it is the user's own SQL. Acceptable
        // because configuring a connector instance is already an administrative action with full
        // database credentials — there is no privilege here to escalate.
        String whereClause = config.hasNonNull("where") ? config.get("where").asText() : null;

        int timeout = config.hasNonNull("queryTimeoutSeconds")
                ? config.get("queryTimeoutSeconds").asInt() : 0;

        return new JdbcConfig(url, schema, table, splitColumn, columns, whereClause,
                writeMode, keyColumns, timeout);
    }

    /**
     * Fully-qualified and quoted for the target database.
     *
     * <p>Safe to interpolate because every part already passed {@link #IDENTIFIER}; the quoting is
     * about reserved words and case folding, not injection. Oracle in particular folds unquoted
     * names to upper case, so a table created as {@code orders} is really {@code ORDERS}.
     */
    String qualifiedTable(JdbcDialect dialect) {
        return schema == null ? dialect.quote(table) : dialect.quote(schema) + "." + dialect.quote(table);
    }

    String selectList(JdbcDialect dialect) {
        return columns.isEmpty()
                ? "*"
                : columns.stream().map(dialect::quote).reduce((a, b) -> a + ", " + b).orElse("*");
    }

    boolean isSplittable() {
        return splitColumn != null;
    }

    private static WriteMode parseWriteMode(String value) {
        try {
            return WriteMode.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Unknown writeMode '" + value + "'. Use INSERT, UPSERT or INSERT_IGNORE.");
        }
    }

    private static String text(JsonNode config, String field, boolean required) {
        JsonNode node = config.get(field);
        if (node == null || node.isNull() || node.asText().isBlank()) {
            if (required) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "Configuration field '" + field + "' is required");
            }
            return null;
        }
        return node.asText().strip();
    }

    private static String identifier(JsonNode config, String field, String fallback) {
        String value = text(config, field, fallback == null);
        if (value == null) {
            return fallback;
        }
        if (!IDENTIFIER.matcher(value).matches()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "'" + field + "' must be a plain SQL identifier (letters, digits, underscores), "
                            + "but was: " + value);
        }
        return value;
    }

    private static List<String> identifiers(JsonNode config, String field) {
        JsonNode node = config.get(field);
        if (node == null || !node.isArray()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        for (JsonNode element : node) {
            String value = element.asText().strip();
            if (!IDENTIFIER.matcher(value).matches()) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "'" + field + "' contains an invalid SQL identifier: " + value);
            }
            values.add(value);
        }
        return List.copyOf(values);
    }
}
