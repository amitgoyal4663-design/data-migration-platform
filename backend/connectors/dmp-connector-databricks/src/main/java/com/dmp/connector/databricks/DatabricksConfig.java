package com.dmp.connector.databricks;

import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.Locale;

/**
 * One connector instance's Databricks settings.
 *
 * @param host           workspace URL, e.g. {@code https://adb-1234.5.azuredatabricks.net}
 * @param warehouseId    the SQL warehouse the statement runs on
 * @param sql            the query a source runs
 * @param catalog        Unity Catalog catalog the statement resolves names against; optional
 * @param schema         schema within that catalog; optional
 * @param queryTimeout   how long the platform waits for the statement to finish before cancelling
 *                       it and failing the run
 * @param pollInterval   how often the statement's status is checked while it runs
 * @param disposition    how Databricks returns the result — see {@link Disposition}
 * @param keyColumn      column holding each record's natural key; optional
 * @param rowLimit       cap on rows the statement returns; 0 means no cap
 * @param typedValues    whether to convert values using the result's declared column types
 * @param auth           how the workspace is authenticated
 */
record DatabricksConfig(
        String host,
        String warehouseId,
        String sql,
        String catalog,
        String schema,
        Duration queryTimeout,
        Duration pollInterval,
        Disposition disposition,
        String keyColumn,
        long rowLimit,
        boolean typedValues,
        Auth auth) {

    /**
     * How Databricks hands back a result set.
     *
     * <p>The default is deliberate. {@code INLINE} caps a result at 25 MiB in total, which is a
     * rounding error for a migration and turns a working pipeline into a failing one the moment the
     * table grows. {@code EXTERNAL_LINKS} stages results to cloud storage and returns pre-signed
     * URLs with no such ceiling, at the cost of a second request per chunk. Anything moving real
     * volume wants the second one; {@code INLINE} is kept for small lookups and for environments
     * where egress to the storage account is blocked.
     */
    enum Disposition {
        INLINE,
        EXTERNAL_LINKS
    }

    /**
     * How the workspace is authenticated.
     *
     * <p>Both are offered because enterprises are moving between them. A personal access token is
     * what almost every existing workspace has, and refusing to accept one would make the connector
     * unusable in exactly the places that need a migration. OAuth machine-to-machine is what a
     * service principal uses and what most security teams will insist on for anything scheduled, so
     * a pipeline built on a token today can move to one without being rebuilt.
     */
    enum Auth {
        TOKEN,
        OAUTH
    }

    static DatabricksConfig from(ConnectorContext context) {
        JsonNode config = context.config();

        String host = text(config, "host", null);
        if (host == null) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "A Databricks connector needs 'host' — the workspace URL, for example "
                            + "https://adb-1234567890.5.azuredatabricks.net");
        }
        String warehouseId = text(config, "warehouseId", null);
        if (warehouseId == null) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "A Databricks connector needs 'warehouseId' — the SQL warehouse the statement "
                            + "runs on. It is the last path segment of the warehouse's URL in the "
                            + "workspace, for example 1234567890abcdef.");
        }

        return new DatabricksConfig(
                stripTrailingSlash(host),
                warehouseId,
                text(config, "sql", null),
                text(config, "catalog", null),
                text(config, "schema", null),
                // An hour by default. Long enough that an ordinary warehouse query on a cold
                // warehouse is not cut off — a stopped warehouse takes minutes to start before the
                // query has run at all — and short enough that a statement nobody is waiting for
                // does not hold a run open overnight.
                seconds(config, "queryTimeoutSeconds", 3600, 1),
                seconds(config, "pollSeconds", 5, 1),
                parseDisposition(text(config, "disposition", "EXTERNAL_LINKS")),
                text(config, "keyColumn", null),
                Math.max(0, config.path("rowLimit").asLong(0)),
                config.path("typedValues").asBoolean(true),
                parseAuth(text(config, "authMethod", "TOKEN")));
    }

    /** The statements endpoint, e.g. {@code https://host/api/2.0/sql/statements}. */
    String statementsUrl() {
        return host + "/api/2.0/sql/statements";
    }

    String warehouseUrl() {
        return host + "/api/2.0/sql/warehouses/" + warehouseId;
    }

    String describe() {
        StringBuilder where = new StringBuilder();
        if (catalog != null) {
            where.append(catalog);
            if (schema != null) {
                where.append('.').append(schema);
            }
            where.append(" on ");
        }
        return where + host + " (warehouse " + warehouseId + ")";
    }

    /**
     * Fails a source with no query, at session open.
     *
     * <p>Separate from {@link #from} because the same configuration object is valid for a
     * connection test, which reaches the warehouse without running anything.
     */
    void requireQuery() {
        if (sql == null || sql.isBlank()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "A Databricks source needs 'sql' — the query whose results are migrated, for "
                            + "example SELECT * FROM main.sales.orders.");
        }
    }

    private static Disposition parseDisposition(String value) {
        try {
            return Disposition.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Unknown disposition '" + value + "'. Use EXTERNAL_LINKS for anything large, "
                            + "or INLINE for small results.");
        }
    }

    private static Auth parseAuth(String value) {
        try {
            return Auth.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Unknown authMethod '" + value + "'. Use TOKEN for a personal access token or "
                            + "OAUTH for a service principal's client credentials.");
        }
    }

    private static Duration seconds(JsonNode config, String field, int fallback, int minimum) {
        long value = config.path(field).asLong(fallback);
        return Duration.ofSeconds(Math.max(minimum, value));
    }

    private static String text(JsonNode config, String field, String fallback) {
        JsonNode node = config.get(field);
        return node == null || node.isNull() || node.asText().isBlank() ? fallback : node.asText().strip();
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
