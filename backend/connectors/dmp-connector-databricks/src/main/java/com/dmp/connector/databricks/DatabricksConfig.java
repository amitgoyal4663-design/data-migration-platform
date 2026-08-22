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
        ChunkMode chunkMode,
        String orderBy,
        Duration waitTimeout,
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
                parseChunkMode(text(config, "chunkMode", "RESULT_CHUNKS")),
                text(config, "orderBy", null),
                waitTimeout(config),
                Math.max(0, config.path("rowLimit").asLong(0)),
                config.path("typedValues").asBoolean(true),
                parseAuth(text(config, "authMethod", "TOKEN")));
    }

    /**
     * How the work is divided, and therefore how many times the query runs.
     *
     * <p>The two are genuinely different trades rather than one being better.
     */
    enum ChunkMode {
        /**
         * One statement for the whole run, split by the manifest the warehouse returns.
         *
         * <p>The query runs once. The division is exact — the manifest states the row count of
         * every piece — so no counting, no key column and no guessing, and chunks cannot come out
         * uneven on skewed data. The cost is a shared, perishable result: the warehouse holds it,
         * and when it stops holding it every remaining chunk fails together.
         */
        RESULT_CHUNKS,
        /**
         * A statement per chunk, each fetching its own slice by row offset.
         *
         * <p>The query runs once per chunk, which a warehouse charges for. In return nothing is
         * shared and nothing perishes: a chunk can be retried an hour later and simply re-runs its
         * own query, and a chunk that fails takes no other chunk with it. For a table that is
         * already sitting there — not a computed result somebody is waiting on — that is usually
         * the better trade, and it is the shape most existing migration jobs already have.
         *
         * <p>Needs {@code orderBy}. An OFFSET without a total order is meaningless: the same query
         * may return rows in a different sequence on each run, so chunk 3 and chunk 4 could both
         * contain a row, and some other row nothing at all.
         */
        OFFSET
    }

    /**
     * How long the submit call itself waits for the query before giving up and returning an id.
     *
     * <p>Thirty seconds by default, which turns the common case into a single request: a query that
     * reads a slice of a table already sitting there usually finishes well inside it, and the
     * workspace then answers the submission with the rows attached. A query that does not finish
     * returns an id and is polled exactly as before, so a slow query costs what it always did.
     *
     * <p>Zero disables the wait entirely and makes every submission asynchronous. That is the right
     * setting for a single statement covering a whole run — an hour-long query has nothing to gain
     * from a thirty-second wait and a worker would be held for it.
     *
     * <p>Databricks accepts {@code 0s} or a value from {@code 5s} to {@code 50s} and refuses
     * anything between, which is a rule worth enforcing here rather than discovering as a 400 on
     * the first run.
     */
    private static Duration waitTimeout(JsonNode config) {
        long seconds = config.path("waitTimeoutSeconds").asLong(30);
        if (seconds == 0) {
            return Duration.ZERO;
        }
        if (seconds < 5 || seconds > 50) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "waitTimeoutSeconds must be 0, or between 5 and 50 — Databricks refuses "
                            + "anything else. 0 submits the query and polls for it; 5 to 50 lets "
                            + "the submission itself wait, so a query that finishes in time comes "
                            + "back in one request with its rows.");
        }
        return Duration.ofSeconds(seconds);
    }

    static ChunkMode parseChunkMode(String value) {
        try {
            return ChunkMode.valueOf(value.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (RuntimeException e) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Unknown chunkMode '" + value + "'. Use RESULT_CHUNKS to run the query once and "
                            + "split the result the warehouse returns, or OFFSET to run a separate "
                            + "query per chunk.");
        }
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
