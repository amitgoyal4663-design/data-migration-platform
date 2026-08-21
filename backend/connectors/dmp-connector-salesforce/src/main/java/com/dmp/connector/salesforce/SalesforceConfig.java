package com.dmp.connector.salesforce;

import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Locale;

/**
 * One connector instance's Salesforce settings.
 *
 * @param loginUrl    the org's login host — production or a named sandbox
 * @param apiVersion  the REST API version path segment, e.g. {@code v62.0}
 * @param object      the sObject to read or write, e.g. {@code Account}
 * @param operation   how records are applied when writing
 * @param externalIdField the field an upsert matches on; required only for {@code upsert}
 * @param soql        the query a source runs; the object is derived from it when absent
 * @param lineEnding  what Salesforce should expect in the uploaded CSV
 * @param collectRecordResults whether to download the job's failed-records file so each rejection
 *                    reaches the dead-letter queue. Off by default: the run's failure count comes
 *                    free from the status the engine already polls, while naming the records means
 *                    downloading and parsing every rejected row — hundreds of kilobytes for a
 *                    chunk that fails wholesale, to produce a number we already had. Turn it on
 *                    when a few records failing among many is the interesting case and you want
 *                    them replayable; leave it off when failures here are systemic, which against
 *                    this destination they usually are.
 */
record SalesforceConfig(
        String loginUrl,
        String apiVersion,
        String object,
        Operation operation,
        String externalIdField,
        String soql,
        String lineEnding,
        int pollSeconds,
        boolean collectRecordResults) {

    /**
     * How a record is applied.
     *
     * <p>{@code upsert} is the only one that is idempotent, and it is idempotent only because
     * Salesforce matches on an external id rather than on the record's own. That distinction is
     * what the engine's checkpoint interval depends on, so it is a first-class choice here rather
     * than a string passed through.
     */
    enum Operation {
        INSERT("insert", false),
        UPDATE("update", true),
        UPSERT("upsert", true),
        DELETE("delete", true),
        HARD_DELETE("hardDelete", true);

        final String wireName;
        final boolean idempotent;

        Operation(String wireName, boolean idempotent) {
            this.wireName = wireName;
            this.idempotent = idempotent;
        }
    }

    static SalesforceConfig from(ConnectorContext context) {
        JsonNode config = context.config();

        Operation operation = parseOperation(text(config, "operation", "insert"));
        String externalIdField = text(config, "externalIdField", null);

        if (operation == Operation.UPSERT && (externalIdField == null || externalIdField.isBlank())) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "An upsert needs 'externalIdField' — the field Salesforce matches on to decide "
                            + "whether a record already exists. Without it Salesforce cannot tell "
                            + "an update from an insert, and the operation is refused rather than "
                            + "silently turned into one or the other.");
        }

        return new SalesforceConfig(
                stripTrailingSlash(text(config, "loginUrl", "https://login.salesforce.com")),
                text(config, "apiVersion", "v62.0"),
                text(config, "object", null),
                operation,
                externalIdField,
                text(config, "soql", null),
                text(config, "lineEnding", "LF"),
                Math.max(1, config.path("pollSeconds").asInt(5)),
                config.path("collectRecordResults").asBoolean(false));
    }

    /** The object a source reads, taken from the query when it was not configured separately. */
    String queryObject() {
        if (object != null && !object.isBlank()) {
            return object;
        }
        if (soql == null) {
            return "records";
        }
        String upper = soql.toUpperCase(Locale.ROOT);
        int from = upper.indexOf(" FROM ");
        if (from < 0) {
            return "records";
        }
        String rest = soql.substring(from + 6).strip();
        int end = rest.indexOf(' ');
        return end < 0 ? rest : rest.substring(0, end);
    }

    String describe() {
        return (object == null || object.isBlank() ? queryObject() : object) + " on " + loginUrl;
    }

    private static Operation parseOperation(String value) {
        for (Operation candidate : Operation.values()) {
            if (candidate.wireName.equalsIgnoreCase(value) || candidate.name().equalsIgnoreCase(value)) {
                return candidate;
            }
        }
        throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                "Unknown Salesforce operation '" + value + "'. Use insert, update, upsert, delete "
                        + "or hardDelete.");
    }

    private static String text(JsonNode config, String field, String fallback) {
        JsonNode node = config.get(field);
        return node == null || node.isNull() || node.asText().isBlank() ? fallback : node.asText().strip();
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
