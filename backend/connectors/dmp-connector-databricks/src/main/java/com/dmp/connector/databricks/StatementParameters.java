package com.dmp.connector.databricks;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.api.QueryParameters;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Binds a run's values into a parameterised SQL statement.
 *
 * <p>A migration is rarely "move this table" and usually "move what changed since yesterday", so
 * the query differs on every run while the pipeline that owns it does not. Writing {@code :from}
 * and {@code :to} in the SQL and supplying them per run is what separates the two.
 *
 * <p><b>Bound, never substituted.</b> Databricks accepts named parameters as a separate list, and
 * that is what this produces. Pasting a value into the SQL text would make any value containing a
 * quote either break the statement or change what it means, and would turn a scheduled job into an
 * injection point the first time a parameter came from somewhere less trusted than a schedule.
 */
final class StatementParameters {

    /** ISO-8601 instants, which is what a window boundary looks like when it comes from a clock. */
    private static final Pattern TIMESTAMP = Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}(:\\d{2}(\\.\\d+)?)?(Z|[+-]\\d{2}:?\\d{2})?$");

    private static final Pattern DATE = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");
    private static final Pattern INTEGER = Pattern.compile("^-?\\d{1,18}$");
    private static final Pattern DECIMAL = Pattern.compile("^-?\\d+\\.\\d+$");

    private StatementParameters() {
    }

    /** The placeholder names a statement uses, in the order they first appear. */
    static LinkedHashSet<String> referencedBy(String sql) {
        return QueryParameters.referencedBy(sql);
    }

    /**
     * Builds the {@code parameters} list for a statement, or null when it uses none.
     *
     * @param supplied the values the run was started with
     * @throws ConnectorException naming any placeholder nothing supplied a value for
     */
    /**
     * A statement with every list placeholder expanded, and the values to bind to it.
     *
     * <p>SQL has no way to bind a list. {@code IN (:policyNos)} with ten values must become
     * {@code IN (:policyNos_0, …, :policyNos_9)} with ten bindings — expanded into markers, never
     * concatenated into the statement. A policy number carrying a quote has to stay data; building
     * the SQL from it would make it code.
     *
     * @param sql the statement as written, with {@code :name} placeholders
     */
    record Bound(String sql, ArrayNode parameters) {
    }

    static Bound expand(String sql, JsonNode supplied) {
        JsonNode values = Json.orEmpty(supplied);
        String expanded = sql;

        for (String name : referencedBy(sql)) {
            JsonNode value = values.get(name);
            if (value == null || !value.isArray() || value.isEmpty()) {
                continue;
            }
            List<String> markers = new ArrayList<>(value.size());
            for (int i = 0; i < value.size(); i++) {
                markers.add(":" + name + "_" + i);
            }
            // Word-bounded, so :policy and :policyNos are not confused for one another.
            expanded = expanded.replaceAll(":" + Pattern.quote(name) + "\\b",
                    String.join(", ", markers));
        }
        return new Bound(expanded, bind(expanded, flatten(sql, values)));
    }

    /**
     * The supplied values with every list flattened into the markers {@link #expand} created.
     *
     * <p>Element by element, so each is typed on its own — a list of numeric ids bound as text
     * matches nothing against a numeric column, and says nothing about why.
     */
    private static JsonNode flatten(String originalSql, JsonNode values) {
        ObjectNode flat = Json.newObject();
        values.fields().forEachRemaining(field -> {
            if (field.getValue().isArray()) {
                for (int i = 0; i < field.getValue().size(); i++) {
                    flat.set(field.getKey() + "_" + i, field.getValue().get(i));
                }
            } else {
                flat.set(field.getKey(), field.getValue());
            }
        });
        return flat;
    }

    static ArrayNode bind(String sql, JsonNode supplied) {
        Set<String> referenced = referencedBy(sql);
        if (referenced.isEmpty()) {
            return null;
        }

        JsonNode values = Json.orEmpty(supplied);
        List<String> missing = new ArrayList<>();
        ArrayNode parameters = Json.mapper().createArrayNode();

        for (String name : referenced) {
            JsonNode value = values.get(name);
            if (value == null || value.isNull()
                    || (value.isTextual() && value.asText().isBlank())) {
                missing.add(name);
                continue;
            }
            parameters.add(parameter(name, value));
        }

        if (!missing.isEmpty()) {
            // Named, and refused before the statement is submitted. Databricks would otherwise
            // answer with a syntax error mentioning a parameter marker, which sends whoever reads
            // it into the SQL rather than to the run that was started without a value.
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "This query expects " + quoted(referenced) + ", but the run was started without "
                            + quoted(missing) + ". Supply " + (missing.size() == 1 ? "it" : "them")
                            + " when starting the run, or remove the placeholder"
                            + (missing.size() == 1 ? "" : "s") + " from the query.");
        }
        return parameters;
    }

    /**
     * One parameter, with the type Databricks should read the value as.
     *
     * <p>Typed rather than left as a string, because the comparison depends on it: a
     * {@code BIGINT} column compared against the string {@code "5000"} may work, may cost a full
     * scan, or may fail outright, and which of the three you get is not worth discovering in
     * production. The type is inferred from the value's own shape, so nothing has to be declared.
     */
    private static ObjectNode parameter(String name, JsonNode value) {
        ObjectNode parameter = Json.newObject();
        parameter.put("name", name);

        if (value.isBoolean()) {
            return typed(parameter, value.asText(), "BOOLEAN");
        }
        if (value.isIntegralNumber()) {
            return typed(parameter, value.asText(), "BIGINT");
        }
        if (value.isNumber()) {
            return typed(parameter, value.asText(), "DOUBLE");
        }

        String text = value.asText().strip();
        if (INTEGER.matcher(text).matches()) {
            return typed(parameter, text, "BIGINT");
        }
        if (DECIMAL.matcher(text).matches()) {
            return typed(parameter, text, "DOUBLE");
        }
        if (TIMESTAMP.matcher(text).matches()) {
            return typed(parameter, text, "TIMESTAMP");
        }
        if (DATE.matcher(text).matches()) {
            return typed(parameter, text, "DATE");
        }
        // No type: Databricks treats it as a string, which is the right answer for anything that
        // is not recognisably a number or a moment in time.
        parameter.put("value", text);
        return parameter;
    }

    private static ObjectNode typed(ObjectNode parameter, String value, String type) {
        parameter.put("value", value);
        parameter.put("type", type);
        return parameter;
    }

    private static String quoted(java.util.Collection<String> names) {
        return names.stream().map(name -> "'" + name + "'")
                .collect(java.util.stream.Collectors.joining(", "));
    }
}
