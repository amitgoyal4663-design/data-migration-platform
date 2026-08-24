package com.dmp.connector.mongodb;

import com.dmp.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.JsonNode;
import org.bson.Document;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Puts a run's values into a MongoDB filter.
 *
 * <p>MongoDB has no prepared statements, so this cannot work the way SQL does — and it does not
 * need to. A filter is a <em>document</em>, already parsed into a tree of fields and values before
 * this runs, and a placeholder is replaced by walking that tree and swapping one leaf for a typed
 * value. Nothing is concatenated, so nothing a value contains can change which fields the filter
 * mentions or which operators it uses. The structure was fixed the moment the JSON parsed.
 *
 * <p>The typing is the part that has to be right. Mongo compares a date field against a
 * {@code Date}, not against the string that spells one — {@code {"updatedAt": {"$gt":
 * "2026-08-01T00:00:00Z"}}} matches nothing at all against a real date field, silently, and looks
 * exactly like a window with no data in it.
 */
final class FilterParameters {

    /** A placeholder, as the whole value of a field: {@code {"id": {"$gt": ":from"}}}. */
    private static final Pattern PLACEHOLDER = Pattern.compile("^:([A-Za-z_][A-Za-z0-9_]*)$");

    private static final Pattern INTEGER = Pattern.compile("^-?\\d{1,18}$");
    private static final Pattern DECIMAL = Pattern.compile("^-?\\d+\\.\\d+$");

    private FilterParameters() {
    }

    /** The placeholder names a filter uses, in the order they appear. */
    static Set<String> referencedBy(String filterJson) {
        Set<String> names = new LinkedHashSet<>();
        if (filterJson == null || filterJson.isBlank()) {
            return names;
        }
        try {
            collect(Document.parse(filterJson), names);
        } catch (RuntimeException e) {
            // A filter that does not parse is a configuration error the connector reports far
            // better at run time. Answering "no parameters" here only means the Run dialog shows
            // no boxes for a pipeline that was never going to start.
            return Set.of();
        }
        return names;
    }

    /**
     * Returns the filter with every placeholder replaced by the value the run supplied.
     *
     * @throws ConnectorException naming any placeholder nothing supplied a value for
     */
    /**
     * The placeholders that take a list, read out of the filter itself.
     *
     * <p>An operator that compares against a set — {@code $in}, {@code $nin}, {@code $all} — takes
     * an array, and a placeholder underneath one is therefore plural. Nothing else in a filter is.
     */
    static Set<String> listsIn(String filterJson) {
        Set<String> names = new LinkedHashSet<>();
        if (filterJson == null || filterJson.isBlank()) {
            return names;
        }
        try {
            collectLists(Document.parse(filterJson), false, names);
        } catch (RuntimeException e) {
            return Set.of();
        }
        return names;
    }

    private static void collectLists(Object value, boolean underSetOperator, Set<String> names) {
        if (value instanceof Document document) {
            document.forEach((key, nested) ->
                    collectLists(nested, SET_OPERATORS.contains(key), names));
        } else if (value instanceof List<?> list) {
            list.forEach(nested -> collectLists(nested, underSetOperator, names));
        } else if (underSetOperator && value instanceof String text) {
            var matcher = PLACEHOLDER.matcher(text);
            if (matcher.matches()) {
                names.add(matcher.group(1));
            }
        }
    }

    private static final Set<String> SET_OPERATORS = Set.of("$in", "$nin", "$all");

    static Document bind(Document filter, JsonNode supplied) {
        List<String> missing = new ArrayList<>();
        Document bound = substitute(filter, supplied, missing);

        if (!missing.isEmpty()) {
            // Refused rather than left in place. A filter still containing ":from" asks MongoDB
            // for documents whose field equals the literal string ":from", which matches nothing
            // and completes as a successful run that moved nothing.
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "This pipeline's filter expects " + missing.stream()
                            .map(name -> "':" + name + "'")
                            .collect(java.util.stream.Collectors.joining(", "))
                            + ", but the run was started without "
                            + (missing.size() == 1 ? "it" : "them")
                            + ". Supply " + (missing.size() == 1 ? "it" : "them")
                            + " when starting the run, or remove the placeholder"
                            + (missing.size() == 1 ? "" : "s") + " from the filter.");
        }
        return bound;
    }

    private static void collect(Object value, Set<String> names) {
        if (value instanceof Document document) {
            document.values().forEach(nested -> collect(nested, names));
        } else if (value instanceof List<?> list) {
            list.forEach(nested -> collect(nested, names));
        } else if (value instanceof String text) {
            var matcher = PLACEHOLDER.matcher(text);
            if (matcher.matches()) {
                names.add(matcher.group(1));
            }
        }
    }

    private static Document substitute(Document filter, JsonNode supplied, List<String> missing) {
        Document result = new Document();
        for (Map.Entry<String, Object> field : filter.entrySet()) {
            result.put(field.getKey(), substituteValue(field.getValue(), supplied, missing));
        }
        return result;
    }

    private static Object substituteValue(Object value, JsonNode supplied, List<String> missing) {
        if (value instanceof Document document) {
            return substitute(document, supplied, missing);
        }
        if (value instanceof List<?> list) {
            List<Object> substituted = new ArrayList<>(list.size());
            list.forEach(nested -> substituted.add(substituteValue(nested, supplied, missing)));
            return substituted;
        }
        if (!(value instanceof String text)) {
            return value;
        }

        var matcher = PLACEHOLDER.matcher(text);
        if (!matcher.matches()) {
            return value;
        }

        String name = matcher.group(1);
        JsonNode bound = supplied == null ? null : supplied.get(name);

        // An empty list counts as missing, and deliberately: "$in: []" matches nothing and
        // completes as a successful run that moved zero records, which is the exact failure the
        // refusal below exists to prevent.
        if (bound == null || bound.isNull()
                || (bound.isTextual() && bound.asText().isBlank())
                || (bound.isArray() && bound.isEmpty())) {
            missing.add(name);
            return value;
        }
        return typed(bound);
    }

    /**
     * Converts a supplied value to what MongoDB should compare against.
     *
     * <p>Inferred from the value's shape, as everywhere else, so a window script that returns an
     * ISO timestamp produces a {@code Date} and one that returns a number produces a number —
     * without anybody declaring a type on the filter.
     */
    private static Object typed(JsonNode value) {
        // A list, for $in. The only parameter shape that is plural, and the one that makes
        // "find these ten policies" expressible as a filter rather than as a feature.
        //
        // Each element is typed on its own rather than the list being taken as strings: a list of
        // numeric ids compared against a numeric field matches nothing when its members arrive as
        // text, and does so without an error.
        if (value.isArray()) {
            List<Object> values = new ArrayList<>(value.size());
            value.forEach(element -> values.add(typed(element)));
            return values;
        }
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        if (value.isIntegralNumber()) {
            return value.asLong();
        }
        if (value.isNumber()) {
            return value.asDouble();
        }

        String text = value.asText().strip();
        if (INTEGER.matcher(text).matches()) {
            return Long.parseLong(text);
        }
        if (DECIMAL.matcher(text).matches()) {
            return Double.parseDouble(text);
        }
        try {
            // A Date, not the string that spells one. Compared against a real date field, the
            // string matches nothing and does it silently.
            return Date.from(Instant.parse(text));
        } catch (DateTimeParseException notAnInstant) {
            try {
                return Date.from(java.time.OffsetDateTime.parse(text).toInstant());
            } catch (DateTimeParseException stillNotAnInstant) {
                return text;
            }
        }
    }
}
