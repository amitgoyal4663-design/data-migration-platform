package com.dmp.connector.api;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds the {@code :name} placeholders in a piece of SQL.
 *
 * <p>Shared rather than written once per connector, because the two rules that make it correct are
 * both easy to get wrong and neither announces itself when you do. A cast written {@code x::STRING}
 * is not a parameter called {@code STRING}; a colon inside a quoted string is not a parameter at
 * all. A connector that missed either would demand a value nobody can supply, and fail every run
 * of a query that was always valid.
 *
 * <p>Only the <em>finding</em> is shared. How a value is then bound is the database's business:
 * Databricks takes a typed JSON list, JDBC takes {@code ?} markers and a
 * {@link java.sql.PreparedStatement}. Both are safe; neither is string substitution.
 */
public final class QueryParameters {

    /**
     * A {@code :name} placeholder.
     *
     * <p>The lookbehind rejects a second colon, so Databricks' cast operator is left alone, and
     * rejects a word character so an email or a time in unquoted text cannot start one.
     */
    private static final Pattern PLACEHOLDER =
            Pattern.compile("(?<![:\\w]):([A-Za-z_][A-Za-z0-9_]*)");

    private QueryParameters() {
    }

    /** The placeholder names the SQL uses, in the order they first appear. */
    public static LinkedHashSet<String> referencedBy(String sql) {
        LinkedHashSet<String> names = new LinkedHashSet<>();
        if (sql == null || sql.isBlank()) {
            return names;
        }
        Matcher matcher = PLACEHOLDER.matcher(withoutStringLiterals(sql));
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        return names;
    }

    /**
     * Rewrites {@code :name} placeholders to JDBC's {@code ?} markers.
     *
     * <p>The returned names are in the order their markers appear, which is the order a
     * {@code PreparedStatement} expects them to be bound in. A name used twice yields two markers
     * and appears twice, because each marker must be bound separately.
     */
    public static Positional toPositional(String sql) {
        if (sql == null || sql.isBlank()) {
            return new Positional(sql, List.of());
        }

        String scanned = withoutStringLiterals(sql);
        Matcher matcher = PLACEHOLDER.matcher(scanned);

        StringBuilder rewritten = new StringBuilder();
        List<String> order = new ArrayList<>();
        int copied = 0;

        while (matcher.find()) {
            // Copied from the original rather than from the scanned copy, so the quoted text that
            // was blanked out for scanning survives into the query that actually runs.
            rewritten.append(sql, copied, matcher.start()).append('?');
            order.add(matcher.group(1));
            copied = matcher.end();
        }
        rewritten.append(sql, copied, sql.length());

        return new Positional(rewritten.toString(), List.copyOf(order));
    }

    /** SQL with {@code ?} markers, and the parameter names to bind to them in order. */
    public record Positional(String sql, List<String> names) {
    }

    /** A list-aware pattern: {@code IN (:name)}, the one place a placeholder stands for many values. */
    private static final Pattern LIST =
            Pattern.compile("(?i)\\bIN\\s*\\(\\s*:([A-Za-z_][A-Za-z0-9_]*)\\s*\\)");

    /**
     * Rewrites each {@code IN (:name)} to one marker per value supplied, and flattens the values
     * to match.
     *
     * <p>SQL has no way to bind a list to a single marker. {@code IN (?)} bound to three policy
     * numbers is not three policy numbers — it is one value that happens to contain commas, and it
     * matches nothing. That is the worst shape a failure can take here: the query is valid, the
     * driver is happy, the run completes, and it reports success having read no rows.
     *
     * <p>So {@code action IN (:actions)} with two values becomes {@code action IN (:actions_0,
     * :actions_1)}, and the returned parameters carry {@code actions_0} and {@code actions_1}
     * separately. Each is still bound, never pasted in, so a value containing a quote stays a value.
     *
     * @param supplied the run's parameters, as an object of name to value
     * @return the rewritten SQL and the parameters to bind against it
     */
    public static Expanded expand(String sql, com.fasterxml.jackson.databind.JsonNode supplied) {
        com.fasterxml.jackson.databind.node.ObjectNode parameters =
                com.dmp.common.json.Json.newObject();
        if (supplied != null && supplied.isObject()) {
            supplied.fields().forEachRemaining(field -> parameters.set(field.getKey(), field.getValue()));
        }
        if (sql == null || sql.isBlank()) {
            return new Expanded(sql, parameters);
        }

        Matcher matcher = LIST.matcher(withoutStringLiterals(sql));
        StringBuilder rewritten = new StringBuilder();
        int copied = 0;

        while (matcher.find()) {
            String name = matcher.group(1);
            com.fasterxml.jackson.databind.JsonNode value = parameters.get(name);
            if (value == null || !value.isArray()) {
                continue;
            }
            if (value.isEmpty()) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "This query selects records where ':" + name + "' matches one of a list, "
                                + "and the run supplied an empty list. That would match nothing and "
                                + "complete as a run that moved no records, so it is refused here "
                                + "instead. Supply at least one value.");
            }

            List<String> markers = new ArrayList<>();
            for (int element = 0; element < value.size(); element++) {
                String expandedName = name + "_" + element;
                markers.add(":" + expandedName);
                parameters.set(expandedName, value.get(element));
            }
            parameters.remove(name);

            rewritten.append(sql, copied, matcher.start())
                    .append("IN (").append(String.join(", ", markers)).append(')');
            copied = matcher.end();
        }
        rewritten.append(sql, copied, sql.length());

        return new Expanded(rewritten.toString(), parameters);
    }

    /** SQL with one marker per supplied value, and the parameters that match it. */
    public record Expanded(String sql, com.fasterxml.jackson.databind.node.ObjectNode parameters) {
    }


    /**
     * Blanks out quoted text so a colon inside it is not read as a placeholder.
     *
     * <p>{@code WHERE note = 'call me :from tomorrow'} asks the database to bind nothing, and
     * failing a run over that colon would be a bug nobody could work around except by rewriting
     * data.
     */
    private static String withoutStringLiterals(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        char quote = 0;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (quote == 0 && (c == '\'' || c == '"' || c == '`')) {
                quote = c;
                out.append(' ');
            } else if (quote != 0 && c == quote) {
                quote = 0;
                out.append(' ');
            } else {
                out.append(quote == 0 ? c : ' ');
            }
        }
        return out.toString();
    }

    /**
     * Placeholders that sit inside an {@code IN (…)} list, and therefore take several values.
     *
     * <p>SQL cannot bind a list, so one of these is expanded into a marker per value before the
     * statement is submitted. Knowing which they are is also what lets a run dialog offer a list
     * rather than a single box.
     */
    public static java.util.LinkedHashSet<String> listsIn(String sql) {
        java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();
        if (sql == null || sql.isBlank()) {
            return names;
        }
        java.util.regex.Matcher matcher = java.util.regex.Pattern
                .compile("(?i)\\bIN\\s*\\(\\s*:([A-Za-z_][A-Za-z0-9_]*)\\s*\\)")
                .matcher(sql);
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        return names;
    }
}
