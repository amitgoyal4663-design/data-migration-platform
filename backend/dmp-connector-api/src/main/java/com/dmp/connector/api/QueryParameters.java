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
}
