package com.dmp.domain.audit;

import java.util.regex.Pattern;

/**
 * Collapses many rejection messages into the one distinct fault behind them.
 *
 * <p>Twenty thousand records failing the same rule produce twenty thousand messages that differ
 * only in the identifier each one names. Stored verbatim they are twenty thousand documents saying
 * one thing; grouped by what they have in common they are a single line with a count beside it —
 * which is both smaller and the thing an operator actually wants to read.
 *
 * <p>The normalisation is deliberately aggressive. Over-grouping shows one row where two faults
 * existed and the sample payloads reveal the difference; under-grouping restores the wall of
 * identical text this exists to prevent. Of the two failures the first is far cheaper.
 */
public final class ErrorSignature {

    /** Quoted literals: the offending value is exactly what differs between two identical faults. */
    private static final Pattern QUOTED = Pattern.compile("'[^']*'|\"[^\"]*\"");

    /** UUIDs and Mongo/Salesforce-style object ids, before the bare-number rule shortens them. */
    private static final Pattern IDENTIFIER = Pattern.compile(
            "\\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\b"
                    + "|\\b[0-9a-fA-F]{24}\\b|\\b[a-zA-Z0-9]{15,18}\\b");

    private static final Pattern NUMBER = Pattern.compile("\\d+");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    private static final int MAX_LENGTH = 300;

    private ErrorSignature() {
    }

    /**
     * A stable key for the fault that produced this message.
     *
     * <p>The error code carries most of the meaning and is left untouched — a target's own code is
     * already a classification, and rewriting it would discard the best signal available. Only the
     * message is normalised, and only of the parts that vary per record.
     */
    public static String of(String code, String message) {
        String normalisedCode = code == null || code.isBlank() ? "NO_CODE" : code.strip();
        return normalisedCode + "|" + normalise(message);
    }

    /** The message with everything record-specific replaced, suitable to show as the group label. */
    public static String normalise(String message) {
        if (message == null || message.isBlank()) {
            return "";
        }
        String result = message.strip();
        result = QUOTED.matcher(result).replaceAll("?");
        result = IDENTIFIER.matcher(result).replaceAll("?");
        result = NUMBER.matcher(result).replaceAll("?");
        result = WHITESPACE.matcher(result).replaceAll(" ");

        // Long messages are truncated rather than hashed. A signature nobody can read is a signature
        // nobody can act on, and the leading text is where the fault is named.
        return result.length() > MAX_LENGTH ? result.substring(0, MAX_LENGTH) : result;
    }
}
