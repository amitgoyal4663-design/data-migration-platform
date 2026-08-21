package com.dmp.domain.pipeline;

import java.util.Objects;

/**
 * A single finding from pipeline validation.
 *
 * <p>{@code code} is a stable machine-readable identifier; {@code message} is for humans and may
 * be reworded freely. The designer canvas highlights {@code nodeId} or {@code edgeId} so a user
 * sees the problem on the element that caused it rather than in a list they have to correlate
 * by hand.
 */
public record ValidationIssue(
        Severity severity,
        String code,
        String message,
        String nodeId,
        String edgeId) {

    public ValidationIssue {
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(message, "message");
    }

    public static ValidationIssue error(String code, String message) {
        return new ValidationIssue(Severity.ERROR, code, message, null, null);
    }

    public static ValidationIssue errorAtNode(String code, String message, String nodeId) {
        return new ValidationIssue(Severity.ERROR, code, message, nodeId, null);
    }

    public static ValidationIssue errorAtEdge(String code, String message, String edgeId) {
        return new ValidationIssue(Severity.ERROR, code, message, null, edgeId);
    }

    public static ValidationIssue warningAtNode(String code, String message, String nodeId) {
        return new ValidationIssue(Severity.WARNING, code, message, nodeId, null);
    }

    public boolean isError() {
        return severity == Severity.ERROR;
    }

    /**
     * Severity of a validation finding.
     *
     * <p>Only {@link #ERROR} blocks publication. Warnings are surfaced in the designer but never
     * prevent a user from saving work in progress — a validator that refuses to let someone
     * park a half-built pipeline is a validator people route around.
     */
    public enum Severity {
        ERROR,
        WARNING
    }
}
