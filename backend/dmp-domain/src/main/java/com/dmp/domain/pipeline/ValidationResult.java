package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.util.List;
import java.util.Map;

/** The outcome of validating a pipeline definition. */
public record ValidationResult(List<ValidationIssue> issues) {

    public ValidationResult {
        issues = List.copyOf(issues == null ? List.of() : issues);
    }

    public static ValidationResult valid() {
        return new ValidationResult(List.of());
    }

    /** Valid means no errors. Warnings are informational and never block. */
    public boolean isValid() {
        return issues.stream().noneMatch(ValidationIssue::isError);
    }

    public List<ValidationIssue> errors() {
        return issues.stream().filter(ValidationIssue::isError).toList();
    }

    public List<ValidationIssue> warnings() {
        return issues.stream().filter(i -> !i.isError()).toList();
    }

    /**
     * Throws if validation failed.
     *
     * <p>All errors travel in the exception details rather than only the first, so a user fixing
     * a pipeline sees every problem at once instead of discovering them one save at a time.
     */
    public ValidationResult orThrow() {
        if (isValid()) {
            return this;
        }
        List<Map<String, Object>> detail = errors().stream()
                .map(ValidationIssue::toString)
                .map(s -> Map.<String, Object>of("issue", s))
                .toList();
        throw new DmpException(ErrorCode.VALIDATION_FAILED,
                "Pipeline definition is not valid: " + errors().size() + " error(s)",
                Map.of("errors", detail));
    }
}
