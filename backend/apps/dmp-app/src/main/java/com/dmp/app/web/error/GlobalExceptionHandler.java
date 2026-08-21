package com.dmp.app.web.error;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Translates exceptions into RFC 7807 problem details.
 *
 * <p>The mapping from {@link ErrorCode} to HTTP status lives here and only here. That is what
 * allows the domain to have an opinion about what went wrong without knowing that HTTP exists —
 * and it means a new error code is a compile-time prompt to decide its status rather than a silent
 * 500.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final String PROBLEM_BASE = "https://docs.dmp.io/errors/";

    @ExceptionHandler(DmpException.class)
    public ProblemDetail handleDmpException(DmpException e, HttpServletRequest request) {
        HttpStatus status = statusFor(e.errorCode());

        // A 5xx is a defect in the platform, so it gets a stack trace. A 4xx is the caller being
        // told something true about their request, and logging a trace for each one turns a busy
        // validation endpoint into log noise that buries the real failures.
        if (status.is5xxServerError()) {
            log.error("{} handling {} {}", e.errorCode(), request.getMethod(), request.getRequestURI(), e);
        } else {
            log.debug("{} handling {} {}: {}", e.errorCode(), request.getMethod(),
                    request.getRequestURI(), e.getMessage());
        }

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, e.getMessage());
        problem.setType(URI.create(PROBLEM_BASE + e.errorCode().name().toLowerCase().replace('_', '-')));
        problem.setTitle(titleFor(e.errorCode()));
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", e.errorCode().name());
        problem.setProperty("retryable", e.isRetryable());
        problem.setProperty("timestamp", Instant.now().toString());

        if (!e.details().isEmpty()) {
            problem.setProperty("details", e.details());
        }
        return problem;
    }

    /** Bean-validation failures on request bodies. Every field error travels, not just the first. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException e, HttpServletRequest request) {
        Map<String, Object> fieldErrors = new LinkedHashMap<>();
        e.getBindingResult().getFieldErrors().forEach(error ->
                fieldErrors.put(error.getField(), error.getDefaultMessage()));

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "Request body failed validation");
        problem.setType(URI.create(PROBLEM_BASE + "validation-failed"));
        problem.setTitle("Validation failed");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", ErrorCode.VALIDATION_FAILED.name());
        problem.setProperty("retryable", false);
        problem.setProperty("fieldErrors", fieldErrors);
        problem.setProperty("timestamp", Instant.now().toString());
        return problem;
    }

    /** A malformed path variable, typically an identifier that is not a UUID. */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail handleTypeMismatch(MethodArgumentTypeMismatchException e,
                                            HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "Parameter '" + e.getName() + "' has an invalid value");
        problem.setType(URI.create(PROBLEM_BASE + "validation-failed"));
        problem.setTitle("Validation failed");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", ErrorCode.VALIDATION_FAILED.name());
        problem.setProperty("retryable", false);
        problem.setProperty("parameter", e.getName());
        problem.setProperty("timestamp", Instant.now().toString());
        return problem;
    }

    /**
     * A required query parameter the caller did not send.
     *
     * <p>Without this it fell through to the catch-all and answered 500 — which tells the caller
     * the server is broken when in fact the request was incomplete, and hides the one thing that
     * would let them fix it: the name of the parameter they missed.
     */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ProblemDetail handleMissingParameter(MissingServletRequestParameterException e,
                                                HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "Required parameter '" + e.getParameterName() + "' is missing");
        problem.setType(URI.create(PROBLEM_BASE + "validation-failed"));
        problem.setTitle("Validation failed");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", ErrorCode.VALIDATION_FAILED.name());
        problem.setProperty("retryable", false);
        problem.setProperty("parameter", e.getParameterName());
        problem.setProperty("timestamp", Instant.now().toString());
        return problem;
    }

    /**
     * Anything unhandled.
     *
     * <p>The message is deliberately generic. An unexpected exception's text frequently contains a
     * SQL fragment, a file path or a connection string, and returning it to the caller is how
     * internals leak. The detail goes to the log, where it belongs, correlated by request id.
     */
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception e, HttpServletRequest request) {
        log.error("Unhandled exception handling {} {}", request.getMethod(), request.getRequestURI(), e);

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred. The request id in the response headers identifies "
                        + "this failure in the platform logs.");
        problem.setType(URI.create(PROBLEM_BASE + "internal"));
        problem.setTitle("Internal error");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", ErrorCode.INTERNAL.name());
        problem.setProperty("retryable", false);
        problem.setProperty("timestamp", Instant.now().toString());
        return problem;
    }

    private static HttpStatus statusFor(ErrorCode code) {
        return switch (code) {
            case VALIDATION_FAILED, INVALID_REFERENCE -> HttpStatus.BAD_REQUEST;
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case DUPLICATE, CONCURRENT_MODIFICATION, ILLEGAL_STATE_TRANSITION, IMMUTABLE ->
                    HttpStatus.CONFLICT;
            case RATE_LIMITED -> HttpStatus.TOO_MANY_REQUESTS;
            case UPSTREAM_UNAVAILABLE -> HttpStatus.BAD_GATEWAY;
            case INTERNAL -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }

    private static String titleFor(ErrorCode code) {
        return switch (code) {
            case VALIDATION_FAILED -> "Validation failed";
            case NOT_FOUND -> "Not found";
            case DUPLICATE -> "Already exists";
            case CONCURRENT_MODIFICATION -> "Modified by someone else";
            case ILLEGAL_STATE_TRANSITION -> "Not allowed in the current state";
            case IMMUTABLE -> "Immutable";
            case INVALID_REFERENCE -> "Invalid reference";
            case UPSTREAM_UNAVAILABLE -> "Upstream unavailable";
            case RATE_LIMITED -> "Rate limited";
            case INTERNAL -> "Internal error";
        };
    }
}
