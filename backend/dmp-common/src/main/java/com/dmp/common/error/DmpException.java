package com.dmp.common.error;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base type for every deliberate failure in the platform.
 *
 * <p>Carries an {@link ErrorCode} and an open map of details. The details map exists so that
 * failures survive the trip to the UI with enough context to be actionable — which entity, which
 * field, which state transition was refused — without the web layer having to downcast to
 * specific exception types to find out.
 */
public class DmpException extends RuntimeException {

    private final ErrorCode errorCode;
    private final transient Map<String, Object> details;

    public DmpException(ErrorCode errorCode, String message) {
        this(errorCode, message, Map.of(), null);
    }

    public DmpException(ErrorCode errorCode, String message, Map<String, Object> details) {
        this(errorCode, message, details, null);
    }

    public DmpException(ErrorCode errorCode, String message, Map<String, Object> details, Throwable cause) {
        super(message, cause);
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
        this.details = Map.copyOf(Objects.requireNonNullElseGet(details, Map::of));
    }

    public ErrorCode errorCode() {
        return errorCode;
    }

    public Map<String, Object> details() {
        return details;
    }

    /** Whether an identical retry could plausibly succeed. */
    public boolean isRetryable() {
        return errorCode.isRetryable();
    }

    /** Returns a copy of this exception with an additional detail entry. */
    public DmpException withDetail(String key, Object value) {
        Map<String, Object> merged = new LinkedHashMap<>(details);
        merged.put(key, value);
        return new DmpException(errorCode, getMessage(), merged, getCause());
    }
}
