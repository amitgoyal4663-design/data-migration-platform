package com.dmp.application.common;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.util.Map;

/**
 * Paging and sort request.
 *
 * @param page      zero-based
 * @param size      capped at {@link #MAX_SIZE}
 * @param sortBy    adapter-validated property name
 * @param ascending sort direction
 */
public record PageQuery(int page, int size, String sortBy, boolean ascending) {

    public static final int DEFAULT_SIZE = 25;

    /**
     * Hard ceiling on page size.
     *
     * <p>Enforced rather than merely defaulted: an unbounded {@code size} parameter is how a UI
     * bug or a careless script turns a run-history endpoint into a denial of service against the
     * platform's own database.
     */
    public static final int MAX_SIZE = 200;

    public PageQuery {
        if (page < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "Page must not be negative",
                    Map.of("page", page));
        }
        if (size < 1) {
            size = DEFAULT_SIZE;
        }
        if (size > MAX_SIZE) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Page size must not exceed " + MAX_SIZE,
                    Map.of("size", size, "max", MAX_SIZE));
        }
        if (sortBy != null && sortBy.isBlank()) {
            sortBy = null;
        }
    }

    public static PageQuery firstPage() {
        return new PageQuery(0, DEFAULT_SIZE, null, false);
    }

    public static PageQuery of(int page, int size) {
        return new PageQuery(page, size, null, false);
    }

    public int offset() {
        return page * size;
    }
}
