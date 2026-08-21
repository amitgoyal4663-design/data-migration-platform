package com.dmp.application.common;

import java.util.List;
import java.util.function.Function;

/**
 * A page of results.
 *
 * <p>Deliberately not Spring Data's {@code Page}. This type crosses the application boundary in
 * both directions, and binding it to a persistence framework would leak that framework into the
 * web layer and into every future adapter — including a MongoDB adapter whose paging model is not
 * Spring Data JPA's.
 *
 * @param totalElements exact count where the adapter can supply one cheaply, otherwise -1
 */
public record Page<T>(List<T> content, int page, int size, long totalElements) {

    /** Returned when an adapter cannot count without an expensive scan. */
    public static final long UNKNOWN_TOTAL = -1L;

    public Page {
        content = List.copyOf(content == null ? List.of() : content);
    }

    public static <T> Page<T> of(List<T> content, PageQuery query, long totalElements) {
        return new Page<>(content, query.page(), query.size(), totalElements);
    }

    public static <T> Page<T> empty(PageQuery query) {
        return new Page<>(List.of(), query.page(), query.size(), 0);
    }

    public <R> Page<R> map(Function<T, R> mapper) {
        return new Page<>(content.stream().map(mapper).toList(), page, size, totalElements);
    }

    public boolean hasTotal() {
        return totalElements != UNKNOWN_TOTAL;
    }

    /**
     * Total page count, or -1 when the total is unknown.
     *
     * <p>Clients must handle -1 rather than assuming a total is always available. Over a run
     * history of hundreds of millions of documents, an exact count is a scan nobody wants to pay
     * for on every page request.
     */
    public long totalPages() {
        if (!hasTotal() || size <= 0) {
            return UNKNOWN_TOTAL;
        }
        return (totalElements + size - 1) / size;
    }

    public boolean isEmpty() {
        return content.isEmpty();
    }
}
