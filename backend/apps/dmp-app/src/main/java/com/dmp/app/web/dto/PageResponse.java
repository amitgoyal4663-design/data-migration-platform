package com.dmp.app.web.dto;

import com.dmp.application.common.Page;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.function.Function;

/**
 * Paged API envelope.
 *
 * @param totalElements exact count, or -1 when the store cannot supply one cheaply
 * @param totalPages    derived, or -1 when the total is unknown
 */
@Schema(name = "PageResponse")
public record PageResponse<T>(
        List<T> content,
        int page,
        int size,
        @Schema(description = "Exact count, or -1 when counting would require an expensive scan")
        long totalElements,
        @Schema(description = "Derived from totalElements, or -1 when that is unknown")
        long totalPages,
        boolean hasNext) {

    public static <D, R> PageResponse<R> from(Page<D> page, Function<D, R> mapper) {
        long totalPages = page.totalPages();
        boolean hasNext = page.hasTotal()
                ? (long) (page.page() + 1) * page.size() < page.totalElements()
                // With no total, a full page is the only evidence more may exist. This is why the
                // console must page by following hasNext rather than by computing page counts.
                : page.content().size() == page.size();

        return new PageResponse<>(
                page.content().stream().map(mapper).toList(),
                page.page(),
                page.size(),
                page.totalElements(),
                totalPages,
                hasNext);
    }
}
