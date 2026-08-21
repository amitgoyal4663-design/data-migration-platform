package com.dmp.persistence.postgres.support;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/** Shared plumbing for the PostgreSQL adapters. */
public final class PersistenceSupport {

    private PersistenceSupport() {
    }

    /**
     * Builds a {@link Pageable}, translating a caller-supplied sort key through a whitelist.
     *
     * <p>The whitelist is a correctness and a safety measure. Spring Data appends a native query's
     * sort clause as raw SQL, so an unvalidated property name is an injection vector; and callers
     * name domain fields ({@code createdAt}) while the database has columns ({@code created_at}),
     * so an unmapped key would produce a runtime SQL error rather than a clear rejection.
     *
     * @param allowedSortColumns domain field name to database column name
     * @param defaultColumn      applied when no sort is requested, or the requested one is unknown
     */
    public static Pageable toPageable(PageQuery query, Map<String, String> allowedSortColumns,
                                      String defaultColumn) {
        String requested = query.sortBy();
        String column = requested == null ? defaultColumn : allowedSortColumns.get(requested);

        if (requested != null && column == null) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Cannot sort by '" + requested + "'",
                    Map.of("requested", requested, "allowed", allowedSortColumns.keySet().toString()));
        }

        Sort sort = Sort.by(query.ascending() ? Sort.Direction.ASC : Sort.Direction.DESC, column);
        return PageRequest.of(query.page(), query.size(), sort);
    }

    /** Converts a Spring Data page into the framework-free application type. */
    public static <E, D> Page<D> toPage(org.springframework.data.domain.Page<E> source,
                                        PageQuery query, Function<E, D> mapper) {
        return new Page<>(source.getContent().stream().map(mapper).toList(),
                query.page(), query.size(), source.getTotalElements());
    }

    /**
     * Rejects a write whose base version is no longer current.
     *
     * <p>Necessary because the adapters load the managed entity and copy state onto it, rather than
     * merging a detached instance. That pattern is correct for keeping Hibernate's dirty checking
     * useful, but it silently defeats the {@code @Version} column: the loaded entity always carries
     * the newest version, so Hibernate has nothing to compare against and the second of two
     * concurrent edits overwrites the first without complaint.
     *
     * <p>Comparing the version the caller was working from against the stored one restores the
     * guarantee — two people editing the same pipeline in two browser tabs, and the second save
     * fails loudly instead of discarding the first.
     */
    public static void requireCurrentVersion(long expected, long actual, String entityDescription) {
        if (expected != actual) {
            throw new DmpException(ErrorCode.CONCURRENT_MODIFICATION,
                    entityDescription + " was modified by someone else. Reload and try again.",
                    Map.of("entity", entityDescription, "expectedVersion", expected, "actualVersion", actual));
        }
    }

    /**
     * Runs a write, translating Spring's data-access exceptions into the platform's error taxonomy.
     *
     * <p>Without this the web layer would have to know about {@code OptimisticLockingFailureException}
     * to produce a sensible response, which would leak the persistence framework across two module
     * boundaries. It also means a lost-update collision reaches the user as a retryable
     * CONCURRENT_MODIFICATION rather than an opaque 500.
     */
    public static <T> T translatingExceptions(String entityDescription, Supplier<T> action) {
        try {
            return action.get();
        } catch (OptimisticLockingFailureException e) {
            throw new DmpException(ErrorCode.CONCURRENT_MODIFICATION,
                    entityDescription + " was modified by someone else. Reload and try again.",
                    Map.of("entity", entityDescription), e);
        } catch (DataIntegrityViolationException e) {
            throw new DmpException(ErrorCode.DUPLICATE,
                    entityDescription + " violates a uniqueness or integrity constraint.",
                    Map.of("entity", entityDescription), e);
        }
    }
}
