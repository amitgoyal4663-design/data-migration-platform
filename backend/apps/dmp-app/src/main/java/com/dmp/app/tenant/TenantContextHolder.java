package com.dmp.app.tenant;

import com.dmp.domain.tenant.TenantId;

/**
 * Thread-scoped holder for the current tenant and actor.
 *
 * <p>Populated by {@link TenantFilter} on a request thread, and from Phase 3 by the worker on a
 * consumer thread from the run being executed. The abstraction is shared so both roles resolve
 * tenancy the same way.
 *
 * <p>Uses an inheritable value deliberately not: a child thread inheriting the parent's tenant is
 * how a request-scoped tenant leaks into a background task that outlives the request and then
 * writes to the wrong tenant. Any thread doing tenant-scoped work must be given the tenant
 * explicitly.
 */
public final class TenantContextHolder {

    private static final ThreadLocal<Scope> CURRENT = new ThreadLocal<>();

    private TenantContextHolder() {
    }

    public static void set(TenantId tenantId, String actor) {
        CURRENT.set(new Scope(tenantId, actor));
    }

    public static Scope get() {
        return CURRENT.get();
    }

    /**
     * Clears the holder.
     *
     * <p>Must run in a {@code finally} block. Thread pools reuse threads, so a value left behind
     * becomes the default tenant for whoever gets that thread next — a cross-tenant data leak with
     * no error to indicate it happened.
     */
    public static void clear() {
        CURRENT.remove();
    }

    public record Scope(TenantId tenantId, String actor) {
    }
}
