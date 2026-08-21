package com.dmp.application.common;

import com.dmp.domain.tenant.TenantId;

/**
 * Supplies the tenant the current operation acts on behalf of.
 *
 * <p>An in-port implemented by the web layer (from a request header today, from an SSO token once
 * authentication lands) and by the worker (from the run being executed).
 *
 * <p>Every repository method takes an explicit {@link TenantId} rather than reading this
 * internally. That is deliberate: a repository that silently scopes itself from ambient thread
 * state works correctly in a request thread and returns another tenant's data from a Kafka
 * consumer thread, where no context was ever set. Making the tenant a parameter turns that class
 * of leak into a compile error.
 */
public interface TenantContext {

    /**
     * The current tenant.
     *
     * @throws com.dmp.common.error.DmpException if no tenant is resolvable, which is always a
     *         defect rather than a user error — an unscoped operation must never fall back to a
     *         default tenant or to no filtering at all
     */
    TenantId currentTenant();

    /** The acting principal, for the audit trail. A system identity such as {@code system:scheduler} is valid. */
    String currentActor();

    /**
     * Runs work on behalf of a tenant, on a thread that has no ambient one.
     *
     * <p>Needed by everything that acts without a request behind it — a schedule firing, a sweep
     * reclaiming resources. Those threads have no tenant, and {@link #currentTenant()} correctly
     * refuses rather than guessing, so the tenant has to be supplied for the duration of the work.
     *
     * <p>Implementations must restore whatever was in scope before, in a {@code finally}. Thread
     * pools reuse threads, and a tenant left behind becomes the default for whoever gets that
     * thread next — a cross-tenant leak with nothing to indicate it happened.
     */
    <T> T runAs(TenantId tenantId, String actor, java.util.function.Supplier<T> work);
}
