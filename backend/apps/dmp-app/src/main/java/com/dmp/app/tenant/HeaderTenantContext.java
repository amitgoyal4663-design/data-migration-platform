package com.dmp.app.tenant;

import com.dmp.application.common.TenantContext;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.tenant.TenantId;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * Resolves the current tenant from {@link TenantContextHolder}.
 *
 * <p>Today the holder is populated from a request header. Once company SSO lands, the filter reads
 * the tenant from a validated token instead and nothing else in the platform changes — that
 * substitutability is the reason this indirection exists rather than services reading the header
 * directly.
 */
@Component
public class HeaderTenantContext implements TenantContext {

    @Override
    public TenantId currentTenant() {
        TenantContextHolder.Scope scope = TenantContextHolder.get();
        if (scope == null || scope.tenantId() == null) {
            // Never fall back to a default tenant or to unfiltered access. An operation that
            // reached this point without a tenant is a defect in the caller, and guessing would
            // turn it into a cross-tenant data leak that nothing reports.
            throw new DmpException(ErrorCode.INTERNAL,
                    "No tenant in scope. Every operation must be tenant-scoped.");
        }
        return scope.tenantId();
    }

    @Override
    public String currentActor() {
        TenantContextHolder.Scope scope = TenantContextHolder.get();
        return scope == null || scope.actor() == null ? "system:unknown" : scope.actor();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Restores the previous scope rather than clearing, so nesting is safe and a scheduler
     * thread borrowed from a pool cannot leave a tenant behind for whoever runs on it next.
     */
    @Override
    public <T> T runAs(TenantId tenantId, String actor, Supplier<T> work) {
        TenantContextHolder.Scope previous = TenantContextHolder.get();
        TenantContextHolder.set(tenantId, actor);
        try {
            return work.get();
        } finally {
            if (previous == null) {
                TenantContextHolder.clear();
            } else {
                TenantContextHolder.set(previous.tenantId(), previous.actor());
            }
        }
    }
}
