package com.dmp.application.port.out;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.domain.audit.AuditAction;
import com.dmp.domain.audit.AuditEntry;
import com.dmp.domain.tenant.TenantId;

import java.time.Instant;
import java.util.Set;

/**
 * Append-only control-plane audit trail (ADR-0011). Implemented by the PostgreSQL adapter.
 *
 * <p>There is no update and no delete, here or in the adapter. The database role the application
 * runs as is granted INSERT and SELECT on {@code audit_log} and nothing further, so tampering is
 * blocked at the database even if this interface were widened by mistake.
 */
public interface AuditLogPort {

    /**
     * Appends an entry.
     *
     * <p>Must participate in the caller's transaction. If the audit write fails, the change it
     * describes must fail with it — an unaudited change is not an acceptable degraded mode, and
     * this is the deliberate trade-off recorded in ADR-0011.
     */
    void record(AuditEntry entry);

    Page<AuditEntry> search(TenantId tenantId, AuditSearch criteria, PageQuery pageQuery);

    record AuditSearch(String resourceType, String resourceId, String actor,
                       Set<AuditAction> actions, Instant occurredAfter, Instant occurredBefore) {

        public AuditSearch {
            actions = Set.copyOf(actions == null ? Set.of() : actions);
        }

        public static AuditSearch none() {
            return new AuditSearch(null, null, null, Set.of(), null, null);
        }

        /** The trail for one resource — what the UI's History tab shows. */
        public static AuditSearch forResource(String resourceType, String resourceId) {
            return new AuditSearch(resourceType, resourceId, null, Set.of(), null, null);
        }
    }
}
