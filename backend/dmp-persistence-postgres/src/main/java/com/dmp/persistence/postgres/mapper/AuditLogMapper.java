package com.dmp.persistence.postgres.mapper;

import com.dmp.domain.audit.AuditAction;
import com.dmp.domain.audit.AuditEntry;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.AuditLogEntity;

/**
 * Translates between {@link AuditEntry} and its JPA entity.
 *
 * <p>There is no {@code applyTo}. Audit entries are append-only, so an update path would be a
 * method nobody should ever be able to call.
 */
public final class AuditLogMapper {

    private AuditLogMapper() {
    }

    public static AuditEntry toDomain(AuditLogEntity entity) {
        return new AuditEntry(
                entity.getId(),
                TenantId.of(entity.getTenantId()),
                entity.getOccurredAt(),
                entity.getActor(),
                AuditAction.valueOf(entity.getAction()),
                entity.getResourceType(),
                entity.getResourceId(),
                entity.getSummary(),
                entity.getBeforeState(),
                entity.getAfterState(),
                entity.getRequestId(),
                entity.getSourceIp());
    }

    public static AuditLogEntity toEntity(AuditEntry domain) {
        return new AuditLogEntity(
                domain.id(),
                domain.tenantId().value(),
                domain.occurredAt(),
                domain.actor(),
                domain.action().name(),
                domain.resourceType(),
                domain.resourceId(),
                domain.summary(),
                domain.before(),
                domain.after(),
                domain.requestId(),
                domain.sourceIp());
    }
}
