package com.dmp.persistence.postgres.repository;

import com.dmp.persistence.postgres.entity.AuditLogEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.UUID;

/**
 * Spring Data repository for {@code audit_log}.
 *
 * <p>Insert and select only. {@code JpaRepository} technically exposes {@code delete} methods; the
 * adapter does not surface them, {@code @Immutable} on the entity blocks updates at Hibernate, and
 * a database trigger blocks both regardless of what any Java code attempts.
 */
public interface AuditLogJpaRepository extends JpaRepository<AuditLogEntity, UUID> {

    @Query("""
            SELECT a FROM AuditLogEntity a
            WHERE a.tenantId = :tenantId
              AND (:resourceType IS NULL OR a.resourceType = :resourceType)
              AND (:resourceId IS NULL OR a.resourceId = :resourceId)
              AND (:actor IS NULL OR a.actor = :actor)
              AND (:noActionFilter = TRUE OR a.action IN :actions)
              AND (CAST(:occurredAfter AS timestamp) IS NULL OR a.occurredAt >= :occurredAfter)
              AND (CAST(:occurredBefore AS timestamp) IS NULL OR a.occurredAt <= :occurredBefore)
            ORDER BY a.occurredAt DESC
            """)
    Page<AuditLogEntity> search(@Param("tenantId") UUID tenantId,
                                @Param("resourceType") String resourceType,
                                @Param("resourceId") String resourceId,
                                @Param("actor") String actor,
                                @Param("noActionFilter") boolean noActionFilter,
                                @Param("actions") Collection<String> actions,
                                @Param("occurredAfter") Instant occurredAfter,
                                @Param("occurredBefore") Instant occurredBefore,
                                Pageable pageable);
}
