package com.dmp.persistence.postgres.repository;

import com.dmp.persistence.postgres.entity.ScheduleEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ScheduleJpaRepository extends JpaRepository<ScheduleEntity, UUID> {

    Optional<ScheduleEntity> findByIdAndTenantId(UUID id, UUID tenantId);

    List<ScheduleEntity> findByTenantIdOrderByNameAsc(UUID tenantId);

    List<ScheduleEntity> findByPipelineIdAndTenantId(UUID pipelineId, UUID tenantId);

    /**
     * Every live rule across every tenant.
     *
     * <p>Deliberately not tenant-scoped: the control plane registers all of them with Quartz at
     * startup and has no request context to take a tenant from.
     */
    List<ScheduleEntity> findByEnabledTrue();

    boolean existsByTenantIdAndName(UUID tenantId, String name);
}
