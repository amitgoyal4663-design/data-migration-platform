package com.dmp.application.port.out;

import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.schedule.Schedule;
import com.dmp.domain.schedule.ScheduleId;
import com.dmp.domain.tenant.TenantId;

import java.util.List;
import java.util.Optional;

/** Persistence port for schedules. Implemented by the PostgreSQL adapter (ADR-0005). */
public interface ScheduleRepository {

    Schedule create(Schedule schedule);

    Schedule update(Schedule schedule);

    Optional<Schedule> findById(TenantId tenantId, ScheduleId id);

    List<Schedule> findByTenant(TenantId tenantId);

    List<Schedule> findByPipeline(TenantId tenantId, PipelineId pipelineId);

    /**
     * Every enabled schedule across every tenant.
     *
     * <p>Not tenant-scoped on purpose: the control plane loads all of them into Quartz at startup,
     * and it has no request to take a tenant from.
     */
    List<Schedule> findAllEnabled();

    boolean existsByName(TenantId tenantId, String name);

    void delete(TenantId tenantId, ScheduleId id);
}
