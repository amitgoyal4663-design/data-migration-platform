package com.dmp.persistence.postgres.adapter;

import com.dmp.application.port.out.ScheduleRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.schedule.Schedule;
import com.dmp.domain.schedule.ScheduleId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.ScheduleEntity;
import com.dmp.persistence.postgres.mapper.ScheduleMapper;
import com.dmp.persistence.postgres.repository.ScheduleJpaRepository;
import com.dmp.persistence.postgres.support.PersistenceSupport;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** PostgreSQL adapter for {@link ScheduleRepository}. */
@Repository
public class ScheduleRepositoryAdapter implements ScheduleRepository {

    private final ScheduleJpaRepository jpa;

    public ScheduleRepositoryAdapter(ScheduleJpaRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public Schedule create(Schedule schedule) {
        return PersistenceSupport.translatingExceptions("Schedule '" + schedule.name() + "'",
                () -> ScheduleMapper.toDomain(jpa.save(ScheduleMapper.toEntity(schedule))));
    }

    @Override
    public Schedule update(Schedule schedule) {
        String description = "Schedule '" + schedule.name() + "'";
        return PersistenceSupport.translatingExceptions(description, () -> {
            ScheduleEntity entity = jpa
                    .findByIdAndTenantId(schedule.id().value(), schedule.tenantId().value())
                    .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                            "Schedule not found", Map.of("scheduleId", schedule.id().toString())));

            // Same guard as every other aggregate: without it a load-then-apply always reads the
            // current version and the optimistic check can never fail, silently overwriting a
            // concurrent edit.
            PersistenceSupport.requireCurrentVersion(
                    schedule.rowVersion(), entity.getRowVersion(), description);

            ScheduleMapper.applyTo(entity, schedule);
            return ScheduleMapper.toDomain(jpa.save(entity));
        });
    }

    @Override
    public Optional<Schedule> findById(TenantId tenantId, ScheduleId id) {
        return jpa.findByIdAndTenantId(id.value(), tenantId.value()).map(ScheduleMapper::toDomain);
    }

    @Override
    public List<Schedule> findByTenant(TenantId tenantId) {
        return jpa.findByTenantIdOrderByNameAsc(tenantId.value()).stream()
                .map(ScheduleMapper::toDomain).toList();
    }

    @Override
    public List<Schedule> findByPipeline(TenantId tenantId, PipelineId pipelineId) {
        return jpa.findByPipelineIdAndTenantId(pipelineId.value(), tenantId.value()).stream()
                .map(ScheduleMapper::toDomain).toList();
    }

    @Override
    public List<Schedule> findAllEnabled() {
        return jpa.findByEnabledTrue().stream().map(ScheduleMapper::toDomain).toList();
    }

    @Override
    public boolean existsByName(TenantId tenantId, String name) {
        return jpa.existsByTenantIdAndName(tenantId.value(), name);
    }

    @Override
    public void delete(TenantId tenantId, ScheduleId id) {
        jpa.findByIdAndTenantId(id.value(), tenantId.value()).ifPresent(jpa::delete);
    }
}
