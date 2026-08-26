package com.dmp.persistence.postgres.mapper;

import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.schedule.Schedule;
import com.dmp.domain.schedule.ScheduleId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.ScheduleEntity;

import java.time.ZoneId;

/** Translates between the {@link Schedule} aggregate and its row. */
public final class ScheduleMapper {

    private ScheduleMapper() {
    }

    public static Schedule toDomain(ScheduleEntity entity) {
        return new Schedule(
                ScheduleId.of(entity.getId()),
                TenantId.of(entity.getTenantId()),
                PipelineId.of(entity.getPipelineId()),
                entity.getName(),
                entity.getCronExpression(),
                ZoneId.of(entity.getTimezone()),
                entity.getWindowScript(),
                entity.getQueryName(),
                entity.isEnabled(),
                entity.getDescription(),
                entity.getLastFiredAt(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getRowVersion());
    }

    public static ScheduleEntity toEntity(Schedule schedule) {
        ScheduleEntity entity = new ScheduleEntity();
        entity.setId(schedule.id().value());
        entity.setTenantId(schedule.tenantId().value());
        entity.setPipelineId(schedule.pipelineId().value());
        entity.setCreatedAt(schedule.createdAt());
        applyTo(entity, schedule);
        return entity;
    }

    /** Copies mutable state onto a managed entity so Hibernate issues a targeted UPDATE. */
    public static void applyTo(ScheduleEntity entity, Schedule schedule) {
        entity.setName(schedule.name());
        entity.setCronExpression(schedule.cronExpression());
        entity.setTimezone(schedule.timezone().getId());
        entity.setWindowScript(schedule.windowScript());
        entity.setQueryName(schedule.queryName());
        entity.setEnabled(schedule.enabled());
        entity.setDescription(schedule.description());
        entity.setLastFiredAt(schedule.lastFiredAt());
        entity.setUpdatedAt(schedule.updatedAt());
    }
}
