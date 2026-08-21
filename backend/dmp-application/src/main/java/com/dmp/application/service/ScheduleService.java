package com.dmp.application.service;

import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.AuditLogPort;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.port.out.ScheduleRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.audit.AuditAction;
import com.dmp.domain.audit.AuditEntry;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.schedule.Schedule;
import com.dmp.domain.schedule.ScheduleId;
import com.dmp.domain.tenant.TenantId;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Creating, editing and removing recurring schedules.
 *
 * <p>Knows nothing about Quartz. The scheduler is notified through a callback the engine registers,
 * so this module stays free of a scheduling library and a deployment without one still manages
 * schedule records correctly — the same separation every other outbound concern here follows.
 */
@Service
public class ScheduleService {

    private final ScheduleRepository schedules;
    private final PipelineRepository pipelines;
    private final AuditLogPort auditLog;
    private final TenantContext tenantContext;
    private final Clock clock;

    /** Set by the engine at startup. Absent in a deployment with no scheduler. */
    private volatile Consumer<Schedule> onRegistered = schedule -> { };
    private volatile Consumer<ScheduleId> onRemoved = id -> { };

    public ScheduleService(ScheduleRepository schedules,
                           PipelineRepository pipelines,
                           AuditLogPort auditLog,
                           TenantContext tenantContext,
                           Clock clock) {
        this.schedules = schedules;
        this.pipelines = pipelines;
        this.auditLog = auditLog;
        this.tenantContext = tenantContext;
        this.clock = clock;
    }

    public void onChange(Consumer<Schedule> registered, Consumer<ScheduleId> removed) {
        this.onRegistered = registered;
        this.onRemoved = removed;
    }

    @Transactional
    public Schedule create(PipelineId pipelineId, String name, String cronExpression,
                           String windowScript,
                           String timezone, String description) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();

        requireRunnablePipeline(tenantId, pipelineId);

        if (schedules.existsByName(tenantId, name)) {
            throw new DmpException(ErrorCode.DUPLICATE,
                    "A schedule named '" + name + "' already exists", Map.of("name", name));
        }

        Schedule schedule = schedules.create(Schedule.create(
                tenantId, pipelineId, name, cronExpression, zone(timezone), windowScript, description, now));

        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(),
                AuditAction.SCHEDULE_CHANGE, "schedule", schedule.id().toString(),
                "Created schedule '" + name + "' (" + cronExpression + ")", null, null, now));

        onRegistered.accept(schedule);
        return schedule;
    }

    @Transactional
    public Schedule update(ScheduleId id, String name, String cronExpression, String timezone,
                           String windowScript,
                           String description) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();

        Schedule existing = require(tenantId, id);
        Schedule updated = schedules.update(existing
                .renamed(name, description, now)
                .withRule(cronExpression, zone(timezone), windowScript, now));

        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(),
                AuditAction.SCHEDULE_CHANGE, "schedule", id.toString(),
                "Updated schedule '" + name + "' (" + cronExpression + ")", null, null, now));

        // Re-registered rather than patched: a changed cron or timezone means a different trigger,
        // and replacing it is the only way to be sure the old one is gone.
        if (updated.enabled()) {
            onRegistered.accept(updated);
        } else {
            onRemoved.accept(id);
        }
        return updated;
    }

    /**
     * Turns a schedule on or off.
     *
     * <p>Separate from deleting because pausing during an incident is common, and deleting would
     * lose the rule, its history, and whatever the description explained about why it exists.
     */
    @Transactional
    public Schedule setEnabled(ScheduleId id, boolean enabled) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();

        Schedule updated = schedules.update(require(tenantId, id).enabled(enabled, now));

        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(),
                AuditAction.SCHEDULE_CHANGE,
                "schedule", id.toString(),
                (enabled ? "Enabled" : "Disabled") + " schedule '" + updated.name() + "'",
                null, null, now));

        if (enabled) {
            onRegistered.accept(updated);
        } else {
            onRemoved.accept(id);
        }
        return updated;
    }

    @Transactional
    public void delete(ScheduleId id) {
        TenantId tenantId = tenantContext.currentTenant();
        Schedule existing = require(tenantId, id);

        onRemoved.accept(id);
        schedules.delete(tenantId, id);

        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(),
                AuditAction.SCHEDULE_CHANGE, "schedule", id.toString(),
                "Deleted schedule '" + existing.name() + "'", null, null, clock.instant()));
    }

    @Transactional(readOnly = true)
    public List<Schedule> list() {
        return schedules.findByTenant(tenantContext.currentTenant());
    }

    @Transactional(readOnly = true)
    public List<Schedule> listForPipeline(PipelineId pipelineId) {
        return schedules.findByPipeline(tenantContext.currentTenant(), pipelineId);
    }

    @Transactional(readOnly = true)
    public Schedule get(ScheduleId id) {
        return require(tenantContext.currentTenant(), id);
    }

    private Schedule require(TenantId tenantId, ScheduleId id) {
        return schedules.findById(tenantId, id)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Schedule not found", Map.of("scheduleId", id.toString())));
    }

    /**
     * Refuses a schedule for a pipeline that could not run.
     *
     * <p>Caught here rather than at 03:00 in a log nobody is reading. A schedule on an unpublished
     * pipeline is always a mistake, and it is a silent one until the night it should have loaded.
     */
    private void requireRunnablePipeline(TenantId tenantId, PipelineId pipelineId) {
        Pipeline pipeline = pipelines.findById(tenantId, pipelineId)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Pipeline not found", Map.of("pipelineId", pipelineId.toString())));

        if (!pipeline.isRunnable()) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "Pipeline '" + pipeline.name() + "' has no published version, so a schedule "
                            + "for it would fail every time it fired. Publish a version first.",
                    Map.of("pipelineId", pipelineId.toString()));
        }
    }

    private static ZoneId zone(String timezone) {
        if (timezone == null || timezone.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A schedule needs a timezone. '3am' is not a moment until a zone is named — "
                            + "the same rule fires at different times in different zones.");
        }
        try {
            return ZoneId.of(timezone);
        } catch (DateTimeException e) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "'" + timezone + "' is not a known timezone. Use an IANA name such as "
                            + "'Europe/London' or 'Asia/Kolkata'.",
                    Map.of("timezone", timezone));
        }
    }
}
