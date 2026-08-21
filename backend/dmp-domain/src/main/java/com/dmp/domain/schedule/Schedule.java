package com.dmp.domain.schedule;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.tenant.TenantId;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A recurring rule that starts runs of a pipeline.
 *
 * <p>Holds the rule and nothing about execution. A schedule decides <em>when</em>; the engine
 * decides how and for how long. Keeping the two apart is what stops a six-hour migration from
 * occupying a scheduler thread — see ADR-0010, where that separation is the central constraint.
 *
 * <p>The timezone is stored with the expression and is not optional. "Every day at 03:00" is not a
 * fact until a zone is named: the same rule fires at different moments in London and Mumbai, and
 * twice or never on the day the clocks change. Defaulting it to the server's zone would make a
 * schedule's behaviour depend on which host happened to run it.
 */
public record Schedule(
        ScheduleId id,
        TenantId tenantId,
        PipelineId pipelineId,
        String name,
        String cronExpression,
        ZoneId timezone,
        /**
         * JavaScript deciding which range of data each firing covers, or null for all of it.
         *
         * <p>Separate from the cron on purpose. The cron says <em>when</em> a run starts; this says
         * <em>what</em> it reads, and the two are genuinely independent — a job that fires at 10am
         * to process the previous midnight-to-midnight day has nothing in common between the two
         * numbers. Deriving the window from the cron looks tempting until a weekday-only schedule
         * has a 72-hour gap after Friday and the inferred window is silently wrong on Mondays.
         *
         * <p>Null means the run reads whatever its query says with no parameters, which is every
         * schedule that existed before this.
         */
        String windowScript,
        boolean enabled,
        String description,
        Instant lastFiredAt,
        Instant createdAt,
        Instant updatedAt,
        long rowVersion) {

    private static final int MAX_NAME_LENGTH = 255;

    public Schedule {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(timezone, "timezone");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");

        if (name == null || name.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A schedule needs a name, so it can be recognised in a list of them");
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Schedule name must be at most " + MAX_NAME_LENGTH + " characters",
                    Map.of("length", name.length()));
        }
        if (cronExpression == null || cronExpression.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A schedule needs a cron expression");
        }
        cronExpression = cronExpression.trim();
    }

    public static Schedule create(TenantId tenantId, PipelineId pipelineId, String name,
                                  String cronExpression, ZoneId timezone, String description,
                                  Instant now) {
        return create(tenantId, pipelineId, name, cronExpression, timezone, null, description, now);
    }

    public static Schedule create(TenantId tenantId, PipelineId pipelineId, String name,
                                  String cronExpression, ZoneId timezone, String windowScript,
                                  String description, Instant now) {
        return new Schedule(ScheduleId.newId(), tenantId, pipelineId, name, cronExpression,
                timezone, windowScript, true, description, null, now, now, 0L);
    }

    public Schedule withRule(String newCron, ZoneId newTimezone, Instant now) {
        return withRule(newCron, newTimezone, windowScript, now);
    }

    public Schedule withRule(String newCron, ZoneId newTimezone, String newWindowScript, Instant now) {
        return new Schedule(id, tenantId, pipelineId, name, newCron, newTimezone, newWindowScript,
                enabled, description, lastFiredAt, createdAt, now, rowVersion);
    }

    public Schedule renamed(String newName, String newDescription, Instant now) {
        return new Schedule(id, tenantId, pipelineId, newName, cronExpression, timezone,
                windowScript, enabled, newDescription, lastFiredAt, createdAt, now, rowVersion);
    }

    /**
     * Turns the schedule on or off without deleting it.
     *
     * <p>Deleting and recreating loses the rule, its history and whatever was written in the
     * description explaining why it exists. Pausing during an incident is common enough that it
     * should not cost that.
     */
    public Schedule enabled(boolean nowEnabled, Instant now) {
        return new Schedule(id, tenantId, pipelineId, name, cronExpression, timezone, windowScript,
                nowEnabled, description, lastFiredAt, createdAt, now, rowVersion);
    }

    public Schedule fired(Instant now) {
        return new Schedule(id, tenantId, pipelineId, name, cronExpression, timezone, windowScript,
                enabled, description, now, createdAt, now, rowVersion);
    }

    public Optional<Instant> lastFired() {
        return Optional.ofNullable(lastFiredAt);
    }

    /**
     * The key that makes a fire idempotent.
     *
     * <p>Derived from the schedule and the instant it was scheduled to fire, not the instant it
     * actually did. Two control-plane replicas racing on the same trigger — which clustering should
     * prevent and which this survives if it does not — produce the same key, so the second run is
     * rejected by the unique index rather than duplicating a migration.
     */
    public String idempotencyKeyFor(Instant scheduledFireTime) {
        return "schedule:" + id + ":" + scheduledFireTime.toEpochMilli();
    }
}
