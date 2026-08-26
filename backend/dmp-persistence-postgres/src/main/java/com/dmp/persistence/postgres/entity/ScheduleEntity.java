package com.dmp.persistence.postgres.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA mapping for {@code schedule}.
 *
 * <p>Separate from the {@link com.dmp.domain.schedule.Schedule} aggregate for the same reason as
 * every other entity here: annotating the domain record would put Hibernate on the domain's
 * classpath and break the rule that keeps it framework-free.
 */
@Entity
@Table(name = "schedule")
public class ScheduleEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "tenant_id", nullable = false, updatable = false)
    private UUID tenantId;

    @Column(name = "pipeline_id", nullable = false, updatable = false)
    private UUID pipelineId;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    @Column(name = "cron_expression", nullable = false, length = 255)
    private String cronExpression;

    /** An IANA zone id, stored as text. Never null — see the migration for why. */
    @Column(name = "timezone", nullable = false, length = 64)
    private String timezone;

    /** JavaScript computing the range each firing covers. Null means the whole query. */
    @Column(name = "window_script", columnDefinition = "text")
    private String windowScript;

    @Column(name = "query_name", columnDefinition = "text")
    private String queryName;

    @Column(name = "enabled", nullable = false)
    private boolean enabled;

    @Column(name = "description")
    private String description;

    @Column(name = "last_fired_at")
    private Instant lastFiredAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(name = "row_version", nullable = false)
    private long rowVersion;

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public UUID getTenantId() { return tenantId; }
    public void setTenantId(UUID tenantId) { this.tenantId = tenantId; }

    public UUID getPipelineId() { return pipelineId; }
    public void setPipelineId(UUID pipelineId) { this.pipelineId = pipelineId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getCronExpression() { return cronExpression; }
    public void setCronExpression(String cronExpression) { this.cronExpression = cronExpression; }

    public String getWindowScript() { return windowScript; }
    public void setWindowScript(String windowScript) { this.windowScript = windowScript; }

    public String getQueryName() { return queryName; }
    public void setQueryName(String queryName) { this.queryName = queryName; }

    public String getTimezone() { return timezone; }
    public void setTimezone(String timezone) { this.timezone = timezone; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public Instant getLastFiredAt() { return lastFiredAt; }
    public void setLastFiredAt(Instant lastFiredAt) { this.lastFiredAt = lastFiredAt; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    public long getRowVersion() { return rowVersion; }
    public void setRowVersion(long rowVersion) { this.rowVersion = rowVersion; }
}
