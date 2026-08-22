package com.dmp.persistence.postgres.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notifier")
public class NotifierEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "tenant_id", nullable = false, updatable = false)
    private UUID tenantId;

    @Column(name = "pipeline_id")
    private UUID pipelineId;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    @Column(name = "url", nullable = false, columnDefinition = "text")
    private String url;

    @Column(name = "events", nullable = false, length = 512)
    private String events;

    @Column(name = "secret_header", length = 128)
    private String secretHeader;

    @Column(name = "secret_ref", length = 255)
    private String secretRef;

    @Column(name = "enabled", nullable = false)
    private boolean enabled;

    @Column(name = "description", columnDefinition = "text")
    private String description;

    @Column(name = "last_attempt_at")
    private Instant lastAttemptAt;

    @Column(name = "last_attempt_succeeded", nullable = false)
    private boolean lastAttemptSucceeded;

    @Column(name = "last_attempt_error", columnDefinition = "text")
    private String lastAttemptError;

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
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public String getEvents() { return events; }
    public void setEvents(String events) { this.events = events; }
    public String getSecretHeader() { return secretHeader; }
    public void setSecretHeader(String secretHeader) { this.secretHeader = secretHeader; }
    public String getSecretRef() { return secretRef; }
    public void setSecretRef(String secretRef) { this.secretRef = secretRef; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public Instant getLastAttemptAt() { return lastAttemptAt; }
    public void setLastAttemptAt(Instant lastAttemptAt) { this.lastAttemptAt = lastAttemptAt; }
    public boolean isLastAttemptSucceeded() { return lastAttemptSucceeded; }
    public void setLastAttemptSucceeded(boolean v) { this.lastAttemptSucceeded = v; }
    public String getLastAttemptError() { return lastAttemptError; }
    public void setLastAttemptError(String e) { this.lastAttemptError = e; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public long getRowVersion() { return rowVersion; }
    public void setRowVersion(long rowVersion) { this.rowVersion = rowVersion; }
}
