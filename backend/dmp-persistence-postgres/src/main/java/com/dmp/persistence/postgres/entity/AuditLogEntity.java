package com.dmp.persistence.postgres.entity;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA mapping for {@code audit_log}.
 *
 * <p>{@link Immutable} stops Hibernate from ever issuing an UPDATE for this entity, which turns a
 * mistake into a no-op at the ORM rather than an exception from the database trigger. Both layers
 * enforce it: the annotation documents the intent to the next developer, the trigger enforces it
 * against everything that bypasses Hibernate.
 *
 * <p>There are no setters and no {@code apply} method, unlike every other entity here. That is the
 * point.
 */
@Entity
@Immutable
@Table(name = "audit_log")
public class AuditLogEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "tenant_id", nullable = false, updatable = false)
    private UUID tenantId;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    @Column(name = "actor", nullable = false, updatable = false, length = 255)
    private String actor;

    @Column(name = "action", nullable = false, updatable = false, length = 64)
    private String action;

    @Column(name = "resource_type", nullable = false, updatable = false, length = 64)
    private String resourceType;

    @Column(name = "resource_id", updatable = false, length = 128)
    private String resourceId;

    @Column(name = "summary", updatable = false, length = 1000)
    private String summary;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "before_state", updatable = false)
    private JsonNode beforeState;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "after_state", updatable = false)
    private JsonNode afterState;

    @Column(name = "request_id", updatable = false, length = 128)
    private String requestId;

    @Column(name = "source_ip", updatable = false, length = 64)
    private String sourceIp;

    protected AuditLogEntity() {
    }

    public AuditLogEntity(UUID id, UUID tenantId, Instant occurredAt, String actor, String action,
                          String resourceType, String resourceId, String summary,
                          JsonNode beforeState, JsonNode afterState, String requestId, String sourceIp) {
        this.id = id;
        this.tenantId = tenantId;
        this.occurredAt = occurredAt;
        this.actor = actor;
        this.action = action;
        this.resourceType = resourceType;
        this.resourceId = resourceId;
        this.summary = summary;
        this.beforeState = beforeState;
        this.afterState = afterState;
        this.requestId = requestId;
        this.sourceIp = sourceIp;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public String getActor() {
        return actor;
    }

    public String getAction() {
        return action;
    }

    public String getResourceType() {
        return resourceType;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getSummary() {
        return summary;
    }

    public JsonNode getBeforeState() {
        return beforeState;
    }

    public JsonNode getAfterState() {
        return afterState;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getSourceIp() {
        return sourceIp;
    }
}
