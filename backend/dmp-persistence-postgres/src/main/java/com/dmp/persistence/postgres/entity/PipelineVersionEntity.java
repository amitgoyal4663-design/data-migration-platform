package com.dmp.persistence.postgres.entity;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA mapping for {@code pipeline_version}.
 *
 * <p>No {@code @Version} column. Once published a row is immutable and a database trigger blocks
 * any UPDATE, so optimistic locking would guard a write that cannot happen. Drafts are edited by
 * one author at a time in practice, and the version number's unique constraint already prevents
 * the concurrent-create race that matters.
 */
@Entity
@Table(name = "pipeline_version")
public class PipelineVersionEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "pipeline_id", nullable = false, updatable = false)
    private UUID pipelineId;

    @Column(name = "tenant_id", nullable = false, updatable = false)
    private UUID tenantId;

    @Column(name = "version_number", nullable = false, updatable = false)
    private int versionNumber;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    /** The DAG. Opaque here; interpreted by the domain and validated by PipelineValidator. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "definition", nullable = false)
    private JsonNode definition;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "chunking_policy", nullable = false)
    private JsonNode chunkingPolicy;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "execution_policy", nullable = false)
    private JsonNode executionPolicy;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "audit_policy", nullable = false)
    private JsonNode auditPolicy;

    /**
     * Nullable, unlike its siblings. Every row written before delivery existed has no value here,
     * and back-filling them would claim an author chose something they never saw. The domain
     * record substitutes the default on read, which is exactly the behaviour those rows had.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "delivery_policy")
    private JsonNode deliveryPolicy;

    @Column(name = "mode", nullable = false, length = 32)
    private String mode;

    @Column(name = "change_note")
    private String changeNote;

    @Column(name = "created_by", length = 255)
    private String createdBy;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected PipelineVersionEntity() {
    }

    public PipelineVersionEntity(UUID id, UUID pipelineId, UUID tenantId, int versionNumber,
                                 String status, JsonNode definition, JsonNode chunkingPolicy,
                                 JsonNode executionPolicy, JsonNode auditPolicy,
                                 JsonNode deliveryPolicy, String mode, String changeNote,
                                 String createdBy, Instant createdAt, Instant publishedAt) {
        this.id = id;
        this.pipelineId = pipelineId;
        this.tenantId = tenantId;
        this.versionNumber = versionNumber;
        this.status = status;
        this.definition = definition;
        this.chunkingPolicy = chunkingPolicy;
        this.executionPolicy = executionPolicy;
        this.auditPolicy = auditPolicy;
        this.deliveryPolicy = deliveryPolicy;
        this.mode = mode;
        this.changeNote = changeNote;
        this.createdBy = createdBy;
        this.createdAt = createdAt;
        this.publishedAt = publishedAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getPipelineId() {
        return pipelineId;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public int getVersionNumber() {
        return versionNumber;
    }

    public String getStatus() {
        return status;
    }

    public JsonNode getDefinition() {
        return definition;
    }

    public JsonNode getChunkingPolicy() {
        return chunkingPolicy;
    }

    public JsonNode getExecutionPolicy() {
        return executionPolicy;
    }

    public JsonNode getAuditPolicy() {
        return auditPolicy;
    }

    public String getMode() {
        return mode;
    }

    public String getChangeNote() {
        return changeNote;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public JsonNode getDeliveryPolicy() {
        return deliveryPolicy;
    }

    public void apply(String newStatus, JsonNode newDefinition, JsonNode newChunkingPolicy,
                      JsonNode newExecutionPolicy, JsonNode newAuditPolicy,
                      JsonNode newDeliveryPolicy, String newMode,
                      String newChangeNote, Instant newPublishedAt) {
        this.status = newStatus;
        this.definition = newDefinition;
        this.chunkingPolicy = newChunkingPolicy;
        this.executionPolicy = newExecutionPolicy;
        this.auditPolicy = newAuditPolicy;
        this.deliveryPolicy = newDeliveryPolicy;
        this.mode = newMode;
        this.changeNote = newChangeNote;
        this.publishedAt = newPublishedAt;
    }
}
