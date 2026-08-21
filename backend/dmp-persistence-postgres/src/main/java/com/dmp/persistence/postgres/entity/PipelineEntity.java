package com.dmp.persistence.postgres.entity;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA mapping for {@code pipeline}.
 *
 * <p>Separate from the {@link com.dmp.domain.pipeline.Pipeline} aggregate on purpose. Annotating
 * the domain record with JPA would put Hibernate on the domain's classpath, break the ArchUnit
 * rule that keeps it framework-free, and force the aggregate's shape to follow what the ORM can
 * map rather than what the business requires. The cost is a mapper; the benefit is that the domain
 * stays the thing being modelled.
 *
 * <p>Mutable with a no-arg constructor because Hibernate requires it. Nothing outside this package
 * sees one of these.
 */
@Entity
@Table(name = "pipeline")
public class PipelineEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "tenant_id", nullable = false, updatable = false)
    private UUID tenantId;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "folder", length = 512)
    private String folder;

    /** JSONB array. Stored as a document so the GIN containment index can serve tag filters. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "tags", nullable = false)
    private JsonNode tags;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "published_version")
    private Integer publishedVersion;

    @Column(name = "latest_version", nullable = false)
    private int latestVersion;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /**
     * Optimistic lock.
     *
     * <p>Two users editing the same pipeline in two browser tabs is routine, and last-write-wins
     * silently discards one of them. This makes the second write fail loudly instead.
     */
    @Version
    @Column(name = "row_version", nullable = false)
    private long rowVersion;

    protected PipelineEntity() {
    }

    public PipelineEntity(UUID id, UUID tenantId, String name, String description, String folder,
                          JsonNode tags, String status, Integer publishedVersion, int latestVersion,
                          Instant createdAt, Instant updatedAt, long rowVersion) {
        this.id = id;
        this.tenantId = tenantId;
        this.name = name;
        this.description = description;
        this.folder = folder;
        this.tags = tags;
        this.status = status;
        this.publishedVersion = publishedVersion;
        this.latestVersion = latestVersion;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.rowVersion = rowVersion;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getFolder() {
        return folder;
    }

    public JsonNode getTags() {
        return tags;
    }

    public String getStatus() {
        return status;
    }

    public Integer getPublishedVersion() {
        return publishedVersion;
    }

    public int getLatestVersion() {
        return latestVersion;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public long getRowVersion() {
        return rowVersion;
    }

    /** Copies mutable state onto a managed instance, leaving Hibernate to detect the change. */
    public void apply(String newName, String newDescription, String newFolder, JsonNode newTags,
                      String newStatus, Integer newPublishedVersion, int newLatestVersion,
                      Instant newUpdatedAt) {
        this.name = newName;
        this.description = newDescription;
        this.folder = newFolder;
        this.tags = newTags;
        this.status = newStatus;
        this.publishedVersion = newPublishedVersion;
        this.latestVersion = newLatestVersion;
        this.updatedAt = newUpdatedAt;
    }
}
