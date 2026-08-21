package com.dmp.persistence.postgres.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/** JPA mapping for {@code tenant}. */
@Entity
@Table(name = "tenant")
public class TenantEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "slug", nullable = false, updatable = false, length = 64)
    private String slug;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected TenantEntity() {
    }

    public TenantEntity(UUID id, String slug, String name, String status,
                        Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.slug = slug;
        this.name = name;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public UUID getId() {
        return id;
    }

    public String getSlug() {
        return slug;
    }

    public String getName() {
        return name;
    }

    public String getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void apply(String newName, String newStatus, Instant newUpdatedAt) {
        this.name = newName;
        this.status = newStatus;
        this.updatedAt = newUpdatedAt;
    }
}
