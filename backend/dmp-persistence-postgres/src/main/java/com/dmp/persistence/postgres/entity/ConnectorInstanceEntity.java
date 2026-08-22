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

/** JPA mapping for {@code connector_instance}. */
@Entity
@Table(name = "connector_instance")
public class ConnectorInstanceEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "tenant_id", nullable = false, updatable = false)
    private UUID tenantId;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    /** Plugin identifier such as {@code jdbc-postgres}. Immutable — changing it is a new instance. */
    @Column(name = "connector_type", nullable = false, updatable = false, length = 128)
    private String connectorType;

    @Column(name = "direction", nullable = false, updatable = false, length = 16)
    private String direction;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "config", nullable = false)
    private JsonNode config;

    /** References to secrets, never values. Safe to log and to return over the API. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "secret_refs", nullable = false)
    private JsonNode secretRefs;

    /**
     * The rate the far end agreed to, or null for no agreement.
     *
     * <p>Not secret and not a credential: it is a number somebody in a meeting said out loud. It
     * belongs beside the configuration rather than in the secret store.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "rate_limit")
    private JsonNode rateLimit;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "description")
    private String description;

    @Column(name = "last_tested_at")
    private Instant lastTestedAt;

    @Column(name = "last_test_error")
    private String lastTestError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(name = "row_version", nullable = false)
    private long rowVersion;

    protected ConnectorInstanceEntity() {
    }

    public ConnectorInstanceEntity(UUID id, UUID tenantId, String name, String connectorType,
                                   String direction, JsonNode config, JsonNode secretRefs,
                                   String status, String description, Instant lastTestedAt,
                                   String lastTestError, Instant createdAt, Instant updatedAt,
                                   long rowVersion, JsonNode rateLimit) {
        this.id = id;
        this.tenantId = tenantId;
        this.name = name;
        this.connectorType = connectorType;
        this.direction = direction;
        this.rateLimit = rateLimit;
        this.config = config;
        this.secretRefs = secretRefs;
        this.status = status;
        this.description = description;
        this.lastTestedAt = lastTestedAt;
        this.lastTestError = lastTestError;
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

    public String getConnectorType() {
        return connectorType;
    }

    public String getDirection() {
        return direction;
    }

    public JsonNode getConfig() {
        return config;
    }

    public JsonNode getSecretRefs() {
        return secretRefs;
    }

    public String getStatus() {
        return status;
    }

    public String getDescription() {
        return description;
    }

    public Instant getLastTestedAt() {
        return lastTestedAt;
    }

    public String getLastTestError() {
        return lastTestError;
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

    public JsonNode getRateLimit() {
        return rateLimit;
    }

    public void apply(String newName, JsonNode newConfig, JsonNode newSecretRefs, String newStatus,
                      String newDescription, Instant newLastTestedAt, String newLastTestError,
                      Instant newUpdatedAt, JsonNode newRateLimit) {
        this.name = newName;
        this.config = newConfig;
        this.secretRefs = newSecretRefs;
        this.status = newStatus;
        this.description = newDescription;
        this.lastTestedAt = newLastTestedAt;
        this.lastTestError = newLastTestError;
        this.updatedAt = newUpdatedAt;
        this.rateLimit = newRateLimit;
    }
}
