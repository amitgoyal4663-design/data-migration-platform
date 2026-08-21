package com.dmp.persistence.mongo.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * MongoDB document for a run.
 *
 * <p>Execution data lives here rather than in PostgreSQL (ADR-0005) for two reasons that reinforce
 * each other. A run is naturally a single nested document — metrics, preparation handles, error
 * detail — so no object-relational mapping is needed. And it is one of the highest-churn writes in
 * the platform, where PostgreSQL's MVCC would leave a dead tuple behind every progress update.
 *
 * <p>Being a single document is also what makes a state change an atomic compare-and-swap:
 * {@code findAndModify} with the expected state in the query. That is a stronger concurrency
 * primitive than read-modify-write against a version column, not a weaker one.
 */
@Document(collection = "run")
public class RunDocument {

    @Id
    private UUID id;

    @Field("tenantId")
    private UUID tenantId;

    @Field("pipelineId")
    private UUID pipelineId;

    @Field("pipelineVersionId")
    private UUID pipelineVersionId;

    @Field("versionNumber")
    private int versionNumber;

    @Field("mode")
    private String mode;

    @Field("trigger")
    private String trigger;

    /** The run this one re-attempts. Null for a run that stands alone. */
    @Field("retryOf")
    private UUID retryOf;

    @Field("state")
    private String state;

    /**
     * Guards against a delay-queue redelivery starting the same migration twice.
     *
     * <p>Backed by a partial unique index — partial so that the many runs with no key do not all
     * collide on null. ADR-0002 accepts at-least-once firing, which makes this constraint
     * load-bearing rather than defensive.
     */
    @Field("idempotencyKey")
    private String idempotencyKey;

    @Field("metrics")
    private Map<String, Object> metrics;

    /**
     * Chunks in flight across the whole fleet, for concurrency limiting.
     *
     * <p>Incremented by an atomic conditional update that also enforces the limit, so the check
     * and the reservation cannot be separated by another pod's write.
     */
    @Field("activeSlots")
    private int activeSlots;

    /**
     * Connector handles for in-flight external jobs, keyed by node id (ADR-0012).
     *
     * <p>Persisted rather than held in worker memory: the worker that submitted a Salesforce bulk
     * job is frequently not the one that observes it finishing, or releases it.
     */
    @Field("preparationState")
    private Map<String, Object> preparationState;

    /** Values bound into the source's query — the window or range this run covers. */
    @Field("parameters")
    private Map<String, Object> parameters;

    @Field("errorCode")
    private String errorCode;

    @Field("errorMessage")
    private String errorMessage;

    @Field("triggeredBy")
    private String triggeredBy;

    @Field("createdAt")
    private Instant createdAt;

    @Field("startedAt")
    private Instant startedAt;

    @Field("endedAt")
    private Instant endedAt;

    @Field("updatedAt")
    private Instant updatedAt;

    @Version
    @Field("rowVersion")
    private Long rowVersion;

    public RunDocument() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public void setTenantId(UUID tenantId) {
        this.tenantId = tenantId;
    }

    public UUID getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(UUID pipelineId) {
        this.pipelineId = pipelineId;
    }

    public UUID getPipelineVersionId() {
        return pipelineVersionId;
    }

    public void setPipelineVersionId(UUID pipelineVersionId) {
        this.pipelineVersionId = pipelineVersionId;
    }

    public int getVersionNumber() {
        return versionNumber;
    }

    public void setVersionNumber(int versionNumber) {
        this.versionNumber = versionNumber;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getTrigger() {
        return trigger;
    }

    public void setTrigger(String trigger) {
        this.trigger = trigger;
    }

    public UUID getRetryOf() {
        return retryOf;
    }

    public void setRetryOf(UUID retryOf) {
        this.retryOf = retryOf;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    public int getActiveSlots() {
        return activeSlots;
    }

    public void setActiveSlots(int activeSlots) {
        this.activeSlots = activeSlots;
    }

    public Map<String, Object> getPreparationState() {
        return preparationState;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public void setPreparationState(Map<String, Object> preparationState) {
        this.preparationState = preparationState;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getTriggeredBy() {
        return triggeredBy;
    }

    public void setTriggeredBy(String triggeredBy) {
        this.triggeredBy = triggeredBy;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getEndedAt() {
        return endedAt;
    }

    public void setEndedAt(Instant endedAt) {
        this.endedAt = endedAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Long getRowVersion() {
        return rowVersion;
    }

    public void setRowVersion(Long rowVersion) {
        this.rowVersion = rowVersion;
    }
}
