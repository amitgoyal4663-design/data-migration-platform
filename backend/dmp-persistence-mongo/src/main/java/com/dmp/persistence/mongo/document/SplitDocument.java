package com.dmp.persistence.mongo.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * MongoDB document for a split.
 *
 * <p>{@code spec} is connector-defined and opaque: a primary-key range for JDBC, a chunk index for
 * a Databricks statement, a topic-partition for Kafka, a file path for object storage. Only the
 * connector that produced it interprets it, which is what allows a new connector to introduce a
 * new splitting strategy without a schema change here.
 *
 * <p>One warning encoded by that opacity: a split spec must hold a stable reference, never a
 * resolved one. A pre-signed URL or an expiring locator stored here works in a test with three
 * splits and fails in production when split four hundred is claimed forty minutes later.
 */
@Document(collection = "split")
public class SplitDocument {

    @Id
    private UUID id;

    @Field("runId")
    private UUID runId;

    @Field("tenantId")
    private UUID tenantId;

    @Field("index")
    private int index;

    @Field("state")
    private String state;

    @Field("spec")
    private Map<String, Object> spec;

    /** Worker holding this split. Retained after failure so orphans are attributable. */
    @Field("assignedTo")
    private String assignedTo;

    /**
     * When this worker's claim lapses.
     *
     * <p>A worker retains its split by extending this; one that stops — because it died, was
     * partitioned, or is wedged — loses the split to the reclaim sweep. This is what makes worker
     * failure recoverable without pods having to know about each other.
     */
    @Field("leaseExpiresAt")
    private Instant leaseExpiresAt;

    @Field("attempt")
    private int attempt;

    @Field("errorCode")
    private String errorCode;

    @Field("errorMessage")
    private String errorMessage;

    @Field("createdAt")
    private Instant createdAt;

    @Field("startedAt")
    private Instant startedAt;

    @Field("endedAt")
    private Instant endedAt;

    /** Also the liveness signal: the stale-split sweep queries on this. */
    @Field("updatedAt")
    private Instant updatedAt;

    /**
     * A handle on work already submitted to an external system, or absent.
     *
     * <p>The one field on this document that outlives the worker on purpose. A Salesforce bulk job
     * takes minutes to decide, and until this was written down the job id existed only in the
     * memory of the pod that created it — so a restart mid-job left the org processing records
     * nobody was watching, its rejections unfetched and counted as successes.
     *
     * <p>Connector-defined and opaque, exactly like {@code spec}, and subject to the same warning:
     * it must be a stable reference. An expiring URL stored here works in a test and fails when a
     * chunk is resumed forty minutes later.
     */
    @Field("externalJob")
    private Map<String, Object> externalJob;

    /**
     * When a parked chunk's external job should next be asked whether it has finished.
     *
     * <p>Indexed by the fallback sweep, which looks for parked chunks whose poll is overdue —
     * a Quartz trigger that never fired because the node holding it died before it did.
     */
    @Field("dueAt")
    private Instant dueAt;

    public SplitDocument() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getRunId() {
        return runId;
    }

    public void setRunId(UUID runId) {
        this.runId = runId;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public void setTenantId(UUID tenantId) {
        this.tenantId = tenantId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Map<String, Object> getSpec() {
        return spec;
    }

    public void setSpec(Map<String, Object> spec) {
        this.spec = spec;
    }

    public String getAssignedTo() {
        return assignedTo;
    }

    public void setAssignedTo(String assignedTo) {
        this.assignedTo = assignedTo;
    }

    public Instant getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public void setLeaseExpiresAt(Instant leaseExpiresAt) {
        this.leaseExpiresAt = leaseExpiresAt;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
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

    public Map<String, Object> getExternalJob() {
        return externalJob;
    }

    public void setExternalJob(Map<String, Object> externalJob) {
        this.externalJob = externalJob;
    }

    public Instant getDueAt() {
        return dueAt;
    }

    public void setDueAt(Instant dueAt) {
        this.dueAt = dueAt;
    }
}
