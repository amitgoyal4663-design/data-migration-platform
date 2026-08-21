package com.dmp.persistence.mongo.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.UUID;

/**
 * One distinct fault within a run, with a count of how many records hit it.
 *
 * <p>Serves two purposes that turn out to be the same document. It bounds how many payloads the
 * dead-letter queue stores for a fault, by holding the running total atomically; and it is the
 * grouped view the console reads, because a list of twenty thousand identical rows is not something
 * anybody scrolls through.
 *
 * <p>The count here is exact and unsampled. However few payloads survive the cap, the run still
 * reports precisely how many records were rejected and for what — the sampling costs evidence,
 * never arithmetic.
 *
 * <p>{@code _id} is derived from the tenant, run and signature rather than random, so the atomic
 * upsert that increments the count needs no prior read and two pods incrementing the same fault
 * converge on one document.
 */
@Document(collection = "record_error_signature")
public class RecordErrorSignatureDocument {

    @Id
    private String id;

    @Field("tenantId")
    private UUID tenantId;

    @Field("runId")
    private UUID runId;

    /** Which pipeline node rejected the records, so a multi-stage failure stays attributable. */
    @Field("nodeId")
    private String nodeId;

    /** The normalised key these records were grouped on. */
    @Field("signature")
    private String signature;

    /** The target's own error code, verbatim and unnormalised. */
    @Field("code")
    private String code;

    /** A representative message, with the per-record identifiers replaced. */
    @Field("message")
    private String message;

    /** Every record that hit this fault, whether or not its payload was kept. */
    @Field("count")
    private long count;

    /** Payloads actually written to the dead-letter queue for this fault. */
    @Field("samplesStored")
    private long samplesStored;

    @Field("firstSeenAt")
    private Instant firstSeenAt;

    @Field("lastSeenAt")
    private Instant lastSeenAt;

    /** TTL boundary, taken from the pipeline's audit retention like the payloads themselves. */
    @Field("expiresAt")
    private Instant expiresAt;

    public static String idFor(UUID tenantId, UUID runId, String signature) {
        return tenantId + ":" + runId + ":" + Integer.toHexString(signature.hashCode());
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public UUID getTenantId() {
        return tenantId;
    }

    public void setTenantId(UUID tenantId) {
        this.tenantId = tenantId;
    }

    public UUID getRunId() {
        return runId;
    }

    public void setRunId(UUID runId) {
        this.runId = runId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSamplesStored() {
        return samplesStored;
    }

    public void setSamplesStored(long samplesStored) {
        this.samplesStored = samplesStored;
    }

    public Instant getFirstSeenAt() {
        return firstSeenAt;
    }

    public void setFirstSeenAt(Instant firstSeenAt) {
        this.firstSeenAt = firstSeenAt;
    }

    public Instant getLastSeenAt() {
        return lastSeenAt;
    }

    public void setLastSeenAt(Instant lastSeenAt) {
        this.lastSeenAt = lastSeenAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }
}
