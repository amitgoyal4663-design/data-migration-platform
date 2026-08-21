package com.dmp.persistence.mongo.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * One rejected record, kept so a failure is actionable rather than a bare count.
 *
 * <p>Serves as both the dead-letter queue and the {@code ERRORS} audit tier. A rejected record and
 * an audited failure are the same event; storing it twice would only create two accounts that can
 * disagree.
 *
 * <p>{@code expiresAt} drives a TTL index, so retention follows the pipeline's audit policy rather
 * than a global setting — a payments pipeline can keep failures for a year while an analytics one
 * keeps them for a week.
 *
 * <p>The payload is redacted <em>before</em> it reaches this document. Nothing here undoes that,
 * and nothing should try: by the time a record is written, an unredacted value is already on disk.
 */
@Document(collection = "record_error")
public class RecordErrorDocument {

    @Id
    private UUID id;

    @Field("tenantId")
    private UUID tenantId;

    @Field("runId")
    private UUID runId;

    @Field("splitId")
    private UUID splitId;

    /** Which pipeline node rejected it, so a multi-stage failure is attributable. */
    @Field("nodeId")
    private String nodeId;

    @Field("seq")
    private long seq;

    @Field("recordKey")
    private String recordKey;

    /** The external system's own code, verbatim — not translated into a platform code. */
    @Field("code")
    private String code;

    @Field("message")
    private String message;

    @Field("payload")
    private Map<String, Object> payload;

    @Field("occurredAt")
    private Instant occurredAt;

    @Field("expiresAt")
    private Instant expiresAt;

    public RecordErrorDocument() {
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

    public UUID getRunId() {
        return runId;
    }

    public void setRunId(UUID runId) {
        this.runId = runId;
    }

    public UUID getSplitId() {
        return splitId;
    }

    public void setSplitId(UUID splitId) {
        this.splitId = splitId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public String getRecordKey() {
        return recordKey;
    }

    public void setRecordKey(String recordKey) {
        this.recordKey = recordKey;
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

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }
}
