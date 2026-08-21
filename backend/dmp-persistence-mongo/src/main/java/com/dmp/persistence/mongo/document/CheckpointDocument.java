package com.dmp.persistence.mongo.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * MongoDB document for a checkpoint. Keyed by split id — exactly one per split, overwritten in place.
 *
 * <p>This is the hottest write in the platform. A ten-thousand-split run committing a batch every
 * few seconds produces a sustained stream of overwrites, which is the concrete reason execution
 * data does not live in PostgreSQL: each of those writes would leave a dead tuple for vacuum.
 *
 * <p>The write ordering it participates in is not negotiable. The sink must have durably accepted
 * the batch <em>before</em> this document advances — and for an asynchronous sink such as
 * Salesforce Bulk v2, "durably accepted" means the job status poll reported complete, not that the
 * upload call returned (ADR-0009, ADR-0012). Advancing early loses records silently on resume.
 */
@Document(collection = "checkpoint")
public class CheckpointDocument {

    /** The split id. A checkpoint has no identity of its own. */
    @Id
    private UUID splitId;

    @Field("runId")
    private UUID runId;

    @Field("tenantId")
    private UUID tenantId;

    /** Connector-defined resume position: a key value, an oplog timestamp, an offset, a page token. */
    @Field("sourceCursor")
    private Map<String, Object> sourceCursor;

    @Field("lastSeq")
    private long lastSeq;

    @Field("recordsRead")
    private long recordsRead;

    /** Records the transform stage handed to the sink; differs from read when a script filters. */
    @Field("recordsProduced")
    private long recordsProduced;

    @Field("recordsWritten")
    private long recordsWritten;

    @Field("recordsFailed")
    private long recordsFailed;

    @Field("recordsFiltered")
    private long recordsFiltered;

    @Field("bytesRead")
    private long bytesRead;

    @Field("batchesCommitted")
    private int batchesCommitted;

    @Field("createdAt")
    private Instant createdAt;

    @Field("updatedAt")
    private Instant updatedAt;

    public CheckpointDocument() {
    }

    public UUID getSplitId() {
        return splitId;
    }

    public void setSplitId(UUID splitId) {
        this.splitId = splitId;
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

    public Map<String, Object> getSourceCursor() {
        return sourceCursor;
    }

    public void setSourceCursor(Map<String, Object> sourceCursor) {
        this.sourceCursor = sourceCursor;
    }

    public long getLastSeq() {
        return lastSeq;
    }

    public void setLastSeq(long lastSeq) {
        this.lastSeq = lastSeq;
    }

    public long getRecordsRead() {
        return recordsRead;
    }

    public void setRecordsRead(long recordsRead) {
        this.recordsRead = recordsRead;
    }

    public long getRecordsProduced() {
        return recordsProduced;
    }

    public void setRecordsProduced(long recordsProduced) {
        this.recordsProduced = recordsProduced;
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    public void setRecordsWritten(long recordsWritten) {
        this.recordsWritten = recordsWritten;
    }

    public long getRecordsFailed() {
        return recordsFailed;
    }

    public void setRecordsFailed(long recordsFailed) {
        this.recordsFailed = recordsFailed;
    }

    public long getRecordsFiltered() {
        return recordsFiltered;
    }

    public void setRecordsFiltered(long recordsFiltered) {
        this.recordsFiltered = recordsFiltered;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public int getBatchesCommitted() {
        return batchesCommitted;
    }

    public void setBatchesCommitted(int batchesCommitted) {
        this.batchesCommitted = batchesCommitted;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
