package com.dmp.persistence.mongo.mapper;

import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.mongo.document.CheckpointDocument;
import com.dmp.persistence.mongo.support.JsonDocuments;

/** Translates between the {@link Checkpoint} aggregate and its MongoDB document. */
public final class CheckpointMapper {

    private CheckpointMapper() {
    }

    public static Checkpoint toDomain(CheckpointDocument doc) {
        return new Checkpoint(
                SplitId.of(doc.getSplitId()),
                RunId.of(doc.getRunId()),
                TenantId.of(doc.getTenantId()),
                JsonDocuments.toJson(doc.getSourceCursor()),
                doc.getLastSeq(),
                doc.getRecordsRead(),
                doc.getRecordsProduced(),
                doc.getRecordsWritten(),
                doc.getRecordsFailed(),
                doc.getRecordsFiltered(),
                doc.getBytesRead(),
                doc.getBatchesCommitted(),
                doc.getCreatedAt(),
                doc.getUpdatedAt());
    }

    public static CheckpointDocument toDocument(Checkpoint checkpoint) {
        CheckpointDocument doc = new CheckpointDocument();
        doc.setSplitId(checkpoint.splitId().value());
        doc.setRunId(checkpoint.runId().value());
        doc.setTenantId(checkpoint.tenantId().value());
        doc.setSourceCursor(JsonDocuments.toMap(checkpoint.sourceCursor()));
        doc.setLastSeq(checkpoint.lastSeq());
        doc.setRecordsRead(checkpoint.recordsRead());
        doc.setRecordsProduced(checkpoint.recordsProduced());
        doc.setRecordsWritten(checkpoint.recordsWritten());
        doc.setRecordsFailed(checkpoint.recordsFailed());
        doc.setRecordsFiltered(checkpoint.recordsFiltered());
        doc.setBytesRead(checkpoint.bytesRead());
        doc.setBatchesCommitted(checkpoint.batchesCommitted());
        doc.setCreatedAt(checkpoint.createdAt());
        doc.setUpdatedAt(checkpoint.updatedAt());
        return doc;
    }
}
