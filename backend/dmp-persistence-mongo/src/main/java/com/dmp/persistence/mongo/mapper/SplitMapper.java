package com.dmp.persistence.mongo.mapper;

import com.dmp.domain.run.RunId;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.run.SplitState;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.mongo.document.SplitDocument;
import com.dmp.persistence.mongo.support.JsonDocuments;

/** Translates between the {@link Split} aggregate and its MongoDB document. */
public final class SplitMapper {

    private SplitMapper() {
    }

    public static Split toDomain(SplitDocument doc) {
        return new Split(
                SplitId.of(doc.getId()),
                RunId.of(doc.getRunId()),
                TenantId.of(doc.getTenantId()),
                doc.getIndex(),
                SplitState.valueOf(doc.getState()),
                JsonDocuments.toJson(doc.getSpec()),
                doc.getAssignedTo(),
                doc.getLeaseExpiresAt(),
                doc.getAttempt(),
                doc.getErrorCode(),
                doc.getErrorMessage(),
                doc.getCreatedAt(),
                doc.getStartedAt(),
                doc.getEndedAt(),
                doc.getUpdatedAt(),
                // An absent handle round-trips as an empty object, which the Split constructor
                // collapses back to null — so "no remote job" survives the trip in both directions.
                JsonDocuments.toJson(doc.getExternalJob()),
                doc.getDueAt(),
                // Absent on every split written before chunks knew their own size, which reads
                // back as 0 — "cannot say" — and behaves exactly as it did before.
                doc.getPlannedRows());
    }

    public static SplitDocument toDocument(Split split) {
        SplitDocument doc = new SplitDocument();
        doc.setId(split.id().value());
        doc.setRunId(split.runId().value());
        doc.setTenantId(split.tenantId().value());
        doc.setIndex(split.index());
        doc.setState(split.state().name());
        doc.setSpec(JsonDocuments.toMap(split.spec()));
        doc.setAssignedTo(split.assignedTo());
        doc.setLeaseExpiresAt(split.leaseExpiresAt());
        doc.setAttempt(split.attempt());
        doc.setErrorCode(split.errorCode());
        doc.setErrorMessage(split.errorMessage());
        doc.setCreatedAt(split.createdAt());
        doc.setStartedAt(split.startedAt());
        doc.setEndedAt(split.endedAt());
        doc.setUpdatedAt(split.updatedAt());
        doc.setExternalJob(JsonDocuments.toMap(split.externalJob()));
        doc.setDueAt(split.dueAt());
        doc.setPlannedRows(split.plannedRows());
        return doc;
    }
}
