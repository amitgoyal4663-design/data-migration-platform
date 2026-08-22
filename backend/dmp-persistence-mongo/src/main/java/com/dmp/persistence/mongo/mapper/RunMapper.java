package com.dmp.persistence.mongo.mapper;

import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.RunMetrics;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.RunTrigger;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.mongo.document.RunDocument;
import com.dmp.persistence.mongo.support.JsonDocuments;

import java.util.LinkedHashMap;
import java.util.Map;

/** Translates between the {@link Run} aggregate and its MongoDB document. */
public final class RunMapper {

    private RunMapper() {
    }

    public static Run toDomain(RunDocument doc) {
        return new Run(
                RunId.of(doc.getId()),
                TenantId.of(doc.getTenantId()),
                PipelineId.of(doc.getPipelineId()),
                PipelineVersionId.of(doc.getPipelineVersionId()),
                doc.getVersionNumber(),
                PipelineMode.valueOf(doc.getMode()),
                RunTrigger.valueOf(doc.getTrigger()),
                doc.getRetryOf() == null ? null : RunId.of(doc.getRetryOf()),
                RunState.valueOf(doc.getState()),
                doc.getIdempotencyKey(),
                toMetrics(doc.getMetrics()),
                doc.getActiveSlots(),
                JsonDocuments.toJson(doc.getPreparationState()),
                JsonDocuments.toJson(doc.getParameters()),
                Boolean.TRUE.equals(doc.getDryRun()),
                doc.getErrorCode(),
                doc.getErrorMessage(),
                doc.getTriggeredBy(),
                doc.getCreatedAt(),
                doc.getStartedAt(),
                doc.getEndedAt(),
                doc.getUpdatedAt(),
                doc.getRowVersion() == null ? 0L : doc.getRowVersion());
    }

    public static RunDocument toDocument(Run run) {
        RunDocument doc = new RunDocument();
        doc.setId(run.id().value());
        doc.setTenantId(run.tenantId().value());
        doc.setPipelineId(run.pipelineId().value());
        doc.setPipelineVersionId(run.pipelineVersionId().value());
        doc.setVersionNumber(run.versionNumber());
        doc.setMode(run.mode().name());
        doc.setTrigger(run.trigger().name());
        doc.setRetryOf(run.retryOf() == null ? null : run.retryOf().value());
        doc.setState(run.state().name());
        doc.setDryRun(run.dryRun());
        doc.setIdempotencyKey(run.idempotencyKey());
        doc.setMetrics(toMetricsMap(run.metrics()));
        doc.setActiveSlots(run.activeSlots());
        doc.setPreparationState(JsonDocuments.toMap(run.preparationState()));
        doc.setParameters(JsonDocuments.toMap(run.parameters()));
        doc.setErrorCode(run.errorCode());
        doc.setErrorMessage(run.errorMessage());
        doc.setTriggeredBy(run.triggeredBy());
        doc.setCreatedAt(run.createdAt());
        doc.setStartedAt(run.startedAt());
        doc.setEndedAt(run.endedAt());
        doc.setUpdatedAt(run.updatedAt());
        // rowVersion is left null on insert so Spring Data initialises it; on update the adapter
        // copies the loaded document's value rather than trusting one carried through the domain.
        return doc;
    }

    /**
     * Metrics as a flat map.
     *
     * <p>Flat rather than nested so that concurrent workers can apply {@code $inc} to individual
     * counters — {@code metrics.recordsWritten} — without reading the document. Counters only ever
     * increase, so relative increments need no ordering guarantee between workers and cannot lose
     * an update, on what is the busiest write path in a run.
     */
    private static Map<String, Object> toMetricsMap(RunMetrics metrics) {
        return new LinkedHashMap<>(metrics.asMap());
    }

    private static RunMetrics toMetrics(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return RunMetrics.ZERO;
        }
        return new RunMetrics(
                asLong(map.get("recordsRead")),
                asLong(map.get("recordsProduced")),
                asLong(map.get("recordsWritten")),
                asLong(map.get("recordsFailed")),
                asLong(map.get("recordsFiltered")),
                asLong(map.get("bytesRead")),
                (int) asLong(map.get("splitsTotal")),
                (int) asLong(map.get("splitsCompleted")),
                (int) asLong(map.get("splitsFailed")));
    }

    /**
     * Reads a counter defensively.
     *
     * <p>BSON stores an integer as int32 or int64 depending on magnitude, and {@code $inc} can
     * promote a field from one to the other mid-run. Casting to {@code Long} directly would work
     * until a counter crossed 2^31 in production.
     */
    private static long asLong(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }
}
