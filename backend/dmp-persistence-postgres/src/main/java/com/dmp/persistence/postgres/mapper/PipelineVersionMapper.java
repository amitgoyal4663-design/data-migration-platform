package com.dmp.persistence.postgres.mapper;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.DeliveryPolicy;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.PipelineDefinition;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.pipeline.PipelineVersionStatus;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.PipelineVersionEntity;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

/**
 * Translates between {@link PipelineVersion} and its JPA entity.
 *
 * <p>The DAG, the chunking policy and the audit policy are stored as JSONB and converted through
 * Jackson. Records deserialise natively provided the compiler ran with {@code -parameters}, which
 * the parent POM sets — without it, constructor parameter names are erased and every conversion
 * here fails at runtime rather than at build time.
 */
public final class PipelineVersionMapper {

    private PipelineVersionMapper() {
    }

    public static PipelineVersion toDomain(PipelineVersionEntity entity) {
        return new PipelineVersion(
                PipelineVersionId.of(entity.getId()),
                PipelineId.of(entity.getPipelineId()),
                TenantId.of(entity.getTenantId()),
                entity.getVersionNumber(),
                PipelineVersionStatus.valueOf(entity.getStatus()),
                read(entity.getDefinition(), PipelineDefinition.class, "definition"),
                read(entity.getChunkingPolicy(), ChunkingPolicy.class, "chunkingPolicy"),
                read(entity.getExecutionPolicy(), ExecutionPolicy.class, "executionPolicy"),
                read(entity.getAuditPolicy(), AuditPolicy.class, "auditPolicy"),
                // Null for every row written before delivery existed. The record's compact
                // constructor substitutes the default, which is the behaviour those rows had.
                read(entity.getDeliveryPolicy(), DeliveryPolicy.class, "deliveryPolicy"),
                PipelineMode.valueOf(entity.getMode()),
                entity.getChangeNote(),
                entity.getCreatedBy(),
                entity.getCreatedAt(),
                entity.getPublishedAt());
    }

    public static PipelineVersionEntity toEntity(PipelineVersion domain) {
        return new PipelineVersionEntity(
                domain.id().value(),
                domain.pipelineId().value(),
                domain.tenantId().value(),
                domain.versionNumber(),
                domain.status().name(),
                Json.mapper().valueToTree(domain.definition()),
                Json.mapper().valueToTree(domain.chunkingPolicy()),
                Json.mapper().valueToTree(domain.executionPolicy()),
                Json.mapper().valueToTree(domain.auditPolicy()),
                Json.mapper().valueToTree(domain.deliveryPolicy()),
                domain.mode().name(),
                domain.changeNote(),
                domain.createdBy(),
                domain.createdAt(),
                domain.publishedAt());
    }

    public static void applyTo(PipelineVersionEntity entity, PipelineVersion domain) {
        entity.apply(
                domain.status().name(),
                Json.mapper().valueToTree(domain.definition()),
                Json.mapper().valueToTree(domain.chunkingPolicy()),
                Json.mapper().valueToTree(domain.executionPolicy()),
                Json.mapper().valueToTree(domain.auditPolicy()),
                Json.mapper().valueToTree(domain.deliveryPolicy()),
                domain.mode().name(),
                domain.changeNote(),
                domain.publishedAt());
    }

    /**
     * Converts a stored JSON column into a domain value.
     *
     * <p>A failure here means a row was written by a version of the platform whose model has since
     * changed incompatibly. Surfacing it as INTERNAL with the offending field named is the honest
     * outcome — quietly substituting a default would let a pipeline run with configuration nobody
     * chose.
     */
    private static <T> T read(JsonNode node, Class<T> type, String field) {
        if (node == null || node.isNull()) {
            return null;
        }
        try {
            return Json.mapper().treeToValue(node, type);
        } catch (Exception e) {
            throw new DmpException(ErrorCode.INTERNAL,
                    "Stored pipeline version field '" + field + "' could not be read as "
                            + type.getSimpleName() + ". The persisted format is incompatible with "
                            + "the current model.",
                    Map.of("field", field, "type", type.getSimpleName()), e);
        }
    }
}
