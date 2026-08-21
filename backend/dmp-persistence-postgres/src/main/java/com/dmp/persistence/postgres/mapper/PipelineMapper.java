package com.dmp.persistence.postgres.mapper;

import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineStatus;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.PipelineEntity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Translates between the {@link Pipeline} aggregate and its JPA entity.
 *
 * <p>Hand-written rather than generated. The mapping is small, the translation of tags between a
 * {@code Set<String>} and a JSONB array is not something a generic mapper would get right without
 * configuration anyway, and an annotation processor in the build is a cost paid on every
 * compilation for the life of the project.
 */
public final class PipelineMapper {

    private PipelineMapper() {
    }

    public static Pipeline toDomain(PipelineEntity entity) {
        return new Pipeline(
                PipelineId.of(entity.getId()),
                TenantId.of(entity.getTenantId()),
                entity.getName(),
                entity.getDescription(),
                entity.getFolder(),
                toTagSet(entity.getTags()),
                PipelineStatus.valueOf(entity.getStatus()),
                entity.getPublishedVersion(),
                entity.getLatestVersion(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getRowVersion());
    }

    public static PipelineEntity toEntity(Pipeline domain) {
        return new PipelineEntity(
                domain.id().value(),
                domain.tenantId().value(),
                domain.name(),
                domain.description(),
                domain.folder(),
                toTagArray(domain.tags()),
                domain.status().name(),
                domain.publishedVersion(),
                domain.latestVersion(),
                domain.createdAt(),
                domain.updatedAt(),
                domain.rowVersion());
    }

    /** Copies mutable state onto a managed entity so Hibernate performs an update, not an insert. */
    public static void applyTo(PipelineEntity entity, Pipeline domain) {
        entity.apply(
                domain.name(),
                domain.description(),
                domain.folder(),
                toTagArray(domain.tags()),
                domain.status().name(),
                domain.publishedVersion(),
                domain.latestVersion(),
                domain.updatedAt());
    }

    private static Set<String> toTagSet(JsonNode tags) {
        if (tags == null || !tags.isArray()) {
            return Set.of();
        }
        Set<String> result = new LinkedHashSet<>();
        tags.forEach(node -> result.add(node.asText()));
        return result;
    }

    private static JsonNode toTagArray(Set<String> tags) {
        ArrayNode array = Json.mapper().createArrayNode();
        if (tags != null) {
            tags.forEach(array::add);
        }
        return array;
    }
}
