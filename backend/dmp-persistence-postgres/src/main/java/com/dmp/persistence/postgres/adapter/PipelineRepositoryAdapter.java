package com.dmp.persistence.postgres.adapter;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.PipelineEntity;
import com.dmp.persistence.postgres.mapper.PipelineMapper;
import com.dmp.persistence.postgres.repository.PipelineJpaRepository;
import com.dmp.persistence.postgres.support.PersistenceSupport;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;

/** PostgreSQL adapter for {@link PipelineRepository}. */
@Repository
public class PipelineRepositoryAdapter implements PipelineRepository {

    private static final Map<String, String> SORTABLE = Map.of(
            "name", "name",
            "createdAt", "created_at",
            "updatedAt", "updated_at",
            "status", "status");

    private final PipelineJpaRepository jpa;

    public PipelineRepositoryAdapter(PipelineJpaRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public Pipeline save(Pipeline pipeline) {
        String description = "Pipeline '" + pipeline.name() + "'";
        return PersistenceSupport.translatingExceptions(description, () -> {
            // Load the managed entity and copy state onto it, so Hibernate's dirty checking issues
            // a targeted UPDATE. The version check below is what keeps that from silently
            // discarding a concurrent edit — see requireCurrentVersion.
            PipelineEntity entity = jpa
                    .findByTenantIdAndId(pipeline.tenantId().value(), pipeline.id().value())
                    .orElse(null);

            if (entity == null) {
                entity = PipelineMapper.toEntity(pipeline);
            } else {
                PersistenceSupport.requireCurrentVersion(
                        pipeline.rowVersion(), entity.getRowVersion(), description);
                PipelineMapper.applyTo(entity, pipeline);
            }
            return PipelineMapper.toDomain(jpa.save(entity));
        });
    }

    @Override
    public Optional<Pipeline> findById(TenantId tenantId, PipelineId id) {
        return jpa.findByTenantIdAndId(tenantId.value(), id.value()).map(PipelineMapper::toDomain);
    }

    @Override
    public Optional<Pipeline> findByName(TenantId tenantId, String name) {
        return jpa.findByTenantIdAndName(tenantId.value(), name).map(PipelineMapper::toDomain);
    }

    @Override
    public Page<Pipeline> search(TenantId tenantId, PipelineSearch criteria, PageQuery pageQuery) {
        return PersistenceSupport.toPage(
                jpa.search(tenantId.value(),
                        criteria.nameContains(),
                        criteria.folder(),
                        criteria.status() == null ? null : criteria.status().name(),
                        toTagsJson(criteria),
                        PersistenceSupport.toPageable(pageQuery, SORTABLE, "updated_at")),
                pageQuery, PipelineMapper::toDomain);
    }

    @Override
    public boolean existsByName(TenantId tenantId, String name) {
        return jpa.existsByTenantIdAndName(tenantId.value(), name);
    }

    @Override
    public void delete(TenantId tenantId, PipelineId id) {
        jpa.deleteByTenantIdAndId(tenantId.value(), id.value());
    }

    /**
     * Renders the tag filter as a JSON array for the {@code @>} containment operator.
     *
     * <p>Containment is conjunctive, which matches the intent: filtering by {@code finance} and
     * {@code daily} should return pipelines carrying both, not either.
     */
    private String toTagsJson(PipelineSearch criteria) {
        if (criteria.tags().isEmpty()) {
            return null;
        }
        ArrayNode array = Json.mapper().createArrayNode();
        criteria.tags().forEach(array::add);
        return array.toString();
    }
}
