package com.dmp.persistence.postgres.adapter;

import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.pipeline.PipelineVersionStatus;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.PipelineVersionEntity;
import com.dmp.persistence.postgres.mapper.PipelineVersionMapper;
import com.dmp.persistence.postgres.repository.PipelineVersionJpaRepository;
import com.dmp.persistence.postgres.support.PersistenceSupport;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** PostgreSQL adapter for {@link PipelineVersionRepository}. */
@Repository
public class PipelineVersionRepositoryAdapter implements PipelineVersionRepository {

    private final PipelineVersionJpaRepository jpa;

    public PipelineVersionRepositoryAdapter(PipelineVersionJpaRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public PipelineVersion save(PipelineVersion version) {
        String description = "Pipeline version " + version.versionNumber();
        return PersistenceSupport.translatingExceptions(description, () -> {
            PipelineVersionEntity entity = jpa
                    .findByTenantIdAndId(version.tenantId().value(), version.id().value())
                    .orElse(null);

            if (entity == null) {
                entity = PipelineVersionMapper.toEntity(version);
            } else {
                PipelineVersionMapper.applyTo(entity, version);
            }
            return PipelineVersionMapper.toDomain(jpa.save(entity));
        });
    }

    @Override
    public Optional<PipelineVersion> findById(TenantId tenantId, PipelineVersionId id) {
        return jpa.findByTenantIdAndId(tenantId.value(), id.value())
                .map(PipelineVersionMapper::toDomain);
    }

    @Override
    public Optional<PipelineVersion> findByNumber(TenantId tenantId, PipelineId pipelineId, int versionNumber) {
        return jpa.findByTenantIdAndPipelineIdAndVersionNumber(
                        tenantId.value(), pipelineId.value(), versionNumber)
                .map(PipelineVersionMapper::toDomain);
    }

    @Override
    public List<PipelineVersion> findAllForPipeline(TenantId tenantId, PipelineId pipelineId) {
        return jpa.findByTenantIdAndPipelineIdOrderByVersionNumberDesc(tenantId.value(), pipelineId.value())
                .stream().map(PipelineVersionMapper::toDomain).toList();
    }

    @Override
    public int highestVersionNumber(TenantId tenantId, PipelineId pipelineId) {
        return jpa.highestVersionNumber(tenantId.value(), pipelineId.value());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Checked here as well as by the database trigger. The trigger is the guarantee; this check
     * exists so the caller receives a comprehensible IMMUTABLE error naming the version, rather
     * than a driver-level constraint violation.
     */
    @Override
    public void deleteDraft(TenantId tenantId, PipelineVersionId id) {
        PipelineVersionEntity entity = jpa.findByTenantIdAndId(tenantId.value(), id.value())
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Pipeline version not found", Map.of("versionId", id.toString())));

        if (PipelineVersionStatus.valueOf(entity.getStatus()) == PipelineVersionStatus.PUBLISHED) {
            throw new DmpException(ErrorCode.IMMUTABLE,
                    "Version " + entity.getVersionNumber() + " is published and cannot be deleted. "
                            + "Runs reference it, and removing it would make their history "
                            + "uninterpretable.",
                    Map.of("versionId", id.toString(), "versionNumber", entity.getVersionNumber()));
        }
        jpa.delete(entity);
    }

    @Override
    public boolean isConnectorReferenced(TenantId tenantId,
                                         com.dmp.domain.connector.ConnectorInstanceId connectorId) {
        return jpa.existsReferencingConnector(tenantId.value(), connectorId.toString());
    }
}
