package com.dmp.application.port.out;

import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.tenant.TenantId;

import java.util.List;
import java.util.Optional;

/** Persistence port for pipeline versions. Implemented by the PostgreSQL adapter. */
public interface PipelineVersionRepository {

    PipelineVersion save(PipelineVersion version);

    Optional<PipelineVersion> findById(TenantId tenantId, PipelineVersionId id);

    Optional<PipelineVersion> findByNumber(TenantId tenantId, PipelineId pipelineId, int versionNumber);

    /** Newest first. */
    List<PipelineVersion> findAllForPipeline(TenantId tenantId, PipelineId pipelineId);

    /** The highest version number allocated, or 0 if none exist. */
    int highestVersionNumber(TenantId tenantId, PipelineId pipelineId);

    /**
     * Whether any version's DAG wires this connector instance into a node.
     *
     * <p>Asked before a connector instance is deleted. Published versions are immutable, so a
     * deleted instance leaves every pipeline referencing it permanently unrunnable — the reference
     * cannot be repaired, only replaced by a whole new version. Checking first turns a silent,
     * irreversible breakage into a refusal the user can act on.
     */
    boolean isConnectorReferenced(TenantId tenantId, com.dmp.domain.connector.ConnectorInstanceId connectorId);

    /**
     * Deletes an unpublished draft.
     *
     * <p>Implementations must refuse to delete a PUBLISHED version. Runs live in MongoDB and
     * reference versions by id, so no foreign key protects that reference (ADR-0005). A deleted
     * published version would turn every run that executed it into an unexplainable record —
     * which is precisely the outcome versioning exists to prevent.
     *
     * @throws com.dmp.common.error.DmpException {@code IMMUTABLE} if the version is published
     */
    void deleteDraft(TenantId tenantId, PipelineVersionId id);
}
