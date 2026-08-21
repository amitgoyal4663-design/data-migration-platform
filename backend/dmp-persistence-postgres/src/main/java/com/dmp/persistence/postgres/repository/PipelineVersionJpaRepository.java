package com.dmp.persistence.postgres.repository;

import com.dmp.persistence.postgres.entity.PipelineVersionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Spring Data repository for {@code pipeline_version}. */
public interface PipelineVersionJpaRepository extends JpaRepository<PipelineVersionEntity, UUID> {

    Optional<PipelineVersionEntity> findByTenantIdAndId(UUID tenantId, UUID id);

    Optional<PipelineVersionEntity> findByTenantIdAndPipelineIdAndVersionNumber(
            UUID tenantId, UUID pipelineId, int versionNumber);

    List<PipelineVersionEntity> findByTenantIdAndPipelineIdOrderByVersionNumberDesc(
            UUID tenantId, UUID pipelineId);

    /**
     * The highest version number allocated for a pipeline, or 0 when none exist.
     *
     * <p>Reads the maximum rather than counting rows: drafts can be deleted, so a count would
     * eventually reissue a version number that history already used.
     */
    @Query("""
            SELECT COALESCE(MAX(v.versionNumber), 0)
            FROM PipelineVersionEntity v
            WHERE v.tenantId = :tenantId AND v.pipelineId = :pipelineId
            """)
    int highestVersionNumber(@Param("tenantId") UUID tenantId, @Param("pipelineId") UUID pipelineId);

    /** Whether any version references a connector instance. Used to block unsafe deletion. */
    @Query(value = """
            SELECT EXISTS (
                SELECT 1 FROM pipeline_version v,
                     jsonb_array_elements(v.definition -> 'nodes') AS node
                WHERE v.tenant_id = :tenantId
                  AND node ->> 'connectorInstanceId' = CAST(:connectorInstanceId AS text)
            )
            """, nativeQuery = true)
    boolean existsReferencingConnector(@Param("tenantId") UUID tenantId,
                                       @Param("connectorInstanceId") String connectorInstanceId);
}
