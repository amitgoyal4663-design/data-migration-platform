package com.dmp.persistence.postgres.repository;

import com.dmp.persistence.postgres.entity.ConnectorInstanceEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Spring Data repository for {@code connector_instance}. */
public interface ConnectorInstanceJpaRepository extends JpaRepository<ConnectorInstanceEntity, UUID> {

    Optional<ConnectorInstanceEntity> findByTenantIdAndId(UUID tenantId, UUID id);

    List<ConnectorInstanceEntity> findByTenantIdAndIdIn(UUID tenantId, Collection<UUID> ids);

    boolean existsByTenantIdAndName(UUID tenantId, String name);

    void deleteByTenantIdAndId(UUID tenantId, UUID id);

    @Query(value = """
            SELECT * FROM connector_instance c
            WHERE c.tenant_id = :tenantId
              AND (CAST(:nameContains AS text) IS NULL
                   OR c.name ILIKE '%' || CAST(:nameContains AS text) || '%')
              AND (CAST(:connectorType AS text) IS NULL
                   OR c.connector_type = CAST(:connectorType AS text))
              AND (CAST(:direction AS text) IS NULL
                   OR c.direction = CAST(:direction AS text) OR c.direction = 'BOTH')
              AND (CAST(:status AS text) IS NULL OR c.status = CAST(:status AS text))
            """,
            countQuery = """
            SELECT count(*) FROM connector_instance c
            WHERE c.tenant_id = :tenantId
              AND (CAST(:nameContains AS text) IS NULL
                   OR c.name ILIKE '%' || CAST(:nameContains AS text) || '%')
              AND (CAST(:connectorType AS text) IS NULL
                   OR c.connector_type = CAST(:connectorType AS text))
              AND (CAST(:direction AS text) IS NULL
                   OR c.direction = CAST(:direction AS text) OR c.direction = 'BOTH')
              AND (CAST(:status AS text) IS NULL OR c.status = CAST(:status AS text))
            """,
            nativeQuery = true)
    Page<ConnectorInstanceEntity> search(@Param("tenantId") UUID tenantId,
                                         @Param("nameContains") String nameContains,
                                         @Param("connectorType") String connectorType,
                                         @Param("direction") String direction,
                                         @Param("status") String status,
                                         Pageable pageable);
}
