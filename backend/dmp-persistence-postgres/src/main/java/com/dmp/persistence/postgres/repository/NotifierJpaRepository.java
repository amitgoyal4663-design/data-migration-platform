package com.dmp.persistence.postgres.repository;

import com.dmp.persistence.postgres.entity.NotifierEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NotifierJpaRepository extends JpaRepository<NotifierEntity, UUID> {

    Optional<NotifierEntity> findByIdAndTenantId(UUID id, UUID tenantId);

    List<NotifierEntity> findByTenantIdOrderByNameAsc(UUID tenantId);

    List<NotifierEntity> findByTenantIdAndEnabledTrue(UUID tenantId);
}
