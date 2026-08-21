package com.dmp.persistence.postgres.repository;

import com.dmp.persistence.postgres.entity.TenantEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

/** Spring Data repository for {@code tenant}. */
public interface TenantJpaRepository extends JpaRepository<TenantEntity, UUID> {

    Optional<TenantEntity> findBySlug(String slug);

    boolean existsBySlug(String slug);
}
