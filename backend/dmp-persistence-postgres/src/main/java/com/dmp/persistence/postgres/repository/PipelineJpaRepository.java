package com.dmp.persistence.postgres.repository;

import com.dmp.persistence.postgres.entity.PipelineEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data repository for {@code pipeline}.
 *
 * <p>Search is a native query rather than a Specification because the tag filter needs the JSONB
 * containment operator {@code @>}, which is what makes the GIN index usable. Expressing that
 * through the Criteria API means a vendor-specific function call with no type safety anyway — at
 * which point the SQL is clearer about what it does.
 *
 * <p>The {@code CAST(:param AS text) IS NULL} idiom is not decoration: PostgreSQL cannot infer a
 * parameter's type from a bare {@code IS NULL} comparison and rejects the statement without it.
 */
public interface PipelineJpaRepository extends JpaRepository<PipelineEntity, UUID> {

    Optional<PipelineEntity> findByTenantIdAndId(UUID tenantId, UUID id);

    Optional<PipelineEntity> findByTenantIdAndName(UUID tenantId, String name);

    boolean existsByTenantIdAndName(UUID tenantId, String name);

    void deleteByTenantIdAndId(UUID tenantId, UUID id);

    @Query(value = """
            SELECT * FROM pipeline p
            WHERE p.tenant_id = :tenantId
              AND (CAST(:nameContains AS text) IS NULL
                   OR p.name ILIKE '%' || CAST(:nameContains AS text) || '%')
              AND (CAST(:folder AS text) IS NULL OR p.folder = CAST(:folder AS text))
              AND (CAST(:status AS text) IS NULL OR p.status = CAST(:status AS text))
              AND (CAST(:tagsJson AS text) IS NULL OR p.tags @> CAST(:tagsJson AS jsonb))
            """,
            countQuery = """
            SELECT count(*) FROM pipeline p
            WHERE p.tenant_id = :tenantId
              AND (CAST(:nameContains AS text) IS NULL
                   OR p.name ILIKE '%' || CAST(:nameContains AS text) || '%')
              AND (CAST(:folder AS text) IS NULL OR p.folder = CAST(:folder AS text))
              AND (CAST(:status AS text) IS NULL OR p.status = CAST(:status AS text))
              AND (CAST(:tagsJson AS text) IS NULL OR p.tags @> CAST(:tagsJson AS jsonb))
            """,
            nativeQuery = true)
    Page<PipelineEntity> search(@Param("tenantId") UUID tenantId,
                                @Param("nameContains") String nameContains,
                                @Param("folder") String folder,
                                @Param("status") String status,
                                @Param("tagsJson") String tagsJson,
                                Pageable pageable);
}
