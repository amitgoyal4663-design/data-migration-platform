package com.dmp.persistence.postgres.adapter;

import com.dmp.application.port.out.TenantRepository;
import com.dmp.domain.tenant.Tenant;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.TenantEntity;
import com.dmp.persistence.postgres.mapper.TenantMapper;
import com.dmp.persistence.postgres.repository.TenantJpaRepository;
import com.dmp.persistence.postgres.support.PersistenceSupport;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/** PostgreSQL adapter for {@link TenantRepository}. */
@Repository
public class TenantRepositoryAdapter implements TenantRepository {

    private final TenantJpaRepository jpa;

    public TenantRepositoryAdapter(TenantJpaRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public Tenant save(Tenant tenant) {
        return PersistenceSupport.translatingExceptions("Tenant '" + tenant.slug() + "'", () -> {
            TenantEntity entity = jpa.findById(tenant.id().value()).orElse(null);
            if (entity == null) {
                entity = TenantMapper.toEntity(tenant);
            } else {
                TenantMapper.applyTo(entity, tenant);
            }
            return TenantMapper.toDomain(jpa.save(entity));
        });
    }

    @Override
    public Optional<Tenant> findById(TenantId id) {
        return jpa.findById(id.value()).map(TenantMapper::toDomain);
    }

    @Override
    public Optional<Tenant> findBySlug(String slug) {
        return jpa.findBySlug(slug).map(TenantMapper::toDomain);
    }

    @Override
    public List<Tenant> findAll() {
        return jpa.findAll().stream().map(TenantMapper::toDomain).toList();
    }

    @Override
    public boolean existsBySlug(String slug) {
        return jpa.existsBySlug(slug);
    }
}
