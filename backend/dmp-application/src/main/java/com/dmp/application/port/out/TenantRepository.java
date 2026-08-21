package com.dmp.application.port.out;

import com.dmp.domain.tenant.Tenant;
import com.dmp.domain.tenant.TenantId;

import java.util.List;
import java.util.Optional;

/** Persistence port for tenants. Implemented by the PostgreSQL adapter. */
public interface TenantRepository {

    Tenant save(Tenant tenant);

    Optional<Tenant> findById(TenantId id);

    Optional<Tenant> findBySlug(String slug);

    List<Tenant> findAll();

    boolean existsBySlug(String slug);
}
