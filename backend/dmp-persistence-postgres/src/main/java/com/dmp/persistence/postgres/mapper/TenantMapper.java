package com.dmp.persistence.postgres.mapper;

import com.dmp.domain.tenant.Tenant;
import com.dmp.domain.tenant.TenantId;
import com.dmp.domain.tenant.TenantStatus;
import com.dmp.persistence.postgres.entity.TenantEntity;

/** Translates between {@link Tenant} and its JPA entity. */
public final class TenantMapper {

    private TenantMapper() {
    }

    public static Tenant toDomain(TenantEntity entity) {
        return new Tenant(
                TenantId.of(entity.getId()),
                entity.getSlug(),
                entity.getName(),
                TenantStatus.valueOf(entity.getStatus()),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

    public static TenantEntity toEntity(Tenant domain) {
        return new TenantEntity(
                domain.id().value(),
                domain.slug(),
                domain.name(),
                domain.status().name(),
                domain.createdAt(),
                domain.updatedAt());
    }

    public static void applyTo(TenantEntity entity, Tenant domain) {
        entity.apply(domain.name(), domain.status().name(), domain.updatedAt());
    }
}
