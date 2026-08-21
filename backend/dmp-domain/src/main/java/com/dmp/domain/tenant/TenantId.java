package com.dmp.domain.tenant;

import com.dmp.common.id.EntityId;
import com.dmp.common.id.Ids;

import java.util.Objects;
import java.util.UUID;

/** Identifies a tenant. Present from Phase 1 because retrofitting tenancy is a rewrite. */
public record TenantId(UUID value) implements EntityId {

    public TenantId {
        Objects.requireNonNull(value, "tenant id value");
    }

    public static TenantId newId() {
        return new TenantId(Ids.newId());
    }

    public static TenantId of(UUID value) {
        return new TenantId(value);
    }

    public static TenantId parse(String value) {
        return new TenantId(UUID.fromString(value));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
