package com.dmp.domain.tenant;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A tenant: the isolation boundary for every other aggregate.
 *
 * <p>Authentication is deferred pending company SSO, but the boundary is not. Every query in the
 * platform is scoped by tenant from Phase 1, so that turning on SSO later is a matter of
 * populating the tenant context from a token rather than a schema migration.
 */
public record Tenant(
        TenantId id,
        String slug,
        String name,
        TenantStatus status,
        Instant createdAt,
        Instant updatedAt) {

    /** Slugs appear in URLs and log lines, so they are constrained to a safe, stable shape. */
    private static final Pattern SLUG = Pattern.compile("^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$");
    private static final int MAX_NAME_LENGTH = 255;

    public Tenant {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");

        if (slug == null || !SLUG.matcher(slug).matches()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Tenant slug must be 3-64 lowercase alphanumeric characters or hyphens, "
                            + "and may not start or end with a hyphen",
                    Map.of("slug", String.valueOf(slug)));
        }
        if (name == null || name.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "Tenant name must not be blank");
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Tenant name exceeds " + MAX_NAME_LENGTH + " characters",
                    Map.of("length", name.length()));
        }
    }

    public static Tenant create(String slug, String name, Instant now) {
        return new Tenant(TenantId.newId(), slug, name, TenantStatus.ACTIVE, now, now);
    }

    public Tenant rename(String newName, Instant now) {
        return new Tenant(id, slug, newName, status, createdAt, now);
    }

    public Tenant suspend(Instant now) {
        return new Tenant(id, slug, name, TenantStatus.SUSPENDED, createdAt, now);
    }

    public Tenant activate(Instant now) {
        return new Tenant(id, slug, name, TenantStatus.ACTIVE, createdAt, now);
    }

    public boolean isActive() {
        return status == TenantStatus.ACTIVE;
    }
}
