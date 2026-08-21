package com.dmp.app.config;

import com.dmp.application.port.out.TenantRepository;
import com.dmp.domain.tenant.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;

/**
 * Creates the default tenant on first start, so the platform is usable immediately.
 *
 * <p>Without this the API returns "no tenant in scope" for every request until someone inserts a
 * row by hand — a first-run experience that teaches people the platform is broken.
 *
 * <p>Idempotent, and disabled by setting {@code dmp.tenancy.bootstrap-default-tenant=false} in
 * deployments where tenants are provisioned externally.
 */
@Component
public class DefaultTenantInitializer {

    private static final Logger log = LoggerFactory.getLogger(DefaultTenantInitializer.class);

    private final TenantRepository tenants;
    private final Clock clock;
    private final String defaultSlug;
    private final boolean enabled;

    public DefaultTenantInitializer(TenantRepository tenants,
                                    Clock clock,
                                    @Value("${dmp.tenancy.default-tenant-slug:default}") String defaultSlug,
                                    @Value("${dmp.tenancy.bootstrap-default-tenant:true}") boolean enabled) {
        this.tenants = tenants;
        this.clock = clock;
        this.defaultSlug = defaultSlug;
        this.enabled = enabled;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bootstrap() {
        if (!enabled) {
            return;
        }
        if (tenants.existsBySlug(defaultSlug)) {
            return;
        }
        Tenant tenant = tenants.save(Tenant.create(defaultSlug, "Default", clock.instant()));
        log.info("Created default tenant '{}' with id {}", tenant.slug(), tenant.id());
    }
}
