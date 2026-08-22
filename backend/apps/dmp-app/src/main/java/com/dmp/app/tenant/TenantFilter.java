package com.dmp.app.tenant;

import com.dmp.application.port.out.TenantRepository;
import com.dmp.domain.tenant.Tenant;
import com.dmp.domain.tenant.TenantId;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * Establishes the tenant and actor for the duration of a request.
 *
 * <p>Authentication is deferred pending company SSO (ADR-0004 defers security, not tenancy), so
 * for now the tenant comes from the {@code X-Tenant-Id} header — a slug or a UUID — falling back to
 * a configured default. **This is a development posture and nothing more.** A header the caller
 * chooses is not an authorisation decision, and this filter must be replaced by token validation
 * before the platform is exposed to anyone untrusted. The seam is here precisely so that
 * replacement touches one class.
 *
 * <p>Also populates the SLF4J MDC, so every log line emitted while handling a request carries its
 * tenant and request id without any call site having to pass them along.
 */
@Component
@Order(1)
public class TenantFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(TenantFilter.class);

    public static final String TENANT_HEADER = "X-Tenant-Id";
    public static final String REQUEST_ID_HEADER = "X-Request-Id";
    public static final String ACTOR_HEADER = "X-Actor";

    private static final String MDC_TENANT = "tenantId";
    private static final String MDC_REQUEST = "requestId";
    private static final String MDC_TRACE = "trace";

    private final TenantRepository tenants;
    private final String defaultTenantSlug;

    public TenantFilter(TenantRepository tenants,
                        @Value("${dmp.tenancy.default-tenant-slug:default}") String defaultTenantSlug) {
        this.tenants = tenants;
        this.defaultTenantSlug = defaultTenantSlug;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String requestId = Optional.ofNullable(request.getHeader(REQUEST_ID_HEADER))
                .filter(value -> !value.isBlank())
                .orElseGet(() -> UUID.randomUUID().toString());

        try {
            resolveTenant(request).ifPresent(tenant -> {
                String actor = Optional.ofNullable(request.getHeader(ACTOR_HEADER))
                        .filter(value -> !value.isBlank())
                        .orElse("anonymous");
                TenantContextHolder.set(tenant.id(), actor);
                MDC.put(MDC_TENANT, tenant.id().toString());
            });

            MDC.put(MDC_REQUEST, requestId);
            // The same field the engine writes while a chunk runs, so one column in the log
            // identifies the work whatever produced it.
            MDC.put(MDC_TRACE, requestId);
            response.setHeader(REQUEST_ID_HEADER, requestId);
            chain.doFilter(request, response);
        } finally {
            // Always cleared: the container reuses this thread, and a value left behind would
            // become the default tenant for the next request to land on it.
            TenantContextHolder.clear();
            MDC.remove(MDC_TENANT);
            MDC.remove(MDC_REQUEST);
            MDC.remove(MDC_TRACE);
        }
    }

    private Optional<Tenant> resolveTenant(HttpServletRequest request) {
        String header = request.getHeader(TENANT_HEADER);

        if (header != null && !header.isBlank()) {
            Optional<Tenant> byId = parseUuid(header).flatMap(id -> tenants.findById(TenantId.of(id)));
            if (byId.isPresent()) {
                return byId;
            }
            Optional<Tenant> bySlug = tenants.findBySlug(header.strip());
            if (bySlug.isPresent()) {
                return bySlug;
            }
            log.warn("Unknown tenant '{}' in {} header; falling back to default", header, TENANT_HEADER);
        }
        return tenants.findBySlug(defaultTenantSlug);
    }

    private Optional<UUID> parseUuid(String value) {
        try {
            return Optional.of(UUID.fromString(value.strip()));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    /** Actuator and API docs are not tenant-scoped and must work before any tenant exists. */
    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path.startsWith("/actuator")
                || path.startsWith("/v3/api-docs")
                || path.startsWith("/swagger-ui");
    }
}
