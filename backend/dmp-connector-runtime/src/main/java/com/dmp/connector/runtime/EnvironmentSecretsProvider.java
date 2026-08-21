package com.dmp.connector.runtime;

import com.dmp.domain.tenant.TenantId;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Resolves {@code env:NAME} references from the process environment and Spring configuration.
 *
 * <p>Suitable for development and for deployments where secrets arrive as environment variables
 * from Kubernetes or a sidecar — which covers a great many production setups honestly and well.
 *
 * <p>It is <b>not</b> tenant-isolated: every tenant resolving {@code env:PG_PASSWORD} gets the same
 * value, because a process has one environment. That is acceptable in a single-organisation
 * deployment and unacceptable in a shared multi-tenant one. A vault-backed provider is the answer
 * there, and this interface is the seam for it.
 */
@Component
public class EnvironmentSecretsProvider implements SecretsProvider {

    private static final String SCHEME = "env";

    private final Environment environment;

    public EnvironmentSecretsProvider(Environment environment) {
        this.environment = environment;
    }

    @Override
    public Optional<String> resolve(TenantId tenantId, String reference) {
        if (reference == null || reference.isBlank()) {
            return Optional.empty();
        }
        String name = reference.startsWith(SCHEME + ":")
                ? reference.substring(SCHEME.length() + 1)
                : reference;

        // Spring's Environment already covers actual environment variables, so this handles both
        // exported variables and values supplied through application configuration.
        return Optional.ofNullable(environment.getProperty(name))
                .or(() -> Optional.ofNullable(System.getenv(name)));
    }

    @Override
    public String scheme() {
        return SCHEME;
    }
}
