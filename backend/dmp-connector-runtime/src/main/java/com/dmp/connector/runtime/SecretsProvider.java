package com.dmp.connector.runtime;

import com.dmp.domain.tenant.TenantId;

import java.util.Optional;

/**
 * Resolves a secret reference to its value.
 *
 * <p>Connector instances store references such as {@code env:PG_PASSWORD} or
 * {@code vault:finance/postgres#password}, never values. Resolution happens here, in the worker, at
 * execution time. Nothing secret is written to the definition store, the audit log, an API response
 * or a log line, which is what makes those artefacts safe to read, export and share.
 *
 * <p>Authentication is deferred pending company SSO, but this seam is not: when a vault or
 * SSO-issued credential arrives, it becomes another implementation of this interface and nothing
 * else in the platform changes.
 */
public interface SecretsProvider {

    /**
     * Resolves a reference for a tenant.
     *
     * <p>Tenant-scoped so one tenant cannot read another's credentials by guessing a reference.
     *
     * @return the value, or empty if the reference does not resolve
     */
    Optional<String> resolve(TenantId tenantId, String reference);

    /** Which reference scheme this provider handles, for example {@code env} or {@code vault}. */
    String scheme();
}
