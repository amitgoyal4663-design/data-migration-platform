package com.dmp.application.service;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.AuditLogPort;
import com.dmp.application.port.out.ConnectorInstanceRepository;
import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.dmp.domain.audit.AuditAction;
import com.dmp.domain.audit.AuditEntry;
import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;

/**
 * Connector instance lifecycle.
 *
 * <p>Configuration is stored as an opaque {@code JsonNode} and is not validated here. Validation
 * against the connector's declared JSON Schema requires the plugin runtime, which arrives in
 * Phase 2 (ADR-0006). Until then a connector instance is a well-named container for configuration
 * whose shape the platform does not yet know — which is the correct position, since inventing a
 * schema here would have to be unpicked when the real one arrives.
 *
 * <p>Nothing in this service handles secret <em>values</em>. {@code secretRefs} holds references
 * only, resolved in the worker at execution time through the {@code SecretsProvider} SPI. That
 * separation is what keeps this service, its logs and its audit entries safe to read.
 */
@Service
public class ConnectorInstanceService {

    private static final Logger log = LoggerFactory.getLogger(ConnectorInstanceService.class);
    private static final String RESOURCE_TYPE = "connector-instance";

    private final ConnectorInstanceRepository repository;
    private final PipelineVersionRepository versions;
    private final AuditLogPort auditLog;
    private final TenantContext tenantContext;
    private final Clock clock;

    public ConnectorInstanceService(ConnectorInstanceRepository repository,
                                    PipelineVersionRepository versions,
                                    AuditLogPort auditLog,
                                    TenantContext tenantContext,
                                    Clock clock) {
        this.repository = repository;
        this.versions = versions;
        this.auditLog = auditLog;
        this.tenantContext = tenantContext;
        this.clock = clock;
    }

    @Transactional
    public ConnectorInstance create(CreateConnectorInstance command) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();

        if (repository.existsByName(tenantId, command.name())) {
            throw new DmpException(ErrorCode.DUPLICATE,
                    "A connector instance named '" + command.name() + "' already exists",
                    Map.of("name", command.name()));
        }

        ConnectorInstance created = repository.save(ConnectorInstance.create(
                tenantId, command.name(), command.connectorType(), command.direction(),
                command.config(), command.secretRefs(), command.description(), now));

        audit(tenantId, AuditAction.CREATE, created, null, created,
                "Created connector instance '" + created.name() + "' of type " + created.connectorType(),
                now);
        log.info("Created connector instance {} '{}' type={} for tenant {}",
                created.id(), created.name(), created.connectorType(), tenantId);
        return created;
    }

    @Transactional
    public ConnectorInstance update(ConnectorInstanceId id, UpdateConnectorInstance command) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        ConnectorInstance existing = require(tenantId, id);

        if (!existing.name().equals(command.name()) && repository.existsByName(tenantId, command.name())) {
            throw new DmpException(ErrorCode.DUPLICATE,
                    "A connector instance named '" + command.name() + "' already exists",
                    Map.of("name", command.name()));
        }

        // The aggregate resets status to UNTESTED on any configuration change. A prior successful
        // connectivity check says nothing about a configuration that has since been edited, and
        // carrying ACTIVE forward would present a stale assurance as a current one.
        ConnectorInstance updated = repository.save(existing.updateConfiguration(
                command.name(), command.config(), command.secretRefs(), command.description(),
                command.direction(), now));

        audit(tenantId, AuditAction.UPDATE, updated, existing, updated,
                "Updated connector instance '" + updated.name() + "'", now);
        return updated;
    }

    @Transactional(readOnly = true)
    public ConnectorInstance get(ConnectorInstanceId id) {
        return require(tenantContext.currentTenant(), id);
    }

    @Transactional(readOnly = true)
    public Page<ConnectorInstance> search(ConnectorInstanceRepository.ConnectorSearch criteria,
                                          PageQuery pageQuery) {
        return repository.search(tenantContext.currentTenant(), criteria, pageQuery);
    }

    @Transactional
    public ConnectorInstance disable(ConnectorInstanceId id) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        ConnectorInstance existing = require(tenantId, id);

        ConnectorInstance disabled = repository.save(existing.disable(now));
        audit(tenantId, AuditAction.DISABLE, disabled, existing, disabled,
                "Disabled connector instance '" + disabled.name() + "'", now);
        return disabled;
    }

    @Transactional
    public ConnectorInstance enable(ConnectorInstanceId id) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        ConnectorInstance existing = require(tenantId, id);

        ConnectorInstance enabled = repository.save(existing.enable(now));
        audit(tenantId, AuditAction.ENABLE, enabled, existing, enabled,
                "Enabled connector instance '" + enabled.name() + "'", now);
        return enabled;
    }

    /**
     * Records the outcome of a connectivity test.
     *
     * <p>Phase 1 has no plugin runtime, so nothing calls this yet with a real result. It exists now
     * because the state it maintains — last tested, last error — is what the connector list in the
     * UI is built around, and adding the field later would mean a migration over live data.
     */
    @Transactional
    public ConnectorInstance recordTestResult(ConnectorInstanceId id, boolean success, String error) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        ConnectorInstance existing = require(tenantId, id);

        ConnectorInstance tested = repository.save(success
                ? existing.recordTestSuccess(now)
                : existing.recordTestFailure(error, now));

        audit(tenantId, AuditAction.TEST_CONNECTION, tested, existing, tested,
                success ? "Connection test succeeded" : "Connection test failed: " + error, now);
        return tested;
    }

    /**
     * Deletes a connector instance, refusing if any pipeline still references it.
     *
     * <p>The refusal is not caution. Pipeline versions reference instances by id, and published
     * versions are immutable — so deleting a referenced instance leaves every pipeline using it
     * permanently unrunnable, with a dangling reference that cannot be repaired. The only recovery
     * is to recreate the connection and publish an entirely new version.
     *
     * <p>Disabling is the operation people usually want: it stops the connection being used
     * without destroying the pipelines built on it.
     */
    @Transactional
    public void delete(ConnectorInstanceId id) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        ConnectorInstance existing = require(tenantId, id);

        if (versions.isConnectorReferenced(tenantId, id)) {
            throw new DmpException(ErrorCode.INVALID_REFERENCE,
                    "'" + existing.name() + "' is used by at least one pipeline and cannot be "
                            + "deleted. Published versions cannot be edited, so removing it would "
                            + "leave those pipelines permanently unrunnable. Disable it instead.",
                    Map.of("connectorInstanceId", id.toString(), "name", existing.name()));
        }

        audit(tenantId, AuditAction.DELETE, existing, existing, null,
                "Deleted connector instance '" + existing.name() + "'", now);
        repository.delete(tenantId, id);
        log.warn("Deleted connector instance {} '{}' for tenant {}", id, existing.name(), tenantId);
    }

    private ConnectorInstance require(TenantId tenantId, ConnectorInstanceId id) {
        return repository.findById(tenantId, id)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Connector instance not found", Map.of("connectorInstanceId", id.toString())));
    }

    private void audit(TenantId tenantId, AuditAction action, ConnectorInstance subject,
                       ConnectorInstance before, ConnectorInstance after, String summary, Instant now) {
        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(), action,
                RESOURCE_TYPE, subject.id().toString(), summary,
                before == null ? null : Json.mapper().valueToTree(before),
                after == null ? null : Json.mapper().valueToTree(after),
                now));
    }

    public record CreateConnectorInstance(String name, String connectorType, ConnectorDirection direction,
                                          JsonNode config, JsonNode secretRefs, String description) {
    }

    public record UpdateConnectorInstance(String name, JsonNode config, JsonNode secretRefs,
                                          String description,
                                          com.dmp.domain.connector.ConnectorDirection direction) {
    }
}
