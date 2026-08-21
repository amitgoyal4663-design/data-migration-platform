package com.dmp.application.service;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.AuditLogPort;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.dmp.domain.audit.AuditAction;
import com.dmp.domain.audit.AuditEntry;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.tenant.TenantId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Pipeline lifecycle use cases.
 *
 * <p>One service per aggregate rather than one class per use case. At this granularity the
 * single-class-per-use-case pattern produces thirty files that each hold one method and share a
 * constructor, which obscures the aggregate's behaviour rather than clarifying it. The boundary
 * that matters — application depending only on ports, never on adapters — is preserved either way
 * and enforced by ArchUnit.
 *
 * <p>{@link Clock} is injected so tests control time rather than tolerate it.
 */
@Service
public class PipelineService {

    private static final Logger log = LoggerFactory.getLogger(PipelineService.class);
    private static final String RESOURCE_TYPE = "pipeline";

    private final PipelineRepository pipelines;
    private final PipelineVersionRepository versions;
    private final AuditLogPort auditLog;
    private final TenantContext tenantContext;
    private final Clock clock;

    public PipelineService(PipelineRepository pipelines,
                           PipelineVersionRepository versions,
                           AuditLogPort auditLog,
                           TenantContext tenantContext,
                           Clock clock) {
        this.pipelines = pipelines;
        this.versions = versions;
        this.auditLog = auditLog;
        this.tenantContext = tenantContext;
        this.clock = clock;
    }

    @Transactional
    public Pipeline create(CreatePipeline command) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();

        // Checked explicitly so the caller gets a clear DUPLICATE rather than a constraint
        // violation surfacing as an opaque 500. The unique index remains the real guard against
        // the race between this check and the insert.
        if (pipelines.existsByName(tenantId, command.name())) {
            throw new DmpException(ErrorCode.DUPLICATE,
                    "A pipeline named '" + command.name() + "' already exists",
                    Map.of("name", command.name()));
        }

        Pipeline pipeline = pipelines.save(Pipeline.create(
                tenantId, command.name(), command.description(),
                command.folder(), command.tags(), now));

        audit(tenantId, AuditAction.CREATE, pipeline, null, pipeline,
                "Created pipeline '" + pipeline.name() + "'", now);
        log.info("Created pipeline {} '{}' for tenant {}", pipeline.id(), pipeline.name(), tenantId);
        return pipeline;
    }

    @Transactional
    public Pipeline update(PipelineId id, UpdatePipeline command) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        Pipeline existing = require(tenantId, id);

        if (!existing.name().equals(command.name()) && pipelines.existsByName(tenantId, command.name())) {
            throw new DmpException(ErrorCode.DUPLICATE,
                    "A pipeline named '" + command.name() + "' already exists",
                    Map.of("name", command.name()));
        }

        Pipeline updated = pipelines.save(existing.updateMetadata(
                command.name(), command.description(), command.folder(), command.tags(), now));

        audit(tenantId, AuditAction.UPDATE, updated, existing, updated,
                "Updated pipeline metadata", now);
        return updated;
    }

    @Transactional(readOnly = true)
    public Pipeline get(PipelineId id) {
        return require(tenantContext.currentTenant(), id);
    }

    @Transactional(readOnly = true)
    public Page<Pipeline> search(PipelineRepository.PipelineSearch criteria, PageQuery pageQuery) {
        return pipelines.search(tenantContext.currentTenant(), criteria, pageQuery);
    }

    @Transactional
    public Pipeline archive(PipelineId id) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        Pipeline existing = require(tenantId, id);

        Pipeline archived = pipelines.save(existing.archive(now));
        audit(tenantId, AuditAction.ARCHIVE, archived, existing, archived,
                "Archived pipeline '" + archived.name() + "'", now);
        return archived;
    }

    @Transactional
    public Pipeline restore(PipelineId id) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        Pipeline existing = require(tenantId, id);

        Pipeline restored = pipelines.save(existing.restore(now));
        audit(tenantId, AuditAction.RESTORE, restored, existing, restored,
                "Restored pipeline '" + restored.name() + "'", now);
        return restored;
    }

    /**
     * Permanently deletes a pipeline and its versions.
     *
     * <p>Refused once anything has been published. Runs live in MongoDB and reference version ids
     * with no foreign key to protect them (ADR-0005), so deleting a published pipeline would turn
     * every run that executed it into an unexplainable record. Archiving is the intended path and
     * this exists only for drafts created by mistake.
     */
    @Transactional
    public void delete(PipelineId id) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        Pipeline existing = require(tenantId, id);

        if (existing.publishedVersionNumber().isPresent()) {
            throw new DmpException(ErrorCode.IMMUTABLE,
                    "Pipeline '" + existing.name() + "' has published versions and cannot be deleted. "
                            + "Archive it instead, so that its run history stays interpretable.",
                    Map.of("pipelineId", id.toString(),
                            "publishedVersion", existing.publishedVersionNumber().orElseThrow()));
        }

        audit(tenantId, AuditAction.DELETE, existing, existing, null,
                "Deleted pipeline '" + existing.name() + "'", now);
        pipelines.delete(tenantId, id);
        log.info("Deleted pipeline {} for tenant {}", id, tenantId);
    }

    private Pipeline require(TenantId tenantId, PipelineId id) {
        return pipelines.findById(tenantId, id)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Pipeline not found", Map.of("pipelineId", id.toString())));
    }

    private void audit(TenantId tenantId, AuditAction action, Pipeline subject,
                       Pipeline before, Pipeline after, String summary, Instant now) {
        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(), action,
                RESOURCE_TYPE, subject.id().toString(), summary,
                before == null ? null : Json.mapper().valueToTree(before),
                after == null ? null : Json.mapper().valueToTree(after),
                now));
    }

    /** @param tags normalised to lowercase by the aggregate */
    public record CreatePipeline(String name, String description, String folder, Set<String> tags) {
    }

    public record UpdatePipeline(String name, String description, String folder, Set<String> tags) {
    }
}
