package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.tenant.TenantId;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * An immutable-once-published snapshot of a pipeline's DAG and execution policy.
 *
 * <p>A run records the version identifier it executed. Because a published version cannot change,
 * that reference remains an accurate account of what actually ran, however much the pipeline is
 * edited afterwards. This is what makes execution history trustworthy and version comparison
 * meaningful.
 */
public record PipelineVersion(
        PipelineVersionId id,
        PipelineId pipelineId,
        TenantId tenantId,
        int versionNumber,
        PipelineVersionStatus status,
        PipelineDefinition definition,
        ChunkingPolicy chunkingPolicy,
        ExecutionPolicy executionPolicy,
        AuditPolicy auditPolicy,
        DeliveryPolicy deliveryPolicy,
        PipelineMode mode,
        String changeNote,
        String createdBy,
        Instant createdAt,
        Instant publishedAt) {

    private static final int MAX_CHANGE_NOTE_LENGTH = 2_000;

    public PipelineVersion {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(createdAt, "createdAt");

        definition = definition == null ? PipelineDefinition.empty() : definition;
        chunkingPolicy = chunkingPolicy == null ? ChunkingPolicy.DEFAULT : chunkingPolicy;
        executionPolicy = executionPolicy == null ? ExecutionPolicy.DEFAULT : executionPolicy;
        auditPolicy = auditPolicy == null ? AuditPolicy.DEFAULT : auditPolicy;

        // Absent means the default, so every version stored before delivery existed keeps loading
        // and keeps behaving exactly as it did — the whole batch in one call.
        deliveryPolicy = deliveryPolicy == null ? DeliveryPolicy.DEFAULT : deliveryPolicy;

        if (versionNumber < 1) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Version numbers start at 1", Map.of("versionNumber", versionNumber));
        }
        if (changeNote != null && changeNote.length() > MAX_CHANGE_NOTE_LENGTH) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Change note exceeds " + MAX_CHANGE_NOTE_LENGTH + " characters",
                    Map.of("length", changeNote.length()));
        }
        if (status == PipelineVersionStatus.PUBLISHED && publishedAt == null) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A published version must record when it was published");
        }
    }

    public static PipelineVersion createDraft(PipelineId pipelineId, TenantId tenantId, int versionNumber,
                                              PipelineDefinition definition, ChunkingPolicy chunkingPolicy,
                                              ExecutionPolicy executionPolicy, AuditPolicy auditPolicy,
                                              PipelineMode mode,
                                              String changeNote, String createdBy, Instant now) {
        return createDraft(pipelineId, tenantId, versionNumber, definition, chunkingPolicy,
                executionPolicy, auditPolicy, DeliveryPolicy.DEFAULT, mode, changeNote, createdBy, now);
    }

    public static PipelineVersion createDraft(PipelineId pipelineId, TenantId tenantId, int versionNumber,
                                              PipelineDefinition definition, ChunkingPolicy chunkingPolicy,
                                              ExecutionPolicy executionPolicy, AuditPolicy auditPolicy,
                                              DeliveryPolicy deliveryPolicy, PipelineMode mode,
                                              String changeNote, String createdBy, Instant now) {
        return new PipelineVersion(PipelineVersionId.newId(), pipelineId, tenantId, versionNumber,
                PipelineVersionStatus.DRAFT, definition, chunkingPolicy, executionPolicy, auditPolicy,
                deliveryPolicy, mode, changeNote, createdBy, now, null);
    }

    /** Replaces the DAG. Rejected once published. */
    public PipelineVersion withDefinition(PipelineDefinition newDefinition) {
        requireMutable("definition");
        return new PipelineVersion(id, pipelineId, tenantId, versionNumber, PipelineVersionStatus.DRAFT,
                newDefinition, chunkingPolicy, executionPolicy, auditPolicy, deliveryPolicy, mode, changeNote,
                createdBy, createdAt, publishedAt);
    }

    public PipelineVersion withChunkingPolicy(ChunkingPolicy newPolicy) {
        requireMutable("chunkingPolicy");
        return new PipelineVersion(id, pipelineId, tenantId, versionNumber, status,
                definition, newPolicy, executionPolicy, auditPolicy, deliveryPolicy, mode, changeNote,
                createdBy, createdAt, publishedAt);
    }

    public PipelineVersion withExecutionPolicy(ExecutionPolicy newPolicy) {
        requireMutable("executionPolicy");
        return new PipelineVersion(id, pipelineId, tenantId, versionNumber, status,
                definition, chunkingPolicy, newPolicy, auditPolicy, deliveryPolicy, mode, changeNote, createdBy,
                createdAt, publishedAt);
    }

    public PipelineVersion withAuditPolicy(AuditPolicy newPolicy) {
        requireMutable("auditPolicy");
        return new PipelineVersion(id, pipelineId, tenantId, versionNumber, status,
                definition, chunkingPolicy, executionPolicy, newPolicy, deliveryPolicy, mode, changeNote, createdBy,
                createdAt, publishedAt);
    }

    public PipelineVersion withDeliveryPolicy(DeliveryPolicy newPolicy) {
        requireMutable("deliveryPolicy");
        return new PipelineVersion(id, pipelineId, tenantId, versionNumber, status,
                definition, chunkingPolicy, executionPolicy, auditPolicy, newPolicy, mode, changeNote,
                createdBy, createdAt, publishedAt);
    }

    public PipelineVersion withMode(PipelineMode newMode) {
        requireMutable("mode");
        return new PipelineVersion(id, pipelineId, tenantId, versionNumber, status,
                definition, chunkingPolicy, executionPolicy, auditPolicy, deliveryPolicy, newMode, changeNote,
                createdBy, createdAt, publishedAt);
    }

    /** Records the outcome of structural validation without altering the definition. */
    public PipelineVersion markValidated() {
        requireMutable("status");
        return new PipelineVersion(id, pipelineId, tenantId, versionNumber, PipelineVersionStatus.VALIDATED,
                definition, chunkingPolicy, executionPolicy, auditPolicy, deliveryPolicy, mode, changeNote, createdBy, createdAt, publishedAt);
    }

    /**
     * Freezes this version.
     *
     * <p>Validation is enforced here rather than trusted from {@code status}, because a caller
     * could otherwise publish a version that was marked valid and then edited. Re-validating at
     * the freeze point is cheap and removes the ordering assumption entirely.
     */
    public PipelineVersion publish(PipelineValidator validator, Instant now) {
        if (status == PipelineVersionStatus.PUBLISHED) {
            throw new DmpException(ErrorCode.IMMUTABLE,
                    "Version " + versionNumber + " is already published",
                    Map.of("versionNumber", versionNumber));
        }
        validator.validate(definition).orThrow();
        return new PipelineVersion(id, pipelineId, tenantId, versionNumber, PipelineVersionStatus.PUBLISHED,
                definition, chunkingPolicy, executionPolicy, auditPolicy, deliveryPolicy, mode, changeNote, createdBy, createdAt, now);
    }

    /**
     * Copies this version's content into a new draft.
     *
     * <p>The path for editing something already published: fork rather than mutate.
     */
    public PipelineVersion forkAsDraft(int newVersionNumber, String note, String author, Instant now) {
        return new PipelineVersion(PipelineVersionId.newId(), pipelineId, tenantId, newVersionNumber,
                PipelineVersionStatus.DRAFT, definition, chunkingPolicy, executionPolicy, auditPolicy, deliveryPolicy, mode, note, author, now, null);
    }

    public ChannelType channelType() {
        return mode.channelType();
    }

    public boolean isPublished() {
        return status == PipelineVersionStatus.PUBLISHED;
    }

    public Optional<Instant> publishedAtInstant() {
        return Optional.ofNullable(publishedAt);
    }

    private void requireMutable(String field) {
        if (!status.isMutable()) {
            throw new DmpException(ErrorCode.IMMUTABLE,
                    "Version " + versionNumber + " is published and cannot be modified. "
                            + "Create a new version instead.",
                    Map.of("versionNumber", versionNumber, "field", field));
        }
    }
}
