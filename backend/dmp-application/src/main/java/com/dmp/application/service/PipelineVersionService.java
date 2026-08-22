package com.dmp.application.service;

import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.AuditLogPort;
import com.dmp.application.port.out.ConnectorCapabilityPort;
import com.dmp.application.port.out.ConnectorInstanceRepository;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.dmp.domain.audit.AuditAction;
import com.dmp.domain.audit.AuditEntry;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.DeliveryPolicy;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineDefinition;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineValidator;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.pipeline.PipelineVersionStatus;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.pipeline.ValidationIssue;
import com.dmp.domain.pipeline.ValidationResult;
import com.dmp.domain.tenant.TenantId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Version lifecycle: draft, edit, validate, publish, roll back.
 *
 * <p>Validation happens at two levels and both matter. {@link PipelineValidator} answers "is this
 * graph executable in principle" using nothing but the definition. This service adds the questions
 * that need other aggregates: do the referenced connector instances exist, are they usable, and is
 * a sink-only connector wired into a source node. Catching the second class at publish time rather
 * than at run start is the difference between an error on a screen someone is looking at and a
 * failure at 03:00.
 */
@Service
public class PipelineVersionService {

    private static final Logger log = LoggerFactory.getLogger(PipelineVersionService.class);
    private static final String RESOURCE_TYPE = "pipeline-version";

    /**
     * Below this, a chunk costs more to administer than to run.
     *
     * <p>Each one is a claim, a checkpoint, a state transition and an event. At a hundred rows a
     * chunk that overhead was measured at seventy per cent of a real run's wall clock. A warning
     * rather than an error, because a deliberately tiny chunk is legitimate while testing.
     */
    private static final int MIN_SENSIBLE_CHUNK = 1_000;

    /**
     * Whether this deployment can archive full record lineage to object storage.
     *
     * <p>Phase 11 delivers it. Until then {@code AuditPolicy.FULL} is refused at publish rather
     * than silently downgraded, because a user believing they have complete lineage when they do
     * not is worse than an error (ADR-0011).
     */
    private static final boolean ARCHIVAL_PIPELINE_AVAILABLE = false;

    private final PipelineRepository pipelines;
    private final PipelineVersionRepository versions;
    private final ConnectorInstanceRepository connectors;
    private final ConnectorCapabilityPort connectorCapabilities;
    private final PipelineValidator validator;
    private final AuditLogPort auditLog;
    private final TenantContext tenantContext;
    private final Clock clock;

    public PipelineVersionService(PipelineRepository pipelines,
                                  PipelineVersionRepository versions,
                                  ConnectorInstanceRepository connectors,
                                  ConnectorCapabilityPort connectorCapabilities,
                                  PipelineValidator validator,
                                  AuditLogPort auditLog,
                                  TenantContext tenantContext,
                                  Clock clock) {
        this.pipelines = pipelines;
        this.versions = versions;
        this.connectors = connectors;
        this.connectorCapabilities = connectorCapabilities;
        this.validator = validator;
        this.auditLog = auditLog;
        this.tenantContext = tenantContext;
        this.clock = clock;
    }

    /** Creates the next draft version, seeded either from the request or from the latest version. */
    @Transactional
    public PipelineVersion createDraft(PipelineId pipelineId, CreateVersion command) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        Pipeline pipeline = requirePipeline(tenantId, pipelineId);

        int latestNumber = versions.highestVersionNumber(tenantId, pipelineId);
        int nextNumber = latestNumber + 1;

        // Settings are inherited from the version this one succeeds, not reset to platform
        // defaults. A new version is a change to the graph; nobody editing the canvas expects
        // their concurrency limit, rejection threshold and audit retention to be silently
        // discarded along with it — and because the designer creates a version on every publish,
        // resetting here meant no execution setting survived more than one edit.
        PipelineVersion previous = latestNumber < 1 ? null
                : versions.findByNumber(tenantId, pipelineId, latestNumber).orElse(null);

        // The graph is inherited too, for the same reason the settings are. A new version is
        // almost always an edit of the last one, and starting from an empty canvas discards work
        // nobody asked to discard — then fails validation with "a pipeline must contain at least
        // one node", which reads as a bug in the platform rather than as an empty draft.
        // An explicitly supplied definition still wins, including an explicitly empty one.
        PipelineVersion draft = PipelineVersion.createDraft(
                pipelineId, tenantId, nextNumber,
                inherited(command.definition(), previous == null ? null : previous.definition(),
                        PipelineDefinition.empty()),
                inherited(command.chunkingPolicy(), previous == null ? null : previous.chunkingPolicy(),
                        ChunkingPolicy.DEFAULT),
                inherited(command.executionPolicy(), previous == null ? null : previous.executionPolicy(),
                        ExecutionPolicy.DEFAULT),
                inherited(command.auditPolicy(), previous == null ? null : previous.auditPolicy(),
                        AuditPolicy.DEFAULT),
                inherited(command.deliveryPolicy(), previous == null ? null : previous.deliveryPolicy(),
                        DeliveryPolicy.DEFAULT),
                inherited(command.mode(), previous == null ? null : previous.mode(),
                        PipelineMode.FULL_LOAD),
                command.changeNote(), tenantContext.currentActor(), now);

        PipelineVersion saved = versions.save(draft);
        pipelines.save(pipeline.withNewVersion(nextNumber, now));

        audit(tenantId, AuditAction.CREATE, saved, null, saved,
                "Created draft version " + nextNumber, now);
        return saved;
    }

    /** What the caller asked for, else what the previous version used, else the platform default. */
    private static <T> T inherited(T requested, T previous, T fallback) {
        if (requested != null) {
            return requested;
        }
        return previous != null ? previous : fallback;
    }

    /** Replaces the DAG of a draft. Rejected on a published version by the aggregate. */
    @Transactional
    public PipelineVersion updateDefinition(PipelineVersionId versionId, PipelineDefinition definition) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        PipelineVersion existing = requireVersion(tenantId, versionId);

        PipelineVersion updated = versions.save(existing.withDefinition(definition));
        audit(tenantId, AuditAction.UPDATE, updated, existing, updated,
                "Updated definition of version " + updated.versionNumber(), now);
        return updated;
    }

    @Transactional
    public PipelineVersion updatePolicies(PipelineVersionId versionId, UpdatePolicies command) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        PipelineVersion existing = requireVersion(tenantId, versionId);

        PipelineVersion updated = existing;
        if (command.chunkingPolicy() != null) {
            updated = updated.withChunkingPolicy(command.chunkingPolicy());
        }
        if (command.executionPolicy() != null) {
            updated = updated.withExecutionPolicy(command.executionPolicy());
        }
        if (command.auditPolicy() != null) {
            updated = updated.withAuditPolicy(command.auditPolicy());
        }
        if (command.deliveryPolicy() != null) {
            updated = updated.withDeliveryPolicy(command.deliveryPolicy());
        }
        if (command.mode() != null) {
            updated = updated.withMode(command.mode());
        }
        updated = versions.save(updated);

        audit(tenantId, AuditAction.UPDATE, updated, existing, updated,
                "Updated policies of version " + updated.versionNumber(), now);
        return updated;
    }

    /**
     * Runs full validation without changing anything the caller can see.
     *
     * <p>Structural issues and connector-reference issues are returned together. A user fixing a
     * pipeline should see every problem at once rather than discovering them one save at a time.
     */
    @Transactional
    public ValidationResult validate(PipelineVersionId versionId) {
        TenantId tenantId = tenantContext.currentTenant();
        PipelineVersion version = requireVersion(tenantId, versionId);

        ValidationResult result = fullValidation(tenantId, version);
        if (result.isValid() && version.status().isMutable()) {
            versions.save(version.markValidated());
        }
        return result;
    }

    /**
     * Freezes a version and makes it the pipeline's published one.
     *
     * <p>Publishing is the only irreversible step in the editing flow, so every check runs here
     * even if it ran a moment ago during an explicit validate — the definition may have changed in
     * between, and re-checking is cheap compared with the alternative.
     */
    @Transactional
    public PipelineVersion publish(PipelineId pipelineId, int versionNumber) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        Pipeline pipeline = requirePipeline(tenantId, pipelineId);
        PipelineVersion version = versions.findByNumber(tenantId, pipelineId, versionNumber)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Version " + versionNumber + " not found",
                        Map.of("pipelineId", pipelineId.toString(), "versionNumber", versionNumber)));

        fullValidation(tenantId, version).orThrow();
        version.auditPolicy().requireSupported(ARCHIVAL_PIPELINE_AVAILABLE);

        // A version that is already frozen is a rollback, not a publish. Freezing does two things —
        // it makes the version immutable, and it points the pipeline at it — and only the second
        // applies here. Calling publish() on it threw "already published", which made the console's
        // Roll back action fail on every version it was ever offered for: the only versions it
        // appears on are frozen ones.
        boolean rollingBack = version.status() == PipelineVersionStatus.PUBLISHED;
        PipelineVersion published = rollingBack ? version : versions.save(version.publish(validator, now));
        pipelines.save(pipeline.publishVersion(versionNumber, now));

        audit(tenantId, rollingBack ? AuditAction.ROLLBACK : AuditAction.PUBLISH,
                published, version, published,
                (rollingBack ? "Rolled back to version " : "Published version ") + versionNumber, now);
        log.info("{} pipeline {} to version {} for tenant {}",
                rollingBack ? "Rolled back" : "Published", pipelineId, versionNumber, tenantId);
        return published;
    }

    /**
     * Republishes an earlier version.
     *
     * <p>Rollback is deliberately not a new version containing old content. Pointing the pipeline
     * back at the version that already exists keeps the history honest: the run that executed v1
     * last week and the run that executes v1 tonight reference the same immutable definition, and
     * the version list shows what actually happened rather than a copy pretending to be new work.
     */
    @Transactional
    public Pipeline rollback(PipelineId pipelineId, int targetVersionNumber) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        Pipeline pipeline = requirePipeline(tenantId, pipelineId);

        PipelineVersion target = versions.findByNumber(tenantId, pipelineId, targetVersionNumber)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Version " + targetVersionNumber + " not found",
                        Map.of("pipelineId", pipelineId.toString(), "versionNumber", targetVersionNumber)));

        if (!target.isPublished()) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "Cannot roll back to version " + targetVersionNumber + " because it was never published",
                    Map.of("versionNumber", targetVersionNumber, "status", target.status().name()));
        }

        Pipeline rolledBack = pipelines.save(pipeline.publishVersion(targetVersionNumber, now));
        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(), AuditAction.ROLLBACK,
                "pipeline", pipelineId.toString(),
                "Rolled back from version " + pipeline.publishedVersionNumber().orElse(0)
                        + " to version " + targetVersionNumber,
                Json.mapper().valueToTree(pipeline), Json.mapper().valueToTree(rolledBack), now));
        log.warn("Rolled back pipeline {} to version {} for tenant {}",
                pipelineId, targetVersionNumber, tenantId);
        return rolledBack;
    }

    @Transactional(readOnly = true)
    public List<PipelineVersion> listVersions(PipelineId pipelineId) {
        TenantId tenantId = tenantContext.currentTenant();
        requirePipeline(tenantId, pipelineId);
        return versions.findAllForPipeline(tenantId, pipelineId);
    }

    @Transactional(readOnly = true)
    public PipelineVersion get(PipelineVersionId versionId) {
        return requireVersion(tenantContext.currentTenant(), versionId);
    }

    @Transactional
    public void deleteDraft(PipelineVersionId versionId) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        PipelineVersion existing = requireVersion(tenantId, versionId);

        audit(tenantId, AuditAction.DELETE, existing, existing, null,
                "Deleted draft version " + existing.versionNumber(), now);
        versions.deleteDraft(tenantId, versionId);

        // The pipeline tracks its highest version, and the next draft's number is checked against
        // it for contiguity. Deleting the newest draft without lowering the counter left the
        // pipeline unable to create another one at all: the only number the table would allow was
        // the one the counter had already spent.
        Pipeline pipeline = requirePipeline(tenantId, existing.pipelineId());
        int highestRemaining = versions.highestVersionNumber(tenantId, existing.pipelineId());
        Pipeline adjusted = pipeline.withHighestVersion(highestRemaining, now);
        if (adjusted != pipeline) {
            pipelines.save(adjusted);
        }
    }

    /**
     * Structural validation plus the cross-aggregate checks the validator cannot perform.
     */
    private ValidationResult fullValidation(TenantId tenantId, PipelineVersion version) {
        List<ValidationIssue> issues = new ArrayList<>(validator.validate(version.definition()).issues());
        issues.addAll(validateConnectorReferences(tenantId, version.definition()));

        issues.addAll(validateRecordIdentity(tenantId, version));
        issues.addAll(validateConfiguredSteps(version.definition()));
        issues.addAll(validateSizes(version));
        issues.addAll(validateDelivery(tenantId, version));

        if (version.auditPolicy().capturesPayloadsWithoutRedaction()) {
            issues.add(new ValidationIssue(ValidationIssue.Severity.WARNING,
                    "AUDIT_NO_REDACTION",
                    "This pipeline captures record payloads in its audit trail but declares no "
                            + "redacted fields. Any personal data in these records will be stored "
                            + "unmasked.", null, null));
        }
        return new ValidationResult(issues);
    }

    /**
     * Warns when a pipeline is set to index its records but its source cannot identify them.
     *
     * <p>That combination is the worst way for a feature to be unavailable: the run reads, writes,
     * completes and reports success, and the index it was supposed to fill is empty — so the first
     * anyone learns of it is when a search for a record answers "not transferred" about a record
     * that was. Records with no key are skipped deliberately, because an index of anonymous rows
     * answers nothing; this is where that decision is surfaced instead of being silent.
     *
     * <p>A warning rather than an error. A pipeline may legitimately index only the records that do
     * have keys, and blocking publication over it would be the platform overruling its user — but
     * saying nothing would be the platform hiding from them.
     */
    private List<ValidationIssue> validateRecordIdentity(TenantId tenantId, PipelineVersion version) {
        if (!version.auditPolicy().level().indexesEveryRecord()) {
            return List.of();
        }

        List<NodeDefinition> sources = version.definition().nodesOfType(NodeType.SOURCE);
        List<ValidationIssue> issues = new ArrayList<>();

        for (NodeDefinition node : sources) {
            if (!node.hasConnector()) {
                continue;
            }
            ConnectorInstance instance = connectors
                    .findById(tenantId, ConnectorInstanceId.of(node.connectorInstanceId()))
                    .orElse(null);
            if (instance == null) {
                continue;  // Already reported as CONNECTOR_NOT_FOUND.
            }

            if (!connectorCapabilities.identifiesRecords(
                    instance.connectorType(), instance.config())) {

                issues.add(new ValidationIssue(
                        ValidationIssue.Severity.WARNING, "NO_RECORD_IDENTITY",
                        "This pipeline indexes every record so they can be searched later, but '"
                                + instance.name() + "' does not say which field identifies a "
                                + "record. Records without an identity are skipped, so the index "
                                + "would stay empty and a search would report them as never "
                                + "transferred. Set the record key on the connector, or lower the "
                                + "audit level.",
                        node.id(), null));
            }
        }
        return issues;
    }

    /**
     * Checks the three sizes describe a shape that can exist.
     *
     * <p>A chunk is the work one worker takes; a batch and a fetch are bites inside it. When either
     * bite was larger than the chunk the setting silently stopped meaning anything — a chunk of 100
     * with a batch of 1,000 wrote one batch of 100, and the same pipeline with a fetch of 500
     * pulled five hundred rows across the network to use a hundred. Both ran, both looked
     * configured, and neither did what the numbers on the screen said.
     *
     * <p>The executor clamps these anyway, so nothing here changes what a run does. The point is to
     * say so at publish time, while somebody is looking at the field they got wrong.
     */
    /**
     * Steps that were placed on the canvas and never filled in.
     *
     * <p>Caught here rather than when a run resolves, because the engine's refusal arrives after
     * the version is published, after the run is created, on a worker — and by then the pipeline
     * looks correct to everybody who approved it. An empty mapper would send empty records; an
     * empty validation is a step that silently does nothing, which is worse than no step because
     * the canvas says the records were checked.
     */
    private List<ValidationIssue> validateConfiguredSteps(PipelineDefinition definition) {
        List<ValidationIssue> issues = new ArrayList<>();

        for (NodeDefinition node : definition.nodesOfType(NodeType.MAPPER)) {
            if (!node.config().path("mappings").isArray()
                    || node.config().path("mappings").isEmpty()) {
                issues.add(new ValidationIssue(ValidationIssue.Severity.ERROR, "MAPPER_EMPTY",
                        "'" + node.name() + "' maps no fields, so it would hand an empty record to "
                                + "the next step. Add at least one mapping, or remove the step.",
                        node.id(), null));
            }
        }

        for (NodeDefinition node : definition.nodesOfType(NodeType.VALIDATION)) {
            if (!node.config().path("rules").isArray() || node.config().path("rules").isEmpty()) {
                issues.add(new ValidationIssue(ValidationIssue.Severity.ERROR, "VALIDATION_EMPTY",
                        "'" + node.name() + "' has no rules, so every record passes it unchecked. "
                                + "Add a rule, or remove the step — a validation that checks "
                                + "nothing is worse than none, because the canvas says otherwise.",
                        node.id(), null));
            }
        }
        return issues;
    }

    private List<ValidationIssue> validateSizes(PipelineVersion version) {
        ChunkingPolicy chunking = version.chunkingPolicy();
        int rowsPerChunk = version.executionPolicy()
                .effectiveRowsPerChunk(chunking.readFetchSizeOrDefault());

        List<ValidationIssue> issues = new ArrayList<>();

        // No BATCH_LARGER_THAN_CHUNK check any more: the batch *is* the chunk, so the two
        // cannot disagree. That rule existed to catch a contradiction the model now cannot express.
        if (chunking.readFetchSize() > rowsPerChunk) {
            issues.add(new ValidationIssue(ValidationIssue.Severity.ERROR, "FETCH_LARGER_THAN_CHUNK",
                    "The read fetch is " + chunking.readFetchSize() + " records but a chunk only "
                            + "holds " + rowsPerChunk + ". Each chunk would pull "
                            + chunking.readFetchSize() + " records across the network and use "
                            + rowsPerChunk + " of them. Raise rows per chunk, or lower the fetch "
                            + "size.", null, null));
        }
        if (rowsPerChunk < MIN_SENSIBLE_CHUNK) {
            issues.add(new ValidationIssue(ValidationIssue.Severity.WARNING, "CHUNK_TOO_SMALL",
                    "Rows per chunk is " + rowsPerChunk + ". Every chunk costs a claim, a "
                            + "checkpoint, a state update and an event, so at this size most of "
                            + "the run is bookkeeping rather than data. Use at least "
                            + MIN_SENSIBLE_CHUNK + "; ten thousand is a good starting point.",
                    null, null));
        }
        return issues;
    }

    /**
     * Checks the sink can honour how the pipeline wants to be called.
     *
     * <p>Only per-record delivery can be refused, and only by a sink whose unit of work is the
     * chunk rather than the call. Offering a setting that provably does nothing is worse than not
     * offering it: the author reads the screen, believes each record is sent on its own, and only
     * finds out otherwise by inspecting the destination.
     */
    private List<ValidationIssue> validateDelivery(TenantId tenantId, PipelineVersion version) {
        if (!version.deliveryPolicy().isPerRecord()) {
            return List.of();
        }

        List<ValidationIssue> issues = new ArrayList<>();
        for (NodeDefinition node : version.definition().nodesOfType(NodeType.SINK)) {
            if (!node.hasConnector()) {
                continue;
            }
            ConnectorInstance instance = connectors
                    .findById(tenantId, ConnectorInstanceId.of(node.connectorInstanceId()))
                    .orElse(null);
            if (instance == null) {
                continue;  // Already reported as CONNECTOR_NOT_FOUND.
            }
            if (!connectorCapabilities.supportsPerRecordDelivery(instance.connectorType())) {
                issues.add(new ValidationIssue(ValidationIssue.Severity.ERROR,
                        "PER_RECORD_NOT_SUPPORTED",
                        "This pipeline asks for one record per call, but '" + instance.name()
                                + "' (" + instance.connectorType() + ") works a chunk at a time — "
                                + "records are staged and handed over once, so sending them singly "
                                + "would produce exactly the same result. Use the whole batch, and "
                                + "size the chunk instead.",
                        node.id(), null));
            }
        }
        return issues;
    }

    private List<ValidationIssue> validateConnectorReferences(TenantId tenantId, PipelineDefinition definition) {
        Set<ConnectorInstanceId> referenced = definition.nodes().stream()
                .filter(NodeDefinition::hasConnector)
                .map(node -> ConnectorInstanceId.of(node.connectorInstanceId()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (referenced.isEmpty()) {
            return List.of();
        }

        // One bulk lookup rather than one per node: publish latency should not scale with the
        // number of nodes on the canvas.
        Map<ConnectorInstanceId, ConnectorInstance> found = connectors
                .findAllById(tenantId, referenced).stream()
                .collect(Collectors.toMap(ConnectorInstance::id, Function.identity()));

        List<ValidationIssue> issues = new ArrayList<>();
        for (NodeDefinition node : definition.nodes()) {
            if (!node.hasConnector()) {
                continue;
            }
            ConnectorInstance instance = found.get(ConnectorInstanceId.of(node.connectorInstanceId()));
            if (instance == null) {
                issues.add(ValidationIssue.errorAtNode("CONNECTOR_NOT_FOUND",
                        "Node '" + node.id() + "' references connector instance "
                                + node.connectorInstanceId() + ", which does not exist",
                        node.id()));
                continue;
            }
            try {
                instance.requireUsableAs(node.type());
            } catch (DmpException e) {
                issues.add(ValidationIssue.errorAtNode("CONNECTOR_NOT_USABLE", e.getMessage(), node.id()));
            }
        }
        return issues;
    }

    private Pipeline requirePipeline(TenantId tenantId, PipelineId id) {
        return pipelines.findById(tenantId, id)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Pipeline not found", Map.of("pipelineId", id.toString())));
    }

    private PipelineVersion requireVersion(TenantId tenantId, PipelineVersionId id) {
        return versions.findById(tenantId, id)
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Pipeline version not found", Map.of("versionId", id.toString())));
    }

    private void audit(TenantId tenantId, AuditAction action, PipelineVersion subject,
                       PipelineVersion before, PipelineVersion after, String summary, Instant now) {
        auditLog.record(AuditEntry.of(tenantId, tenantContext.currentActor(), action,
                RESOURCE_TYPE, subject.id().toString(), summary,
                before == null ? null : Json.mapper().valueToTree(before),
                after == null ? null : Json.mapper().valueToTree(after),
                now));
    }

    public record CreateVersion(PipelineDefinition definition, ChunkingPolicy chunkingPolicy,
                                ExecutionPolicy executionPolicy, AuditPolicy auditPolicy,
                                DeliveryPolicy deliveryPolicy,
                                PipelineMode mode, String changeNote) {
    }

    /** Any field left null is left as it was, so a caller may change one policy without the rest. */
    public record UpdatePolicies(ChunkingPolicy chunkingPolicy, ExecutionPolicy executionPolicy,
                                 com.dmp.domain.audit.AuditPolicy auditPolicy,
                                 DeliveryPolicy deliveryPolicy, PipelineMode mode) {
    }
}
