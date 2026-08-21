package com.dmp.persistence.postgres;

import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.DeliveryPolicy;
import com.dmp.domain.pipeline.EdgeDefinition;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineDefinition;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineStatus;
import com.dmp.domain.pipeline.PipelineValidator;
import com.dmp.domain.pipeline.PipelineVersion;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for the PostgreSQL definition store. */
class PipelinePersistenceIT extends AbstractPostgresIT {

    private static final Instant NOW = Instant.parse("2026-08-07T00:00:00Z");

    @Autowired
    private PipelineRepository pipelines;

    @Autowired
    private PipelineVersionRepository versions;

    private final PipelineValidator validator = new PipelineValidator();

    private Pipeline newPipeline(String name) {
        return pipelines.save(Pipeline.create(tenantId, name, "desc", "/finance/daily",
                Set.of("finance", "daily"), NOW));
    }

    private PipelineDefinition validDefinition() {
        UUID connector = UUID.randomUUID();
        return new PipelineDefinition(
                List.of(new NodeDefinition("src", NodeType.SOURCE, "Source", connector, null),
                        new NodeDefinition("dst", NodeType.SINK, "Sink", connector, null)),
                List.of(EdgeDefinition.of("e1", "src", "dst")));
    }

    @Test
    @DisplayName("round-trips a pipeline including its JSONB tag array")
    void roundTrip() {
        Pipeline saved = newPipeline("Orders to Databricks");

        Pipeline loaded = pipelines.findById(tenantId, saved.id()).orElseThrow();

        assertThat(loaded.name()).isEqualTo("Orders to Databricks");
        assertThat(loaded.tags()).containsExactlyInAnyOrder("finance", "daily");
        assertThat(loaded.folder()).isEqualTo("/finance/daily");
        assertThat(loaded.status()).isEqualTo(PipelineStatus.DRAFT);
    }

    @Test
    @DisplayName("scopes every lookup by tenant")
    void tenantIsolation() {
        Pipeline saved = newPipeline("Tenant scoped");
        var otherTenant = com.dmp.domain.tenant.TenantId.newId();

        // A pipeline must be invisible to a tenant that does not own it, even given its exact id.
        assertThat(pipelines.findById(otherTenant, saved.id())).isEmpty();
    }

    @Test
    @DisplayName("matches tags conjunctively through the JSONB containment operator")
    void tagSearchRequiresEveryTag() {
        newPipeline("Both tags");
        pipelines.save(Pipeline.create(tenantId, "One tag", null, null, Set.of("finance"), NOW));

        var bothTags = pipelines.search(tenantId,
                new PipelineRepository.PipelineSearch(null, null, Set.of("finance", "daily"), null),
                PageQuery.firstPage());

        assertThat(bothTags.content()).extracting(Pipeline::name).containsExactly("Both tags");
    }

    @Test
    @DisplayName("searches names case-insensitively")
    void nameSearchIsCaseInsensitive() {
        newPipeline("Salesforce Nightly Load");

        var result = pipelines.search(tenantId,
                new PipelineRepository.PipelineSearch("salesforce", null, Set.of(), null),
                PageQuery.firstPage());

        assertThat(result.content()).hasSize(1);
    }

    @Test
    @DisplayName("rejects a duplicate name within a tenant")
    void duplicateNameRejected() {
        newPipeline("Unique name");

        assertThatThrownBy(() -> newPipeline("Unique name"))
                .isInstanceOf(DmpException.class)
                .satisfies(e -> assertThat(((DmpException) e).errorCode()).isEqualTo(ErrorCode.DUPLICATE));
    }

    @Test
    @DisplayName("detects a concurrent modification through the version column")
    void optimisticLocking() {
        Pipeline saved = newPipeline("Contended");

        // Two independent edits of the same loaded state — two browser tabs, or two API clients.
        Pipeline firstEdit = saved.updateMetadata("First", null, null, Set.of(), NOW);
        Pipeline secondEdit = saved.updateMetadata("Second", null, null, Set.of(), NOW);

        pipelines.save(firstEdit);

        assertThatThrownBy(() -> pipelines.save(secondEdit))
                .isInstanceOf(DmpException.class)
                .satisfies(e -> assertThat(((DmpException) e).errorCode())
                        .isEqualTo(ErrorCode.CONCURRENT_MODIFICATION));
    }

    @Test
    @DisplayName("round-trips a version's DAG and all three policies through JSONB")
    void versionPoliciesRoundTrip() {
        Pipeline pipeline = newPipeline("With policies");
        pipelines.save(pipeline.withNewVersion(1, NOW));

        var chunking = new ChunkingPolicy(100, 8L * 1024 * 1024, Duration.ofSeconds(5), 2,
                ChunkingPolicy.CHECKPOINT_AUTO);
        var execution = ExecutionPolicy.sequential();

        PipelineVersion saved = versions.save(PipelineVersion.createDraft(
                pipeline.id(), tenantId, 1, validDefinition(), chunking, execution,
                AuditPolicy.DEFAULT, PipelineMode.FULL_LOAD, "initial", "tester", NOW));

        PipelineVersion loaded = versions.findById(tenantId, saved.id()).orElseThrow();

        assertThat(loaded.definition().nodes()).hasSize(2);
        assertThat(loaded.definition().edges()).hasSize(1);
        assertThat(loaded.chunkingPolicy().readFetchSize()).isEqualTo(100);
        // Duration survives the JSONB round trip as ISO-8601 rather than becoming a number of
        // unspecified units.
        assertThat(loaded.chunkingPolicy().flushInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(loaded.executionPolicy().isSequential()).isTrue();
        assertThat(loaded.executionPolicy().chunkLease()).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    @DisplayName("blocks any update to a published version at the database")
    void publishedVersionIsImmutableInTheDatabase() {
        Pipeline pipeline = newPipeline("Immutable version");
        pipelines.save(pipeline.withNewVersion(1, NOW));

        PipelineVersion draft = versions.save(PipelineVersion.createDraft(
                pipeline.id(), tenantId, 1, validDefinition(), ChunkingPolicy.DEFAULT,
                ExecutionPolicy.DEFAULT, AuditPolicy.DEFAULT, PipelineMode.FULL_LOAD,
                "initial", "tester", NOW));

        PipelineVersion published = versions.save(draft.publish(validator, NOW));
        assertThat(published.isPublished()).isTrue();

        // The domain refuses this, but the trigger is what makes the guarantee hold against
        // anything that bypasses the domain — a migration script, a manual fix, a future bug.
        assertThatThrownBy(() -> versions.save(
                new PipelineVersion(published.id(), published.pipelineId(), tenantId, 1,
                        published.status(), PipelineDefinition.empty(), ChunkingPolicy.DEFAULT,
                        ExecutionPolicy.DEFAULT, AuditPolicy.DEFAULT, DeliveryPolicy.DEFAULT,
                        PipelineMode.FULL_LOAD, "tampered", "tester", NOW, NOW)))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("refuses to delete a published version")
    void publishedVersionCannotBeDeleted() {
        Pipeline pipeline = newPipeline("Undeletable version");
        pipelines.save(pipeline.withNewVersion(1, NOW));

        PipelineVersion published = versions.save(versions.save(PipelineVersion.createDraft(
                pipeline.id(), tenantId, 1, validDefinition(), ChunkingPolicy.DEFAULT,
                ExecutionPolicy.DEFAULT, AuditPolicy.DEFAULT, PipelineMode.FULL_LOAD,
                "initial", "tester", NOW)).publish(validator, NOW));

        assertThatThrownBy(() -> versions.deleteDraft(tenantId, published.id()))
                .isInstanceOf(DmpException.class)
                .satisfies(e -> assertThat(((DmpException) e).errorCode()).isEqualTo(ErrorCode.IMMUTABLE));
    }

    @Test
    @DisplayName("never reissues a version number after a draft is deleted")
    void versionNumbersAreNeverReused() {
        // Counting rows would reissue 2 after deleting draft 2, and a run referencing "version 2"
        // would then point at different content than it executed.
        Pipeline pipeline = newPipeline("Version numbering");
        pipelines.save(pipeline.withNewVersion(1, NOW));

        PipelineVersion first = versions.save(PipelineVersion.createDraft(
                pipeline.id(), tenantId, 1, validDefinition(), ChunkingPolicy.DEFAULT,
                ExecutionPolicy.DEFAULT, AuditPolicy.DEFAULT, PipelineMode.FULL_LOAD, null, "t", NOW));

        versions.save(PipelineVersion.createDraft(
                pipeline.id(), tenantId, 2, validDefinition(), ChunkingPolicy.DEFAULT,
                ExecutionPolicy.DEFAULT, AuditPolicy.DEFAULT, PipelineMode.FULL_LOAD, null, "t", NOW));

        assertThat(versions.highestVersionNumber(tenantId, pipeline.id())).isEqualTo(2);

        versions.deleteDraft(tenantId, first.id());

        assertThat(versions.highestVersionNumber(tenantId, pipeline.id())).isEqualTo(2);
    }
}
