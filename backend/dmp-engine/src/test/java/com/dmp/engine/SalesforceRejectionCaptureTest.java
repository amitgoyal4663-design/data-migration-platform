package com.dmp.engine;

import com.dmp.common.json.Json;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.audit.StageLogPolicy;
import com.dmp.domain.audit.RecordAuditLevel;
import com.dmp.domain.audit.RedactionMode;
import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.connector.ConnectorInstanceStatus;
import com.dmp.domain.pipeline.EdgeDefinition;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.pipeline.PipelineDefinition;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.tenant.TenantId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A destination that cannot name the records it refused must not be given a dead-letter queue.
 *
 * <p>Salesforce reports how many records a bulk job rejected in the status the engine already
 * polls, and names <em>which</em> ones only in a separate results file holding every rejected row.
 * The connector therefore reports the count and not the list, which means a queue for its
 * rejections could only ever be empty — and an empty queue is worse than none, because it invites
 * somebody to look for records there, find nothing, and conclude the platform lost them.
 *
 * <p>The enforcement lives at resolution rather than at save time on purpose. A policy stored
 * before the rule existed, or written straight to the API by something that never saw the console,
 * still cannot make the engine promise what the destination cannot deliver.
 */
class SalesforceRejectionCaptureTest {

    private static final Instant NOW = Instant.parse("2026-08-09T04:00:00Z");

    @Test
    void aSalesforceSinkCannotKeepRejectedRecordsHoweverThePolicyWasStored() {
        // Deliberately the awkward case: capture explicitly switched on, as an API caller could.
        AuditPolicy insists = new AuditPolicy(RecordAuditLevel.ERRORS, Set.of(), RedactionMode.HASH,
                Duration.ofDays(30), 10, 32_768, false, true, StageLogPolicy.OFF);

        ResolvedPipeline resolved = resolve("salesforce", insists);

        assertThat(resolved.audit().capturesRejectedPayloads())
                .as("no stored value may make the engine keep records the destination never names")
                .isFalse();
    }

    @Test
    void theRestOfTheAuditPolicySurvivesUntouched() {
        AuditPolicy indexed = new AuditPolicy(RecordAuditLevel.INDEXED, Set.of("/email"),
                RedactionMode.HASH, Duration.ofDays(30), 10, 32_768, false, true, StageLogPolicy.OFF);

        ResolvedPipeline resolved = resolve("salesforce", indexed);

        assertThat(resolved.audit().level())
                .as("a per-record index this sink can never complete is not kept half-written: "
                        + "entries would sit at SENT for the life of the index, reading as an "
                        + "answer without being one")
                .isEqualTo(RecordAuditLevel.ERRORS);
        assertThat(resolved.audit().indexesPayloads())
                .as("and nothing indexes payloads underneath a level that indexes nothing")
                .isFalse();
        assertThat(resolved.audit().redactedFields())
                .as("redaction is a property of the data, not of the destination")
                .containsExactly("/email");
    }

    @Test
    void everyOtherSinkKeepsWhateverThePipelineAskedFor() {
        AuditPolicy wantsCapture = new AuditPolicy(RecordAuditLevel.ERRORS, Set.of(),
                RedactionMode.HASH, Duration.ofDays(30), 10, 32_768, false, true, StageLogPolicy.OFF);

        assertThat(resolve("mongodb", wantsCapture).audit().capturesRejectedPayloads())
                .as("a sink that reports rejections per record is not affected by this rule")
                .isTrue();
    }

    @Test
    void aPipelineThatSwitchedCaptureOffIsLeftOff() {
        AuditPolicy off = AuditPolicy.DEFAULT.withoutRejectedPayloads();

        assertThat(resolve("mongodb", off).audit().capturesRejectedPayloads()).isFalse();
    }

    // ------------------------------------------------------------------ setup

    private static ResolvedPipeline resolve(String sinkConnectorType, AuditPolicy audit) {
        TenantId tenantId = TenantId.newId();

        ConnectorInstance source = instance(tenantId, "mongodb", "source");
        ConnectorInstance sink = instance(tenantId, sinkConnectorType, "sink");

        NodeDefinition sourceNode = new NodeDefinition("src", NodeType.SOURCE, "Source",
                source.id().value(), Json.emptyObject());
        NodeDefinition sinkNode = new NodeDefinition("dst", NodeType.SINK, "Sink",
                sink.id().value(), Json.emptyObject());

        PipelineVersion version = PipelineVersion.createDraft(
                PipelineId.newId(), tenantId, 1,
                new PipelineDefinition(List.of(sourceNode, sinkNode),
                        List.of(new EdgeDefinition("e1", "src", "dst", null))),
                ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT, audit,
                PipelineMode.FULL_LOAD, null, "tester", NOW);

        return ResolvedPipeline.resolve(version, Map.of(
                source.id().toString(), source,
                sink.id().toString(), sink));
    }

    private static ConnectorInstance instance(TenantId tenantId, String type, String name) {
        return new ConnectorInstance(ConnectorInstanceId.newId(), tenantId, name, type,
                ConnectorDirection.BOTH, Json.emptyObject(), Json.emptyObject(),
                ConnectorInstanceStatus.ACTIVE, null, null, null, NOW, NOW, 1);
    }
}
