package com.dmp.engine;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.RecordBatch;
import com.dmp.connector.api.Sink;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.audit.RecordAuditLevel;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.EdgeDefinition;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.pipeline.PipelineDefinition;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.tenant.TenantId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("A dry run")
class DryRunTest {

    private static final Instant NOW = Instant.parse("2026-08-22T10:00:00Z");

    @Test
    @DisplayName("accepts every record and says what it would have written")
    void acceptsEverything() {
        Sink.SinkSession session = new DryRunSink(sink()).openSink(context());

        RecordBatch batch = RecordBatch.of(List.of(
                DataRecord.of(Json.newObject().put("id", 1), 1),
                DataRecord.of(Json.newObject().put("id", 2), 2)));

        Sink.WriteResult result = session.write(batch);

        assertThat(result.written()).isEqualTo(2);
        assertThat(result.failed()).isZero();
        assertThat(result.errors()).isEmpty();
        // Said in the write's own details, so the stage log entry carries it and a timeline read
        // months later does not depend on somebody having noticed the banner on the run page.
        assertThat(result.details().path("dryRun").asBoolean()).isTrue();
        assertThat(result.details().path("wouldHaveWritten").asInt()).isEqualTo(2);
    }

    @Test
    @DisplayName("commits synchronously, so no chunk is parked waiting for a verdict")
    void neverParksAChunk() {
        Sink.SinkSession session = new DryRunSink(sink()).openSink(context());

        // An asynchronous commit would leave every chunk in WAITING_EXTERNAL, polling a
        // destination that was never asked anything and will never answer.
        assertThat(session.capabilities().commitIsAsynchronous()).isFalse();
        assertThat(session.capabilities().writeIsIdempotent()).isTrue();
    }

    @Test
    @DisplayName("answers the envelope question the way the real destination would")
    void borrowsTheRealCapabilities() {
        // The bug this exists to prevent: the stand-in declared it could not take a batch as one
        // payload, so a dry run failed a pipeline whose batch transform returns one envelope —
        // reporting a configuration error, naming the real connector, for a rule it does not have.
        DryRunSink dry = new DryRunSink(sink());

        assertThat(dry.sendsBatchAsSinglePayload()).isTrue();
        assertThat(dry.openSink(context()).capabilities().sendsBatchAsSinglePayload()).isTrue();
    }

    @Test
    @DisplayName("describes itself as the destination it rehearses, not as the stand-in")
    void borrowsTheRealSpec() {
        // A stage log reading "wrote to dry-run" would name the mechanism instead of the thing
        // being tested, and the timeline of a rehearsal should be readable as the real one.
        assertThat(new DryRunSink(sink()).spec().type()).isEqualTo("salesforce");
    }

    @Test
    @DisplayName("never indexes records, whatever the pipeline's audit policy asks for")
    void forcesTheIndexOff() {
        AuditPolicy indexing = AuditPolicy.DEFAULT.indexing(RecordAuditLevel.INDEXED, true);

        // The index answers one question — was this record transferred? — and a rehearsal's
        // entries would answer it wrongly for every record it touched, in the store somebody
        // consults precisely when they are unsure.
        assertThat(resolve(indexing, true).audit().level()).isEqualTo(RecordAuditLevel.ERRORS);
        assertThat(resolve(indexing, true).audit().indexPayloads()).isFalse();

        // And the same pipeline delivering for real is left exactly as its author set it.
        assertThat(resolve(indexing, false).audit().level()).isEqualTo(RecordAuditLevel.INDEXED);
    }

    @Test
    @DisplayName("still keeps rejected payloads, which is the main thing it is run to find")
    void keepsTheDeadLetterQueue() {
        // ERRORS rather than COUNTERS: a record a script threw on is a fact about the record and
        // the script, true whether or not anything was delivered.
        assertThat(resolve(AuditPolicy.DEFAULT.indexing(RecordAuditLevel.INDEXED, true), true)
                .audit().capturesRejectedPayloads()).isTrue();
    }

    // ------------------------------------------------------------------ setup

    /**
     * A stand-in for the real destination, which declares it takes a batch as one payload.
     *
     * <p>True of Kafka and of REST, and the reason this matters: the dry run must answer that
     * question the way the destination would, or it rejects a batch transform the real run accepts.
     */
    private static Sink sink() {
        return new Sink() {
            @Override
            public ConnectorSpec spec() {
                return new ConnectorSpec("salesforce", "Salesforce", "The real one",
                        ConnectorSpec.Direction.SINK, Json.emptyObject(), java.util.Set.of(),
                        "1.0.0");
            }

            @Override
            public boolean sendsBatchAsSinglePayload() {
                return true;
            }

            @Override
            public SinkSession openSink(ConnectorContext context) {
                throw new AssertionError(
                        "A dry run must never open the real destination — that is the whole point");
            }
        };
    }

    private static ConnectorContext context() {
        return new ConnectorContext() {
            @Override
            public com.fasterxml.jackson.databind.JsonNode config() {
                return Json.emptyObject();
            }

            @Override
            public java.util.Optional<String> secret(String name) {
                return java.util.Optional.empty();
            }

            @Override
            public String workerId() {
                return "worker-1";
            }

            @Override
            public String runId() {
                return "run-1";
            }

            @Override
            public org.slf4j.Logger log() {
                return org.slf4j.LoggerFactory.getLogger(DryRunTest.class);
            }
        };
    }

    private static ResolvedPipeline resolve(AuditPolicy audit, boolean dryRun) {
        TenantId tenantId = TenantId.newId();
        ConnectorInstance source = instance(tenantId, "mongodb", "source");
        ConnectorInstance sink = instance(tenantId, "rest", "sink");

        PipelineVersion version = PipelineVersion.createDraft(
                PipelineId.newId(), tenantId, 1,
                new PipelineDefinition(
                        List.of(new NodeDefinition("src", NodeType.SOURCE, "Source",
                                        source.id().value(), Json.emptyObject()),
                                new NodeDefinition("dst", NodeType.SINK, "Sink",
                                        sink.id().value(), Json.emptyObject())),
                        List.of(new EdgeDefinition("e1", "src", "dst", null))),
                ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT, audit,
                PipelineMode.FULL_LOAD, null, "tester", NOW);

        return ResolvedPipeline.resolve(version,
                Map.of(source.id().toString(), source, sink.id().toString(), sink),
                Json.emptyObject(), dryRun);
    }

    private static ConnectorInstance instance(TenantId tenantId, String type, String name) {
        return new ConnectorInstance(ConnectorInstanceId.newId(), tenantId, name, type,
                com.dmp.domain.connector.ConnectorDirection.BOTH, Json.emptyObject(),
                Json.emptyObject(), com.dmp.domain.connector.ConnectorInstanceStatus.ACTIVE,
                null, null, null, NOW, NOW, 0L, null);
    }
}
