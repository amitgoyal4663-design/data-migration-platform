package com.dmp.engine;

import com.dmp.application.port.out.StageLogPort;
import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.application.port.out.RecordErrorPort;
import com.dmp.application.port.out.RecordIndexPort;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.RecordBatch;
import com.dmp.connector.api.Sink;
import com.dmp.connector.api.Source;
import com.dmp.connector.runtime.ConnectorContexts;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.connector.ConnectorInstanceStatus;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.DeliveryPolicy;
import com.dmp.domain.pipeline.EdgeDefinition;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.pipeline.PipelineDefinition;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersion;
import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.Split;
import com.dmp.domain.tenant.TenantId;
import com.dmp.transform.api.RecordTransform;
import com.dmp.transform.api.TransformFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Why records were rejected is recorded whether or not the records themselves are.
 *
 * <p>These are two settings' worth of behaviour that used to be one. Turning off rejected-payload
 * capture — a reasonable choice, and the only one available for data nobody may keep a copy of —
 * also silently turned off the error code and the error message, because both were written past an
 * early return.
 *
 * <p>The result was a run that reported "40 of 40 record(s) were rejected (100%)" with an empty
 * dead-letter queue and no signature: no code, no message, nothing anywhere to say what the
 * destination had objected to. The percentage is not a diagnosis. A reason costs one row per
 * distinct fault however many million records hit it, which is the cheapest thing in this system
 * and the first thing anybody asks for.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RejectionReasonTest {

    private static final Instant NOW = Instant.parse("2026-08-20T10:00:00Z");

    @Mock private ConnectorRegistry connectors;
    @Mock private CheckpointRepository checkpoints;
    @Mock private SplitRepository splits;
    @Mock private RecordErrorPort recordErrors;
    @Mock private RecordIndexPort recordIndex;
    @Mock private StageLogPort stageLog;
    @Mock private TransformFactory transforms;
    @Mock private ReplaySource replaySource;

    private ChunkExecutor executor;
    private TenantId tenantId;
    private RunId runId;
    private Split split;

    @BeforeEach
    void setUp() {
        tenantId = TenantId.newId();
        runId = RunId.newId();
        split = Split.plan(runId, tenantId, 0, Json.emptyObject(), NOW)
                .claim("worker-1", NOW, Duration.ofMinutes(5));

        executor = new ChunkExecutor(connectors, new ConnectorContexts(List.of()), checkpoints,
                splits, recordErrors, recordIndex, stageLog, transforms, replaySource,
                Clock.fixed(NOW, ZoneOffset.UTC));

        when(checkpoints.findOrCreate(any(), any(), any())).thenAnswer(call ->
                Checkpoint.initial(split.id(), runId, tenantId, NOW));
        when(checkpoints.save(any())).thenAnswer(call -> call.getArgument(0));
        when(splits.heartbeat(any(), any(), anyString(), any(), any()))
                .thenAnswer(call -> Optional.empty());
        when(connectors.source(anyString())).thenReturn(new FixedSource(40));
        when(connectors.sink(anyString())).thenReturn(new RefusingSink());
        when(transforms.compile(any())).thenReturn(RecordTransform.IDENTITY);
        when(recordErrors.reserveSamples(any(), anyLong(), anyInt(), anyInt(), any(), any()))
                .thenAnswer(call -> (int) call.getArgument(2));
    }

    @Test
    @DisplayName("a pipeline that keeps no rejected payloads still records the reason")
    void reasonSurvivesWithoutPayloads() {
        assertThatThrownBy(() -> run(AuditPolicy.DEFAULT.withoutRejectedPayloads()))
                .isInstanceOf(RejectionThresholdExceededException.class);

        ArgumentCaptor<RecordErrorPort.SignatureKey> key =
                ArgumentCaptor.forClass(RecordErrorPort.SignatureKey.class);
        ArgumentCaptor<Long> occurrences = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Integer> wanted = ArgumentCaptor.forClass(Integer.class);

        verify(recordErrors).reserveSamples(key.capture(), occurrences.capture(), wanted.capture(),
                anyInt(), any(), any());

        assertThat(key.getValue().code())
                .as("the destination's own code, which is the whole diagnosis")
                .isEqualTo("INVALID_FIELD");
        assertThat(key.getValue().message())
                .as("and its message, normalised into a signature")
                .contains("No such column");
        assertThat(occurrences.getValue())
                .as("counted in full — the count was never the part at risk")
                .isEqualTo(40L);
        assertThat(wanted.getValue())
                .as("no payloads wanted: that, and only that, is what the setting turns off")
                .isZero();

        verify(recordErrors, never()).recordAll(any());
    }

    @Test
    @DisplayName("keeping payloads stores the records as well as the reason")
    void payloadsStoredWhenAskedFor() {
        assertThatThrownBy(() -> run(AuditPolicy.DEFAULT))
                .isInstanceOf(RejectionThresholdExceededException.class);

        ArgumentCaptor<Integer> wanted = ArgumentCaptor.forClass(Integer.class);
        verify(recordErrors).reserveSamples(any(), anyLong(), wanted.capture(), anyInt(), any(),
                any());
        assertThat(wanted.getValue()).isEqualTo(40);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<RecordErrorPort.RecordErrorEntry>> stored =
                ArgumentCaptor.forClass(List.class);
        verify(recordErrors).recordAll(stored.capture());

        assertThat(stored.getValue()).hasSize(40);
        assertThat(stored.getValue()).allSatisfy(entry -> {
            assertThat(entry.code()).isEqualTo("INVALID_FIELD");
            assertThat(entry.message()).contains("No such column");
        });
    }

    // ------------------------------------------------------------------ setup

    private ChunkResult run(AuditPolicy audit) {
        return executor.execute(pipeline(audit), split, "worker-1");
    }

    private ResolvedPipeline pipeline(AuditPolicy audit) {
        ConnectorInstance instance = new ConnectorInstance(
                ConnectorInstanceId.newId(), tenantId, "store", "mongodb",
                ConnectorDirection.BOTH, Json.emptyObject(), Json.emptyObject(),
                ConnectorInstanceStatus.ACTIVE, null, null, null, NOW, NOW, 1);

        NodeDefinition source = new NodeDefinition("src", NodeType.SOURCE, "Source",
                instance.id().value(), Json.emptyObject());
        NodeDefinition sinkNode = new NodeDefinition("dst", NodeType.SINK, "Sink",
                instance.id().value(), Json.emptyObject());

        PipelineVersion version = new PipelineVersion(
                com.dmp.domain.pipeline.PipelineVersionId.newId(), PipelineId.newId(), tenantId, 1,
                com.dmp.domain.pipeline.PipelineVersionStatus.DRAFT,
                new PipelineDefinition(List.of(source, sinkNode),
                        List.of(EdgeDefinition.of("e1", "src", "dst"))),
                ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT, audit, DeliveryPolicy.DEFAULT,
                PipelineMode.FULL_LOAD, null, "tester", NOW, null);

        return new ResolvedPipeline(version, source, instance, sinkNode, instance, List.of(),
                ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT, audit, Json.emptyObject());
    }

    /** Refuses every record with one and the same fault, as a misconfigured mapping would. */
    private static final class RefusingSink implements Sink {

        @Override
        public ConnectorSpec spec() {
            return null;
        }

        @Override
        public void testConnection(ConnectorContext context) {
        }

        @Override
        public SinkSession openSink(ConnectorContext context) {
            return new SinkSession() {
                @Override
                public Capabilities capabilities() {
                    return new Capabilities(true, null, false, false, false, true, 0, 0);
                }

                @Override
                public WriteResult write(RecordBatch batch) {
                    List<RecordError> errors = batch.records().stream()
                            .map(record -> new RecordError(record.seq(), record.key(),
                                    "INVALID_FIELD",
                                    "No such column 'Xyz__c' on entity Account",
                                    record.payload()))
                            .toList();
                    return WriteResult.partial(0, batch.totalBytes(), errors);
                }

                @Override
                public void close() {
                }
            };
        }
    }

    /** Emits a fixed number of keyed records, then stops. */
    private static final class FixedSource implements Source {

        private final int count;

        FixedSource(int count) {
            this.count = count;
        }

        @Override
        public ConnectorSpec spec() {
            return null;
        }

        @Override
        public void testConnection(ConnectorContext context) {
        }

        @Override
        public SourceSession openSource(ConnectorContext context) {
            return new SourceSession() {
                @Override
                public List<SplitSpec> plan(Preparation preparation, PlanRequest request) {
                    return List.of(SplitSpec.single());
                }

                @Override
                public RecordStream read(SplitSpec split, JsonNode from, int fetchSize) {
                    return new RecordStream() {
                        private int emitted;

                        @Override
                        public DataRecord next() {
                            if (emitted >= count) {
                                return null;
                            }
                            emitted++;
                            ObjectNode payload = Json.newObject();
                            payload.put("n", emitted);
                            return DataRecord.of(payload, String.valueOf(emitted), emitted);
                        }

                        @Override
                        public JsonNode cursor() {
                            return Json.emptyObject();
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
            };
        }
    }
}
