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
import com.dmp.transform.api.TransformSpec;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * How a buffered batch becomes calls on the sink.
 *
 * <p>Batch size used to decide two unrelated things at once: how much was buffered before a
 * checkpoint, and how much the sink was handed. So the only way to reach an API wanting one record
 * per request was {@code writeBatchSize = 1} — which also collapsed the checkpoint interval to one
 * write per record, and made the bookkeeping cost as much as the work.
 *
 * <p>They are separate now, and these tests pin what that separation must preserve. The counts a
 * run reports, the dead-letter queue, and the rejection threshold are all derived from a single
 * merged result per batch, so a batch split into fifty calls has to be indistinguishable from one
 * call as far as every one of them is concerned.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BatchDeliveryTest {

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
    private RecordingSink sink;

    @BeforeEach
    void setUp() {
        tenantId = TenantId.newId();
        runId = RunId.newId();
        sink = new RecordingSink();

        executor = new ChunkExecutor(connectors, new ConnectorContexts(List.of()), checkpoints,
                splits, recordErrors, recordIndex, stageLog, transforms, replaySource,
                Clock.fixed(NOW, ZoneOffset.UTC));

        when(checkpoints.findOrCreate(any(), any(), any())).thenAnswer(call ->
                Checkpoint.initial(call.getArgument(2), runId, tenantId, NOW));
        when(checkpoints.save(any())).thenAnswer(call -> call.getArgument(0));
        when(splits.heartbeat(any(), any(), anyString(), any(), any()))
                .thenAnswer(call -> java.util.Optional.empty());
        when(connectors.sink(anyString())).thenReturn(sink);
        when(connectors.source(anyString())).thenReturn(new FixedSource(10));
        when(transforms.compile(any())).thenReturn(RecordTransform.IDENTITY);
    }

    @Test
    @DisplayName("the whole batch in one call is still the default")
    void wholeBatchByDefault() {
        ChunkResult result = run(DeliveryPolicy.DEFAULT);

        assertThat(sink.calls).as("one call carrying everything buffered").hasSize(1);
        assertThat(sink.calls.get(0)).hasSize(10);
        assertThat(result.recordsWritten()).isEqualTo(10);
    }

    @Test
    @DisplayName("one record at a time makes one call per record, and still one checkpoint")
    void perRecordDelivery() {
        ChunkResult result = run(DeliveryPolicy.DEFAULT.withGroupSize(DeliveryPolicy.PER_RECORD));

        assertThat(sink.calls).hasSize(10);
        assertThat(sink.calls).allSatisfy(call -> assertThat(call).hasSize(1));

        assertThat(result.recordsWritten())
                .as("ten calls of one is still ten records written, not ten batches")
                .isEqualTo(10);
    }

    @Test
    @DisplayName("a fixed group size divides the batch, remainder included")
    void fixedGroupSize() {
        run(DeliveryPolicy.DEFAULT.withGroupSize(3));

        assertThat(sink.calls).hasSize(4);
        assertThat(sink.calls).extracting(List::size).containsExactly(3, 3, 3, 1);
    }

    @Test
    @DisplayName("a group size at or above the batch leaves it whole rather than making one group")
    void oversizedGroupIsANoOp() {
        run(DeliveryPolicy.DEFAULT.withGroupSize(500));

        assertThat(sink.calls).hasSize(1);
        assertThat(sink.calls.get(0)).hasSize(10);
    }

    @Test
    @DisplayName("every record is written exactly once, however the batch was divided")
    void nothingIsLostOrDuplicatedByGrouping() {
        run(DeliveryPolicy.DEFAULT.withGroupSize(3));

        List<Long> delivered = sink.calls.stream()
                .flatMap(List::stream)
                .map(DataRecord::seq)
                .sorted()
                .toList();

        assertThat(delivered)
                .as("dividing a batch must not lose a record or send one twice")
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    }

    @Test
    @DisplayName("failures from every group are merged into one answer for the batch")
    void failuresAcrossGroupsAreMerged() {
        // The case that matters for the dead-letter queue and the rejection threshold: two separate
        // calls each refuse a record. Reported per call, the batch would look like two failures of
        // one; the run's accounting needs one failure of two.
        sink.rejectSeq(2);
        sink.rejectSeq(8);

        ChunkResult result = run(DeliveryPolicy.DEFAULT.withGroupSize(3));

        assertThat(result.recordsFailed())
                .as("both rejections belong to the batch that produced them")
                .isEqualTo(2);
        assertThat(result.recordsWritten()).isEqualTo(8);
        assertThat(result.recordsProduced())
                .as("written plus failed must still account for everything read")
                .isEqualTo(10);
    }

    @Test
    @DisplayName("a rejection in one group does not stop the groups after it")
    void oneBadGroupDoesNotAbandonTheRest() {
        sink.rejectSeq(1);

        ChunkResult result = run(DeliveryPolicy.DEFAULT.withGroupSize(3));

        assertThat(sink.calls).as("all four calls are still made").hasSize(4);
        assertThat(result.recordsWritten()).isEqualTo(9);
    }

    @Test
    @DisplayName("a split script decides the groups, in the order their first record appeared")
    void splitScriptGroups() {
        // Labels come from the script; the engine groups by them. Insertion order rather than hash
        // order, so the same pipeline sends the same calls in the same sequence every run.
        when(transforms.compile(any())).thenReturn(labelling(record ->
                record.payload().path("seq").asInt() % 2 == 0 ? "even" : "odd"));

        run(DeliveryPolicy.DEFAULT.withSplitScript("function split(records) { return []; }"));

        assertThat(sink.calls).hasSize(2);
        assertThat(sink.calls.get(0)).extracting(DataRecord::seq)
                .as("record 1 was first, so its group is written first")
                .containsExactly(1L, 3L, 5L, 7L, 9L);
        assertThat(sink.calls.get(1)).extracting(DataRecord::seq)
                .containsExactly(2L, 4L, 6L, 8L, 10L);
    }

    @Test
    @DisplayName("the batch transform runs once per group, not once per batch")
    void batchTransformRunsPerGroup() {
        // The reason grouping and the batch script belong together. A batch script building a
        // request envelope has to describe the records in *that* request — run once for the whole
        // batch, every call would carry a header claiming records the other calls took.
        List<Integer> sizesSeen = new ArrayList<>();
        when(transforms.compile(any())).thenReturn(envelopingEachGroup(sizesSeen));

        run(DeliveryPolicy.DEFAULT.withGroupSize(3));

        assertThat(sizesSeen)
                .as("called once per group, each time with only that group's records")
                .containsExactly(3, 3, 3, 1);

        assertThat(sink.envelopes)
                .as("each call carries its own envelope, describing its own records")
                .containsExactly(3, 3, 3, 1);
    }

    @Test
    @DisplayName("a group's envelope describes that group, not the whole batch")
    void envelopeDescribesItsOwnGroup() {
        when(transforms.compile(any())).thenReturn(envelopingEachGroup(new ArrayList<>()));

        run(DeliveryPolicy.DEFAULT.withGroupSize(4));

        assertThat(sink.envelopes).containsExactly(4, 4, 2);
        assertThat(sink.envelopes.stream().mapToInt(Integer::intValue).sum())
                .as("the counts across every envelope still add up to the batch")
                .isEqualTo(10);
    }

    // ------------------------------------------------------------------ setup

    private ChunkResult run(DeliveryPolicy delivery) {
        Split split = Split.plan(runId, tenantId, 0, Json.emptyObject(), NOW)
                .claim("worker-1", NOW, Duration.ofMinutes(5));
        return executor.execute(pipeline(delivery), split, "worker-1", () -> false);
    }

    private ResolvedPipeline pipeline(DeliveryPolicy delivery) {
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
                ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT, AuditPolicy.DEFAULT, delivery,
                PipelineMode.FULL_LOAD, null, "tester", NOW, null);

        return new ResolvedPipeline(version, source, instance, sinkNode, instance, List.of(),
                ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT, AuditPolicy.DEFAULT,
                Json.emptyObject());
    }

    /**
     * A batch stage that returns an envelope naming how many records it was given.
     *
     * <p>The size is the assertion: run per batch it would always say ten, run per group it says
     * what that group held. Recording the sizes as they arrive is what distinguishes the two.
     */
    private static RecordTransform envelopingEachGroup(List<Integer> sizesSeen) {
        return new RecordTransform() {
            @Override
            public List<DataRecord> applyRecord(DataRecord record) {
                return List.of(record);
            }

            @Override
            public com.dmp.transform.api.BatchResult applyBatch(List<DataRecord> records) {
                sizesSeen.add(records.size());
                ObjectNode envelope = Json.newObject();
                envelope.put("count", records.size());
                return com.dmp.transform.api.BatchResult.enveloping(envelope);
            }

            @Override
            public List<String> split(List<DataRecord> records) {
                return List.of();
            }

            @Override
            public boolean isIdentity() {
                return true;
            }

            @Override
            public boolean hasBatchStage() {
                return true;
            }

            @Override
            public boolean hasSplitStage() {
                return false;
            }

            @Override
            public void close() {
            }
        };
    }

    /** A transform whose only stage is a split, labelling each record by the given function. */
    private static RecordTransform labelling(java.util.function.Function<DataRecord, String> label) {
        return new RecordTransform() {
            @Override
            public List<DataRecord> applyRecord(DataRecord record) {
                return List.of(record);
            }

            @Override
            public com.dmp.transform.api.BatchResult applyBatch(List<DataRecord> records) {
                return com.dmp.transform.api.BatchResult.none();
            }

            @Override
            public List<String> split(List<DataRecord> records) {
                return records.stream().map(label).toList();
            }

            @Override
            public boolean isIdentity() {
                return true;
            }

            @Override
            public boolean hasBatchStage() {
                return false;
            }

            @Override
            public boolean hasSplitStage() {
                return true;
            }

            @Override
            public void close() {
            }
        };
    }

    /** Emits a fixed number of records, numbered from one, then stops. */
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
                public RecordStream read(SplitSpec split, com.fasterxml.jackson.databind.JsonNode from,
                                         int fetchSize) {
                    return new RecordStream() {
                        private int emitted;

                        @Override
                        public DataRecord next() {
                            if (emitted >= count) {
                                return null;
                            }
                            emitted++;
                            ObjectNode payload = Json.newObject();
                            payload.put("seq", emitted);
                            return DataRecord.of(payload, String.valueOf(emitted), emitted);
                        }

                        @Override
                        public com.fasterxml.jackson.databind.JsonNode cursor() {
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

    /**
     * Records each call separately rather than accumulating records.
     *
     * <p>The distinction under test is how many times the sink was called and with what, which a
     * flat list of everything written would hide completely.
     */
    private static final class RecordingSink implements Sink {

        private final List<List<DataRecord>> calls = new ArrayList<>();
        private final List<Integer> envelopes = new ArrayList<>();
        private final Map<Long, String> rejections = new HashMap<>();

        void rejectSeq(long seq) {
            rejections.put(seq, "REFUSED");
        }

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
                    // sendsBatchAsSinglePayload true: an envelope is refused outright otherwise,
                    // and the point here is what the envelope says.
                    return new Capabilities(true, null, false, false, false, true, 0, 0);
                }

                @Override
                public WriteResult write(RecordBatch batch) {
                    calls.add(List.copyOf(batch.records()));
                    batch.envelope().ifPresent(node -> envelopes.add(node.path("count").asInt()));

                    List<RecordError> errors = batch.records().stream()
                            .filter(record -> rejections.containsKey(record.seq()))
                            .map(record -> new RecordError(record.seq(), record.key(),
                                    rejections.get(record.seq()), "refused by the test sink",
                                    record.payload()))
                            .toList();

                    return errors.isEmpty()
                            ? WriteResult.allWritten(batch.size(), batch.totalBytes())
                            : WriteResult.partial(batch.size() - errors.size(), batch.totalBytes(),
                                    errors);
                }
            };
        }
    }
}
