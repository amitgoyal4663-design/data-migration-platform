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
import com.dmp.domain.audit.StageLogPolicy;
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
import org.junit.jupiter.api.Nested;
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
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * What the platform records about the calls it makes.
 *
 * <p>Every other log in this system is about a record. None of them were about a <em>call</em>,
 * and the gap had a cost that was easy to state and hard to work around: a destination handed five
 * hundred records in one request, refusing all of them, produced five hundred rejections and no
 * status code. The one fact that explained the other five hundred was the one the platform threw
 * away.
 *
 * <p>Off by default and stays off. A diagnostic that quietly started writing an index on upgrade
 * would be spending storage on a decision nobody made — so these tests pin the silence as firmly
 * as they pin the content.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StageLogTest {

    private static final Instant NOW = Instant.parse("2026-08-20T10:00:00Z");

    @Mock private ConnectorRegistry connectors;
    @Mock private CheckpointRepository checkpoints;
    @Mock private SplitRepository splits;
    @Mock private RecordErrorPort recordErrors;
    @Mock private RecordIndexPort recordIndex;
    @Mock private TransformFactory transforms;
    @Mock private ReplaySource replaySource;

    private final CollectingStageLog stages = new CollectingStageLog();
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
                splits, recordErrors, recordIndex, stages, transforms, replaySource,
                Clock.fixed(NOW, ZoneOffset.UTC));

        when(checkpoints.findOrCreate(any(), any(), any())).thenAnswer(call ->
                Checkpoint.initial(split.id(), runId, tenantId, NOW));
        when(checkpoints.save(any())).thenAnswer(call -> call.getArgument(0));
        when(splits.heartbeat(any(), any(), anyString(), any(), any()))
                .thenAnswer(call -> Optional.empty());
        when(connectors.source(anyString())).thenReturn(new FixedSource(10));
        when(connectors.sink(anyString())).thenReturn(new AcceptingSink());
        when(transforms.compile(any())).thenReturn(RecordTransform.IDENTITY);
    }

    @Nested
    @DisplayName("off unless the pipeline asked for it")
    class OffByDefault {

        @Test
        @DisplayName("a pipeline with the default audit policy logs no calls at all")
        void nothingByDefault() {
            run(StageLogPolicy.OFF);
            assertThat(stages.entries).isEmpty();
        }

        @Test
        @DisplayName("switching on writes only logs writes")
        void oneSideOnly() {
            run(new StageLogPolicy(false, false, true, false));

            assertThat(stages.entries)
                    .isNotEmpty()
                    .allSatisfy(entry -> assertThat(entry.stage())
                            .isEqualTo(StageLogPort.Stage.WRITE));
        }

        @Test
        @DisplayName("switching on reads only logs reads")
        void otherSideOnly() {
            run(new StageLogPolicy(true, false, false, false));

            assertThat(stages.entries)
                    .isNotEmpty()
                    .allSatisfy(entry -> assertThat(entry.stage())
                            .isEqualTo(StageLogPort.Stage.READ));
        }

        @Test
        @DisplayName("bodies alone is refused, because there is nothing to attach them to")
        void bodiesNeedALog() {
            assertThatThrownBy(() -> new StageLogPolicy(false, false, false, true))
                    .hasMessageContaining("at least one");
        }
    }

    @Nested
    @DisplayName("what an entry carries")
    class Content {

        @Test
        @DisplayName("a write records the batch, the timing, and what the destination said")
        void writeEntry() {
            run(new StageLogPolicy(false, false, true, false));

            StageLogPort.StageEntry entry = stages.entries.get(0);
            assertThat(entry.recordsIn()).isEqualTo(10);
            assertThat(entry.outcome()).isEqualTo(StageLogPort.Outcome.OK);
            assertThat(entry.attempt()).isEqualTo(split.attempt());
            assertThat(entry.details().path("jobId").asText())
                    .as("the connector's own answer, which no count could contain")
                    .isEqualTo("job-77");
        }

        @Test
        @DisplayName("a read records the query the connector actually ran")
        void readEntry() {
            run(new StageLogPolicy(true, false, false, false));

            StageLogPort.StageEntry entry = stages.entries.get(0);
            assertThat(entry.query())
                    .as("the answer to 'why did this move nothing', which used to be discarded")
                    .isEqualTo("db.things.find({ tenant: 1 })");
            assertThat(entry.recordsIn()).isEqualTo(10);
        }

        @Test
        @DisplayName("a call the destination refused outright is an entry of its own")
        void failedCallIsRecorded() {
            when(connectors.sink(anyString())).thenReturn(new ExplodingSink());

            assertThatThrownBy(() -> run(new StageLogPolicy(false, false, true, false)))
                    .isInstanceOf(RuntimeException.class);

            assertThat(stages.entries).hasSize(1);
            StageLogPort.StageEntry entry = stages.entries.get(0);
            assertThat(entry.outcome()).isEqualTo(StageLogPort.Outcome.FAILED);
            assertThat(entry.errorMessage())
                    .as("the sentence that explains every rejection in the batch")
                    .contains("service unavailable");
            assertThat(entry.recordsIn())
                    .as("and how many records were in the request that failed")
                    .isEqualTo(10);
        }

        @Test
        @DisplayName("bodies are absent unless asked for, and present when they are")
        void bodiesFollowTheSwitch() {
            run(new StageLogPolicy(false, false, true, false));
            assertThat(stages.entries.get(0).request())
                    .as("customer data is not stored on a diagnostic nobody switched on")
                    .isNull();

            stages.entries.clear();

            run(new StageLogPolicy(false, false, true, true));
            assertThat(stages.entries.get(0).request())
                    .as("the records as they went over the wire")
                    .hasSize(10);
        }
    }


    @Nested
    @DisplayName("one trace id ties a cycle together")
    class Tracing {

        @Test
        @DisplayName("the read, the transform and the write of one batch share a trace")
        void oneCycleOneTrace() {
            when(transforms.compile(any())).thenReturn(droppingEveryOther());

            run(new StageLogPolicy(true, true, true, false));

            // Ten records, batch of ten, so one cycle: three stages, one trace.
            assertThat(stages.entries).extracting(StageLogPort.StageEntry::traceId)
                    .as("read, transform and write are one story, not three lists")
                    .containsOnly(StageLogPort.Trace.of(split.id(), 0));

            assertThat(stages.entries).extracting(e -> e.stage().name())
                    .as("and they are in the order the work happened")
                    .containsExactly("READ", "TRANSFORM", "WRITE");
        }

        @Test
        @DisplayName("the transform stage is where the record count changes")
        void transformShowsTheDrop() {
            when(transforms.compile(any())).thenReturn(droppingEveryOther());

            run(new StageLogPolicy(true, true, true, false));

            StageLogPort.StageEntry read = stageOf(StageLogPort.Stage.READ);
            StageLogPort.StageEntry transform = stageOf(StageLogPort.Stage.TRANSFORM);
            StageLogPort.StageEntry write = stageOf(StageLogPort.Stage.WRITE);

            assertThat(read.recordsIn()).isEqualTo(10);
            assertThat(transform.recordsIn()).isEqualTo(10);
            assertThat(transform.recordsOut())
                    .as("the filter dropped five, and this is the only entry that says so")
                    .isEqualTo(5);
            assertThat(transform.delta()).isEqualTo(-5);
            assertThat(write.recordsIn())
                    .as("so the destination was handed five, not ten")
                    .isEqualTo(5);
        }

        @Test
        @DisplayName("a trace id names the chunk it belongs to")
        void traceNamesItsChunk() {
            run(new StageLogPolicy(true, false, false, false));

            String traceId = stages.entries.get(0).traceId();
            assertThat(StageLogPort.Trace.chunkOf(traceId))
                    .as("so narrowing a run's log to one chunk is a prefix, not a second lookup")
                    .isEqualTo(split.id().value().toString());
        }
    }

    /** A record transform that keeps every other record, so in and out differ. */
    private static RecordTransform droppingEveryOther() {
        return new RecordTransform() {
            @Override
            public List<DataRecord> applyRecord(DataRecord record) {
                return record.seq() % 2 == 1 ? List.of(record) : List.of();
            }

            @Override
            public com.dmp.transform.api.BatchResult applyBatch(List<DataRecord> records) {
                return com.dmp.transform.api.BatchResult.none();
            }

            @Override
            public List<String> split(List<DataRecord> records) {
                return List.of();
            }

            @Override
            public boolean isIdentity() {
                return false;
            }

            @Override
            public boolean hasBatchStage() {
                return false;
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

    private StageLogPort.StageEntry stageOf(StageLogPort.Stage stage) {
        return stages.entries.stream().filter(e -> e.stage() == stage).findFirst().orElseThrow();
    }

    // ------------------------------------------------------------------ setup

    private ChunkResult run(StageLogPolicy stageLogPolicy) {
        AuditPolicy audit = AuditPolicy.DEFAULT.logging(stageLogPolicy);
        return executor.execute(pipeline(audit), split, "worker-1", () -> false);
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

    /** Collects entries instead of sending them anywhere. */
    private static final class CollectingStageLog implements StageLogPort {

        final List<StageEntry> entries = new ArrayList<>();

        @Override
        public void log(List<StageEntry> batch) {
            entries.addAll(batch);
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public com.dmp.application.common.Page<StageEntry> find(
                TenantId tenantId, RunId runId, com.dmp.domain.run.SplitId splitId, Stage stage,
                com.dmp.application.common.PageQuery pageQuery) {
            return com.dmp.application.common.Page.of(entries, pageQuery, entries.size());
        }
    }

    /** Accepts everything and reports a job id, as a bulk destination would. */
    private static final class AcceptingSink implements Sink {

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
                    ObjectNode details = Json.newObject();
                    details.put("jobId", "job-77");
                    return WriteResult.allWritten(batch.size(), batch.totalBytes())
                            .withDetails(details);
                }

                @Override
                public void close() {
                }
            };
        }
    }

    /** Refuses the whole request, the way an unreachable or misconfigured target does. */
    private static final class ExplodingSink implements Sink {

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
                    throw new IllegalStateException("HTTP 503: service unavailable");
                }

                @Override
                public void close() {
                }
            };
        }
    }

    /** Emits a fixed number of records and can say what it asked for. */
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
                        public String describe() {
                            return "db.things.find({ tenant: 1 })";
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
