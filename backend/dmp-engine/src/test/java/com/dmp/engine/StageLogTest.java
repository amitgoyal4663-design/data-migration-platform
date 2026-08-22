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
        @DisplayName("a write never stores the records it carried, however bodies is set")
        void theBatchIsNeverStoredOnTheCall() {
            // It used to be, and the cost was out of all proportion to the answer. The batch is the
            // records — already in the record index, one document each, searchable by any field
            // they contain. On the call they were one opaque blob, usually over the payload cap and
            // so stored as a truncation marker, and they crowded out the small facts a call log
            // exists for.
            run(new StageLogPolicy(false, false, true, true));

            assertThat(stages.entries.get(0).request())
                    .as("the records are in the index, not written a second time here")
                    .isNull();
            assertThat(stages.entries.get(0).details().path("jobId").asText())
                    .as("what the destination said still comes through — that is the point of the entry")
                    .isEqualTo("job-77");
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
        @DisplayName("one call buffered into two batches is one fetch and two reads")
        void fetchIsCountedOnceHoweverManyBatchesItFills() {
            // Ten rows pulled in a single call, handed to a destination that wants five per
            // request. Before FETCH existed this produced two READ entries carrying the same
            // query — indistinguishable from the query genuinely having run twice, which is what
            // everybody reading it concluded.
            when(connectors.source(anyString())).thenReturn(new OneCallSource(10));
            when(connectors.sink(anyString())).thenReturn(new AcceptingSink(5));

            run(new StageLogPolicy(true, false, true, false));

            assertThat(stages.entries).filteredOn(e -> e.stage() == StageLogPort.Stage.FETCH)
                    .as("the source was asked once, and only the connector could say so")
                    .hasSize(1)
                    .allSatisfy(fetch -> {
                        assertThat(fetch.recordsOut()).isEqualTo(10);
                        assertThat(fetch.query()).isEqualTo("GET /result/chunks/0");
                        assertThat(fetch.durationMs())
                                .as("timed by the connector, not by when the engine collected it")
                                .isEqualTo(42);
                    });

            assertThat(stages.entries).filteredOn(e -> e.stage() == StageLogPort.Stage.READ)
                    .as("while the engine filled two batches out of it")
                    .hasSize(2)
                    .extracting(StageLogPort.StageEntry::recordsIn)
                    .containsExactly(5, 5);

            assertThat(stages.entries).filteredOn(e -> e.stage() == StageLogPort.Stage.WRITE)
                    .hasSize(2);
        }

        @Test
        @DisplayName("the query is on the fetch that made it, not repeated on every read window")
        void theQueryBelongsToTheCall() {
            // One call filling two batches. The query appears once, on the call — repeating it on
            // both read windows is what made a single call look like two.
            when(connectors.source(anyString())).thenReturn(new OneCallSource(10));
            when(connectors.sink(anyString())).thenReturn(new AcceptingSink(5));

            run(new StageLogPolicy(true, false, true, false));

            assertThat(stages.entries).filteredOn(e -> e.stage() == StageLogPort.Stage.FETCH)
                    .extracting(StageLogPort.StageEntry::query)
                    .containsExactly("GET /result/chunks/0");
            assertThat(stages.entries).filteredOn(e -> e.stage() == StageLogPort.Stage.READ)
                    .as("a read window says how much it took in, not what was asked")
                    .allSatisfy(read -> assertThat(read.query()).isNull());
        }

        @Test
        @DisplayName("a source that reports no calls still puts its query on the read")
        void theQuerySurvivesForConnectorsThatReportNothing() {
            // FixedSource implements no drainFetches, so the read window is the only place its
            // query can appear. Losing it would remove the answer to "why did this move nothing?"
            run(new StageLogPolicy(true, false, false, false));

            assertThat(stages.entries).filteredOn(e -> e.stage() == StageLogPort.Stage.READ)
                    .isNotEmpty()
                    .allSatisfy(read -> assertThat(read.query())
                            .isEqualTo("db.things.find({ tenant: 1 })"));
        }

        @Test
        @DisplayName("a chunk that knows its size is read once, whatever the sink prefers")
        void plannedChunkIsOneBatch() {
            // The same ten rows and the same sink asking for five, but this time the chunk says how
            // big it is. Its size wins: a planned range is a contract, and splitting it because the
            // destination has a preference is what made the delivery setting a no-op.
            when(connectors.source(anyString())).thenReturn(new OneCallSource(10));
            when(connectors.sink(anyString())).thenReturn(new AcceptingSink(5));
            split = Split.plan(runId, tenantId, 0, Json.emptyObject(), 10, NOW)
                    .claim("worker-1", NOW, Duration.ofMinutes(5));

            run(new StageLogPolicy(true, false, true, false));

            assertThat(stages.entries).filteredOn(e -> e.stage() == StageLogPort.Stage.READ)
                    .as("one call, one read window, and the log finally says so")
                    .hasSize(1)
                    .allSatisfy(read -> assertThat(read.recordsIn()).isEqualTo(10));
        }

        @Test
        @DisplayName("a chunk that reads fewer rows than it was planned for fails")
        void aShortfallIsNotASuccess() {
            // The source answers an empty result for a chunk the manifest said held ten rows —
            // a statement that expired, a warehouse that restarted. Before this the chunk read
            // nothing, wrote nothing and completed, and the run reported success.
            when(connectors.source(anyString())).thenReturn(new OneCallSource(0));
            split = Split.plan(runId, tenantId, 0, Json.emptyObject(), 10, NOW)
                    .claim("worker-1", NOW, Duration.ofMinutes(5));

            assertThatThrownBy(() -> run(new StageLogPolicy(true, false, true, false)))
                    .isInstanceOf(ChunkShortfallException.class)
                    .hasMessageContaining("planned as 10 row(s) but read 0");
        }

        @Test
        @DisplayName("a chunk of unknown size is not held to a count it never had")
        void unknownSizeIsNotAShortfall() {
            // plannedRows is 0 — a key range, an open-ended cursor, a replay. There is nothing to
            // compare against, and inventing an expectation would fail every such chunk.
            when(connectors.source(anyString())).thenReturn(new OneCallSource(3));

            run(new StageLogPolicy(true, false, true, false));

            assertThat(stages.entries).isNotEmpty();
        }

        @Test
        @DisplayName("a connector that reports nothing still reads normally")
        void fetchIsOptional() {
            // FixedSource does not implement drainFetches. The point of the default is that a
            // connector written before this existed loses one kind of entry, not the ability to run.
            run(new StageLogPolicy(true, false, false, false));

            assertThat(stages.entries).extracting(e -> e.stage().name())
                    .containsOnly("READ");
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

        /** What it asks for per request; 0 leaves the choice to the engine, as most sinks do. */
        private final int preferredBatchSize;

        AcceptingSink() {
            this(0);
        }

        AcceptingSink(int preferredBatchSize) {
            this.preferredBatchSize = preferredBatchSize;
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
                    return new Capabilities(true, null, false, false, false, true, 0,
                            preferredBatchSize);
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
    /**
     * A source that pulls everything in one call, then hands the rows over one at a time.
     *
     * <p>Which is what a warehouse result chunk, a paged API and a database cursor all do, and what
     * makes the read log misleading: the engine only ever sees {@code next()} returning a record,
     * so the one call and the buffering are indistinguishable to it.
     */
    private static final class OneCallSource implements Source {

        private final int count;

        OneCallSource(int count) {
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
                        private final List<Fetch> fetches = new ArrayList<>();

                        @Override
                        public DataRecord next() {
                            if (emitted == 0) {
                                // The whole result, in one request, before a single record is
                                // handed out.
                                fetches.add(Fetch.ok("read result chunk 0", "GET /result/chunks/0", NOW, 42, count, 0));
                            }
                            if (emitted >= count) {
                                return null;
                            }
                            emitted++;
                            ObjectNode payload = Json.newObject();
                            payload.put("n", emitted);
                            return DataRecord.of(payload, String.valueOf(emitted), emitted);
                        }

                        @Override
                        public List<Fetch> drainFetches() {
                            List<Fetch> drained = List.copyOf(fetches);
                            fetches.clear();
                            return drained;
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
