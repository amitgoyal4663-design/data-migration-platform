package com.dmp.engine;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
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
import com.dmp.domain.audit.RecordAuditLevel;
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
import com.dmp.transform.api.BatchResult;
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
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * What identifies a record in the audit index.
 *
 * <p>An index entry needs an id that is <em>stable</em> and <em>unique</em>, and the two pull in
 * opposite directions. Stable, because entries are written before the checkpoint advances: a chunk
 * that dies in between re-indexes on its next attempt what it already indexed, and a fresh id each
 * time would inflate the index and every count taken from it. Unique, because the index exists to
 * answer "did all forty records go across?" and an id two different records share answers thirty.
 *
 * <p>The id was {@code runId:recordKey}, which had stability and not uniqueness. A source holding
 * the same key twice — no more exotic than a table without a unique constraint — filed its two
 * rows as one, and a run reporting forty written left thirty entries behind. These tests pin both
 * properties, because a fix for either one alone is a regression in the other.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RecordIndexIdentityTest {

    private static final Instant NOW = Instant.parse("2026-08-20T10:00:00Z");

    @Mock private ConnectorRegistry connectors;
    @Mock private CheckpointRepository checkpoints;
    @Mock private SplitRepository splits;
    @Mock private RecordErrorPort recordErrors;
    @Mock private StageLogPort stageLog;
    @Mock private TransformFactory transforms;
    @Mock private ReplaySource replaySource;

    private final CollectingIndex index = new CollectingIndex();
    private ChunkExecutor executor;
    private TenantId tenantId;
    private RunId runId;

    /**
     * One chunk, reused across attempts.
     *
     * <p>A field rather than a local, because a retry is the <em>same</em> chunk running again. A
     * fresh {@code Split.plan} per attempt would mint a new id, and the stability tests below
     * would pass on ids that could never collide in the first place — proving nothing.
     */
    private Split split;

    @BeforeEach
    void setUp() {
        tenantId = TenantId.newId();
        runId = RunId.newId();
        split = Split.plan(runId, tenantId, 0, Json.emptyObject(), NOW)
                .claim("worker-1", NOW, Duration.ofMinutes(5));

        executor = new ChunkExecutor(connectors, new ConnectorContexts(List.of()), checkpoints,
                splits, recordErrors, index, stageLog, transforms, replaySource,
                Clock.fixed(NOW, ZoneOffset.UTC));

        when(checkpoints.findOrCreate(any(), any(), any())).thenAnswer(call ->
                Checkpoint.initial(split.id(), runId, tenantId, NOW));
        when(checkpoints.save(any())).thenAnswer(call -> call.getArgument(0));
        when(splits.heartbeat(any(), any(), anyString(), any(), any()))
                .thenAnswer(call -> Optional.empty());
        when(connectors.sink(anyString())).thenReturn(new AcceptingSink());
        when(transforms.compile(any())).thenReturn(RecordTransform.IDENTITY);
    }

    @Nested
    @DisplayName("every record moved leaves an entry")
    class Completeness {

        @Test
        @DisplayName("a source holding the same key twice indexes both rows")
        void duplicateKeysAreDistinctEntries() {
            // Ten records, five keys: 1,1,2,2,3,3,4,4,5,5. What a table with no unique constraint
            // looks like, and what the old id collapsed into five entries.
            when(connectors.source(anyString()))
                    .thenReturn(new FixedSource(10, i -> String.valueOf((i + 1) / 2)));

            ChunkResult result = run();

            assertThat(result.recordsWritten()).isEqualTo(10);
            assertThat(index.ids())
                    .as("ten records written is ten entries, whatever their keys are")
                    .hasSize(10)
                    .doesNotHaveDuplicates();
            assertThat(index.entries).extracting(RecordIndexPort.RecordIndexEntry::recordKey)
                    .as("the key is still recorded — it is how a record is looked up")
                    .containsExactly("1", "1", "2", "2", "3", "3", "4", "4", "5", "5");
        }

        @Test
        @DisplayName("records with no key at all are still indexed")
        void keylessRecordsAreIndexed() {
            // A CSV with no id column, a REST payload with no natural key. These used to be
            // skipped, so a pipeline reading one indexed nothing and the audit trail was empty
            // for a run that had moved every row successfully.
            when(connectors.source(anyString())).thenReturn(new FixedSource(10, i -> null));

            ChunkResult result = run();

            assertThat(result.recordsWritten()).isEqualTo(10);
            assertThat(index.ids()).hasSize(10).doesNotHaveDuplicates();
            assertThat(index.entries).extracting(RecordIndexPort.RecordIndexEntry::recordKey)
                    .as("nothing to look them up by, but they are in the run's own list")
                    .containsOnlyNulls();
        }

        @Test
        @DisplayName("one record transformed into three leaves three entries")
        void fanOutIsThreeEntries() {
            when(connectors.source(anyString()))
                    .thenReturn(new FixedSource(4, i -> "key-" + i));
            when(transforms.compile(any())).thenReturn(fanningOutInto(3));

            ChunkResult result = run();

            assertThat(result.recordsWritten()).isEqualTo(12);
            assertThat(index.ids())
                    .as("the three outputs share the input's seq, so ordinal has to separate them")
                    .hasSize(12)
                    .doesNotHaveDuplicates();
            assertThat(index.entries).filteredOn(e -> e.seq() == 1)
                    .extracting(RecordIndexPort.RecordIndexEntry::ordinal)
                    .containsExactly(0, 1, 2);
        }

        @Test
        @DisplayName("dividing a batch into calls does not change what is indexed")
        void deliveryGroupingDoesNotAffectIdentity() {
            // Groups reorder records, and two records sharing a seq can land in different groups.
            // Numbering within a group would then give both of them ordinal 0.
            when(connectors.source(anyString()))
                    .thenReturn(new FixedSource(9, i -> "key-" + i));
            when(transforms.compile(any())).thenReturn(fanningOutInto(2));

            run(DeliveryPolicy.DEFAULT.withGroupSize(4));

            assertThat(index.ids()).hasSize(18).doesNotHaveDuplicates();
        }
    }

    @Nested
    @DisplayName("re-indexing the same record replaces it")
    class Stability {

        @Test
        @DisplayName("a resumed chunk reuses the ids it wrote before the crash")
        void resumeReusesIds() {
            when(connectors.source(anyString()))
                    .thenReturn(new FixedSource(10, i -> "key-" + i));

            // The first attempt: records 1-10 of the chunk.
            run();
            List<String> firstAttempt = index.ids();
            index.entries.clear();

            // The second attempt, after a crash that checkpointed at 4. The source reopens and
            // numbers from one again, so its record 1 is the chunk's record 5 — which is the
            // whole reason the offset exists.
            when(checkpoints.findOrCreate(any(), any(), any())).thenAnswer(call ->
                    Checkpoint.initial(split.id(), runId, tenantId, NOW)
                            .advance(Json.emptyObject(), 4, 4, 4, 4, 0, 0, 0, NOW));
            when(connectors.source(anyString()))
                    .thenReturn(new FixedSource(6, i -> "key-" + (i + 4)));

            run();

            assertThat(index.ids())
                    .as("the six re-read records land on the ids the first attempt gave them")
                    .containsExactlyElementsOf(firstAttempt.subList(4, 10));
        }

        @Test
        @DisplayName("the id does not depend on the record's content")
        void idIgnoresPayloadAndKey() {
            // A transform that rewrites the key would otherwise move the record to a new id, and a
            // resume would file it twice: once under the old key, once under the new.
            when(connectors.source(anyString())).thenReturn(new FixedSource(5, i -> "before-" + i));
            run();
            List<String> beforeRewrite = index.ids();
            index.entries.clear();

            when(connectors.source(anyString())).thenReturn(new FixedSource(5, i -> "after-" + i));
            run();

            assertThat(index.ids()).containsExactlyElementsOf(beforeRewrite);
        }
    }

    // ------------------------------------------------------------------ setup

    private ChunkResult run() {
        return run(DeliveryPolicy.DEFAULT);
    }

    private ChunkResult run(DeliveryPolicy delivery) {
        return executor.execute(pipeline(delivery), split, "worker-1", () -> false);
    }

    /** Indexing every record is opt-in, so these tests have to opt in. */
    private static final AuditPolicy INDEXED =
            AuditPolicy.DEFAULT.indexing(RecordAuditLevel.INDEXED, false);

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
                ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT, INDEXED, delivery,
                PipelineMode.FULL_LOAD, null, "tester", NOW, null);

        return new ResolvedPipeline(version, source, instance, sinkNode, instance, List.of(),
                ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT, INDEXED, Json.emptyObject());
    }

    /** Collects what was indexed, and reconstructs the id an implementation must derive from it. */
    private static final class CollectingIndex implements RecordIndexPort {

        final List<RecordIndexEntry> entries = new ArrayList<>();

        /**
         * The identity contract, spelled out rather than imported.
         *
         * <p>Written here so a change to how OpenSearch builds its id cannot quietly satisfy these
         * tests: they assert what the entry itself must make possible.
         */
        List<String> ids() {
            return entries.stream()
                    .map(e -> e.splitId().value() + ":" + e.seq() + ":" + e.ordinal())
                    .toList();
        }

        @Override
        public void indexAll(List<RecordIndexEntry> batch) {
            entries.addAll(batch);
        }

        @Override
        public Page<RecordIndexEntry> findByKey(TenantId tenantId, PipelineId pipelineId,
                                                String recordKey, PageQuery pageQuery) {
            return Page.of(List.of(), pageQuery, 0);
        }

        @Override
        public Page<RecordIndexEntry> findByRun(TenantId tenantId, RunId runId, Outcome outcome,
                                                PageQuery pageQuery) {
            return Page.of(List.of(), pageQuery, 0);
        }

        @Override
        public Page<RecordIndexEntry> search(TenantId tenantId, Query query, PageQuery pageQuery) {
            return Page.of(List.of(), pageQuery, 0);
        }

        @Override
        public long countByRun(TenantId tenantId, RunId runId) {
            return entries.size();
        }

        @Override
        public boolean supportsContentSearch() {
            return false;
        }
    }

    /** A transform turning every record into the given number of copies. */
    private static RecordTransform fanningOutInto(int copies) {
        return new RecordTransform() {
            @Override
            public List<DataRecord> applyRecord(DataRecord record) {
                List<DataRecord> out = new ArrayList<>(copies);
                for (int i = 0; i < copies; i++) {
                    // withOrdinal is the engine's, not the script's: GraalJsTransform numbers the
                    // outputs of the whole chain for exactly this reason.
                    out.add(record.withOrdinal(i));
                }
                return out;
            }

            @Override
            public BatchResult applyBatch(List<DataRecord> records) {
                return BatchResult.none();
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

    /** Emits a fixed number of records, keyed by the given function. */
    private static final class FixedSource implements Source {

        private final int count;
        private final Function<Integer, String> key;

        FixedSource(int count, Function<Integer, String> key) {
            this.count = count;
            this.key = key;
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
                            return DataRecord.of(payload, key.apply(emitted), emitted);
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

    /** Accepts everything, reports no per-record failures. */
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
                    return WriteResult.allWritten(batch.size(), batch.totalBytes());
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
