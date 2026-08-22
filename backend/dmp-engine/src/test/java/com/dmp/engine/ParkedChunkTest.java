package com.dmp.engine;

import com.dmp.application.port.out.StageLogPort;
import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.application.port.out.RecordErrorPort;
import com.dmp.application.port.out.RecordIndexPort;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
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
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitState;
import com.dmp.domain.tenant.TenantId;
import com.dmp.transform.api.TransformFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * What happens to a chunk that has handed its records to a system which answers later.
 *
 * <p>The behaviour under test is the reason this mechanism exists at all. A Salesforce bulk job
 * takes minutes to decide, and the old shape held a worker in a sleep loop for the whole of it —
 * which wasted a slot, and, far worse, kept the job's id only in that worker's memory. A restart
 * during the wait lost it: the org went on processing records nobody was watching, its per-record
 * rejections were never fetched, and every one of them was reported as written.
 *
 * <p>The chunk now parks. These tests pin the two things that must be true about that, both of
 * which are silent when wrong.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ParkedChunkTest {

    private static final Instant NOW = Instant.parse("2026-08-09T10:00:00Z");

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

    @BeforeEach
    void setUp() {
        tenantId = TenantId.newId();
        runId = RunId.newId();

        executor = new ChunkExecutor(connectors, new ConnectorContexts(List.of()), checkpoints,
                splits, recordErrors, recordIndex, stageLog, transforms, replaySource,
                Clock.fixed(NOW, ZoneOffset.UTC));

        when(checkpoints.findOrCreate(any(), any(), any())).thenAnswer(call ->
                Checkpoint.initial(call.getArgument(2), runId, tenantId, NOW)
                        .advance(Json.emptyObject(), 5_000, 5_000, 5_000, 5_000, 0, 0, 1_000, NOW));
        when(checkpoints.save(any())).thenAnswer(call -> call.getArgument(0));
    }

    /**
     * The single most dangerous line in the executor.
     *
     * <p>A chunk resumed with a job handle has already read everything it is going to read and has
     * already handed it over. Falling through to the normal path would open the source, read from
     * the checkpoint and submit a <em>second</em> bulk job for records the first one is still
     * processing — which under an insert operation is how a migration silently doubles its data.
     *
     * <p>Nothing about the run would look wrong afterwards. The counters would agree with each
     * other, the chunk would report success, and the duplication would only be visible by counting
     * rows in the org.
     */
    @Test
    void aChunkResumedOnARemoteJobNeverTouchesTheSourceAgain() {
        FakeAsyncSink sink = new FakeAsyncSink(Preparation.Status.ready());
        when(connectors.sink(anyString())).thenReturn(sink);

        ChunkResult result = executor.execute(pipeline(), parkedSplit(), "worker-2");

        verify(connectors, never()).source(anyString());
        assertThat(sink.wrote)
                .as("a settled chunk writes nothing; its records left before it parked")
                .isEmpty();
        assertThat(sink.prepared)
                .as("preparing again would create a second remote job for the same records")
                .isFalse();

        assertThat(sink.harvested).as("the destination's per-record verdicts must be collected").isTrue();
        assertThat(sink.released).as("an unreleased job consumes the org's quota").isTrue();

        assertThat(result.recordsWritten())
                .as("the counts come from the checkpoint written before it parked")
                .isEqualTo(5_000);
    }

    /**
     * A destination that failed a whole job is not retried.
     *
     * <p>Salesforce fails a job for reasons that do not change on a second attempt — an object that
     * does not exist, a field the integration user cannot write. Re-uploading finds the same answer,
     * and against a system that meters bulk jobs it finds it at a price.
     */
    @Test
    void aJobTheDestinationFailedIsReportedAsSuchRatherThanReRun() {
        when(connectors.sink(anyString()))
                .thenReturn(new FakeAsyncSink(Preparation.Status.failed("InvalidBatch: bad field")));

        assertThatThrownBy(() -> executor.execute(pipeline(), parkedSplit(), "worker-2"))
                .isInstanceOf(com.dmp.connector.api.ConnectorException.class)
                .hasMessageContaining("bad field")
                .matches(e -> !((com.dmp.connector.api.ConnectorException) e).isRetryable(),
                        "a whole-job failure must not be retried");

        verify(connectors, never()).source(anyString());
    }

    /** Claimed a shade early: park again rather than hold a slot for a job that is not ready. */
    @Test
    void aChunkPickedUpBeforeItsJobFinishedParksAgain() {
        when(connectors.sink(anyString())).thenReturn(
                new FakeAsyncSink(Preparation.Status.pending(Duration.ofSeconds(7))));

        assertThatThrownBy(() -> executor.execute(pipeline(), parkedSplit(), "worker-2"))
                .isInstanceOf(ChunkParkedException.class)
                .extracting(e -> ((ChunkParkedException) e).retryAfter())
                .isEqualTo(Duration.ofSeconds(7));
    }

    /**
     * The park has to remember whether the source had run out.
     *
     * <p>Decided by the read loop and consumed long after it has gone, to decide whether a lazily
     * chunked run needs a successor. Left out, every parked chunk would look like the end of its
     * run and a Salesforce migration would stop at its first chunk having read a fraction of the
     * data — reporting success.
     */
    @Test
    void theParkRemembersWhetherTheSourceHadMoreToGive() {
        ChunkParkedException moreToRead = new ChunkParkedException(
                3, Preparation.of(handle()), Duration.ofSeconds(5), false);

        assertThat(ChunkParkedException.sourceWasExhausted(moreToRead.parkedState())).isFalse();
        assertThat(ChunkParkedException.sinkJobOf(moreToRead.parkedState()).state().path("jobId").asText())
                .isEqualTo("750xx000001abcAAA");
    }

    /**
     * A retry drops the handle; a completed wait keeps it.
     *
     * <p>The whole difference between the two paths. A retry is a fresh attempt from the checkpoint
     * that submits new work, so carrying a stale handle into it would send the executor down the
     * settle path to harvest a job that has nothing to do with the records that attempt is about to
     * move.
     */
    @Test
    void aRetryStartsCleanWhileAFinishedWaitCarriesItsJobForward() {
        Split parked = waitingSplit();

        assertThat(parked.externalJobFinished(NOW).hasExternalJob())
                .as("the worker settling this chunk needs the handle to know which job to harvest")
                .isTrue();
        assertThat(parked.externalJobFinished(NOW).attempt())
                .as("waiting for a destination is not a failed attempt")
                .isEqualTo(parked.attempt());

        assertThat(parked.fail("X", "boom", NOW).scheduleRetry(NOW).hasExternalJob())
                .as("a retry re-reads and submits new work; the old job is not its business")
                .isFalse();
    }

    /** A parked chunk holds its run open — a bulk job still deciding is not a finished migration. */
    @Test
    void aParkedChunkKeepsItsRunOutstanding() {
        assertThat(SplitState.WAITING_EXTERNAL.isOutstanding()).isTrue();
        assertThat(SplitState.WAITING_EXTERNAL.isTerminal()).isFalse();
        assertThat(SplitState.RUNNING.canTransitionTo(SplitState.WAITING_EXTERNAL)).isTrue();
        assertThat(SplitState.WAITING_EXTERNAL.canTransitionTo(SplitState.PENDING)).isTrue();

        assertThat(SplitState.WAITING_EXTERNAL.canTransitionTo(SplitState.COMPLETED))
                .as("nothing may call a chunk done before its rejections have been collected")
                .isFalse();
    }

    // ------------------------------------------------------------------ setup

    /** Mid-park: handed to the destination, held by nobody, waiting to be asked about. */
    private Split waitingSplit() {
        ObjectNode parked = Json.newObject();
        parked.set("sink", handle());
        parked.put("sourceExhausted", true);

        return Split.plan(runId, tenantId, 3, Json.emptyObject(), NOW)
                .claim("worker-1", NOW, Duration.ofMinutes(5))
                .parkOnExternalJob(parked, NOW.plusSeconds(5), NOW);
    }

    /**
     * The same chunk after the poller found the job done and a second worker claimed it — which is
     * the state the executor must recognise as "settle this", never as "run this".
     */
    private Split parkedSplit() {
        return waitingSplit()
                .externalJobFinished(NOW)
                .claim("worker-2", NOW, Duration.ofMinutes(5));
    }

    private static ObjectNode handle() {
        ObjectNode job = Json.newObject();
        job.put("jobId", "750xx000001abcAAA");
        return job;
    }

    private ResolvedPipeline pipeline() {
        ConnectorInstance instance = new ConnectorInstance(
                ConnectorInstanceId.newId(), tenantId, "org", "salesforce",
                ConnectorDirection.BOTH, Json.emptyObject(), Json.emptyObject(),
                ConnectorInstanceStatus.ACTIVE, null, null, null, NOW, NOW, 1);

        NodeDefinition source = new NodeDefinition("src", NodeType.SOURCE, "Source",
                instance.id().value(), Json.emptyObject());
        NodeDefinition sink = new NodeDefinition("dst", NodeType.SINK, "Salesforce",
                instance.id().value(), Json.emptyObject());

        return new ResolvedPipeline(null, source, instance, sink, instance, List.of(),
                ChunkingPolicy.DEFAULT, ExecutionPolicy.DEFAULT, AuditPolicy.DEFAULT, Json.emptyObject());
    }

    /**
     * A sink that behaves like a bulk destination: it takes work and decides later.
     *
     * <p>Records what it was asked to do rather than what it returned, because the assertions here
     * are about which calls happen at all — "did anything read the source again", "was a second job
     * created" — and those are only visible in the sequence.
     */
    private static final class FakeAsyncSink implements Sink {

        private final Preparation.Status answer;

        private boolean prepared;
        private boolean harvested;
        private boolean released;
        private final List<RecordBatch> wrote = new ArrayList<>();

        FakeAsyncSink(Preparation.Status answer) {
            this.answer = answer;
        }

        @Override
        public com.dmp.connector.api.ConnectorSpec spec() {
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
                    return new Capabilities(true, null, false, true, true, false, 10_000, 10_000);
                }

                @Override
                public WriteResult write(RecordBatch batch) {
                    wrote.add(batch);
                    return WriteResult.allWritten(batch.size(), batch.totalBytes());
                }

                @Override
                public Preparation prepare() {
                    prepared = true;
                    return Preparation.of(handle());
                }

                @Override
                public Preparation.Status checkCommit(Preparation commit) {
                    return answer;
                }

                @Override
                public Harvest harvest(Preparation commit) {
                    harvested = true;
                    return Harvest.none();
                }

                @Override
                public void release(Preparation preparation) {
                    released = true;
                }
            };
        }
    }
}
