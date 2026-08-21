package com.dmp.engine;

import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.AuditLogPort;
import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.application.port.out.RunEventPublisher;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.RetryOptions;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.RunTrigger;
import com.dmp.domain.run.Split;
import com.dmp.domain.tenant.TenantId;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Re-attempting a finished run without re-doing the parts that worked.
 *
 * <p>Before this existed the only recovery from a run that failed on two of forty chunks was a
 * fresh run of all forty — re-reading everything, and against a sink that cannot absorb a repeated
 * write, colliding on every record the first run had correctly written.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RunRetryTest {

    private static final Instant NOW = Instant.parse("2026-08-07T12:00:00Z");

    @Mock private PipelineRepository pipelines;
    @Mock private PipelineVersionRepository versions;
    @Mock private RunRepository runs;
    @Mock private SplitRepository splits;
    @Mock private CheckpointRepository checkpoints;
    @Mock private com.dmp.application.port.out.RecordErrorPort recordErrors;
    @Mock private RunPlanner planner;
    @Mock private RunEventPublisher events;
    @Mock private AuditLogPort auditLog;
    @Mock private TenantContext tenantContext;
    @Mock private com.dmp.engine.schedule.ExternalJobScheduler externalJobs;

    private RunOrchestrator orchestrator;
    private TenantId tenantId;
    private Run original;

    @BeforeEach
    void setUp() {
        tenantId = TenantId.newId();
        orchestrator = new RunOrchestrator(pipelines, versions, runs, splits, checkpoints, recordErrors, planner,
                events, auditLog, tenantContext, externalJobs, Clock.fixed(NOW, ZoneOffset.UTC));

        when(tenantContext.currentTenant()).thenReturn(tenantId);
        when(tenantContext.currentActor()).thenReturn("someone");
        when(events.isEnabled()).thenReturn(true);
        when(runs.create(any())).thenAnswer(call -> call.getArgument(0, Run.class));

        original = failedRun();
        when(runs.findById(tenantId, original.id())).thenReturn(Optional.of(original));
    }

    private Run failedRun() {
        Run run = Run.create(tenantId, PipelineId.newId(), PipelineVersionId.newId(), 3,
                PipelineMode.FULL_LOAD, RunTrigger.MANUAL, "key", "someone", NOW);
        return run.markValidated(NOW)
                .recordPreparation("source", Json.emptyObject(), NOW)
                .withSplitPlan(40)
                .start(NOW)
                .finalizing(NOW)
                .fail("CHUNKS_ABANDONED", "2 of 40 chunk(s) failed", NOW);
    }

    /** 38 completed, one abandoned, one cancelled — the shape of a stopped-then-failed run. */
    private List<Split> mixedPlan() {
        List<Split> plan = new ArrayList<>();
        for (int i = 0; i < 38; i++) {
            plan.add(Split.plan(original.id(), tenantId, i, Json.emptyObject(), NOW)
                    .claim("worker-a", NOW, java.time.Duration.ofMinutes(5))
                    .complete(NOW));
        }
        plan.add(Split.plan(original.id(), tenantId, 38, Json.emptyObject(), NOW)
                .claim("worker-a", NOW, java.time.Duration.ofMinutes(5))
                .fail("UNAVAILABLE", "connection reset", NOW)
                .abandon(NOW));
        plan.add(Split.plan(original.id(), tenantId, 39, Json.emptyObject(), NOW).cancel(NOW));
        return plan;
    }

    @SuppressWarnings("unchecked")
    private List<Split> savedSplits() {
        ArgumentCaptor<List<Split>> saved = ArgumentCaptor.forClass(List.class);
        verify(splits).saveAll(saved.capture());
        return saved.getValue();
    }

    @Test
    @DisplayName("re-runs only the chunks that failed, never the ones that worked")
    void completedChunksAreNotRepeated() {
        // The entire value of the feature. Re-running the 38 that succeeded would re-read their
        // rows and, on a sink that inserts, collide on every one of them.
        when(splits.findByRun(tenantId, original.id())).thenReturn(mixedPlan());

        orchestrator.retry(original.id(), RetryOptions.resumingFailed());

        assertThat(savedSplits()).extracting(Split::index).containsExactly(38);
    }

    @Test
    @DisplayName("FAILED_AND_CANCELLED also picks up chunks that never started")
    void cancelledChunksResumeAStoppedRun() {
        // How a stopped run is resumed. There is no second mechanism for it, deliberately: a chunk
        // cancelled mid-run and one abandoned mid-run both simply need running.
        when(splits.findByRun(tenantId, original.id())).thenReturn(mixedPlan());

        orchestrator.retry(original.id(),
                new RetryOptions(RetryOptions.From.CHECKPOINT,
                        RetryOptions.Scope.FAILED_AND_CANCELLED, false));

        assertThat(savedSplits()).extracting(Split::index).containsExactly(38, 39);
    }

    @Test
    @DisplayName("the retry is a new run linked to the original, not a reopening of it")
    void retryIsANewRunWithLineage() {
        // Reopening would rewrite a finished run's duration and metrics, and break the terminal
        // states the reaper and the console both depend on.
        when(splits.findByRun(tenantId, original.id())).thenReturn(mixedPlan());

        Run retry = orchestrator.retry(original.id(), RetryOptions.resumingFailed());

        assertThat(retry.id()).isNotEqualTo(original.id());
        assertThat(retry.retryOf()).isEqualTo(original.id());
        assertThat(retry.trigger()).isEqualTo(RunTrigger.RETRY);
        assertThat(retry.state()).isEqualTo(RunState.CREATED);
    }

    @Test
    @DisplayName("it runs the version the original ran, not whatever is published now")
    void versionIsPinnedToTheOriginal() {
        // Retrying against a definition edited since would leave half the chunks carrying the old
        // logic and half the new — a different migration wearing the failed run's name.
        when(splits.findByRun(tenantId, original.id())).thenReturn(mixedPlan());

        Run retry = orchestrator.retry(original.id(), RetryOptions.resumingFailed());

        assertThat(retry.pipelineVersionId()).isEqualTo(original.pipelineVersionId());
        assertThat(retry.versionNumber()).isEqualTo(original.versionNumber());
    }

    @Test
    @DisplayName("resuming carries each chunk's saved position onto its replacement")
    void checkpointsAreCarriedForward() {
        List<Split> plan = mixedPlan();
        Split abandoned = plan.get(38);
        Checkpoint progress = Checkpoint.initial(abandoned.id(), original.id(), tenantId, NOW)
                .advance(Json.emptyObject(), 9_000, 9_000, 9_000, 9_000, 0, 0, 1_024, NOW);

        when(splits.findByRun(tenantId, original.id())).thenReturn(plan);
        when(checkpoints.findBySplit(tenantId, abandoned.id())).thenReturn(Optional.of(progress));

        orchestrator.retry(original.id(), RetryOptions.resumingFailed());

        ArgumentCaptor<Checkpoint> copied = ArgumentCaptor.forClass(Checkpoint.class);
        verify(checkpoints).save(copied.capture());

        // Same position and counters, new chunk. Resetting the counters would understate the run
        // by everything the first attempt had already done.
        assertThat(copied.getValue().splitId()).isNotEqualTo(abandoned.id());
        assertThat(copied.getValue().lastSeq()).isEqualTo(9_000);
        assertThat(copied.getValue().recordsWritten()).isEqualTo(9_000);
    }

    @Test
    @DisplayName("starting over does not carry the position forward")
    void restartDiscardsCheckpoints() {
        when(splits.findByRun(tenantId, original.id())).thenReturn(mixedPlan());

        orchestrator.retry(original.id(),
                new RetryOptions(RetryOptions.From.CHUNK_START, RetryOptions.Scope.FAILED, true));

        verify(checkpoints, never()).save(any());
    }

    @Test
    @DisplayName("starting over is refused when it would re-send records already written")
    void restartNeedsAcknowledgementWhenRecordsAreAtRisk() {
        List<Split> plan = mixedPlan();
        Split abandoned = plan.get(38);
        Checkpoint progress = Checkpoint.initial(abandoned.id(), original.id(), tenantId, NOW)
                .advance(Json.emptyObject(), 9_000, 9_000, 9_000, 9_000, 0, 0, 1_024, NOW);

        when(splits.findByRun(tenantId, original.id())).thenReturn(plan);
        when(checkpoints.findByRun(tenantId, original.id())).thenReturn(List.of(progress));

        assertThatThrownBy(() -> orchestrator.retry(original.id(),
                new RetryOptions(RetryOptions.From.CHUNK_START, RetryOptions.Scope.FAILED, false)))
                .isInstanceOf(DmpException.class)
                // The number is the point. "This may duplicate records" is ignorable; "this will
                // send 9,000 records a second time" is a decision someone can actually make.
                .hasMessageContaining("9000");

        verify(splits, never()).saveAll(any());
    }

    @Test
    @DisplayName("nothing written means nothing to duplicate, so no acknowledgement is asked for")
    void restartIsFreeWhenNothingWasWritten() {
        // A chunk that failed on its first batch has written nothing, and starting it over is
        // identical to resuming it. Demanding confirmation there would be noise.
        when(splits.findByRun(tenantId, original.id())).thenReturn(mixedPlan());
        when(checkpoints.findByRun(tenantId, original.id())).thenReturn(List.of());

        orchestrator.retry(original.id(),
                new RetryOptions(RetryOptions.From.CHUNK_START, RetryOptions.Scope.FAILED, false));

        assertThat(savedSplits()).hasSize(1);
    }

    @Test
    @DisplayName("a run still going cannot be retried")
    void unfinishedRunsAreRefused() {
        Run running = Run.create(tenantId, PipelineId.newId(), PipelineVersionId.newId(), 1,
                        PipelineMode.FULL_LOAD, RunTrigger.MANUAL, "k", "someone", NOW)
                .markValidated(NOW)
                .recordPreparation("source", Json.emptyObject(), NOW)
                .withSplitPlan(4)
                .start(NOW);
        when(runs.findById(tenantId, running.id())).thenReturn(Optional.of(running));

        assertThatThrownBy(() -> orchestrator.retry(running.id(), RetryOptions.resumingFailed()))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("stop it first");
    }

    @Test
    @DisplayName("a run where everything succeeded has nothing to retry")
    void fullySuccessfulRunIsRefused() {
        List<Split> allDone = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            allDone.add(Split.plan(original.id(), tenantId, i, Json.emptyObject(), NOW)
                    .claim("worker-a", NOW, java.time.Duration.ofMinutes(5))
                    .complete(NOW));
        }
        when(splits.findByRun(tenantId, original.id())).thenReturn(allDone);

        assertThatThrownBy(() -> orchestrator.retry(original.id(), RetryOptions.resumingFailed()))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("no chunks to retry");
    }

    @Test
    @DisplayName("a single chunk can be retried on its own")
    void oneChunkAtATime() {
        List<Split> plan = mixedPlan();
        Split abandoned = plan.get(38);
        when(splits.findById(tenantId, abandoned.id())).thenReturn(Optional.of(abandoned));

        orchestrator.retryChunk(original.id(), abandoned.id(),
                RetryOptions.From.CHECKPOINT, false);

        assertThat(savedSplits()).extracting(Split::index).containsExactly(38);
    }

    @Test
    @DisplayName("a chunk that succeeded cannot be retried individually")
    void completedChunkIsRefused() {
        List<Split> plan = mixedPlan();
        Split done = plan.get(0);
        when(splits.findById(tenantId, done.id())).thenReturn(Optional.of(done));

        assertThatThrownBy(() -> orchestrator.retryChunk(original.id(), done.id(),
                RetryOptions.From.CHECKPOINT, false))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("already correct");
    }
}
