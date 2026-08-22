package com.dmp.engine;

import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.json.Json;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunTrigger;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitState;
import com.dmp.domain.tenant.TenantId;
import org.junit.jupiter.api.BeforeEach;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The sweep that makes a lease worth having.
 *
 * <p>A lease that nothing acts on when it lapses is decoration: the chunk stays RUNNING behind a
 * dead worker, no other pod may claim it because it is not PENDING, and the run never finishes.
 * {@code findExpiredLeases} was implemented and unit-tested for months with no production caller,
 * so a pod restart mid-run stalled it permanently.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RunReaperTest {

    private static final Instant NOW = Instant.parse("2026-08-07T10:00:00Z");
    private static final Duration LEASE = Duration.ofMinutes(5);

    @Mock private RunRepository runs;
    @Mock private SplitRepository splits;
    @Mock private RunPlanner planner;
    @Mock private RunOrchestrator orchestrator;
    @Mock private com.dmp.engine.schedule.ExternalJobPoller externalJobs;

    private RunReaper reaper;
    private TenantId tenantId;
    private Run run;

    @BeforeEach
    void setUp() {
        tenantId = TenantId.newId();
        reaper = new RunReaper(runs, splits, planner, orchestrator, externalJobs,
                java.time.Duration.ofMinutes(2), Clock.fixed(NOW, ZoneOffset.UTC));

        run = Run.create(tenantId, PipelineId.newId(), PipelineVersionId.newId(), 1,
                        PipelineMode.FULL_LOAD, RunTrigger.API, null, "tester", NOW)
                .markValidated(NOW)
                .recordPreparation("source", Json.emptyObject(), NOW)
                .start(NOW);

        when(runs.findById(any(), any())).thenReturn(Optional.of(run));
        when(runs.findByStates(any(), anyInt())).thenReturn(List.of());
        when(splits.findExpiredLeases(any(), anyInt())).thenReturn(List.of());
        when(splits.transitionState(any(), any(), any(), any()))
                .thenAnswer(call -> Optional.of(call.getArgument(3, Split.class)));
        when(planner.resolve(any())).thenReturn(resolvedWith(3));
    }

    @Test
    void anExpiredLeaseReturnsTheChunkToThePool() {
        Split orphan = claimedLongAgo(0);
        when(splits.findExpiredLeases(NOW, 200)).thenReturn(List.of(orphan));

        reaper.sweep();

        ArgumentCaptor<Split> written = ArgumentCaptor.forClass(Split.class);
        verify(splits, org.mockito.Mockito.atLeast(2))
                .transitionState(eq(tenantId), eq(orphan.id()), any(), written.capture());

        assertThat(written.getAllValues())
                .extracting(Split::state)
                .containsExactly(SplitState.FAILED, SplitState.PENDING);
    }

    @Test
    void reclaimingAdvancesTheAttemptCounter() {
        // Otherwise a chunk that reliably wedges its worker would be reclaimed and retried forever.
        Split orphan = claimedLongAgo(0);
        when(splits.findExpiredLeases(NOW, 200)).thenReturn(List.of(orphan));

        reaper.sweep();

        ArgumentCaptor<Split> written = ArgumentCaptor.forClass(Split.class);
        verify(splits, org.mockito.Mockito.atLeast(2))
                .transitionState(eq(tenantId), eq(orphan.id()), any(), written.capture());

        Split retried = written.getAllValues().get(1);
        assertThat(retried.attempt()).isEqualTo(orphan.attempt() + 1);
        assertThat(retried.assignedTo()).isNull();
    }

    @Test
    void aDeadWorkersSlotIsReleased() {
        // The slot counter is only ever decremented by the worker that took it. A dead worker
        // never does, and on a strictly sequential run one leaked slot deadlocks the whole run.
        Split orphan = claimedLongAgo(0);
        when(splits.findExpiredLeases(NOW, 200)).thenReturn(List.of(orphan));

        reaper.sweep();

        verify(runs).releaseSlot(tenantId, orphan.runId());
    }

    @Test
    void aChunkPastItsRetryBudgetIsAbandoned() {
        when(planner.resolve(any())).thenReturn(resolvedWith(1));
        Split orphan = claimedLongAgo(0);
        when(splits.findExpiredLeases(NOW, 200)).thenReturn(List.of(orphan));

        reaper.sweep();

        ArgumentCaptor<Split> written = ArgumentCaptor.forClass(Split.class);
        verify(splits, org.mockito.Mockito.atLeast(2))
                .transitionState(eq(tenantId), eq(orphan.id()), any(), written.capture());

        assertThat(written.getAllValues())
                .extracting(Split::state)
                .containsExactly(SplitState.FAILED, SplitState.ABANDONED);
    }

    @Test
    void theOriginalWorkerFinishingFirstWins() {
        // Slow, not dead. It completed between the query and the reclaim, so the conditional
        // transition finds the split no longer RUNNING and this must not touch its result.
        Split orphan = claimedLongAgo(0);
        when(splits.findExpiredLeases(NOW, 200)).thenReturn(List.of(orphan));

        // doReturn, not when(...): when() calls the mock, which would run the setUp answer with
        // null arguments before the new stub replaces it.
        org.mockito.Mockito.doReturn(Optional.empty()).when(splits)
                .transitionState(any(), any(), eq(SplitState.RUNNING), any());

        reaper.sweep();

        verify(splits, never()).transitionState(any(), any(), eq(SplitState.FAILED), any());
        verify(runs, never()).releaseSlot(any(), any());
    }

    @Test
    void slotDriftIsNotCorrectedOnASingleObservation() {
        // A worker reserves a slot and then claims a chunk. In that window the counter legitimately
        // exceeds the running count, and correcting it would hand out a slot already taken.
        Run withSlot = runWithReservedSlot();
        when(runs.findByStates(any(), anyInt())).thenReturn(List.of(withSlot));
        when(splits.countRunning(tenantId, withSlot.id())).thenReturn(0L);

        reaper.sweep();

        verify(runs, never()).reconcileSlots(any(), any(), anyInt());
    }

    @Test
    void slotDriftIsCorrectedOnceItPersists() {
        Run withSlot = runWithReservedSlot();
        when(runs.findByStates(any(), anyInt())).thenReturn(List.of(withSlot));
        when(splits.countRunning(tenantId, withSlot.id())).thenReturn(0L);

        reaper.sweep();
        reaper.sweep();

        verify(runs).reconcileSlots(tenantId, withSlot.id(), 0);
    }

    @Test
    void everyUnsettledRunIsGivenAChanceToFinish() {
        when(runs.findByStates(any(), anyInt())).thenReturn(List.of(run));
        when(splits.countRunning(any(), any())).thenReturn(0L);

        reaper.sweep();

        verify(orchestrator).completeIfFinished(run);
    }

    private Split claimedLongAgo(int index) {
        return Split.plan(run.id(), tenantId, index, Json.emptyObject(), NOW.minusSeconds(600))
                .claim("worker-that-died", NOW.minusSeconds(600), LEASE);
    }

    /**
     * A run holding one slot with nothing actually running — what a worker crash leaves behind.
     *
     * <p>Built through the canonical constructor because {@code activeSlots} is deliberately not
     * settable from the domain: it is owned by atomic database operations, since the limit it
     * enforces spans every pod.
     */
    private Run runWithReservedSlot() {
        return new Run(run.id(), run.tenantId(), run.pipelineId(), run.pipelineVersionId(),
                run.versionNumber(), run.mode(), run.trigger(), run.retryOf(), run.state(),
                run.idempotencyKey(),
                run.metrics(), 1, run.preparationState(), run.parameters(), run.dryRun(),
                run.errorCode(), run.errorMessage(),
                run.triggeredBy(), run.createdAt(), run.startedAt(), run.endedAt(),
                run.updatedAt(), run.rowVersion());
    }

    private ResolvedPipeline resolvedWith(int maxAttempts) {
        ExecutionPolicy execution = new ExecutionPolicy(1, 1, LEASE, maxAttempts,
                ExecutionPolicy.ROWS_PER_CHUNK_AUTO, null, null, false);
        return new ResolvedPipeline(null, null, null, null, null, java.util.List.of(),
                ChunkingPolicy.DEFAULT, execution, AuditPolicy.DEFAULT, com.dmp.common.json.Json.emptyObject());
    }
}
