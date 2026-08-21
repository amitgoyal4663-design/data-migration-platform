package com.dmp.engine;

import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.AuditLogPort;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.port.out.PipelineVersionRepository;
import com.dmp.application.port.out.RunEventPublisher;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunState;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A stopped run must reach STOPPED, not sit in STOPPING forever.
 *
 * <p>The original failure: only RUNNING runs are offered work, so the instant a stop is requested
 * nothing will ever claim the remaining chunks — yet completion was gated on no chunk being
 * outstanding, and a PENDING chunk counts as outstanding. Every stopped run with unclaimed chunks
 * hung permanently.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RunStopSettlementTest {

    private static final Instant NOW = Instant.parse("2026-08-07T10:00:00Z");
    private static final Duration LEASE = Duration.ofMinutes(30);

    @Mock private PipelineRepository pipelines;
    @Mock private PipelineVersionRepository versions;
    @Mock private RunRepository runs;
    @Mock private SplitRepository splits;
    @Mock private com.dmp.application.port.out.CheckpointRepository checkpoints;
    @Mock private com.dmp.application.port.out.RecordErrorPort recordErrors;
    @Mock private RunPlanner planner;
    @Mock private RunEventPublisher events;
    @Mock private AuditLogPort auditLog;
    @Mock private TenantContext tenantContext;
    @Mock private com.dmp.engine.schedule.ExternalJobScheduler externalJobs;

    private RunOrchestrator orchestrator;
    private TenantId tenantId;

    @BeforeEach
    void setUp() {
        tenantId = TenantId.newId();
        orchestrator = new RunOrchestrator(pipelines, versions, runs, splits, checkpoints, recordErrors, planner, events,
                auditLog, tenantContext, externalJobs, Clock.fixed(NOW, ZoneOffset.UTC));

        when(events.isEnabled()).thenReturn(true);

        // Every conditional transition succeeds, returning what the caller asked for. The
        // contention cases are covered separately; here the question is which transitions happen.
        when(runs.transitionState(any(), any(), any(), any()))
                .thenAnswer(call -> Optional.of(call.getArgument(3, Run.class)));
        when(splits.transitionState(any(), any(), any(), any()))
                .thenAnswer(call -> Optional.of(call.getArgument(3, Split.class)));
    }

    @Test
    void stoppingRunWithUnclaimedChunksReachesStopped() {
        Run run = stoppingRun();
        List<Split> plan = new ArrayList<>();
        for (int i = 0; i < 35; i++) {
            plan.add(completedSplit(run, i));
        }
        for (int i = 35; i < 40; i++) {
            plan.add(Split.plan(run.id(), tenantId, i, Json.emptyObject(), NOW));
        }

        when(splits.findByRun(tenantId, run.id())).thenReturn(plan);
        when(runs.findById(tenantId, run.id())).thenReturn(Optional.of(run));

        orchestrator.completeIfFinished(run);

        ArgumentCaptor<Run> transitions = ArgumentCaptor.forClass(Run.class);
        verify(runs, org.mockito.Mockito.atLeastOnce())
                .transitionState(eq(tenantId), eq(run.id()), any(), transitions.capture());

        assertThat(transitions.getAllValues())
                .extracting(Run::state)
                .containsExactly(RunState.FINALIZING, RunState.STOPPED);
    }

    @Test
    void unclaimedChunksAreCancelledRatherThanLeftPending() {
        Run run = stoppingRun();
        Split pending = Split.plan(run.id(), tenantId, 35, Json.emptyObject(), NOW);

        when(splits.findByRun(tenantId, run.id()))
                .thenReturn(List.of(completedSplit(run, 0), pending));
        when(runs.findById(tenantId, run.id())).thenReturn(Optional.of(run));

        orchestrator.completeIfFinished(run);

        ArgumentCaptor<Split> cancelled = ArgumentCaptor.forClass(Split.class);
        verify(splits).transitionState(eq(tenantId), eq(pending.id()), eq(SplitState.PENDING),
                cancelled.capture());

        assertThat(cancelled.getValue().state()).isEqualTo(SplitState.CANCELLED);
    }

    @Test
    void chunksStillExecutingKeepTheRunStopping() {
        Run run = stoppingRun();
        Split draining = Split.plan(run.id(), tenantId, 0, Json.emptyObject(), NOW)
                .claim("worker-1", NOW, LEASE);

        when(splits.findByRun(tenantId, run.id())).thenReturn(List.of(draining));
        when(runs.findById(tenantId, run.id())).thenReturn(Optional.of(run));

        orchestrator.completeIfFinished(run);

        // A chunk mid-batch drains to its next checkpoint. Terminating the run now would report it
        // stopped while a worker was still writing.
        verify(runs, never()).transitionState(any(), any(), any(), any());
    }

    @Test
    void aStoppedRunIsNotStoppedTwice() {
        Run run = stoppingRun();
        Run alreadyStopped = run.finalizing(NOW).stopped(NOW);

        when(splits.findByRun(tenantId, run.id())).thenReturn(List.of(completedSplit(run, 0)));
        when(runs.findById(tenantId, run.id())).thenReturn(Optional.of(alreadyStopped));

        orchestrator.completeIfFinished(run);

        verify(runs, never()).transitionState(any(), any(), any(), any());
    }

    private Run stoppingRun() {
        return Run.create(tenantId, PipelineId.newId(), PipelineVersionId.newId(), 1,
                        PipelineMode.FULL_LOAD, RunTrigger.API, null, "tester", NOW)
                .markValidated(NOW)
                .recordPreparation("source", Json.emptyObject(), NOW)
                .start(NOW)
                .requestStop(NOW);
    }

/**
     * A run stopped because a chunk gave up is a failure, not a stop.
     *
     * <p>The word matters. STOPPED reads as "somebody halted this on purpose" and nobody
     * investigates one; FAILED reads as "something went wrong". Reporting an abandoned chunk as a
     * stop made a failed migration indistinguishable in the run list from a deliberate one.
     *
     * <p>It was also inconsistent with itself: with {@code stopRunOnChunkFailure} off, the ordinary
     * completion path called the identical situation FAILED. The same event must not produce two
     * different outcomes depending on how eagerly the run was configured to stop.
     */
    @Test
    void aRunStoppedByAnAbandonedChunkEndsFailedRatherThanStopped() {
        Run run = stoppingRun();
        List<Split> plan = new ArrayList<>();
        plan.add(completedSplit(run, 0));
        plan.add(abandonedSplit(run, 1));

        when(splits.findByRun(tenantId, run.id())).thenReturn(plan);
        when(runs.findById(tenantId, run.id())).thenReturn(Optional.of(run));

        orchestrator.completeIfFinished(run);

        ArgumentCaptor<Run> transitions = ArgumentCaptor.forClass(Run.class);
        verify(runs, org.mockito.Mockito.atLeastOnce())
                .transitionState(eq(tenantId), eq(run.id()), any(), transitions.capture());

        assertThat(transitions.getAllValues())
                .extracting(Run::state)
                .containsExactly(RunState.FINALIZING, RunState.FAILED);
    }

    /** A genuine stop, with nothing abandoned, still reports as a stop. */
    @Test
    void aRunStoppedByHandStillEndsStopped() {
        Run run = stoppingRun();
        List<Split> plan = new ArrayList<>();
        plan.add(completedSplit(run, 0));
        plan.add(Split.plan(run.id(), tenantId, 1, Json.emptyObject(), NOW));

        when(splits.findByRun(tenantId, run.id())).thenReturn(plan);
        when(runs.findById(tenantId, run.id())).thenReturn(Optional.of(run));

        orchestrator.completeIfFinished(run);

        ArgumentCaptor<Run> transitions = ArgumentCaptor.forClass(Run.class);
        verify(runs, org.mockito.Mockito.atLeastOnce())
                .transitionState(eq(tenantId), eq(run.id()), any(), transitions.capture());

        assertThat(transitions.getAllValues())
                .extracting(Run::state)
                .containsExactly(RunState.FINALIZING, RunState.STOPPED);
    }

    private Split abandonedSplit(Run run, int index) {
        return Split.plan(run.id(), tenantId, index, Json.emptyObject(), NOW)
                .claim("worker-1", NOW, LEASE)
                .fail("REJECTION_THRESHOLD_EXCEEDED", "every record was rejected", NOW)
                .abandon(NOW);
    }

    private Split completedSplit(Run run, int index) {
        return Split.plan(run.id(), tenantId, index, Json.emptyObject(), NOW)
                .claim("worker-1", NOW, LEASE)
                .complete(NOW);
    }
}
