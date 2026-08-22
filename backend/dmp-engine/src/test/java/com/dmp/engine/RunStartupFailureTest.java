package com.dmp.engine;

import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.application.port.out.RunEventPublisher;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorException;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunState;
import com.dmp.domain.run.RunTrigger;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * What happens when a run cannot be started.
 *
 * <p>A run is moved to VALIDATED and then PREPARING <em>before</em> the work that can actually
 * fail — planning, and the source's own preparation. So when something throws, the copy of the run
 * the failure handler was given is two states out of date.
 *
 * <p>Using that stale copy as the precondition for the conditional update matched nothing and did
 * so silently: the run stayed in PREPARING for ever, with no error on it and nothing to say why it
 * had stopped. It looked correct for exactly as long as failures happened before the first
 * transition, which is why a query missing a parameter was the thing that finally exposed it.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RunStartupFailureTest {

    private static final Instant NOW = Instant.parse("2026-08-20T10:00:00Z");

    /** Nothing here is rate limited, so every chunk is allowed and nothing is ever handed back. */
    private static final com.dmp.application.port.out.RateLimiter GRANTS_EVERYTHING =
            new com.dmp.application.port.out.RateLimiter() {
                @Override
                public Optional<Duration> tryAcquire(
                        com.dmp.domain.connector.ConnectorInstanceId connector,
                        com.dmp.domain.connector.RateLimitPolicy policy, long records, long calls) {
                    return Optional.empty();
                }

                @Override
                public void returnUnused(com.dmp.domain.connector.ConnectorInstanceId connector,
                                         com.dmp.domain.connector.RateLimitPolicy policy,
                                         long reservedRecords, long reservedCalls,
                                         long usedRecords, long usedCalls) {
                }
            };

    @Mock private RunRepository runs;
    @Mock private SplitRepository splits;
    @Mock private CheckpointRepository checkpoints;
    @Mock private RunPlanner planner;
    @Mock private RunOrchestrator orchestrator;
    @Mock private ChunkExecutor executor;
    @Mock private RunEventPublisher events;
    @Mock private com.dmp.engine.schedule.ExternalJobScheduler externalJobs;

    private WorkerLoop worker;
    private TenantId tenantId;
    private Run created;

    @BeforeEach
    void setUp() {
        tenantId = TenantId.newId();
        created = Run.create(tenantId, PipelineId.newId(), PipelineVersionId.newId(), 1,
                PipelineMode.FULL_LOAD, RunTrigger.API, null, "tester", NOW);

        worker = new WorkerLoop(runs, splits, checkpoints, planner, orchestrator, executor, events,
                externalJobs, GRANTS_EVERYTHING,
                new com.dmp.connector.runtime.ConnectorRegistry("plugins"),
                EngineMetrics.NONE,
                Clock.fixed(NOW, ZoneOffset.UTC), "worker-1", 4,
                Duration.ofSeconds(5), Duration.ofMillis(200));

        when(runs.findByStates(any(), anyInt())).thenReturn(List.of(created));
        when(splits.findExpiredLeases(any(), anyInt())).thenReturn(List.of());
        when(runs.transitionState(any(), any(), any(), any()))
                .thenAnswer(call -> Optional.of(call.getArgument(3, Run.class)));

        // The same poll pass also looks for chunks to claim. Nothing here is about that, but it
        // must not throw on the way past.
        when(planner.resolve(any())).thenReturn(new ResolvedPipeline(
                null, null, null, null, null, List.of(),
                com.dmp.domain.pipeline.ChunkingPolicy.DEFAULT,
                com.dmp.domain.pipeline.ExecutionPolicy.DEFAULT,
                com.dmp.domain.audit.AuditPolicy.DEFAULT, Json.emptyObject()));
        when(splits.claimNextPending(any(), any(), any(), any(), any())).thenReturn(Optional.empty());
    }

    @Test
    void aRunThatCannotStartIsMarkedFailedFromWhateverStateItReached() {
        // The run is already PREPARING by the time planning throws — which is the situation the
        // stale copy could not express.
        Run preparing = created.markValidated(NOW).recordPreparation("src", Json.emptyObject(), NOW);
        when(runs.findById(tenantId, created.id())).thenReturn(Optional.of(preparing));

        doThrow(new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                "This pipeline's filter expects ':to', but the run was started without it."))
                .when(orchestrator).advanceToRunning(any(), any());

        worker.pollOnce();

        ArgumentCaptor<Run> written = ArgumentCaptor.forClass(Run.class);
        verify(runs).transitionState(eq(tenantId), eq(created.id()),
                eq(RunState.PREPARING), written.capture());

        assertThat(written.getValue().state()).isEqualTo(RunState.FAILED);
        assertThat(written.getValue().errorMessage()).contains("':to'");
    }

    @Test
    void theStoredStateIsUsedAsThePreconditionRatherThanTheCallersCopy() {
        // The regression itself. The caller holds a CREATED run; the store says PREPARING. A
        // conditional update expecting CREATED matches nothing and the run is stranded.
        Run preparing = created.markValidated(NOW).recordPreparation("src", Json.emptyObject(), NOW);
        when(runs.findById(tenantId, created.id())).thenReturn(Optional.of(preparing));

        doThrow(new IllegalStateException("anything at all"))
                .when(orchestrator).advanceToRunning(any(), any());

        worker.pollOnce();

        verify(runs).transitionState(any(), any(), eq(RunState.PREPARING), any());
    }
}
