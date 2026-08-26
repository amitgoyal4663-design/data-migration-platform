package com.dmp.engine.schedule;

import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.ScheduleRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunTrigger;
import com.dmp.domain.schedule.Schedule;
import com.dmp.domain.tenant.TenantId;
import com.dmp.engine.RunOrchestrator;
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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ScheduledRunStarterTest {

    private static final Instant NOW = Instant.parse("2026-08-07T03:00:04Z");
    private static final Instant DUE = Instant.parse("2026-08-07T03:00:00Z");

    @Mock private ScheduleRepository schedules;
    @Mock private RunOrchestrator orchestrator;
    @Mock private TenantContext tenantContext;
    @Mock private com.dmp.transform.api.WindowScript windowScript;

    private ScheduledRunStarter starter;
    private TenantId tenantId;
    private PipelineId pipelineId;
    private Schedule schedule;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        tenantId = TenantId.newId();
        pipelineId = PipelineId.newId();
        schedule = Schedule.create(tenantId, pipelineId, "Nightly orders", "0 0 3 * * ?",
                ZoneId.of("Asia/Kolkata"), null, NOW);

        starter = new ScheduledRunStarter(schedules, orchestrator, tenantContext, windowScript,
                Clock.fixed(NOW, ZoneOffset.UTC));

        when(schedules.findAllEnabled()).thenReturn(List.of(schedule));
        when(orchestrator.start(any(), any(), anyString(), any(), anyBoolean(), any()))
                .thenReturn(aRun());

        // The scheduler thread has no ambient tenant, so the work runs inside runAs. Executing the
        // supplier is what that does.
        when(tenantContext.runAs(any(), anyString(), any()))
                .thenAnswer(call -> ((Supplier<Object>) call.getArgument(2)).get());
    }

    private Run aRun() {
        return Run.create(tenantId, pipelineId, PipelineVersionId.newId(), 1,
                PipelineMode.FULL_LOAD, RunTrigger.SCHEDULED, "key", "system:scheduler", NOW);
    }

    @Test
    @DisplayName("starts a run marked as scheduled")
    void startsARun() {
        starter.start(schedule.id(), DUE);

        verify(orchestrator).start(eq(pipelineId), eq(RunTrigger.SCHEDULED), anyString(), any(),
                anyBoolean(), any());
    }

    @Test
    @DisplayName("computes the window from the due time and puts it on the run")
    void theWindowIsComputedFromTheDueTimeAndStoredOnTheRun() {
        // From DUE, not from the clock: a pod starting twenty minutes late must still process the
        // period it was scheduled for rather than one shifted by the delay.
        Schedule windowed = Schedule.create(tenantId, pipelineId, "Nightly orders", "0 0 3 * * ?",
                ZoneId.of("Asia/Kolkata"), "return { from: 'a', to: 'b' }", null, null, NOW);
        when(schedules.findAllEnabled()).thenReturn(List.of(windowed));
        when(windowScript.evaluate(any(), eq(DUE), eq(ZoneId.of("Asia/Kolkata"))))
                .thenReturn(java.util.Map.of("from", "a", "to", "b"));

        starter.start(windowed.id(), DUE);

        ArgumentCaptor<com.fasterxml.jackson.databind.JsonNode> parameters =
                ArgumentCaptor.forClass(com.fasterxml.jackson.databind.JsonNode.class);
        verify(orchestrator).start(any(), any(), anyString(), parameters.capture(),
                anyBoolean(), any());

        assertThat(parameters.getValue().path("from").asText()).isEqualTo("a");
        assertThat(parameters.getValue().path("to").asText()).isEqualTo("b");
    }

    @Test
    @DisplayName("a schedule with no window script starts a run with no parameters")
    void noScriptMeansNoParameters() {
        // Every schedule that exists today. None of them may start behaving differently.
        starter.start(schedule.id(), DUE);

        ArgumentCaptor<com.fasterxml.jackson.databind.JsonNode> parameters =
                ArgumentCaptor.forClass(com.fasterxml.jackson.databind.JsonNode.class);
        verify(orchestrator).start(any(), any(), anyString(), parameters.capture(),
                anyBoolean(), any());

        assertThat(parameters.getValue().isEmpty()).isTrue();
    }

    @Test
    @DisplayName("runs the query the schedule names, not whichever is declared first")
    void startsTheNamedQuery() {
        // The defect this replaces: a schedule took whichever query the connection happened to
        // declare first, so reordering that list -- or making a different query the default --
        // silently changed what every schedule using that connection read overnight.
        Schedule named = Schedule.create(tenantId, pipelineId, "By region nightly", "0 0 3 * * ?",
                ZoneId.of("Asia/Kolkata"), null, "By region", null, NOW);
        when(schedules.findAllEnabled()).thenReturn(List.of(named));

        starter.start(named.id(), DUE);

        ArgumentCaptor<String> query = ArgumentCaptor.forClass(String.class);
        verify(orchestrator).start(any(), any(), anyString(), any(), anyBoolean(), query.capture());

        assertThat(query.getValue()).isEqualTo("By region");
    }

    @Test
    @DisplayName("a schedule naming no query still gets the first one, as it always did")
    void noQueryNameKeepsTheOldBehaviour() {
        starter.start(schedule.id(), DUE);

        ArgumentCaptor<String> query = ArgumentCaptor.forClass(String.class);
        verify(orchestrator).start(any(), any(), anyString(), any(), anyBoolean(), query.capture());

        assertThat(query.getValue()).isNull();
    }

    @Test
    @DisplayName("acts as the schedule's tenant, since the scheduler thread has none")
    void runsAsTheTenant() {
        starter.start(schedule.id(), DUE);

        verify(tenantContext).runAs(eq(tenantId), eq("system:scheduler"), any());
    }

    @Test
    @DisplayName("keys the run on when the trigger was due, not when it ran")
    void idempotencyKeyUsesTheDueTime() {
        // Two replicas racing on one trigger fire at slightly different instants. Keying on the
        // actual moment would give them different keys and produce two runs of one migration.
        starter.start(schedule.id(), DUE);

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(orchestrator).start(any(), any(), key.capture(), any(), anyBoolean(), any());

        assertThat(key.getValue()).isEqualTo(schedule.idempotencyKeyFor(DUE));
        assertThat(key.getValue()).contains(String.valueOf(DUE.toEpochMilli()));
    }

    @Test
    @DisplayName("does not fire for a schedule disabled since it was registered")
    void disabledScheduleDoesNotFire() {
        // Quartz holds a copy from registration time. Trusting it would keep firing a rule someone
        // switched off an hour ago.
        when(schedules.findAllEnabled()).thenReturn(List.of());

        starter.start(schedule.id(), DUE);

        verify(orchestrator, never()).start(any(), any(), anyString());
    }

    @Test
    @DisplayName("records when it last fired")
    void recordsTheFiring() {
        starter.start(schedule.id(), DUE);

        ArgumentCaptor<Schedule> saved = ArgumentCaptor.forClass(Schedule.class);
        verify(schedules).update(saved.capture());

        assertThat(saved.getValue().lastFired()).contains(NOW);
    }

    @Test
    @DisplayName("a failure does not escape into Quartz")
    void failureIsSwallowed() {
        // Letting this propagate would put the trigger into an error state and stop every future
        // firing — one unpublished pipeline silently killing a nightly load forever.
        when(orchestrator.start(any(), any(), anyString()))
                .thenThrow(new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION, "no published version"));

        assertThatCode(() -> starter.start(schedule.id(), DUE)).doesNotThrowAnyException();
    }
}
