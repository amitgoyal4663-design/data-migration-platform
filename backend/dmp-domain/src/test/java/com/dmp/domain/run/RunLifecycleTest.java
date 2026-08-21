package com.dmp.domain.run;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineMode;
import com.dmp.domain.pipeline.PipelineVersionId;
import com.dmp.domain.tenant.TenantId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the run state machine and the invariants it protects. */
class RunLifecycleTest {

    private static final Instant NOW = Instant.parse("2026-08-07T03:00:00Z");

    private Run newRun(PipelineMode mode) {
        return Run.create(TenantId.newId(), PipelineId.newId(), PipelineVersionId.newId(), 1,
                mode, RunTrigger.MANUAL, null, "tester", NOW);
    }

    @Nested
    @DisplayName("state machine")
    class StateMachine {

        @Test
        @DisplayName("walks the full happy path including preparation and finalization")
        void happyPath() {
            Run run = newRun(PipelineMode.FULL_LOAD)
                    .markValidated(NOW)
                    .recordPreparation("sfdc", Json.newObject().put("jobId", "750xx"), NOW)
                    .start(NOW)
                    .finalizing(NOW)
                    .complete(NOW);

            assertThat(run.state()).isEqualTo(RunState.COMPLETED);
            assertThat(run.isTerminal()).isTrue();
            assertThat(run.endedAt()).isEqualTo(NOW);
        }

        @Test
        @DisplayName("allows PREPARING to be re-entered while polling an external system")
        void preparingIsReentrant() {
            // Each delay-queue poll that finds the external job still pending returns here rather
            // than inventing a state per attempt.
            Run run = newRun(PipelineMode.FULL_LOAD)
                    .markValidated(NOW)
                    .recordPreparation("sfdc", Json.newObject().put("attempt", 1), NOW)
                    .recordPreparation("sfdc", Json.newObject().put("attempt", 2), NOW)
                    .recordPreparation("sfdc", Json.newObject().put("attempt", 3), NOW);

            assertThat(run.state()).isEqualTo(RunState.PREPARING);
            assertThat(run.preparationHandle("sfdc")).isPresent();
        }

        @Test
        @DisplayName("routes a stopped run through FINALIZING so external resources are released")
        void stoppingStillCleansUp() {
            // Skipping cleanup on the cancel path is how external quotas leak.
            Run run = newRun(PipelineMode.FULL_LOAD)
                    .markValidated(NOW)
                    .recordPreparation("sfdc", Json.newObject(), NOW)
                    .start(NOW)
                    .requestStop(NOW);

            assertThat(run.state().canTransitionTo(RunState.FINALIZING)).isTrue();
            assertThat(run.finalizing(NOW).stopped(NOW).state()).isEqualTo(RunState.STOPPED);
        }

        @Test
        @DisplayName("refuses a transition that is not legal from the current state")
        void illegalTransition() {
            Run created = newRun(PipelineMode.FULL_LOAD);

            assertThatThrownBy(() -> created.start(NOW))
                    .isInstanceOf(DmpException.class)
                    .satisfies(e -> assertThat(((DmpException) e).errorCode())
                            .isEqualTo(ErrorCode.ILLEGAL_STATE_TRANSITION));
        }

        @Test
        @DisplayName("treats ARCHIVED as absorbing")
        void archivedIsFinal() {
            assertThat(RunState.ARCHIVED.allowedTransitions()).isEmpty();
        }

        @ParameterizedTest
        @EnumSource(RunState.class)
        @DisplayName("classifies every state as exactly one of active or terminal, or neither")
        void activeAndTerminalAreDisjoint(RunState state) {
            // CREATED and VALIDATED are neither: they exist but occupy no worker capacity.
            assertThat(state.isActive() && state.isTerminal())
                    .as("%s claims to be both active and terminal", state)
                    .isFalse();
        }

        @ParameterizedTest
        @EnumSource(value = RunState.class, names = {"CREATED", "VALIDATED"})
        @DisplayName("knows that a run cannot hold external resources before it prepares")
        void earlyStatesHoldNoResources(RunState state) {
            assertThat(state.mayHoldExternalResources()).isFalse();
        }
    }

    @Nested
    @DisplayName("continuous runs")
    class Continuous {

        @Test
        @DisplayName("refuses to complete a streaming run on its own")
        void streamingCannotComplete() {
            // A streaming run reaching COMPLETED would mean its unbounded source ended, which is
            // a stop, not a success. Allowing it would produce misleading run history.
            Run run = newRun(PipelineMode.STREAMING)
                    .markValidated(NOW)
                    .recordPreparation("kafka", Json.newObject(), NOW)
                    .start(NOW)
                    .finalizing(NOW);

            assertThatThrownBy(() -> run.complete(NOW))
                    .isInstanceOf(DmpException.class)
                    .hasMessageContaining("does not complete on its own");
        }

        @Test
        @DisplayName("lets a streaming run be stopped")
        void streamingCanBeStopped() {
            Run run = newRun(PipelineMode.STREAMING)
                    .markValidated(NOW)
                    .recordPreparation("kafka", Json.newObject(), NOW)
                    .start(NOW)
                    .requestStop(NOW)
                    .finalizing(NOW)
                    .stopped(NOW);

            assertThat(run.state()).isEqualTo(RunState.STOPPED);
        }
    }

    @Nested
    @DisplayName("external resource tracking")
    class ExternalResources {

        @Test
        @DisplayName("reports unreleased resources on a run that failed mid-preparation")
        void failedRunStillHoldsResources() {
            // The reaper's sweep predicate. A worker that died between failing and cleaning up is
            // exactly the case the happy-path FINALIZING transition cannot cover.
            Run run = newRun(PipelineMode.FULL_LOAD)
                    .markValidated(NOW)
                    .recordPreparation("sfdc", Json.newObject().put("jobId", "750xx"), NOW)
                    .fail("UPSTREAM_UNAVAILABLE", "Salesforce timed out", NOW);

            assertThat(run.hasUnreleasedExternalResources()).isTrue();
        }

        @Test
        @DisplayName("stops reporting a resource once it has been released")
        void releaseClearsTheHandle() {
            Run run = newRun(PipelineMode.FULL_LOAD)
                    .markValidated(NOW)
                    .recordPreparation("sfdc", Json.newObject().put("jobId", "750xx"), NOW)
                    .fail("UPSTREAM_UNAVAILABLE", "Salesforce timed out", NOW)
                    .releasePreparation("sfdc", NOW);

            assertThat(run.hasUnreleasedExternalResources()).isFalse();
        }

        @Test
        @DisplayName("keeps handles for several nodes independently")
        void handlesAreKeyedByNode() {
            // A pipeline may have more than one source, each holding its own external job.
            Run run = newRun(PipelineMode.FULL_LOAD)
                    .markValidated(NOW)
                    .recordPreparation("sfdc", Json.newObject().put("jobId", "750xx"), NOW)
                    .recordPreparation("databricks", Json.newObject().put("statementId", "a1b2"), NOW)
                    .releasePreparation("sfdc", NOW);

            assertThat(run.preparationHandle("sfdc")).isEmpty();
            assertThat(run.preparationHandle("databricks")).isPresent();
            assertThat(run.hasUnreleasedExternalResources()).isTrue();
        }
    }

    @Nested
    @DisplayName("metrics")
    class Metrics {

        @Test
        @DisplayName("reports records that were read but never accounted for")
        void unaccountedRecordsAreVisible() {
            // A non-zero value at the end of a run means records went missing. This is the single
            // most important invariant a migration platform can check on itself.
            //
            // Measured against what the transform stage produced, not what was read: a filter is
            // entitled to drop records and a splitter to multiply them, so comparing against the
            // source count would flag every pipeline with a transform in it.
            RunMetrics metrics = RunMetrics.ZERO.accumulate(1000, 975, 900, 50, 25, 4096);

            assertThat(metrics.unaccountedRecords()).isEqualTo(25);
        }

        @Test
        @DisplayName("reports zero unaccounted records when everything balances")
        void balancedRunHasNoGap() {
            RunMetrics metrics = RunMetrics.ZERO.accumulate(1000, 1000, 950, 50, 0, 4096);

            assertThat(metrics.unaccountedRecords()).isZero();
        }

        @Test
        @DisplayName("has no progress figure before a split plan exists")
        void progressUnknownBeforePlanning() {
            assertThat(RunMetrics.ZERO.progress()).isEmpty();
        }

        @Test
        @DisplayName("counts failed splits towards progress, since they will not be retried further")
        void progressIncludesFailures() {
            RunMetrics metrics = RunMetrics.ZERO.withSplitsTotal(10)
                    .splitCompleted().splitCompleted().splitFailed();

            assertThat(metrics.progress()).hasValue(0.3);
        }
    }
}
