package com.dmp.domain.run;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Reconciliation")
class ReconciliationTest {

    /**
     * A thousand read, three hundred filtered, one written batch short of perfect.
     *
     * <p>Built the way the engine actually counts: a record a script threw on is produced
     * <em>and</em> failed, so produced is read minus filtered, and failed holds both the script's
     * casualties and the destination's.
     */
    private static RunMetrics metrics(long read, long filtered, long written, long failed) {
        return RunMetrics.ZERO.accumulate(read, read - filtered, written, failed, filtered, 0);
    }

    @Nested
    @DisplayName("verdict")
    class VerdictTest {

        @Test
        @DisplayName("is INCOMPLETE while the run is still going, however the numbers look")
        void incompleteWhileRunning() {
            // Deliberately balanced: a mid-run sheet must not report a verdict even when it could
            // compute a flattering one, because the number people remember is the one they read
            // first and it will change.
            var report = Reconciliation.of(RunState.RUNNING, metrics(1000, 0, 1000, 0), Map.of());

            assertThat(report.verdict()).isEqualTo(Reconciliation.Verdict.INCOMPLETE);
            assertThat(report.complete()).isFalse();
        }

        @Test
        @DisplayName("is BALANCED when every produced record was written or reported failed")
        void balanced() {
            var report = Reconciliation.of(RunState.COMPLETED,
                    metrics(1000, 300, 650, 50), Map.of());

            assertThat(report.verdict()).isEqualTo(Reconciliation.Verdict.BALANCED);
            assertThat(report.balanced()).isTrue();
        }

        @Test
        @DisplayName("is DISCREPANCY when records reached delivery and were never accounted for")
        void unaccountedRecordsAreADiscrepancy() {
            // 700 produced, 650 written, 25 failed — twenty-five records that reached the sink
            // stage and left no trace either way. This is the failure the whole report exists for.
            var report = Reconciliation.of(RunState.COMPLETED,
                    metrics(1000, 300, 650, 25), Map.of());

            assertThat(report.verdict()).isEqualTo(Reconciliation.Verdict.DISCREPANCY);
            assertThat(report.sheet())
                    .filteredOn(line -> line.kind() == Reconciliation.Kind.BALANCE)
                    .singleElement()
                    .satisfies(line -> assertThat(line.count()).isEqualTo(25));
        }
    }

    @Nested
    @DisplayName("the sheet")
    class SheetTest {

        @Test
        @DisplayName("closes on a balance line, which is the figure that must be zero")
        void closesOnTheBalance() {
            var report = Reconciliation.of(RunState.COMPLETED,
                    metrics(1000, 300, 650, 50), Map.of());

            assertThat(report.sheet()).isNotEmpty();
            assertThat(report.sheet().get(report.sheet().size() - 1).kind())
                    .isEqualTo(Reconciliation.Kind.BALANCE);
        }

        @Test
        @DisplayName("collapses the failure breakdown when there is no index to break it down with")
        void oneFailureLineWithoutAnIndex() {
            var report = Reconciliation.of(RunState.COMPLETED,
                    metrics(1000, 0, 900, 100), Map.of());

            // One line saying 100, not three lines of zero. A breakdown invented to fill the space
            // would read as "nothing was rejected" when nobody counted.
            assertThat(labels(report)).contains("Failed")
                    .doesNotContain("Rejected by destination", "Failed in transform");
            assertThat(report.indexed()).isFalse();
        }

        @Test
        @DisplayName("separates the three kinds of failure when the index can")
        void brokenDownWithAnIndex() {
            var report = Reconciliation.of(RunState.COMPLETED, metrics(1000, 0, 900, 100),
                    Map.of("WRITTEN", 900L, "TRANSFORM_FAILED", 40L,
                            "REJECTED", 55L, "CALL_FAILED", 5L));

            assertThat(labels(report)).contains(
                    "Failed in transform", "Rejected by destination", "Lost to a failed call");
        }

        @Test
        @DisplayName("names a splitter rather than reporting its extra records as an error")
        void fanOutIsExplained() {
            // Five hundred read, a thousand produced. Legitimate, and the note has to say so or
            // every run with a splitter looks broken.
            var report = Reconciliation.of(RunState.COMPLETED,
                    RunMetrics.ZERO.accumulate(500, 1000, 1000, 0, 0, 0), Map.of());

            assertThat(report.sheet())
                    .filteredOn(line -> line.label().equals("Produced by transforms"))
                    .singleElement()
                    .satisfies(line -> assertThat(line.note()).contains("fans records out"));
            assertThat(report.verdict()).isEqualTo(Reconciliation.Verdict.BALANCED);
        }
    }

    @Nested
    @DisplayName("the cross-checks")
    class CheckTest {

        @Test
        @DisplayName("are absent when the pipeline indexed nothing, rather than passing vacuously")
        void noChecksWithoutAnIndex() {
            var report = Reconciliation.of(RunState.COMPLETED,
                    metrics(1000, 0, 1000, 0), Map.of());

            assertThat(report.checks()).isEmpty();
            assertThat(report.verdict()).isEqualTo(Reconciliation.Verdict.BALANCED);
        }

        @Test
        @DisplayName("pass when the index agrees with the run's own counters")
        void agreeing() {
            var report = Reconciliation.of(RunState.COMPLETED, metrics(1000, 300, 650, 50),
                    Map.of("WRITTEN", 650L, "FILTERED", 300L,
                            "REJECTED", 30L, "TRANSFORM_FAILED", 20L));

            assertThat(report.checks()).allSatisfy(check -> assertThat(check.passed()).isTrue());
            assertThat(report.verdict()).isEqualTo(Reconciliation.Verdict.BALANCED);
            assertThat(report.indexedTotal()).isEqualTo(1000);
        }

        @Test
        @DisplayName("fail the whole verdict when a chunk indexed nothing it should have")
        void aChunkThatIndexedNothingIsCaught() {
            // The counters balance perfectly — this is exactly the case a sheet checked only
            // against itself would sign off. Two hundred entries are simply missing from the index.
            var report = Reconciliation.of(RunState.COMPLETED, metrics(1000, 0, 1000, 0),
                    Map.of("WRITTEN", 800L));

            assertThat(report.verdict()).isEqualTo(Reconciliation.Verdict.DISCREPANCY);
            assertThat(report.checks())
                    .filteredOn(check -> !check.passed())
                    .isNotEmpty()
                    .allSatisfy(check -> assertThat(check.difference()).isNegative());
        }

        @Test
        @DisplayName("count a destination that answers later as written, in both places")
        void sentCountsAsWritten() {
            // An async sink indexes SENT and the run counts the records as written. Treating SENT
            // as anything else would report every Salesforce run as short by its whole volume.
            var report = Reconciliation.of(RunState.COMPLETED, metrics(500, 0, 500, 0),
                    Map.of("SENT", 500L));

            assertThat(report.checks())
                    .filteredOn(check -> check.label().equals("Records written"))
                    .singleElement()
                    .satisfies(check -> assertThat(check.passed()).isTrue());
            assertThat(labels(report)).contains("Awaiting verdict");
        }
    }

    @Test
    @DisplayName("treats an outcome reported as zero as not having happened")
    void zeroOutcomesAreDropped() {
        var report = Reconciliation.of(RunState.COMPLETED, metrics(100, 0, 100, 0),
                Map.of("WRITTEN", 100L, "REJECTED", 0L));

        assertThat(report.byOutcome()).containsOnlyKeys("WRITTEN");
    }

    private static List<String> labels(Reconciliation report) {
        return report.sheet().stream().map(Reconciliation.Line::label).toList();
    }
}
