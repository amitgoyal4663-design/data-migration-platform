package com.dmp.domain.run;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The balance sheet for one run: did every record that was read end up somewhere it can be named?
 *
 * <p>This is what a migration is signed off on. Not a duration, not a throughput chart — a page
 * that starts with what the source held and ends with zero, having accounted for every record in
 * between. Everything needed to produce it was already being counted; what was missing was the
 * arithmetic in one place, with a verdict on it.
 *
 * <p><b>Two independent counts, deliberately.</b> The run's own counters are incremented by workers
 * as chunks finish. The record index is written per record, by a different code path, to a
 * different store. Comparing them is the only check here that can catch a defect in the counting
 * itself — a sheet that reconciles against nothing but itself will balance however wrong it is.
 * That is why {@link Check} exists as a separate list rather than being folded into the sheet.
 *
 * <p>Computed on demand rather than stored. The inputs are already durable and this is pure
 * arithmetic over them, so a stored copy could only ever be a second version of the truth that
 * drifts from the first.
 */
public record Reconciliation(
        Verdict verdict,
        /** The balance, in the order it should be read. */
        List<Line> sheet,
        /** Independent cross-checks. Empty when the pipeline indexed nothing to check against. */
        List<Check> checks,
        /** The record index's own tally, by outcome. Empty when the pipeline did not index. */
        Map<String, Long> byOutcome,
        long indexedTotal,
        /** Whether there is an index to compare with at all. */
        boolean indexed,
        /** Whether the run has finished. A mid-run balance is arithmetic about a moving target. */
        boolean complete) {

    /** The overall answer, and the only part most people read. */
    public enum Verdict {

        /** Every record is accounted for, and both ways of counting agree. */
        BALANCED,

        /**
         * Something does not add up.
         *
         * <p>Not necessarily data loss — an index written by a pipeline whose audit level changed
         * mid-flight will disagree legitimately. But it is always something a person should look
         * at before signing anything, which is the point of a verdict.
         */
        DISCREPANCY,

        /**
         * The run has not finished.
         *
         * <p>Reported rather than computed-anyway, because a partial sheet that says
         * "4,000 unaccounted" while chunks are still writing is alarming and meaningless, and
         * people remember the number rather than the caveat.
         */
        INCOMPLETE
    }

    /** What a line means, so a console can style it without parsing the label. */
    public enum Kind {
        /** Where the records came from. */
        TOTAL,
        /** Records removed from the total above, for a stated reason. */
        DEDUCTION,
        /** A running total after the deductions above it. */
        SUBTOTAL,
        /** Records that reached their destination. */
        RESULT,
        /** Handed over, with the destination's verdict still outstanding. */
        PENDING,
        /** The closing figure, which must be zero. */
        BALANCE
    }

    /**
     * One row of the sheet.
     *
     * @param note why this line is here, in the language of the person reading it — a sheet whose
     *             rows are labelled {@code recordsFiltered} is a database dump, not a report
     */
    public record Line(String label, long count, Kind kind, String note) {
    }

    /**
     * One comparison between two things that were counted separately.
     *
     * @param expected what the run's own counters say
     * @param actual   what the record index says
     */
    public record Check(String label, long expected, long actual, String note) {

        public boolean passed() {
            return expected == actual;
        }

        public long difference() {
            return actual - expected;
        }
    }

    /**
     * Builds the sheet from a run's counters and, where there is one, the record index's tally.
     *
     * @param byOutcome outcome name to count, exactly as the index reports it. Null or empty means
     *                  the pipeline did not index, which is a legitimate configuration and not a
     *                  failure — the sheet is still produced, with no cross-checks.
     */
    public static Reconciliation of(RunState state, RunMetrics metrics,
                                    Map<String, Long> byOutcome) {

        Map<String, Long> outcomes = normalise(byOutcome);
        boolean indexed = !outcomes.isEmpty();
        boolean complete = state != null && state.isTerminal();

        long read = metrics.recordsRead();
        long filtered = metrics.recordsFiltered();
        long produced = metrics.recordsProduced();
        long written = metrics.recordsWritten();
        long failed = metrics.recordsFailed();
        long unaccounted = metrics.unaccountedRecords();

        long transformFailed = outcomes.getOrDefault("TRANSFORM_FAILED", 0L);
        long rejected = outcomes.getOrDefault("REJECTED", 0L);
        long callFailed = outcomes.getOrDefault("CALL_FAILED", 0L);
        long sent = outcomes.getOrDefault("SENT", 0L);

        List<Line> sheet = new ArrayList<>();
        sheet.add(new Line("Read from source", read, Kind.TOTAL,
                "Records the source actually handed over"));
        sheet.add(new Line("Filtered by rules", filtered, Kind.DEDUCTION,
                "Dropped deliberately by a transform. Not a failure"));

        // Shown only when it says something. A pipeline with no splitter has produced equal to
        // read-minus-filtered, and a line stating that is noise on every report ever run.
        long expectedFromReads = read - filtered;
        if (produced != expectedFromReads) {
            sheet.add(new Line("Produced by transforms", produced, Kind.SUBTOTAL,
                    produced > expectedFromReads
                            ? "More than were read, because a transform fans records out"
                            : "Fewer than were read and not filtered — records were lost before "
                                    + "delivery, which is a defect"));
        } else {
            sheet.add(new Line("Entered delivery", produced, Kind.SUBTOTAL,
                    "Records handed to the delivery stage"));
        }

        sheet.add(new Line("Written", written, Kind.RESULT,
                "Accepted by the destination"));

        // The failure breakdown comes from the index, because the run's counters hold one number
        // for every kind of failure. Without an index there is one line, which is the honest
        // amount of detail available rather than a breakdown invented to fill the space.
        if (indexed) {
            sheet.add(new Line("Failed in transform", transformFailed, Kind.DEDUCTION,
                    "A script threw. The record never reached the destination"));
            sheet.add(new Line("Rejected by destination", rejected, Kind.DEDUCTION,
                    "The destination looked at this record and refused it"));
            sheet.add(new Line("Lost to a failed call", callFailed, Kind.DEDUCTION,
                    "The call carrying them failed. The destination gave no verdict on them"));
            if (sent > 0) {
                sheet.add(new Line("Awaiting verdict", sent, Kind.PENDING,
                        "Handed to a destination that decides later and has not said"));
            }
        } else {
            sheet.add(new Line("Failed", failed, Kind.DEDUCTION,
                    "Rejected, or lost to a failed call, or thrown on by a transform. This "
                            + "pipeline does not index records, so the three cannot be separated"));
        }

        sheet.add(new Line("Unaccounted", unaccounted, Kind.BALANCE,
                unaccounted == 0
                        ? "Every record produced was either written or accounted for as failed"
                        : "Records that reached delivery and were neither written nor reported "
                                + "failed. This is data loss"));

        List<Check> checks = checks(metrics, outcomes, indexed);
        long indexedTotal = outcomes.values().stream().mapToLong(Long::longValue).sum();

        Verdict verdict;
        if (!complete) {
            verdict = Verdict.INCOMPLETE;
        } else if (unaccounted != 0 || metrics.unexplainedReads() != 0
                || checks.stream().anyMatch(check -> !check.passed())) {
            verdict = Verdict.DISCREPANCY;
        } else {
            verdict = Verdict.BALANCED;
        }

        return new Reconciliation(verdict, List.copyOf(sheet), List.copyOf(checks),
                Map.copyOf(outcomes), indexedTotal, indexed, complete);
    }

    /**
     * The comparisons worth making between the counters and the index.
     *
     * <p>Each one has to be a genuine independent check, not a restatement. "The index total equals
     * the sum of the index's own outcomes" is arithmetic, not evidence, and putting it here would
     * make the report look more rigorous than it is.
     */
    private static List<Check> checks(RunMetrics metrics, Map<String, Long> outcomes,
                                      boolean indexed) {
        if (!indexed) {
            return List.of();
        }

        long written = outcomes.getOrDefault("WRITTEN", 0L);
        long sent = outcomes.getOrDefault("SENT", 0L);
        long filtered = outcomes.getOrDefault("FILTERED", 0L);
        long failedInIndex = outcomes.getOrDefault("REJECTED", 0L)
                + outcomes.getOrDefault("CALL_FAILED", 0L)
                + outcomes.getOrDefault("TRANSFORM_FAILED", 0L);
        long indexedTotal = outcomes.values().stream().mapToLong(Long::longValue).sum();

        List<Check> checks = new ArrayList<>();
        checks.add(new Check("Records written", metrics.recordsWritten(), written + sent,
                "The run's counter against the index's own count of written records. A "
                        + "destination that answers later contributes to both"));
        checks.add(new Check("Records failed", metrics.recordsFailed(), failedInIndex,
                "The run's failure counter against the index's rejected, call-failed and "
                        + "transform-failed entries"));
        checks.add(new Check("Records filtered", metrics.recordsFiltered(), filtered,
                "The run's filtered counter against the index's filtered entries"));
        // Produced already includes transform failures — the engine counts a record the script
        // threw on as produced-and-failed so the delivery-side invariant still closes. Filtered
        // records never enter that population, so they are added back here.
        checks.add(new Check("Total records handled", metrics.recordsProduced()
                + metrics.recordsFiltered(), indexedTotal,
                "Everything the run says it handled against everything the index holds. The "
                        + "broadest check, and the one that catches a chunk that indexed nothing"));
        return List.copyOf(checks);
    }

    /** Whether anything at all is wrong, for a caller that only wants the one bit. */
    public boolean balanced() {
        return verdict == Verdict.BALANCED;
    }

    private static Map<String, Long> normalise(Map<String, Long> byOutcome) {
        if (byOutcome == null || byOutcome.isEmpty()) {
            return Map.of();
        }
        Map<String, Long> copy = new LinkedHashMap<>();
        byOutcome.forEach((outcome, count) -> {
            if (outcome != null && count != null && count > 0) {
                copy.put(outcome, count);
            }
        });
        return copy;
    }
}
