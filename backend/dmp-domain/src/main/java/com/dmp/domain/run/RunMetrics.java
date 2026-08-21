package com.dmp.domain.run;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.time.Duration;
import java.util.Map;
import java.util.OptionalDouble;

/**
 * Aggregate counters for a run.
 *
 * <p>These are the roll-up shown on the run list, maintained in PostgreSQL alongside run state.
 * Per-record detail — which record failed and why — lives in MongoDB (ADR-0005); putting it here
 * would bloat the relational store and thrash vacuum for data that is read once and then aged out.
 *
 * <p>Counters only ever increase within a run, so concurrent worker updates can be applied as
 * relative increments rather than absolute writes, and no ordering guarantee between workers is
 * required.
 */
public record RunMetrics(
        long recordsRead,
        long recordsProduced,
        long recordsWritten,
        long recordsFailed,
        long recordsFiltered,
        long bytesRead,
        int splitsTotal,
        int splitsCompleted,
        int splitsFailed) {

    public static final RunMetrics ZERO = new RunMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0);

    public RunMetrics {
        if (recordsRead < 0 || recordsProduced < 0 || recordsWritten < 0 || recordsFailed < 0
                || recordsFiltered < 0 || bytesRead < 0 || splitsTotal < 0 || splitsCompleted < 0
                || splitsFailed < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "Run metrics must not be negative");
        }
    }

    public RunMetrics withSplitsTotal(int total) {
        return new RunMetrics(recordsRead, recordsProduced, recordsWritten, recordsFailed,
                recordsFiltered, bytesRead, total, splitsCompleted, splitsFailed);
    }

    /** Folds a completed split's counters into the run total. */
    public RunMetrics accumulate(long read, long produced, long written, long failed,
                                 long filtered, long bytes) {
        return new RunMetrics(
                recordsRead + read,
                recordsProduced + produced,
                recordsWritten + written,
                recordsFailed + failed,
                recordsFiltered + filtered,
                bytesRead + bytes,
                splitsTotal, splitsCompleted, splitsFailed);
    }

    public RunMetrics splitCompleted() {
        return new RunMetrics(recordsRead, recordsProduced, recordsWritten, recordsFailed,
                recordsFiltered, bytesRead, splitsTotal, splitsCompleted + 1, splitsFailed);
    }

    public RunMetrics splitFailed() {
        return new RunMetrics(recordsRead, recordsProduced, recordsWritten, recordsFailed,
                recordsFiltered, bytesRead, splitsTotal, splitsCompleted, splitsFailed + 1);
    }

    /** Fraction of splits finished, between 0 and 1. Empty while the plan has no splits yet. */
    public OptionalDouble progress() {
        if (splitsTotal <= 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of((double) (splitsCompleted + splitsFailed) / splitsTotal);
    }

    /** Records written per second over the supplied elapsed time. Empty for a zero duration. */
    public OptionalDouble throughputPerSecond(Duration elapsed) {
        if (elapsed == null || elapsed.isZero() || elapsed.isNegative()) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(recordsWritten / (elapsed.toMillis() / 1000.0));
    }

    /**
     * Records that reached the sink stage and were neither written nor rejected.
     *
     * <p>Measured against {@code recordsProduced} rather than {@code recordsRead}, because a
     * transform is entitled to change how many records exist — a filter drops them, a splitter
     * multiplies them. What no part of this system may do is take a record and lose it silently,
     * and that is what this counts. A non-zero value at the end of a run is the single most
     * important thing a migration platform can notice about itself.
     */
    public long unaccountedRecords() {
        return recordsProduced - recordsWritten - recordsFailed;
    }

    /** Source records that were neither filtered out nor turned into at least one output. */
    public long unexplainedReads() {
        return Math.max(0, (recordsRead - recordsFiltered) - recordsProduced);
    }

    public Map<String, Object> asMap() {
        return Map.of(
                "recordsRead", recordsRead,
                "recordsProduced", recordsProduced,
                "recordsWritten", recordsWritten,
                "recordsFailed", recordsFailed,
                "recordsFiltered", recordsFiltered,
                "bytesRead", bytesRead,
                "splitsTotal", splitsTotal,
                "splitsCompleted", splitsCompleted,
                "splitsFailed", splitsFailed);
    }
}
