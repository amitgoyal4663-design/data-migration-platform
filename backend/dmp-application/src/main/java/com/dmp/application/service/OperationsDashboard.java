package com.dmp.application.service;

import com.dmp.application.common.PageQuery;
import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.port.out.RunRepository;
import com.dmp.application.port.out.ScheduleRepository;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunState;
import com.dmp.domain.schedule.Schedule;
import com.dmp.domain.tenant.TenantId;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * What a support team needs to see every morning, and nothing else.
 *
 * <p>The run list already answers "what happened". It does not answer the question somebody
 * actually arrives with, which is <em>"is anything wrong?"</em> — and that question cannot be
 * answered by a row on its own. Five thousand records is healthy for one pipeline and a catastrophe
 * for another; the only thing that makes a number readable is what that same pipeline usually does.
 *
 * <p><b>The comparison is deliberately arithmetic anybody can restate.</b> A median of recent runs
 * and a band around it. Not a model, not a moving average with a decay constant — because the first
 * question after a flag is always "why is that abnormal", and an answer nobody on the support desk
 * can explain gets ignored within a fortnight, taking the real alerts with it.
 *
 * <p>Read-only and computed on demand. Nothing here is stored, so there is no second copy of the
 * truth to drift from the runs it describes.
 */
@Service
public class OperationsDashboard {

    /**
     * Runs behind a baseline.
     *
     * <p>Ten is a compromise. Fewer and one unusual night moves the median enough to hide the next
     * one; many more and a pipeline whose volume legitimately grew stays flagged for a fortnight
     * after everyone has accepted the new normal.
     */
    private static final int BASELINE_RUNS = 10;

    /** Below this there is no baseline worth the name, and the platform says so rather than guessing. */
    private static final int MIN_BASELINE_RUNS = 3;

    /**
     * How far from the median counts as abnormal.
     *
     * <p>Wide, on purpose. A daily load varies with the business — a bank holiday, a quarter end —
     * and a band tight enough to catch a 10% drift would flag most Mondays. Half and double catches
     * what it is meant to catch: a filter that broke, a window that doubled, a source that emptied.
     */
    private static final double LOW = 0.5;
    private static final double HIGH = 2.0;

    /** Duration is noisier than volume — a busy cluster is not an incident — so it gets more room. */
    private static final double SLOW = 3.0;

    /**
     * Grace after a schedule's expected time before it counts as missed.
     *
     * <p>Absorbs a worker that was busy, a slow start, and the minute or two between a trigger
     * firing and a run appearing. Anything beyond it is a schedule that did not happen.
     */
    private static final Duration LATE_AFTER = Duration.ofMinutes(30);

    private final PipelineRepository pipelines;
    private final RunRepository runs;
    private final ScheduleRepository schedules;
    private final TenantContext tenantContext;
    private final Clock clock;

    public OperationsDashboard(PipelineRepository pipelines, RunRepository runs,
                               ScheduleRepository schedules, TenantContext tenantContext,
                               Clock clock) {
        this.pipelines = pipelines;
        this.runs = runs;
        this.schedules = schedules;
        this.tenantContext = tenantContext;
        this.clock = clock;
    }

    /** Every watched pipeline, with its last run judged against its own history. */
    public List<PipelineHealth> today(Duration window) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant since = clock.instant().minus(window);

        List<Pipeline> watched = pipelines.search(tenantId,
                        new PipelineRepository.PipelineSearch(null, null, null, null),
                        // The page cap, not a number chosen here. A watchlist above two hundred
                        // pipelines is not a watchlist, and paging this screen would be solving
                        // the wrong problem.
                        new PageQuery(0, 200, "name", true))
                .content().stream()
                .filter(Pipeline::monitored)
                .toList();

        List<PipelineHealth> health = new ArrayList<>(watched.size());
        for (Pipeline pipeline : watched) {
            health.add(assess(tenantId, pipeline, since));
        }

        // Worst first. A screen read every morning is scanned from the top, and sorting by name
        // would put the one thing that needs attention wherever the alphabet happened to leave it.
        health.sort(Comparator.comparingInt((PipelineHealth h) -> h.worst().ordinal()).reversed()
                .thenComparing(PipelineHealth::name));
        return health;
    }

    private PipelineHealth assess(TenantId tenantId, Pipeline pipeline, Instant since) {
        List<Run> recent = runs.search(tenantId,
                        new RunRepository.RunSearch(pipeline.id(), Set.of(), null, null, null),
                        new PageQuery(0, BASELINE_RUNS + 5, "createdAt", false))
                .content();

        Run latest = recent.stream()
                .filter(run -> !run.dryRun())
                .findFirst()
                .orElse(null);

        // The baseline excludes the run being judged, and every run that did not finish cleanly. A
        // failed run's counters are a partial story, and letting them pull the median down would
        // make the next failure look normal.
        List<Long> history = recent.stream()
                .filter(run -> latest == null || !run.id().equals(latest.id()))
                .filter(run -> run.state() == RunState.COMPLETED && !run.dryRun())
                .limit(BASELINE_RUNS)
                .map(run -> run.metrics().recordsRead())
                .toList();

        List<Long> durations = recent.stream()
                .filter(run -> latest == null || !run.id().equals(latest.id()))
                .filter(run -> run.state() == RunState.COMPLETED && !run.dryRun())
                .limit(BASELINE_RUNS)
                .map(run -> run.duration(clock.instant()).map(Duration::toSeconds).orElse(0L))
                .filter(seconds -> seconds > 0)
                .toList();

        Long typicalRows = median(history);
        Long typicalSeconds = median(durations);

        List<Finding> findings = new ArrayList<>();
        findings.addAll(scheduleFindings(tenantId, pipeline, latest));
        if (latest != null) {
            findings.addAll(runFindings(latest, typicalRows, typicalSeconds, since));
        }

        return new PipelineHealth(pipeline.id().toString(), pipeline.name(), latest, typicalRows,
                typicalSeconds, history.size(), List.copyOf(findings));
    }

    /**
     * The check nothing else in the platform performs.
     *
     * <p>Every other failure produces an event — a state, a message, a metric. A run that never
     * started produces nothing at all, so it is invisible to anything watching for something to go
     * wrong. It is also the failure most likely to last for days.
     */
    private List<Finding> scheduleFindings(TenantId tenantId, Pipeline pipeline, Run latest) {
        List<Finding> findings = new ArrayList<>();
        Instant now = clock.instant();

        for (Schedule schedule : schedules.findByPipeline(tenantId, pipeline.id())) {
            if (!schedule.enabled()) {
                continue;
            }
            Instant lastFired = schedule.lastFiredAt() == null
                    ? pipeline.createdAt() : schedule.lastFiredAt();

            Optional<Instant> expected = nextFireAfter(schedule, lastFired);
            if (expected.isEmpty()) {
                continue;
            }
            Duration overdue = Duration.between(expected.get(), now);
            if (overdue.compareTo(LATE_AFTER) > 0) {
                findings.add(new Finding(Severity.CRITICAL, "DID_NOT_RUN",
                        "'" + schedule.name() + "' was due " + humanise(overdue)
                                + " ago and has not started. Nothing else reports this: a run that "
                                + "never began produces no failure to notice.",
                        expected.get().toString()));
            }
        }
        return findings;
    }

    /**
     * The next time a schedule should have fired after a given moment.
     *
     * <p>Computed forwards from the last firing rather than backwards from now, because a cron
     * expression can be walked in only one direction — and forwards from a known firing is the
     * question anyway: "should there have been another one by now?"
     */
    private Optional<Instant> nextFireAfter(Schedule schedule, Instant after) {
        try {
            CronExpression cron = CronExpression.parse(schedule.cronExpression());
            ZonedDateTime next = cron.next(ZonedDateTime.ofInstant(after, schedule.timezone()));
            return Optional.ofNullable(next).map(ZonedDateTime::toInstant);
        } catch (RuntimeException e) {
            // A cron the platform cannot parse is a problem, but not this screen's problem — and
            // guessing would produce a permanent false alarm on the one screen that must stay
            // trustworthy.
            return Optional.empty();
        }
    }

    private List<Finding> runFindings(Run run, Long typicalRows, Long typicalSeconds,
                                      Instant since) {
        List<Finding> findings = new ArrayList<>();
        long read = run.metrics().recordsRead();

        if (run.state() == RunState.FAILED) {
            findings.add(new Finding(Severity.CRITICAL, "FAILED",
                    "The last run failed: " + (run.errorMessage() == null
                            ? run.errorCode() : run.errorMessage()), null));
        }

        if (run.metrics().unaccountedRecords() != 0) {
            findings.add(new Finding(Severity.CRITICAL, "UNACCOUNTED",
                    run.metrics().unaccountedRecords() + " record(s) reached delivery and were "
                            + "neither written nor reported failed. This is data loss.", null));
        }

        // A completed run that moved nothing. Almost always a parameter window that no longer
        // matches, or a source that changed under the pipeline — and it reports as success.
        if (run.state() == RunState.COMPLETED && read == 0) {
            findings.add(new Finding(Severity.WARNING, "NO_ROWS",
                    "Completed having read nothing. Usually a query window that no longer matches "
                            + "the data, or a source that has moved.", null));
        }

        if (typicalRows != null && typicalRows > 0 && read > 0
                && run.state() == RunState.COMPLETED) {
            if (read < typicalRows * LOW) {
                findings.add(new Finding(Severity.WARNING, "VOLUME_LOW",
                        "Read " + read + ", where this pipeline usually reads about " + typicalRows
                                + ".", String.valueOf(typicalRows)));
            } else if (read > typicalRows * HIGH) {
                findings.add(new Finding(Severity.WARNING, "VOLUME_HIGH",
                        "Read " + read + ", where this pipeline usually reads about " + typicalRows
                                + ". A widened window will re-send data the destination already has.",
                        String.valueOf(typicalRows)));
            }
        }

        long failed = run.metrics().recordsFailed();
        long produced = run.metrics().recordsProduced();
        if (produced > 0 && failed * 100 / produced >= 10) {
            findings.add(new Finding(Severity.WARNING, "REJECTIONS",
                    failed + " of " + produced + " records (" + (failed * 100 / produced)
                            + "%) did not arrive.", null));
        }

        run.duration(clock.instant()).ifPresent(took -> {
            if (typicalSeconds != null && typicalSeconds > 0
                    && took.toSeconds() > typicalSeconds * SLOW) {
                findings.add(new Finding(Severity.WARNING, "SLOW",
                        "Took " + humanise(took) + ", where this pipeline usually takes about "
                                + humanise(Duration.ofSeconds(typicalSeconds))
                                + ". Often an upstream system degrading before it breaks.",
                        String.valueOf(typicalSeconds)));
            }
        });

        // Said last, because it is context rather than a fault: a pipeline nobody has run inside
        // the window is not broken, but it is also not evidence that anything works.
        if (run.createdAt().isBefore(since)) {
            findings.add(new Finding(Severity.INFO, "NOTHING_RECENT",
                    "No run inside the window. The last one was "
                            + humanise(Duration.between(run.createdAt(), clock.instant()))
                            + " ago.", null));
        }
        return findings;
    }

    /**
     * The middle value, not the mean.
     *
     * <p>One catastrophic night — a run that read ten times its usual volume — drags a mean far
     * enough that the next genuine anomaly falls inside the band. The median ignores it.
     */
    private static Long median(List<Long> values) {
        if (values.size() < MIN_BASELINE_RUNS) {
            return null;
        }
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Comparator.naturalOrder());
        return sorted.get(sorted.size() / 2);
    }

    private static String humanise(Duration duration) {
        long minutes = Math.max(0, duration.toMinutes());
        if (minutes < 60) {
            return minutes + "m";
        }
        long hours = minutes / 60;
        return hours < 48 ? hours + "h" : (hours / 24) + "d";
    }

    /** How loudly a finding should be read. */
    public enum Severity {
        INFO, WARNING, CRITICAL
    }

    /**
     * @param detail the comparison a number was judged against, so the screen can show "5,102,
     *               usually 4,900" rather than asking somebody to take the verdict on trust
     */
    public record Finding(Severity severity, String code, String message, String detail) {
    }

    /**
     * @param baselineRuns how many runs the comparison rests on. Shown, because a judgement from
     *                     three runs and one from ten deserve different amounts of belief.
     */
    public record PipelineHealth(String pipelineId, String name, Run latest, Long typicalRows,
                                 Long typicalSeconds, int baselineRuns, List<Finding> findings) {

        public Severity worst() {
            return findings.stream().map(Finding::severity)
                    .max(Comparator.naturalOrder()).orElse(Severity.INFO);
        }

        public boolean healthy() {
            return findings.stream().noneMatch(f -> f.severity() != Severity.INFO);
        }
    }
}
