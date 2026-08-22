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

    /**
     * How long a run may sit in one state before the board says so.
     *
     * <p>A paused run is somebody's deliberate decision, so it is given a day before being called
     * out — but not forever, because the commonest fate of a paused run is to be forgotten while
     * still holding a slot. Any other unfinished state is different: nothing is deciding anything,
     * so four hours without reaching a terminal state means it is not going to.
     */
    private static final Duration PAUSED_TOO_LONG = Duration.ofHours(24);
    private static final Duration STUCK_AFTER = Duration.ofHours(4);

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

    /**
     * Everything a wall display shows, in one call.
     *
     * <p>One call because a screen on a wall is refreshed on a timer forever, and four calls that
     * can each fail separately produce a board showing three quarters of the truth with nothing to
     * say the last quarter is missing. A board that is wrong without admitting it is worse than a
     * blank one.
     *
     * <p>Covers every run, not only the watchlist. The watchlist exists so one person's morning
     * screen stays small; a wall is read by whoever walks past, and a failure nobody thought to
     * watch is exactly the one that should be up there.
     */
    public Board board(Duration window) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        Instant since = now.minus(window);

        List<Run> active = runs.findActive(tenantId);

        List<Run> recent = runs.search(tenantId,
                        new RunRepository.RunSearch(null, Set.of(), since, null, null),
                        new PageQuery(0, 200, "createdAt", false))
                .content().stream()
                .filter(run -> !run.dryRun())
                .toList();

        // "Running now" means running, not "occupies worker capacity". findActive includes PAUSED
        // because a paused run still holds its slot — true for concurrency accounting, and wrong
        // on a board, where it showed four runs somebody paused a fortnight ago under a heading
        // that says they are happening.
        List<Live> live = active.stream()
                .filter(run -> !run.dryRun())
                .filter(run -> run.state() != RunState.PAUSED)
                .map(run -> new Live(
                        run.id().toString(),
                        pipelineName(tenantId, run.pipelineId()),
                        run.state().name(),
                        run.metrics().progress().isPresent()
                                ? run.metrics().progress().getAsDouble() : null,
                        run.metrics().recordsRead(),
                        run.metrics().recordsWritten(),
                        // From when the run was created, not from when it started. A run stuck in
                        // PREPARING has no start time, so measuring from one reported "0m" for
                        // something that had been going nowhere for an hour.
                        Duration.between(run.startedAt() == null ? run.createdAt()
                                : run.startedAt(), now).toSeconds()))
                .sorted(Comparator.comparing(Live::pipeline))
                .toList();

        Today today = new Today(
                recent.stream().filter(r -> r.state() == RunState.COMPLETED).count(),
                recent.stream().filter(r -> r.state() == RunState.FAILED).count(),
                recent.stream().mapToLong(r -> r.metrics().recordsRead()).sum(),
                recent.stream().mapToLong(r -> r.metrics().recordsWritten()).sum(),
                recent.stream().mapToLong(r -> r.metrics().recordsFailed()).sum(),
                live.size());

        List<Attention> attention = new ArrayList<>();

        // Every failure in the window, whether or not anybody watches that pipeline.
        for (Run run : recent) {
            if (run.state() == RunState.FAILED) {
                attention.add(new Attention(Severity.CRITICAL,
                        pipelineName(tenantId, run.pipelineId()), run.id().toString(),
                        "Run failed",
                        run.errorMessage() == null ? run.errorCode() : run.errorMessage(),
                        run.endedAt() == null ? run.createdAt() : run.endedAt()));
            } else if (run.metrics().unaccountedRecords() != 0) {
                // Green everywhere else, which is the whole reason it belongs on a wall.
                attention.add(new Attention(Severity.CRITICAL,
                        pipelineName(tenantId, run.pipelineId()), run.id().toString(),
                        run.metrics().unaccountedRecords() + " records unaccounted for",
                        "Reached delivery and were neither written nor reported failed",
                        run.endedAt() == null ? run.createdAt() : run.endedAt()));
            }
        }

        // A run nobody finished. Paused two weeks ago, or preparing for an hour against a
        // warehouse that is never going to answer — neither produces a failure, neither appears in
        // any window of recent activity, and both hold a concurrency slot the whole time. Nothing
        // else in the platform reports them, which is exactly why they belong here.
        for (Run run : active) {
            if (run.dryRun()) {
                continue;
            }
            Instant began = run.startedAt() == null ? run.createdAt() : run.startedAt();
            Duration held = Duration.between(began, now);

            if (run.state() == RunState.PAUSED && held.compareTo(PAUSED_TOO_LONG) > 0) {
                attention.add(new Attention(Severity.WARNING,
                        pipelineName(tenantId, run.pipelineId()), run.id().toString(),
                        "Paused for " + humanise(held),
                        "Still holding a slot. Resume it or stop it.", began));

            } else if (run.state() != RunState.PAUSED && held.compareTo(STUCK_AFTER) > 0) {
                attention.add(new Attention(Severity.CRITICAL,
                        pipelineName(tenantId, run.pipelineId()), run.id().toString(),
                        run.state().name().toLowerCase() + " for " + humanise(held),
                        "No longer making progress, and it will not fail on its own.", began));
            }
        }

        // Then the anomalies, which need a baseline and so only exist for watched pipelines.
        for (PipelineHealth health : today(window)) {
            for (Finding finding : health.findings()) {
                if (finding.severity() == Severity.INFO || "FAILED".equals(finding.code())
                        || "UNACCOUNTED".equals(finding.code())) {
                    continue;   // Already listed above, from the run itself.
                }
                attention.add(new Attention(finding.severity(), health.name(),
                        health.latest() == null ? null : health.latest().id().toString(),
                        headlineFor(finding.code()), finding.message(), now));
            }
        }

        attention.sort(Comparator.comparingInt((Attention a) -> a.severity().ordinal()).reversed()
                .thenComparing(Attention::at, Comparator.reverseOrder()));

        Severity verdict = attention.stream().map(Attention::severity)
                .max(Comparator.naturalOrder()).orElse(Severity.INFO);

        return new Board(verdict, live, today, List.copyOf(attention), now);
    }

    /** Short enough to read across a room; the sentence underneath carries the detail. */
    private static String headlineFor(String code) {
        return switch (code) {
            case "DID_NOT_RUN" -> "Scheduled run never started";
            case "NO_ROWS" -> "Completed with no data";
            case "VOLUME_LOW" -> "Volume down";
            case "VOLUME_HIGH" -> "Volume up";
            case "REJECTIONS" -> "Records not delivered";
            case "SLOW" -> "Running slow";
            default -> code;
        };
    }

    private String pipelineName(TenantId tenantId, PipelineId id) {
        return pipelines.findById(tenantId, id).map(Pipeline::name).orElse(id.toString());
    }

    public record Live(String runId, String pipeline, String state, Double progress,
                       long recordsRead, long recordsWritten, long seconds) {
    }

    public record Today(long completed, long failed, long recordsRead, long recordsWritten,
                        long recordsFailed, int running) {
    }

    public record Attention(Severity severity, String pipeline, String runId, String headline,
                            String detail, Instant at) {
    }

    public record Board(Severity verdict, List<Live> live, Today today, List<Attention> attention,
                        Instant generatedAt) {
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
