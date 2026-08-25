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

    /**
     * Failure reasons brought up from the run to the dashboard.
     *
     * <p>Three, because the question at 9am is "what broke", and by the fourth distinct reason the
     * answer is "open the run" anyway. The exact counts are already exact; this is triage.
     */
    private static final int TOP_FAILURES = 3;

    /** Enough history to see a trend at a glance without the row becoming a chart. */
    private static final int HISTORY = 7;

    private final PipelineRepository pipelines;
    private final RunRepository runs;
    private final ScheduleRepository schedules;
    private final com.dmp.application.port.out.RecordErrorPort recordErrors;
    private final TenantContext tenantContext;
    private final Clock clock;

    public OperationsDashboard(PipelineRepository pipelines, RunRepository runs,
                               ScheduleRepository schedules,
                               com.dmp.application.port.out.RecordErrorPort recordErrors,
                               TenantContext tenantContext, Clock clock) {
        this.pipelines = pipelines;
        this.runs = runs;
        this.schedules = schedules;
        this.recordErrors = recordErrors;
        this.tenantContext = tenantContext;
        this.clock = clock;
    }

    /**
     * The whole screen in one call: the watchlist, what is in flight, and the day's totals.
     *
     * <p>One call because the three are read together and must agree. Fetched separately they can
     * disagree by a run — totals counting something the live list has already dropped — and a
     * screen that contradicts itself is one nobody trusts twice.
     */
    public Dashboard dashboard(Duration window) {
        return dashboard(window, true);
    }

    /**
     * @param watchedOnly the watchlist, or every pipeline that has run.
     *
     * <p>Two audiences read this screen and they do not want the same list. A support desk works
     * from a watchlist somebody is accountable for, and an experiment appearing beside it is noise.
     * A product team asking "did anything move yesterday" wants the ones nobody put on a list —
     * that is where an unnoticed pipeline is, by definition.
     */
    public Dashboard dashboard(Duration window, boolean watchedOnly) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant now = clock.instant();
        Instant since = now.minus(window);

        List<PipelineHealth> health = today(window, watchedOnly);

        // Running means running. findActive also returns PAUSED, because a paused run still holds
        // its slot — true for concurrency and wrong under a heading that says these are happening.
        List<Live> live = runs.findActive(tenantId).stream()
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
                        // From creation, not from the start. A run stuck in PREPARING has no start
                        // time, and measuring from one reported "0m" for something that had been
                        // going nowhere for two days.
                        Duration.between(run.startedAt() == null
                                ? run.createdAt() : run.startedAt(), now).toSeconds()))
                .sorted(Comparator.comparing(Live::pipeline))
                .toList();

        List<Run> recent = runs.search(tenantId,
                        new RunRepository.RunSearch(null, Set.of(), since, null, null),
                        new PageQuery(0, 200, "createdAt", false))
                .content().stream()
                .filter(run -> !run.dryRun())
                .toList();

        Totals totals = new Totals(
                recent.stream().filter(r -> r.state() == RunState.COMPLETED).count(),
                recent.stream().filter(r -> r.state() == RunState.FAILED).count(),
                recent.stream().mapToLong(r -> r.metrics().recordsRead()).sum(),
                recent.stream().mapToLong(r -> r.metrics().recordsWritten()).sum(),
                recent.stream().mapToLong(r -> r.metrics().recordsFailed()).sum(),
                live.size());

        return new Dashboard(health, live, totals, headlines(health, live, totals, window), now);
    }

    /**
     * The screen in sentences, biggest first.
     *
     * <p>Everything else here is figures, and a figure has to be interpreted before it means
     * anything: 60,301 is alarming or routine depending on what it is out of and which job it
     * belongs to. A headline has already done that work — somebody reads three lines and knows
     * whether to put their coat back on.
     *
     * <p>Written as whole sentences naming the job, because these are read at a glance and often
     * relayed to somebody else verbatim. "VOLUME_LOW 4,900" is a log line; "the nightly policy
     * transfer moved half its usual volume" is something you can say out loud.
     *
     * <p>Good news is included when there is any. A strip that only ever appears in a crisis is a
     * strip people learn to see as an error box, and then the quiet mornings carry no information
     * at all.
     */
    /** At most this many problems in the strip. Beyond it, a strip is the list it is summarising. */
    private static final int TOP_HEADLINES = 5;

    /**
     * The screen in sentences: one line per job that has something wrong, worst first.
     *
     * <p><b>One line per job, not one per finding.</b> A failing run usually raises three findings
     * — it failed, it delivered nothing, it moved less than usual — and printing each produced a
     * strip that said the same thing three times in three registers. The worst one is the one
     * somebody acts on; the rest are on the card.
     *
     * <p><b>The job's name is a field, not part of the sentence.</b> Concatenating it produced
     * "every failure in one run failed", which is what happens when a name that is not a noun
     * phrase meets a template that assumed one. The console renders the two separately.
     *
     * <p><b>Nothing here estimates.</b> A headline that said "could not deliver a tenth of its
     * records" sat above a detail reading "10,000 of 10,000 (100%) did not arrive" — the template
     * had a fraction written into it and the evidence underneath contradicted it. The numbers live
     * in the detail, which computes them; the headline names the kind of problem and no more.
     */
    private List<Headline> headlines(List<PipelineHealth> health, List<Live> live, Totals totals,
                                     Duration window) {
        List<Headline> headlines = new ArrayList<>();
        String period = window.toHours() < 48
                ? "the last " + window.toHours() + " hours"
                : "the last " + (window.toHours() / 24) + " days";

        List<Headline> problems = new ArrayList<>();
        for (PipelineHealth job : health) {
            job.findings().stream()
                    .filter(finding -> finding.severity() != Severity.INFO)
                    // Worst first, and among equals the first raised — findings are added in the
                    // order they are checked, which puts the cause before its consequences.
                    .max(Comparator.comparing(Finding::severity))
                    .ifPresent(worst -> problems.add(new Headline(
                            worst.severity(),
                            job.name(),
                            describe(worst.code()),
                            worst.message(),
                            job.pipelineId(),
                            job.latest() == null ? null : job.latest().id().toString(),
                            job.latest() == null ? null : job.latest().createdAt())));
        }
        problems.sort(Comparator.comparing(Headline::severity).reversed());

        headlines.addAll(problems.stream().limit(TOP_HEADLINES).toList());
        if (problems.size() > TOP_HEADLINES) {
            headlines.add(new Headline(Severity.WARNING, null,
                    (problems.size() - TOP_HEADLINES) + " more job"
                            + (problems.size() - TOP_HEADLINES == 1 ? "" : "s") + " need attention",
                    "Listed below, worst first.", null, null, null));
        }

        // A run in flight is the one thing on this screen that changes while somebody watches it,
        // so it earns a line even when nothing is wrong.
        if (!live.isEmpty()) {
            headlines.add(new Headline(Severity.INFO,
                    live.size() == 1 ? live.get(0).pipeline() : null,
                    live.size() == 1 ? "Running now" : live.size() + " jobs running now",
                    live.stream().map(Live::pipeline).distinct().limit(3)
                            .reduce((a, b) -> a + ", " + b).orElse(""),
                    null, live.size() == 1 ? live.get(0).runId() : null, null));
        }

        // The figure everybody is asked for, said rather than left to be added up.
        if (totals.recordsWritten() > 0) {
            headlines.add(new Headline(Severity.INFO, null,
                    String.format("%,d records transferred in %s", totals.recordsWritten(), period),
                    String.format("across %d completed run%s%s", totals.completed(),
                            totals.completed() == 1 ? "" : "s",
                            totals.recordsFailed() > 0
                                    ? String.format(", with %,d not delivered", totals.recordsFailed())
                                    : " with nothing lost"),
                    null, null, null));
        }

        if (problems.isEmpty()) {
            headlines.add(0, new Headline(Severity.INFO, null, "Nothing has failed in " + period,
                    "Every watched job ran, moved its usual volume, and lost nothing.",
                    null, null, null));
        }
        return List.copyOf(headlines);
    }

    /** What kind of problem this is, in the fewest words that stay true for every instance of it. */
    private static String describe(String code) {
        return switch (code) {
            case "FAILED" -> "Last run failed";
            case "UNACCOUNTED" -> "Lost records without reporting them";
            case "DID_NOT_RUN" -> "Was due and never started";
            case "NO_ROWS" -> "Ran but moved nothing";
            case "VOLUME_LOW" -> "Moved far less than usual";
            case "VOLUME_HIGH" -> "Moved far more than usual";
            case "REJECTIONS" -> "Records did not reach the destination";
            case "SLOW" -> "Took far longer than usual";
            case "STUCK" -> "Has been running with nothing happening";
            case "PAUSED_TOO_LONG" -> "Left paused, holding its slot";
            default -> "Needs attention";
        };
    }

    /**
     * @param subject the job this is about, rendered as its own element rather than glued into the
     *                sentence. Null for a line about the platform rather than one pipeline.
     * @param at      when the run it describes started, so a headline carries its own age
     */
    public record Headline(Severity severity, String subject, String headline, String detail,
                           String pipelineId, String runId, Instant at) {
    }

    /** Every watched pipeline, with its last run judged against its own history. */
    public List<PipelineHealth> today(Duration window) {
        return today(window, true);
    }

    /** The same, over the watchlist or over every pipeline. See {@link #dashboard(Duration, boolean)}. */
    public List<PipelineHealth> today(Duration window, boolean watchedOnly) {
        TenantId tenantId = tenantContext.currentTenant();
        Instant since = clock.instant().minus(window);

        List<Pipeline> watched = pipelines.search(tenantId,
                        new PipelineRepository.PipelineSearch(null, null, null, null),
                        // The page cap, not a number chosen here. A watchlist above two hundred
                        // pipelines is not a watchlist, and paging this screen would be solving
                        // the wrong problem.
                        new PageQuery(0, 200, "name", true))
                .content().stream()
                .filter(pipeline -> !watchedOnly || pipeline.monitored())
                // Unwatched, a pipeline earns its place by having run. Every draft somebody
                // abandoned would otherwise arrive as a job that has never run, which is a true
                // statement about a hundred rows nobody wants to read.
                .filter(pipeline -> watchedOnly || pipeline.publishedVersionNumber().isPresent())
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

        // The reasons, not just the count. A support desk seeing "222 failed" has to open the run
        // to learn anything; seeing "180 Policy_Number__c is required" already knows whether this
        // is theirs to fix or somebody else's.
        List<FailureReason> reasons = List.of();
        if (latest != null && latest.metrics().recordsFailed() > 0) {
            try {
                reasons = recordErrors.summariseByRun(tenantId, latest.id(), TOP_FAILURES).stream()
                        .map(group -> new FailureReason(group.count(), group.code(),
                                readable(group.message())))
                        .toList();
            } catch (RuntimeException e) {
                // The dead-letter store being unavailable must not blank the whole screen. Every
                // other number here comes from somewhere else and is still true.
                reasons = List.of();
            }
        }

        List<Attempt> trend = recent.stream()
                .filter(run -> !run.dryRun())
                .limit(HISTORY)
                .map(run -> new Attempt(run.id().toString(), run.state().name(),
                        run.createdAt(), run.metrics().recordsRead(),
                        run.metrics().recordsWritten(), run.metrics().recordsFailed(),
                        run.duration(now()).map(Duration::toSeconds).orElse(0L)))
                .toList();

        return new PipelineHealth(pipeline.id().toString(), pipeline.name(), pipeline.monitored(),
                latest, typicalRows, typicalSeconds, history.size(), List.copyOf(findings), reasons,
                trend, scheduleSummary(tenantId, pipeline), volume(recent, since));
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

    private Instant now() {
        return clock.instant();
    }

    /**
     * The wrapper a sandbox adds, removed.
     *
     * <p>"Transform ? threw: Error: amount must be positive" is three layers of packaging around
     * four useful words, and on a dashboard row there is no space for packaging.
     */
    private static String readable(String message) {
        if (message == null) {
            return "";
        }
        String stripped = message.replaceFirst("^Transform .*? threw:\\s*", "")
                .replaceFirst("^(Error|TypeError|ReferenceError|RangeError):\\s*", "")
                .trim();
        return stripped.isEmpty() ? message : stripped;
    }

    /** When this pipeline is next expected, so "it has not run" can be read as late or as early. */
    private ScheduleSummary scheduleSummary(TenantId tenantId, Pipeline pipeline) {
        return schedules.findByPipeline(tenantId, pipeline.id()).stream()
                .filter(Schedule::enabled)
                .findFirst()
                .map(schedule -> new ScheduleSummary(schedule.name(), schedule.cronExpression(),
                        schedule.timezone().getId(), schedule.lastFiredAt(),
                        nextFireAfter(schedule, now()).orElse(null)))
                .orElse(null);
    }

    /** @param reason already stripped of the sandbox's wrapper — a dashboard row has no room for it */
    public record FailureReason(long count, String code, String reason) {
    }

    /** One earlier run, for the trend beside today's number. */
    public record Attempt(String runId, String state, Instant at, long read, long written,
                          long failed, long seconds) {
    }

    public record ScheduleSummary(String name, String cron, String timezone, Instant lastFiredAt,
                                  Instant nextDueAt) {
    }

    private String pipelineName(TenantId tenantId, PipelineId id) {
        return pipelines.findById(tenantId, id).map(Pipeline::name).orElse(id.toString());
    }

    /** @param progress 0 to 1, or null before planning has produced a chunk count */
    public record Live(String runId, String pipeline, String state, Double progress,
                       long recordsRead, long recordsWritten, long seconds) {
    }

    public record Totals(long completed, long failed, long recordsRead, long recordsWritten,
                         long recordsFailed, int running) {
    }

    public record Dashboard(List<PipelineHealth> pipelines, List<Live> live, Totals totals,
                            List<Headline> headlines, Instant generatedAt) {
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
    /**
     * What one pipeline moved over the chosen window, rather than on its last run.
     *
     * <p>The last run answers "is it broken". It does not answer "did we migrate the policies this
     * week", which is the question a product team arrives with and which no single run can settle:
     * seven runs of a nightly job are seven numbers, and the useful figure is their sum next to how
     * many of them finished.
     */
    public record Volume(int runs, int completed, int failed, long read, long written,
                         long recordsFailed, long seconds) {

        /** Of the runs that reached an end, the share that reached a clean one. */
        public Double successRate() {
            int ended = completed + failed;
            return ended == 0 ? null : (double) completed / ended;
        }
    }

    /**
     * Sums the runs that started inside the window.
     *
     * <p>By {@code createdAt} rather than by completion, so a run belongs to the window somebody
     * asked about — a long job started last night and finished this morning is last night's work,
     * and counting it today would make the same records appear in two windows.
     */
    private Volume volume(List<Run> recent, Instant since) {
        int runs = 0;
        int completed = 0;
        int failed = 0;
        long read = 0;
        long written = 0;
        long recordsFailed = 0;
        long seconds = 0;

        for (Run run : recent) {
            if (run.dryRun() || run.createdAt().isBefore(since)) {
                continue;
            }
            runs++;
            if (run.state() == RunState.COMPLETED) {
                completed++;
            } else if (run.state() == RunState.FAILED) {
                failed++;
            }
            read += run.metrics().recordsRead();
            written += run.metrics().recordsWritten();
            recordsFailed += run.metrics().recordsFailed();
            seconds += run.duration(clock.instant()).map(Duration::toSeconds).orElse(0L);
        }
        return new Volume(runs, completed, failed, read, written, recordsFailed, seconds);
    }

    public record PipelineHealth(String pipelineId, String name, boolean watched, Run latest,
                                 Long typicalRows, Long typicalSeconds, int baselineRuns,
                                 List<Finding> findings, List<FailureReason> reasons,
                                 List<Attempt> trend, ScheduleSummary schedule, Volume volume) {

        public Severity worst() {
            return findings.stream().map(Finding::severity)
                    .max(Comparator.naturalOrder()).orElse(Severity.INFO);
        }

        public boolean healthy() {
            return findings.stream().noneMatch(f -> f.severity() != Severity.INFO);
        }
    }
}
