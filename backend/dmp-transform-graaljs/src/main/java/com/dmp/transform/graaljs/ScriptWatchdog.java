package com.dmp.transform.graaljs;

import org.graalvm.polyglot.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Interrupts scripts that will not stop on their own.
 *
 * <p>{@code while (true) {}} in a user's transform would otherwise hold a worker thread until the
 * pod is killed, and take that worker's whole chunk capacity with it. GraalVM Community cannot
 * enforce a CPU-time limit inside the context (ADR-0008), so the enforcement is external:
 * {@code Context.close(true)} from another thread, which interrupts the guest execution.
 *
 * <p>One scanning thread for the whole process, not a timer per invocation. A transform runs
 * millions of times in a large migration, and scheduling and cancelling a task around each one
 * would cost more than the scripts do. Instead each execution registers a deadline and the scanner
 * sweeps for expired ones — the cost per record is one map insert and one removal.
 *
 * <p>The scan interval bounds how far past its deadline a script can run. That imprecision is
 * deliberate: the watchdog exists to stop runaway loops, not to enforce a millisecond-accurate
 * budget, and a tighter scan would burn CPU on every worker to sharpen a limit nobody needs sharp.
 */
final class ScriptWatchdog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScriptWatchdog.class);

    private static final Duration SCAN_INTERVAL = Duration.ofMillis(250);

    private final Map<Long, Execution> active = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scanner;

    ScriptWatchdog() {
        this.scanner = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "dmp-script-watchdog");
            // A daemon: a watchdog with nothing to watch must not keep the JVM alive at shutdown.
            thread.setDaemon(true);
            return thread;
        });
        this.scanner.scheduleWithFixedDelay(this::sweep,
                SCAN_INTERVAL.toMillis(), SCAN_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Registers an execution that must finish before {@code timeout}.
     *
     * <p>Keyed by thread id, so a worker running several chunks concurrently on virtual threads
     * gets one entry per in-flight script rather than one shared entry they overwrite.
     */
    void enter(Context context, Duration timeout, String nodeName) {
        active.put(Thread.currentThread().threadId(),
                new Execution(context, System.nanoTime() + timeout.toNanos(), nodeName));
    }

    void exit() {
        active.remove(Thread.currentThread().threadId());
    }

    private void sweep() {
        long now = System.nanoTime();
        active.forEach((threadId, execution) -> {
            if (now < execution.deadlineNanos) {
                return;
            }
            // Removed first: closing the context makes the guest throw, the executing thread calls
            // exit(), and killing the same context twice would log a second scary message for one
            // runaway script.
            if (active.remove(threadId, execution)) {
                log.error("Transform '{}' exceeded its time limit and was interrupted. "
                                + "The record it was processing will be recorded as failed.",
                        execution.nodeName);
                try {
                    execution.context.close(true);
                } catch (Exception e) {
                    log.debug("Could not close a timed-out script context", e);
                }
            }
        });
    }

    @Override
    public void close() {
        scanner.shutdownNow();
    }

    private record Execution(Context context, long deadlineNanos, String nodeName) {
    }
}
