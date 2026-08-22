package com.dmp.engine;

import com.dmp.domain.run.Run;
import com.dmp.domain.run.Split;
import org.slf4j.MDC;

/**
 * Stamps every log line emitted while a chunk runs with the chunk it belongs to.
 *
 * <p>The API had this from the start — {@code TenantFilter} puts the tenant and a request id into
 * the MDC, so a line logged while serving a request is attributable without any call site passing
 * anything. Chunk execution had nothing. It runs on a worker thread that no filter has touched, so
 * every line the engine and every line a connector logged came out as {@code [—] [—]}: a Salesforce
 * job id, a rate-limit refusal, a query that returned nothing, all unattributable to the chunk that
 * caused them. With one chunk running that is merely awkward; with eight running across three pods
 * it makes the logs unusable for the thing logs are for.
 *
 * <p>The keys are the ones somebody actually searches by, and they match what the console shows:
 * the chunk id is the same value the stage log and the record index carry, so a chunk id copied
 * from a timeline finds that chunk's application logs and nothing else.
 *
 * <p>Restores rather than clears on close. A worker loop is a long-lived thread that may already
 * be inside another context — a scheduled poll, a run-level operation — and clearing would silently
 * strip it.
 */
final class ChunkLogContext implements AutoCloseable {

    private static final String TENANT = "tenantId";
    private static final String RUN = "runId";
    private static final String CHUNK = "chunkId";
    private static final String ATTEMPT = "attempt";
    /**
     * The one field the console log shows, holding whatever identifies this unit of work.
     *
     * <p>A request id while an API call is being served, a run and chunk while a chunk is running.
     * One slot rather than four because a log line has a width and most of those fields would be a
     * dash on most lines — and because the value people paste into a grep is a whole id, not a
     * column. The individual keys are set as well, for a structured backend that wants fields.
     */
    private static final String TRACE = "trace";

    private final String previousTenant;
    private final String previousRun;
    private final String previousChunk;
    private final String previousAttempt;
    private final String previousTrace;

    private ChunkLogContext() {
        this.previousTenant = MDC.get(TENANT);
        this.previousRun = MDC.get(RUN);
        this.previousChunk = MDC.get(CHUNK);
        this.previousAttempt = MDC.get(ATTEMPT);
        this.previousTrace = MDC.get(TRACE);
    }

    /** Everything known about a chunk about to run. */
    static ChunkLogContext of(Split split) {
        ChunkLogContext previous = new ChunkLogContext();
        MDC.put(TENANT, split.tenantId().toString());
        MDC.put(RUN, split.runId().toString());
        MDC.put(CHUNK, split.id().toString());
        // The attempt matters more than it looks: the second attempt of a chunk logs almost exactly
        // what the first did, and without this the two are indistinguishable in a log file.
        MDC.put(ATTEMPT, String.valueOf(split.attempt()));
        // The chunk id alone, because it is the one key every store already agrees on: the stage
        // log carries it, the record index carries it, the console shows it. A composite of run,
        // chunk and attempt was searchable by none of them without knowing how it was assembled —
        // a third format for a thing that already had two. The run and the attempt are separate
        // fields for anyone querying structured logs.
        MDC.put(TRACE, split.id().toString());
        return previous;
    }

    /** Run-level work, before any chunk exists — planning, stopping, reaping. */
    static ChunkLogContext of(Run run) {
        ChunkLogContext previous = new ChunkLogContext();
        MDC.put(TENANT, run.tenantId().toString());
        MDC.put(RUN, run.id().toString());
        MDC.put(TRACE, run.id().toString());
        return previous;
    }

    @Override
    public void close() {
        restore(TENANT, previousTenant);
        restore(RUN, previousRun);
        restore(CHUNK, previousChunk);
        restore(ATTEMPT, previousAttempt);
        restore(TRACE, previousTrace);
    }

    private static void restore(String key, String value) {
        if (value == null) {
            MDC.remove(key);
        } else {
            MDC.put(key, value);
        }
    }
}
