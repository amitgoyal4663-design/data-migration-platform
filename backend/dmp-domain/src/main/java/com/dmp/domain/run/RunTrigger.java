package com.dmp.domain.run;

/**
 * What caused a run to be created.
 *
 * <p>Recorded because "why did this run at 3am" is one of the first questions asked during an
 * incident, and reconstructing it from surrounding evidence is unreliable.
 */
public enum RunTrigger {

    /** A user pressed Run in the console. */
    MANUAL,

    /** A cron or interval schedule fired, via the delay queue (ADR-0002). */
    SCHEDULED,

    /** An external system called the API. */
    API,

    /** Automatic re-execution of a failed run after backoff. */
    RETRY,

    /**
     * Re-delivery of records a previous run rejected, read from the dead-letter queue.
     *
     * <p>Distinct from {@link #RETRY}, which re-reads whole chunks from the pipeline's source. A
     * replay never touches the source: the chunks that produced these records succeeded, and only
     * the individual records inside them did not.
     */
    REPLAY,

    /** A deliberate historical reload over a bounded window. */
    BACKFILL
}
