package com.dmp.connector.api;

/**
 * What one chunk costs the far end, in the units a rate limit on this connector counts.
 *
 * <p>Declared by the connector because only the connector knows its own protocol, and read without
 * opening a session because the engine decides whether a chunk may start <em>before</em> it opens
 * anything. It exists to keep that arithmetic out of the engine: an engine that recognised
 * connector types by name would need editing every time a connector was added, which is the thing
 * the plugin SPI exists to avoid.
 */
public enum CallCost {

    /**
     * One call per request, counted from the pipeline's delivery policy.
     *
     * <p>The whole batch in one call, fixed groups, or one call per record — whatever delivery was
     * configured to do. True of an HTTP endpoint, a database and a broker alike, and the default
     * for every connector that does not say otherwise.
     */
    PER_REQUEST,

    /**
     * One charge for the whole chunk, however many requests it takes underneath.
     *
     * <p>For a destination where a chunk is a <em>job</em> rather than a request: created, fed,
     * told it is complete, polled until it finishes, then asked for its counts. The number of polls
     * depends on how busy the far end happens to be, so counting requests would make the same
     * migration cost more on a busy morning than on a quiet one — a number that measures the
     * destination's queue rather than the work the migration asked of it.
     *
     * <p><b>A limit on such a connector is therefore in jobs.</b> Where the provider meters
     * individual HTTP requests — Salesforce's daily API allowance does — convert before configuring
     * it: a bulk job is roughly fourteen requests, so a fourteen-thousand-request allowance is
     * about a thousand jobs.
     */
    PER_CHUNK;

    /**
     * How many calls a chunk costs, given what delivery would otherwise make of it.
     *
     * @param deliveryCalls one per group the delivery policy would produce
     */
    public long callsFor(long deliveryCalls) {
        return this == PER_CHUNK ? 1 : deliveryCalls;
    }
}
