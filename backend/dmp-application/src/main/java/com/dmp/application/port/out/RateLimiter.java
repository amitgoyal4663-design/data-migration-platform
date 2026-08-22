package com.dmp.application.port.out;

import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.connector.RateLimitPolicy;

import java.time.Duration;
import java.util.Optional;

/**
 * Permission to call somebody else's system at the rate they agreed to.
 *
 * <p>Shared across every pod, because the client counts what arrives, not what any one worker sent.
 * A limiter held in a single process would give a fleet of four exactly four times the agreed rate,
 * and would do it invisibly — the client's numbers would be wrong while every pod's were right.
 *
 * <p><b>Asked once, before a chunk starts.</b> Not per record, not per batch. A chunk reserves
 * everything it will spend before it opens a source, so a chunk that starts always finishes without
 * pausing, and a chunk that cannot start has read nothing and holds nothing. The alternative —
 * checking as records flow — means a cursor held open across a wait, a partially-read batch to
 * discard, and a check on the hottest path in the engine.
 *
 * <p><b>Never blocks.</b> The answer is either "yes" or "not for this long", and the caller decides
 * what to do with the wait: sleep through a short one, park the chunk for a long one. A blocking
 * limiter would hold a worker slot and a chunk lease for minutes, and a fleet whose slots were all
 * blocked on one throttled client would stop working on every other run.
 */
public interface RateLimiter {

    /**
     * Spends the budget for one chunk, or says how long until it could be spent.
     *
     * <p><b>Tokens taken here are never given back.</b> If the call they were taken for then fails,
     * or the pod dies before making it, the budget is still gone. That is deliberate: a refund path
     * cannot tell "the request never left" from "the request arrived and the response was lost", and
     * guessing wrong means quietly exceeding a limit the client was promised. Spending a little too
     * much of our own budget is recoverable; sending a client more than they allowed is not.
     *
     * @param connector whose agreement is being spent — the budget belongs to the endpoint, so every
     *                  pipeline calling the same instance draws on the same one
     * @param policy    what the client allows, as configured on that instance
     * @param records   records this chunk will send
     * @param calls     requests this chunk will make
     * @return empty when the budget was taken and the work may proceed; otherwise how long until
     *         there would be enough, having taken nothing
     */
    Optional<Duration> tryAcquire(ConnectorInstanceId connector, RateLimitPolicy policy,
                                  long records, long calls);

    /**
     * Hands back budget that was reserved and provably never spent.
     *
     * <p><b>Not a refund, and the distinction is the whole safety argument.</b> A refund gives back
     * budget for a call that failed — and a failed call cannot be told apart from a call that
     * arrived and whose response was lost, so refunding one risks sending a client more than they
     * allowed. This gives back only what was never attempted: the chunk finished, the engine made
     * the requests itself and counted them, and the difference is work that did not happen.
     *
     * <p>Two things make that difference non-zero. A split script's group count cannot be known
     * before the records are read, so the reservation assumes the worst — one call per record —
     * and a script producing ten groups from five hundred records would otherwise be charged fifty
     * times what it costs. And the last chunk of a run reserves a whole chunk's worth of records
     * and usually finds fewer.
     *
     * <p>Called only for a chunk that completed. A chunk that failed has an unknown boundary
     * between what was sent and what was not, and the safe reading of an unknown boundary is that
     * everything reserved was spent.
     *
     * @param reservedRecords what {@link #tryAcquire} was asked for
     * @param reservedCalls   likewise
     * @param usedRecords     what the chunk actually handed to the destination
     * @param usedCalls       requests the engine actually made
     */
    void returnUnused(ConnectorInstanceId connector, RateLimitPolicy policy,
                      long reservedRecords, long reservedCalls,
                      long usedRecords, long usedCalls);
}
