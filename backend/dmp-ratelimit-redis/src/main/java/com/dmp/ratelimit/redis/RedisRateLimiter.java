package com.dmp.ratelimit.redis;

import com.dmp.application.port.out.RateLimiter;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.connector.RateLimitPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The budget, in Redis, shared by every pod.
 *
 * <p>See {@link RateLimiter} for why it is shared and why it never blocks. This class is the
 * transport: it turns a policy into the seven numbers the Lua script wants and turns the two it
 * returns back into an answer.
 *
 * <p><b>Unreachable Redis means nobody may call.</b> Not "everybody may call" — that is the
 * tempting default and it is the wrong one. Failing open converts a cache outage into thirty times
 * the agreed rate arriving at a client's door, which is the single outcome this whole feature
 * exists to prevent, and it does it at the moment nobody is watching Redis. A stalled run is
 * recoverable by waiting; a breached quota is recoverable only by apologising.
 */
public class RedisRateLimiter implements RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);

    /**
     * How long to wait before asking again when Redis cannot answer.
     *
     * <p>Long enough that a fleet does not hammer a struggling Redis, short enough that a run
     * resumes on its own once Redis is back rather than needing somebody to notice.
     */
    private static final Duration UNREACHABLE_BACKOFF = Duration.ofSeconds(30);

    /**
     * Keys outlive their window by this much before Redis reclaims them.
     *
     * <p>A bucket that has been idle for longer than its own window is full anyway, so forgetting it
     * loses nothing — and remembering every connector any tenant ever configured, for ever, is how
     * a Redis with no persistence still manages to fill up.
     */
    private static final long TTL_WINDOWS = 2;

    private final StringRedisTemplate redis;
    private final RedisScript<List> takeBudget;
    private final RedisScript<Long> returnBudget;

    public RedisRateLimiter(StringRedisTemplate redis) {
        this.redis = redis;
        this.takeBudget = script("take-budget.lua", List.class);
        this.returnBudget = script("return-budget.lua", Long.class);
    }

    private static <T> RedisScript<T> script(String name, Class<T> resultType) {
        DefaultRedisScript<T> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("dmp/ratelimit/" + name));
        script.setResultType(resultType);
        return script;
    }

    @Override
    public Optional<Duration> tryAcquire(ConnectorInstanceId connector, RateLimitPolicy policy,
                                         long records, long calls) {
        if (policy == null || policy.isUnlimited()) {
            return Optional.empty();
        }

        long recordsWanted = policy.limitsRecords() ? Math.max(0, records) : 0;
        long callsWanted = policy.limitsCalls() ? Math.max(0, calls) : 0;
        if (recordsWanted == 0 && callsWanted == 0) {
            return Optional.empty();
        }

        long ttl = Math.max(windowMillis(policy.recordsWindow()), windowMillis(policy.callsWindow()))
                * TTL_WINDOWS;

        // How much may be held and how fast it refills, which is where the pacing choice lands.
        // The script does arithmetic; the policy decides what arithmetic.
        RateLimitPolicy.Bucket recordsBucket = policy.bucketFor(policy.records(), recordsWanted);
        RateLimitPolicy.Bucket callsBucket = policy.bucketFor(policy.calls(), callsWanted);

        List<?> answer;
        try {
            answer = redis.execute(takeBudget,
                    List.of(key(connector, "records"), key(connector, "calls")),
                    Long.toString(recordsBucket.capacity()),
                    Long.toString(recordsBucket.refill()),
                    Long.toString(windowMillis(policy.recordsWindow())),
                    Long.toString(recordsWanted),
                    Long.toString(callsBucket.capacity()),
                    Long.toString(callsBucket.refill()),
                    Long.toString(windowMillis(policy.callsWindow())),
                    Long.toString(callsWanted),
                    Long.toString(ttl));
        } catch (RuntimeException e) {
            log.warn("Rate limit budget for connector {} could not be read from Redis, so no work "
                            + "will be sent to it for {}s. Nothing is sent while the budget is "
                            + "unknown: allowing calls through would mean exceeding a limit the "
                            + "client was promised.",
                    connector, UNREACHABLE_BACKOFF.toSeconds(), e);
            return Optional.of(UNREACHABLE_BACKOFF);
        }

        if (answer == null || answer.size() < 2) {
            log.warn("Rate limit script returned nothing usable for connector {}; waiting {}s",
                    connector, UNREACHABLE_BACKOFF.toSeconds());
            return Optional.of(UNREACHABLE_BACKOFF);
        }

        long granted = ((Number) answer.get(0)).longValue();
        long waitMillis = ((Number) answer.get(1)).longValue();

        if (granted == 1) {
            return Optional.empty();
        }
        if (granted == -1) {
            // Waiting cannot fix this, so saying "come back later" would park the chunk for ever.
            boolean evenPacingIsTheReason = policy.pacing() == RateLimitPolicy.Pacing.EVEN
                    && (!recordsBucket.isAchievable() || !callsBucket.isAchievable());

            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A chunk of " + recordsWanted + " record(s) and " + callsWanted + " call(s) "
                            + "cannot be sent under this connector's rate limit ("
                            + policy.describe() + "), however long it waits."
                            + (evenPacingIsTheReason
                                    ? " Even pacing holds back one call's worth and refills the"
                                            + " rest, so a chunk that is the whole limit leaves"
                                            + " nothing to refill with. Make the chunk a fraction"
                                            + " of the limit — a tenth sustains about ninety"
                                            + " percent of it."
                                    : " Reduce the chunk size, or raise the limit if the client"
                                            + " allows more."),
                    Map.of("connectorInstanceId", connector.toString(),
                            "records", recordsWanted,
                            "calls", callsWanted,
                            "limit", policy.describe()));
        }
        return Optional.of(Duration.ofMillis(Math.max(1, waitMillis)));
    }

    @Override
    public void returnUnused(ConnectorInstanceId connector, RateLimitPolicy policy,
                             long reservedRecords, long reservedCalls,
                             long usedRecords, long usedCalls) {
        if (policy == null || policy.isUnlimited()) {
            return;
        }
        long spareRecords = Math.max(0, reservedRecords - usedRecords);
        long spareCalls = Math.max(0, reservedCalls - usedCalls);
        if (spareRecords == 0 && spareCalls == 0) {
            return;
        }

        // The buckets the reservation was made against, not fresh ones. Under even pacing the
        // capacity depends on what was asked for, and returning against a larger capacity than the
        // take used would let the bucket hold more than that pacing promises.
        RateLimitPolicy.Bucket recordsBucket = policy.bucketFor(policy.records(), reservedRecords);
        RateLimitPolicy.Bucket callsBucket = policy.bucketFor(policy.calls(), reservedCalls);

        long ttl = Math.max(windowMillis(policy.recordsWindow()), windowMillis(policy.callsWindow()))
                * TTL_WINDOWS;

        try {
            redis.execute(returnBudget,
                    List.of(key(connector, "records"), key(connector, "calls")),
                    Long.toString(recordsBucket.capacity()),
                    Long.toString(recordsBucket.refill()),
                    Long.toString(windowMillis(policy.recordsWindow())),
                    Long.toString(policy.limitsRecords() ? spareRecords : 0),
                    Long.toString(callsBucket.capacity()),
                    Long.toString(callsBucket.refill()),
                    Long.toString(windowMillis(policy.callsWindow())),
                    Long.toString(policy.limitsCalls() ? spareCalls : 0),
                    Long.toString(ttl));

            log.debug("Returned {} unused record(s) and {} unused call(s) to connector {}",
                    spareRecords, spareCalls, connector);

        } catch (RuntimeException e) {
            // Losing a return costs throughput and nothing else: the budget stays spent, refills on
            // its own schedule, and the client is if anything sent less than they allowed. Not
            // worth failing a chunk that has already succeeded.
            log.warn("Could not return unused rate limit budget to connector {}; it will refill on "
                    + "its own instead", connector, e);
        }
    }

    /**
     * Both of a connector's buckets share one hash tag, so a Redis Cluster keeps them in one slot.
     *
     * <p>Without it the two keys can land on different nodes and the script — which touches both —
     * is rejected outright. That failure appears only on a clustered Redis, which is to say only in
     * production.
     */
    private static String key(ConnectorInstanceId connector, String unit) {
        return "dmp:rl:{" + connector.value() + "}:" + unit;
    }

    /** The unit's window in milliseconds; zero when that unit carries no limit. */
    private static long windowMillis(Duration window) {
        return window == null ? 0 : window.toMillis();
    }
}
