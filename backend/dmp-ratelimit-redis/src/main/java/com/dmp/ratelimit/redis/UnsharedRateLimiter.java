package com.dmp.ratelimit.redis;

import com.dmp.application.port.out.RateLimiter;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.connector.RateLimitPolicy;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * What stands in for the limiter when no Redis is configured.
 *
 * <p>Grants everything while nobody has asked for a limit, and <b>refuses outright</b> the moment
 * one is configured. That asymmetry is the point. A deployment with no rate limits anywhere should
 * not be made to run Redis; a deployment that has written down a client's agreement should not be
 * able to ignore it because of a missing connection string.
 *
 * <p>The alternative — an in-memory bucket per pod — would be worse than either. It looks like it
 * works, and it silently multiplies the agreed rate by the number of pods. A limit that is wrong
 * only when you scale out is a limit nobody can trust.
 */
public class UnsharedRateLimiter implements RateLimiter {

    @Override
    public Optional<Duration> tryAcquire(ConnectorInstanceId connector, RateLimitPolicy policy,
                                         long records, long calls) {
        if (policy == null || policy.isUnlimited()) {
            return Optional.empty();
        }
        throw new DmpException(ErrorCode.VALIDATION_FAILED,
                "This connector has a rate limit (" + policy.describe() + ") but no Redis is "
                        + "configured to hold the budget. The count has to be shared by every "
                        + "worker — a per-process counter would let each pod send the full rate, "
                        + "so the client would receive it multiplied by the size of the fleet. "
                        + "Set spring.data.redis.host, or remove the limit.",
                Map.of("connectorInstanceId", connector.toString(), "limit", policy.describe()));
    }

    @Override
    public void returnUnused(ConnectorInstanceId connector, RateLimitPolicy policy,
                             long reservedRecords, long reservedCalls,
                             long usedRecords, long usedCalls) {
        // Nothing was ever taken, because tryAcquire refuses to run at all when a limit is set.
    }
}
