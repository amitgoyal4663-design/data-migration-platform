package com.dmp.ratelimit.redis;

import com.dmp.application.port.out.RateLimiter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Wires the shared budget when Redis is configured, and something honest when it is not.
 *
 * <p>Keyed on {@code spring.data.redis.host} being set rather than on Redis being reachable. A
 * deployment that intends to use Redis and cannot reach it must not quietly fall through to a
 * limiter with no limits — it should fail as an outage, which is what {@link RedisRateLimiter}
 * does by parking the work.
 */
@Configuration
public class RateLimitConfiguration {

    @Bean
    @ConditionalOnProperty(name = "spring.data.redis.host")
    public RateLimiter redisRateLimiter(StringRedisTemplate redis) {
        return new RedisRateLimiter(redis);
    }

    @Bean
    @ConditionalOnMissingBean(RateLimiter.class)
    public RateLimiter unsharedRateLimiter() {
        return new UnsharedRateLimiter();
    }
}
