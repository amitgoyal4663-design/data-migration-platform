package com.dmp.domain.connector;

import com.dmp.common.error.DmpException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a client's sentence turns into.
 *
 * <p>Every case here started as something somebody said in an integration call: "ten thousand
 * records every five minutes", "fifteen thousand API calls a day", "ten a second and a million a
 * day". The point of the record is that all three are expressible and none of them needs a mode,
 * a switch, or a second concept.
 */
class RateLimitPolicyTest {

    @Nested
    @DisplayName("the two units")
    class Units {

        @Test
        @DisplayName("\"10,000 records every 5 minutes\"")
        void recordsOnly() {
            var policy = new RateLimitPolicy(10_000, Duration.ofMinutes(5), 0, null);

            assertThat(policy.limitsRecords()).isTrue();
            assertThat(policy.limitsCalls())
                    .as("they said nothing about requests, so we invent nothing about requests")
                    .isFalse();
            assertThat(policy.recordsPerSecond()).isEqualTo(1000d / 30);
        }

        @Test
        @DisplayName("\"15,000 API calls a day\" — records never enter it")
        void callsOnly() {
            var policy = new RateLimitPolicy(0, null, 15_000, Duration.ofDays(1));

            assertThat(policy.limitsCalls()).isTrue();
            assertThat(policy.limitsRecords()).isFalse();
            assertThat(policy.maxRecordsPerAcquire())
                    .as("a chunk of any size is fine; it is the requests that are counted")
                    .isEqualTo(Long.MAX_VALUE);
        }

        @Test
        @DisplayName("\"10 a second, and a million a day\" — one policy, two windows")
        void bothUnitsKeepTheirOwnWindows() {
            var policy = new RateLimitPolicy(1_000_000, Duration.ofDays(1), 10, Duration.ofSeconds(1));

            // The shape that made two separate windows necessary. A single shared window would
            // have forced this client to be described as either 10/sec or 1M/day, and honouring
            // one of a client's two limits is not honouring their limits.
            assertThat(policy.callsPerSecond()).isEqualTo(10d);
            assertThat(policy.recordsPerSecond()).isCloseTo(11.574, org.assertj.core.data.Offset.offset(0.001));
        }

        @Test
        @DisplayName("no numbers at all is the ordinary case")
        void unlimited() {
            assertThat(RateLimitPolicy.NONE.isUnlimited()).isTrue();
            assertThat(RateLimitPolicy.NONE.describe()).isEqualTo("no rate limit");
        }
    }

    @Nested
    @DisplayName("what it refuses")
    class Refusals {

        @Test
        @DisplayName("an amount with no period, because 10,000 per what?")
        void amountWithoutWindow() {
            assertThatThrownBy(() -> new RateLimitPolicy(10_000, null, 0, null))
                    .isInstanceOf(DmpException.class)
                    .hasMessageContaining("per what");
        }

        @Test
        @DisplayName("a negative limit")
        void negative() {
            assertThatThrownBy(() -> new RateLimitPolicy(-1, Duration.ofMinutes(1), 0, null))
                    .isInstanceOf(DmpException.class);
        }

        @Test
        @DisplayName("a window shorter than a second, which the limiter cannot honour anyway")
        void tooShortAWindow() {
            assertThatThrownBy(() -> new RateLimitPolicy(5, Duration.ofMillis(100), 0, null))
                    .isInstanceOf(DmpException.class)
                    .hasMessageContaining("one second");
        }
    }

    @Nested
    @DisplayName("a period with no amount is forgotten, not kept")
    class Normalisation {

        @Test
        @DisplayName("so two policies that behave the same compare the same")
        void windowWithoutAmountIsDropped() {
            var stated = new RateLimitPolicy(0, Duration.ofMinutes(5), 100, Duration.ofMinutes(1));
            var equivalent = new RateLimitPolicy(0, null, 100, Duration.ofMinutes(1));

            assertThat(stated)
                    .as("a five-minute window on a limit of zero describes nothing at all")
                    .isEqualTo(equivalent);
            assertThat(stated.recordsWindow()).isNull();
        }
    }

    @Nested
    @DisplayName("saying it back")
    class Describing {

        @Test
        void bothUnits() {
            var policy = new RateLimitPolicy(10_000, Duration.ofMinutes(5), 100, Duration.ofMinutes(1));

            assertThat(policy.describe()).isEqualTo("10000 records/5m, 100 calls/1m");
        }

        @Test
        void oneUnit() {
            assertThat(new RateLimitPolicy(0, null, 15_000, Duration.ofDays(1)).describe())
                    .isEqualTo("15000 calls/24h");
        }
    }
}
