package com.dmp.domain.connector;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.time.Duration;
import java.util.Map;

/**
 * How fast the far end has agreed to be called.
 *
 * <p>Not a performance setting. This is somebody else's sentence — "ten thousand records every five
 * minutes", "fifteen thousand API calls a day" — written down in a form the platform can honour. A
 * migration that exceeds it is not merely impolite: quotas get keys revoked, and a client who was
 * promised a ceiling and given thirty times it has a complaint the platform cannot answer.
 *
 * <p><b>Two units, because clients use two.</b> Some count rows, some count requests, and the two
 * are not convertible: five hundred records is one call or five hundred calls depending entirely on
 * how the pipeline is shaped. Either may be left unset, and unset means unlimited — the common case
 * is a client who gave exactly one number.
 *
 * <p><b>Windows are per unit.</b> "Ten requests a second and a million records a day" is one policy
 * with two different windows, and that shape is common enough in enterprise APIs that sharing a
 * single window between the two units would have made the field unusable for them.
 *
 * <p><b>It belongs to the connector instance, not the pipeline.</b> The limit is a property of the
 * endpoint being called. Three pipelines feeding one client draw on one budget; putting the number
 * on the pipeline would multiply the agreed rate by the number of pipelines, and would do it
 * silently, on the day somebody built the second one.
 *
 * <p>Applies to reads as much as writes. A source that is somebody's rate-limited API needs the same
 * protection, and nothing downstream can slow a read.
 *
 * @param records how many records may be sent per {@code recordsWindow}; {@code 0} for no limit
 * @param recordsWindow the period {@code records} is measured over; ignored when {@code records} is 0
 * @param calls how many requests may be made per {@code callsWindow}; {@code 0} for no limit
 * @param callsWindow the period {@code calls} is measured over; ignored when {@code calls} is 0
 */
public record RateLimitPolicy(long records, Duration recordsWindow, long calls, Duration callsWindow,
                              Pacing pacing) {

    /**
     * Whether the window resets, or slides.
     *
     * <p>This is a question about the client's counter, not a preference of ours, and the two
     * answers need different arithmetic. It is worth asking them.
     */
    public enum Pacing {

        /**
         * Spend up to a whole window at once, then wait for it to refill.
         *
         * <p>What "ten thousand every five minutes" means to most people, and what a client whose
         * counter resets on the clock is counting. The first window can deliver its whole allowance
         * immediately.
         *
         * <p>The catch, and it is the reason {@link #EVEN} exists: a client counting the
         * <em>last</em> five minutes at any instant can see up to twice the allowance, because a
         * full bucket spent at the end of one window is followed by a fresh window's refill.
         */
        BURST,

        /**
         * Never more than the stated amount in any window, sliding or otherwise.
         *
         * <p>Bought by holding only one call's worth at a time and refilling more slowly, which
         * costs throughput in proportion to how much of the window one call takes up: a call of a
         * tenth of the allowance sustains ninety percent of it, a call of half sustains half.
         * Smaller chunks buy the rate back.
         */
        EVEN
    }

    /** No agreement with the far end, so nothing to honour. What every connector has by default. */
    public static final RateLimitPolicy NONE = new RateLimitPolicy(0, null, 0, null, Pacing.BURST);

    /**
     * The four-argument form, which pre-dates pacing and means {@link Pacing#BURST}.
     *
     * <p>Burst rather than even, because it is what the connectors configured before the choice
     * existed have been doing, and a default that quietly halves an existing pipeline's throughput
     * would be a worse kind of wrong than one that keeps its current behaviour.
     */
    public RateLimitPolicy(long records, Duration recordsWindow, long calls, Duration callsWindow) {
        this(records, recordsWindow, calls, callsWindow, Pacing.BURST);
    }

    /**
     * The shortest window worth expressing.
     *
     * <p>Below a second the wait between calls is smaller than the round trip to Redis that
     * calculates it, so the limiter would cost more than it saves and would not be accurate anyway.
     */
    private static final Duration MIN_WINDOW = Duration.ofSeconds(1);

    /** A year. Longer than this and a "window" is really a quota with no reset anybody observes. */
    private static final Duration MAX_WINDOW = Duration.ofDays(366);

    public RateLimitPolicy {
        recordsWindow = validate(records, recordsWindow, "records");
        callsWindow = validate(calls, callsWindow, "calls");
        pacing = pacing == null ? Pacing.BURST : pacing;
    }

    /**
     * How much may be held at once, and how fast it refills, for one call of the given size.
     *
     * <p>Under {@link Pacing#BURST} these are simply the stated amount and the stated amount — the
     * bucket holds a window's worth and refills a window's worth. Under {@link Pacing#EVEN} the
     * bucket holds only what this call needs and refills the remainder, which is the arithmetic
     * that makes {@code held + refilled ≤ limit} true across any window rather than across
     * aligned ones.
     *
     * @param limit  the stated amount for this unit
     * @param wanted what this call intends to spend
     */
    public Bucket bucketFor(long limit, long wanted) {
        if (pacing == Pacing.BURST || wanted <= 0) {
            return new Bucket(limit, limit);
        }
        // capacity + refill must not exceed the limit. Capacity is exactly this call, so the refill
        // is whatever is left over — and when nothing is left over, no chunk of this size can ever
        // be sustained, which the caller reports rather than looping on.
        return new Bucket(wanted, limit - wanted);
    }

    /** @param capacity most that may be held at once; @param refill how much accrues per window */
    public record Bucket(long capacity, long refill) {

        /** Whether a call of this size can ever be sustained, or would wait for a refill of zero. */
        public boolean isAchievable() {
            return capacity > 0 && refill > 0;
        }
    }

    private static Duration validate(long amount, Duration window, String unit) {
        if (amount < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A " + unit + " limit cannot be negative. Leave it at 0 for no limit.",
                    Map.of("field", unit, "value", amount));
        }
        if (amount == 0) {
            // Normalised rather than kept. A window with no amount describes nothing, and letting
            // one linger means two policies that behave identically can compare unequal.
            return null;
        }
        if (window == null) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A limit of " + amount + " " + unit + " needs a period — " + amount
                            + " per what? Set the window, or set the limit to 0 for no limit.",
                    Map.of("field", unit + "Window"));
        }
        if (window.compareTo(MIN_WINDOW) < 0 || window.compareTo(MAX_WINDOW) > 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "The " + unit + " window must be between one second and one year",
                    Map.of("field", unit + "Window", "value", window.toString()));
        }
        return window;
    }

    /** Whether there is anything to enforce. */
    public boolean isUnlimited() {
        return records == 0 && calls == 0;
    }

    public boolean limitsRecords() {
        return records > 0;
    }

    public boolean limitsCalls() {
        return calls > 0;
    }

    /**
     * The largest number of records that could ever be sent in one go.
     *
     * <p>A chunk bigger than this can never be granted, however long it waits — the bucket does not
     * hold that many tokens and never will. Callers use it to refuse such a pipeline when it is
     * saved, which is the only moment anybody is in a position to fix it.
     */
    public long maxRecordsPerAcquire() {
        return records == 0 ? Long.MAX_VALUE : records;
    }

    /** Records per second, for estimating how long a run will take. Zero when records are unlimited. */
    public double recordsPerSecond() {
        return records == 0 ? 0 : (double) records / recordsWindow.toMillis() * 1000d;
    }

    /** Calls per second, for the same purpose. Zero when calls are unlimited. */
    public double callsPerSecond() {
        return calls == 0 ? 0 : (double) calls / callsWindow.toMillis() * 1000d;
    }

    /** Human-readable, for logs and for the console — "10000 records/5m, 100 calls/1m". */
    public String describe() {
        if (isUnlimited()) {
            return "no rate limit";
        }
        StringBuilder text = new StringBuilder();
        if (limitsRecords()) {
            text.append(records).append(" records/").append(compact(recordsWindow));
        }
        if (limitsCalls()) {
            if (!text.isEmpty()) {
                text.append(", ");
            }
            text.append(calls).append(" calls/").append(compact(callsWindow));
        }
        return text.toString();
    }

    private static String compact(Duration window) {
        return window.toString().substring(2).toLowerCase();
    }
}
