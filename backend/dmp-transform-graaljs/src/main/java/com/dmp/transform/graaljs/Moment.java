package com.dmp.transform.graaljs;

import org.graalvm.polyglot.HostAccess;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;

/**
 * A moment in the schedule's timezone, with the arithmetic a window script needs.
 *
 * <p>Exists so nobody has to do date arithmetic in JavaScript. `Date` has no notion of a timezone
 * other than the host's, no way to say "the start of this day in Asia/Kolkata", and no calendar
 * arithmetic — and every one of those gaps is a place a window silently drifts by an hour.
 *
 * <p><b>Calendar arithmetic, not duration arithmetic.</b> {@code minus({days: 1})} subtracts a
 * calendar day, which is 23 or 25 hours on the two days a year a timezone changes offset. Doing it
 * as 24 hours instead would lose or duplicate an hour of data twice a year, in a way nobody would
 * connect to the clocks changing. The same reasoning covers months, which are not 30 days.
 */
public final class Moment {

    private final ZonedDateTime value;

    Moment(Instant instant, ZoneId zone) {
        this.value = instant.atZone(zone);
    }

    private Moment(ZonedDateTime value) {
        this.value = value;
    }

    /**
     * Rounds down to the start of the given unit.
     *
     * <p>{@code startOf('day')} on a run that fires at 10:00 gives midnight, which is what makes
     * "the previous calendar day" expressible: the schedule fires when convenient and the window
     * still lines up with day boundaries.
     */
    @HostAccess.Export
    public Moment startOf(String unit) {
        return new Moment(switch (normalise(unit)) {
            case "minute" -> value.truncatedTo(ChronoUnit.MINUTES);
            case "hour" -> value.truncatedTo(ChronoUnit.HOURS);
            case "day" -> value.toLocalDate().atStartOfDay(value.getZone());
            case "week" -> value.toLocalDate().with(java.time.DayOfWeek.MONDAY)
                    .atStartOfDay(value.getZone());
            case "month" -> value.toLocalDate().withDayOfMonth(1).atStartOfDay(value.getZone());
            case "year" -> value.toLocalDate().withDayOfYear(1).atStartOfDay(value.getZone());
            default -> throw new IllegalArgumentException(
                    "Unknown unit '" + unit + "'. Use minute, hour, day, week, month or year.");
        });
    }

    /** {@code minus({days: 1})}, {@code minus({hours: 2})}, and so on. */
    @HostAccess.Export
    public Moment minus(Map<String, Object> amounts) {
        return shift(amounts, -1);
    }

    @HostAccess.Export
    public Moment plus(Map<String, Object> amounts) {
        return shift(amounts, 1);
    }

    /** Monday is 1, Sunday is 7 — so a script can skip weekends without a lookup table. */
    @HostAccess.Export
    public int dayOfWeek() {
        return value.getDayOfWeek().getValue();
    }

    /**
     * ISO-8601 with an offset, which is what a query parameter should carry.
     *
     * <p>The offset matters: {@code 2026-08-01T00:00:00+05:30} is a different instant from the same
     * text with a Z, and a warehouse comparing against a timestamp column needs to be told which
     * one was meant rather than left to assume.
     */
    @HostAccess.Export
    @Override
    public String toString() {
        return value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    private Moment shift(Map<String, Object> amounts, int sign) {
        if (amounts == null) {
            return this;
        }
        ZonedDateTime shifted = value;

        for (Map.Entry<String, Object> amount : amounts.entrySet()) {
            long n = sign * ((Number) amount.getValue()).longValue();
            shifted = switch (normalise(amount.getKey())) {
                case "minute" -> shifted.plusMinutes(n);
                case "hour" -> shifted.plusHours(n);
                case "day" -> shifted.plusDays(n);
                case "week" -> shifted.plusWeeks(n);
                case "month" -> shifted.plusMonths(n);
                case "year" -> shifted.plusYears(n);
                default -> throw new IllegalArgumentException(
                        "Unknown unit '" + amount.getKey()
                                + "'. Use minutes, hours, days, weeks, months or years.");
            };
        }
        return new Moment(shifted);
    }

    /** Accepts {@code day} and {@code days} alike, because both read naturally in a script. */
    private static String normalise(String unit) {
        String lower = unit == null ? "" : unit.toLowerCase(Locale.ROOT).strip();
        return lower.endsWith("s") ? lower.substring(0, lower.length() - 1) : lower;
    }
}
