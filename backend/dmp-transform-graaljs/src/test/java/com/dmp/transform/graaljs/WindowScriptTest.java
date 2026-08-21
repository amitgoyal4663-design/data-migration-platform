package com.dmp.transform.graaljs;

import com.dmp.transform.api.TransformException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The script that decides which range a scheduled run covers.
 *
 * <p>The two properties worth testing are not "does the arithmetic work" but "is the arithmetic
 * <em>calendar</em> arithmetic" and "can the script see a clock". The first loses an hour of data
 * twice a year in a way nobody connects to the clocks changing; the second makes a rerun of the
 * first of August quietly become the second.
 */
class WindowScriptTest {

    private final GraalJsWindowScript scripts = new GraalJsWindowScript();

    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");
    private static final ZoneId LONDON = ZoneId.of("Europe/London");

    @Test
    @DisplayName("the daily job: fires at 10am, covers the previous calendar day")
    void previousCalendarDay() {
        // 2 Aug 10:00 in Kolkata.
        Map<String, String> window = scripts.evaluate("""
                const to = fireTime.startOf('day')
                return { from: to.minus({ days: 1 }), to: to }
                """, Instant.parse("2026-08-02T04:30:00Z"), KOLKATA);

        assertThat(window.get("from")).startsWith("2026-08-01T00:00:00");
        assertThat(window.get("to")).startsWith("2026-08-02T00:00:00");
    }

    @Test
    @DisplayName("the hourly job: fires on the hour, covers the previous hour")
    void previousHour() {
        Map<String, String> window = scripts.evaluate("""
                const to = fireTime.startOf('hour')
                return { from: to.minus({ hours: 1 }), to: to }
                """, Instant.parse("2026-08-02T11:00:00Z"), ZoneId.of("UTC"));

        assertThat(window.get("from")).startsWith("2026-08-02T10:00:00");
        assertThat(window.get("to")).startsWith("2026-08-02T11:00:00");
    }

    @Test
    @DisplayName("a run that fires late still covers the whole previous period")
    void theFireTimeIsRoundedDownNotUsedRaw() {
        // The point of startOf: the schedule fires when convenient, and the window still lines up
        // with day boundaries rather than with whenever the pod got round to it.
        Map<String, String> window = scripts.evaluate("""
                const to = fireTime.startOf('day')
                return { from: to.minus({ days: 1 }), to: to }
                """, Instant.parse("2026-08-02T17:43:11Z"), ZoneId.of("UTC"));

        assertThat(window.get("from")).startsWith("2026-08-01T00:00:00");
        assertThat(window.get("to")).startsWith("2026-08-02T00:00:00");
    }

    @Test
    @DisplayName("a day across a clock change is a calendar day, not 24 hours")
    void daylightSavingDoesNotShiftTheWindow() {
        // Britain's clocks go back at 02:00 on 25 October 2026, making that day 25 hours long.
        // Subtracting a duration would put 'from' at 23:00 on the 24th and re-read an hour;
        // subtracting a calendar day lands on midnight, which is what the window means.
        Map<String, String> window = scripts.evaluate("""
                const to = fireTime.startOf('day')
                return { from: to.minus({ days: 1 }), to: to }
                """, Instant.parse("2026-10-26T10:00:00Z"), LONDON);

        assertThat(window.get("from")).startsWith("2026-10-25T00:00:00");
        assertThat(window.get("to")).startsWith("2026-10-26T00:00:00");
    }

    @Test
    @DisplayName("the timestamp carries its offset, so the instant is unambiguous")
    void theOffsetIsIncluded() {
        Map<String, String> window = scripts.evaluate(
                "return { from: fireTime.startOf('day') }",
                Instant.parse("2026-08-02T04:30:00Z"), KOLKATA);

        // 04:30Z is 10:00 in Kolkata on the 2nd, so the start of that day is the 2nd at midnight
        // — carrying +05:30, which is a different instant from the same text with a Z.
        assertThat(window.get("from")).isEqualTo("2026-08-02T00:00:00+05:30");
    }

    @Test
    void arbitraryLogicIsExpressible() {
        // The reason this is a script rather than a windowUnit and a windowCount: nobody would have
        // put "previous business day" in a config schema, and it would have been asked for.
        String script = """
                const to = fireTime.startOf('day')
                const back = to.dayOfWeek() === 1 ? 3 : 1
                return { from: to.minus({ days: back }), to: to }
                """;

        // A Monday: reaches back over the weekend to Friday.
        assertThat(scripts.evaluate(script, Instant.parse("2026-08-03T10:00:00Z"), ZoneId.of("UTC")))
                .containsEntry("from", "2026-07-31T00:00:00Z");
    }

    @Test
    void aScriptCannotReadTheClock() {
        // The property everything else depends on. A script that could read the wall clock would
        // compute a different window each time it ran, so a rerun would silently cover a different
        // range from the one it is a rerun of.
        assertThatThrownBy(() -> scripts.evaluate("return { now: Date.now() }",
                Instant.parse("2026-08-02T10:00:00Z"), ZoneId.of("UTC")))
                .isInstanceOf(TransformException.class);
    }

    @Test
    void aScriptCannotReachTheHost() {
        assertThatThrownBy(() -> scripts.evaluate(
                "return { x: Java.type('java.lang.System').getenv('PATH') }",
                Instant.parse("2026-08-02T10:00:00Z"), ZoneId.of("UTC")))
                .isInstanceOf(TransformException.class);
    }

    @Test
    void returningSomethingOtherThanAnObjectSaysWhatWasExpected() {
        assertThatThrownBy(() -> scripts.evaluate("return 'yesterday'",
                Instant.parse("2026-08-02T10:00:00Z"), ZoneId.of("UTC")))
                .isInstanceOf(TransformException.class)
                .hasMessageContaining("{ from: ..., to: ... }");
    }

    @Test
    void aBrokenScriptSaysSoRatherThanFiringAnEmptyWindow() {
        assertThatThrownBy(() -> scripts.evaluate("return { from: fireTime.startOf('fortnight') }",
                Instant.parse("2026-08-02T10:00:00Z"), ZoneId.of("UTC")))
                .isInstanceOf(TransformException.class)
                .hasMessageContaining("fortnight");
    }

    @Test
    void noScriptMeansNoParameters() {
        // Every schedule that exists today. They must keep behaving exactly as they do now.
        assertThat(scripts.evaluate(null, Instant.now(), ZoneId.of("UTC"))).isEmpty();
        assertThat(scripts.evaluate("  ", Instant.now(), ZoneId.of("UTC"))).isEmpty();
    }
}
