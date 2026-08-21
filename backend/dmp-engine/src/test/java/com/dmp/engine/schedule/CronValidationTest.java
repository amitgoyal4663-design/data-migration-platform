package com.dmp.engine.schedule;

import com.dmp.common.error.DmpException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A cron expression is rejected while the user is looking at the field they typed it into.
 *
 * <p>The alternative is storing it, registering a trigger that fails to build, and finding out at
 * 03:00 from a log nobody is reading.
 */
class CronValidationTest {

    @Test
    @DisplayName("accepts a daily rule")
    void acceptsDaily() {
        assertThatCode(() -> ScheduleRegistrar.requireValidCron("0 0 3 * * ?"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("accepts weekdays only")
    void acceptsWeekdays() {
        assertThatCode(() -> ScheduleRegistrar.requireValidCron("0 30 2 ? * MON-FRI"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("rejects a five-field Unix cron and explains the difference")
    void rejectsUnixCron() {
        // The commonest mistake by far. Quartz takes six or seven fields, so a Unix-style
        // "0 3 * * *" is not the rule the author meant — naming that beats a bare parse error.
        assertThatThrownBy(() -> ScheduleRegistrar.requireValidCron("0 3 * * *"))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("six or seven fields");
    }

    @Test
    @DisplayName("rejects nonsense before it is stored")
    void rejectsNonsense() {
        assertThatThrownBy(() -> ScheduleRegistrar.requireValidCron("every tuesday"))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("not a valid cron expression");
    }
}
