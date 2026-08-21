package com.dmp.domain.audit;

import com.dmp.common.json.Json;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether a pipeline keeps the records its destination refused.
 *
 * <p>Separate from the audit level because the two answer different questions. The level decides
 * whether every record's <em>identity</em> is searchable; this decides whether a <em>failure's</em>
 * content is stored, which is what makes it replayable. Folding them into one enum forced a choice
 * nobody wanted: an identity index and no stored payloads is a perfectly reasonable pipeline.
 */
class RejectedPayloadCaptureTest {

    @Test
    void aPolicyStoredBeforeThisSettingExistedKeepsCapturing() throws Exception {
        // Exactly what is in the database today: no such key anywhere.
        String stored = """
                {"level":"ERRORS","retention":2592000,"indexPayloads":false,
                 "redactionMode":"HASH","redactedFields":[],"maxPayloadBytes":32768,
                 "samplesPerSignature":10}
                """;

        AuditPolicy policy = Json.mapper().readValue(stored, AuditPolicy.class);

        assertThat(policy.capturesRejectedPayloads())
                .as("a missing field must not silently switch the dead-letter queue off on upgrade")
                .isTrue();
    }

    @Test
    void switchingItOffStopsCaptureWhileLeavingTheLevelAlone() {
        AuditPolicy off = AuditPolicy.DEFAULT.withoutRejectedPayloads();

        assertThat(off.capturesRejectedPayloads()).isFalse();
        assertThat(off.level())
                .as("the level governs indexing and is not what this switch is about")
                .isEqualTo(AuditPolicy.DEFAULT.level());
    }

    @Test
    void anIdentityIndexCanBeKeptWithoutStoringAnyRejectedRecord() {
        AuditPolicy indexOnly = new AuditPolicy(
                RecordAuditLevel.INDEXED, Set.of(), RedactionMode.HASH, Duration.ofDays(30),
                10, 32_768, false, false, StageLogPolicy.OFF);

        assertThat(indexOnly.level().indexesEveryRecord())
                .as("'was record 88291 transferred' stays answerable")
                .isTrue();
        assertThat(indexOnly.capturesRejectedPayloads())
                .as("without paying to store every refused record")
                .isFalse();
    }

    /**
     * COUNTERS wins over the switch.
     *
     * <p>That level is the statement that nothing beyond aggregate counters is kept. Honouring a
     * dead-letter queue underneath it would contradict the thing the user selected, and leave two
     * settings disagreeing about the same behaviour.
     */
    @Test
    void countersOverridesTheSwitch() {
        AuditPolicy counters = new AuditPolicy(
                RecordAuditLevel.COUNTERS, Set.of(), RedactionMode.HASH, Duration.ofDays(30),
                10, 32_768, false, true, StageLogPolicy.OFF);

        assertThat(counters.capturesRejectedPayloads()).isFalse();
    }
}
