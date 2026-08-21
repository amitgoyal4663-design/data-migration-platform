package com.dmp.domain;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the platform-wide JSON conventions that data integrity depends on. */
class JsonConventionsTest {

    @Test
    @DisplayName("writes null fields rather than omitting them")
    void nullsAreWrittenNotDropped() {
        // A null column is a value, not an absence. Dropping it would mean a row whose email is
        // null arrives at the destination with no email field at all — and an upsert would leave
        // whatever was there before instead of clearing it.
        ObjectNode record = Json.newObject();
        record.put("id", 1);
        record.putNull("email");

        assertThat(record.toString()).contains("\"email\":null");
        assertThat(Json.mapper().valueToTree(new WithNull(1, null)).toString())
                .contains("\"name\":null");
    }

    @Test
    @DisplayName("keeps decimal precision rather than rounding through a double")
    void decimalsSurvive() throws Exception {
        // 10698.93 becomes 10698.930000000001 through a double, and a migrated ledger stops
        // reconciling.
        var parsed = Json.mapper().readTree("{\"amount\": 10698.93}");

        assertThat(parsed.get("amount").isBigDecimal()).isTrue();
        assertThat(parsed.get("amount").decimalValue()).isEqualByComparingTo("10698.93");
    }

    @Test
    @DisplayName("tolerates fields the platform does not know about")
    void unknownFieldsAreIgnored() throws Exception {
        // Connector payloads routinely carry fields the platform has no interest in. Rejecting a
        // record for carrying extra data would be hostile in a migration tool.
        var parsed = Json.mapper().readValue("{\"id\":1,\"unexpected\":true}", WithNull.class);

        assertThat(parsed.id()).isEqualTo(1);
    }

    private record WithNull(int id, String name) {
    }
}
