package com.dmp.engine;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A ceiling on one stored payload, for the field that was never meant to hold what it holds.
 *
 * <p>One record carrying an embedded document is a curiosity. Five thousand copies of it, because
 * every record carrying it failed the same validation, is a dead-letter queue filling a disk on
 * behalf of a single fault.
 */
class PayloadTruncationTest {

    private static ObjectNode order(String blobField, int blobSize) {
        ObjectNode node = Json.mapper().createObjectNode();
        node.put("_id", "ORD-88213");
        node.put("status", "cancelled");
        if (blobField != null) {
            node.put(blobField, "x".repeat(blobSize));
        }
        return node;
    }

    @Test
    @DisplayName("an ordinary record passes through untouched")
    void smallPayloadsAreUnchanged() {
        JsonNode payload = order(null, 0);

        assertThat(Payloads.truncate(payload, 32 * 1024)).isSameAs(payload);
    }

    @Test
    @DisplayName("a payload over the ceiling is replaced by a marker, not cut in half")
    void oversizedPayloadsBecomeAMarker() {
        // Cutting the JSON mid-string would produce a document that no longer parses, and anyone
        // reading it later would have to work out whether that was the fault they came to
        // investigate. A marker says what happened.
        JsonNode truncated = Payloads.truncate(order("attachment", 100_000), 1_024);

        assertThat(truncated.path(Payloads.TRUNCATION_MARKER).asBoolean()).isTrue();
        assertThat(truncated.path(Payloads.ORIGINAL_SIZE).asLong()).isGreaterThan(100_000);
        assertThat(truncated.has("attachment")).isFalse();
    }

    @Test
    @DisplayName("the key survives truncation, because without it the entry is barely a count")
    void keyIsPreserved() {
        JsonNode truncated = Payloads.truncate(order("attachment", 100_000), 1_024);

        assertThat(truncated.path("_id").asText()).isEqualTo("ORD-88213");
    }

    @Test
    @DisplayName("a ceiling of zero keeps the payload whatever its size")
    void zeroDisablesTheCeiling() {
        // What a pipeline stored before this setting existed deserialises to. Upgrading the
        // platform must not start discarding evidence somebody was relying on.
        JsonNode large = order("attachment", 100_000);

        assertThat(Payloads.truncate(large, 0)).isSameAs(large);
    }

    @Test
    @DisplayName("a null payload is not an error")
    void nullIsSafe() {
        assertThat(Payloads.truncate(null, 1_024)).isNull();
    }
}
