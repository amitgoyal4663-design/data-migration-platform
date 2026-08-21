package com.dmp.engine;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;

/**
 * Keeps one stored payload from being larger than it is worth.
 *
 * <p>The case this exists for is a field holding something that was never meant to be a field — a
 * base64 document, an embedded image, a serialised response body. One of those is a curiosity. Five
 * thousand copies of one, because every record carrying it failed the same validation, is a
 * dead-letter queue that fills a disk on behalf of a single fault.
 *
 * <p>Replaced rather than trimmed. A JSON document cut off mid-string is not a smaller document,
 * it is a broken one, and anything reading it later has to guess whether the truncation is the
 * fault it is investigating. A marker says plainly what happened and what was lost.
 */
public final class Payloads {

    static final String TRUNCATION_MARKER = "_dmpTruncated";
    static final String ORIGINAL_SIZE = "_dmpOriginalBytes";

    private Payloads() {
    }

    /**
     * Returns the payload, or a marker document if it exceeds the ceiling.
     *
     * @param maxBytes ceiling on the serialised form; {@code 0} keeps the payload whatever its size
     */
    public static JsonNode truncate(JsonNode payload, int maxBytes) {
        if (payload == null || payload.isNull() || maxBytes <= 0) {
            return payload;
        }

        int size = payload.toString().getBytes(StandardCharsets.UTF_8).length;
        if (size <= maxBytes) {
            return payload;
        }

        ObjectNode marker = Json.mapper().createObjectNode();
        marker.put(TRUNCATION_MARKER, true);
        marker.put(ORIGINAL_SIZE, size);

        // The key is kept when there is one. It is what makes the entry actionable at all — a
        // rejected record you cannot identify is barely more use than a count.
        JsonNode key = payload.get("_id");
        if (key != null && key.isValueNode()) {
            marker.set("_id", key);
        }
        return marker;
    }
}
