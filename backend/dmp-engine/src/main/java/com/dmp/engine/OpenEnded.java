package com.dmp.engine;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Marks a chunk whose range is decided as it runs rather than when the run was planned.
 *
 * <p>The marker lives in the chunk's spec, which is otherwise connector-defined and opaque. That is
 * safe here precisely because the engine, not a connector, produced this spec — and a connector
 * reading it looks only for the keys it put there itself, so an unfamiliar one is ignored.
 *
 * <p>The spec deliberately carries no boundaries at all. The position comes entirely from the
 * checkpoint cursor, which is what lets a chunk pick up rows that arrived after the run started
 * instead of stopping at a maximum frozen minutes earlier.
 */
final class OpenEnded {

    private static final String MARKER = "_dmpOpenEnded";

    private OpenEnded() {
    }

    static JsonNode spec() {
        ObjectNode spec = Json.newObject();
        spec.put(MARKER, true);
        return spec;
    }

    static boolean isOpenEnded(JsonNode spec) {
        return spec != null && spec.path(MARKER).asBoolean(false);
    }
}
