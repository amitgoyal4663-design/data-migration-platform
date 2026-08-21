package com.dmp.persistence.mongo.support;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts between the domain's {@code JsonNode} payloads and the {@code Map} shape the MongoDB
 * driver stores.
 *
 * <p>Spring Data MongoDB has no native handling for {@code JsonNode}, and registering a custom
 * converter would apply the conversion invisibly to every field of that type — including ones
 * where the surrounding code has already made assumptions about representation. Converting
 * explicitly at the mapper keeps the translation visible at the point it happens.
 *
 * <p>The round trip is not perfectly lossless and it matters where. BSON has no unlimited-precision
 * decimal in the JSON sense; large decimals become {@code Decimal128} or lose precision depending
 * on magnitude. Nothing stored through this class is a record payload — only connector-defined
 * cursors, split specifications and preparation handles, which are identifiers and positions
 * rather than data. Record payloads take a different path in Phase 3.
 */
public final class JsonDocuments {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private JsonDocuments() {
    }

    /** Never returns null: an absent document becomes an empty map, matching the domain's convention. */
    public static Map<String, Object> toMap(JsonNode node) {
        if (node == null || node.isNull() || node.isEmpty()) {
            return new LinkedHashMap<>();
        }
        return Json.mapper().convertValue(node, MAP_TYPE);
    }

    /** Never returns null: an absent map becomes an empty object node. */
    public static JsonNode toJson(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return Json.emptyObject();
        }
        return Json.mapper().valueToTree(map);
    }
}
