package com.dmp.connector.mongodb;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Converts between BSON and the platform's JSON model.
 *
 * <p>BSON has types JSON does not, and each conversion here is chosen so that loss is either
 * impossible or explicit:
 *
 * <ul>
 *   <li>{@code Decimal128} becomes {@code BigDecimal}. Passing a monetary value through a double
 *       would round it, and a migrated ledger that no longer reconciles is the worst thing this
 *       connector could do.</li>
 *   <li>{@code ObjectId} becomes its 24-character hex string, and is converted back on write —
 *       so a document round-trips with the same {@code _id} rather than acquiring a new one.</li>
 *   <li>Dates become ISO-8601 in UTC, which sorts correctly and keeps the instant unambiguous.</li>
 *   <li>Binary becomes base64, which is lossless.</li>
 * </ul>
 */
final class BsonValues {

    /**
     * Marks a string that was an ObjectId, so it converts back rather than being written as text.
     *
     * <p>Without this the field type would silently change on every migration: a collection copied
     * twice would end up with string {@code _id}s that no longer join to anything.
     */
    private static final String OBJECT_ID_PREFIX = "oid:";

    private BsonValues() {
    }

    static ObjectNode toJson(Document document) {
        ObjectNode node = Json.newObject();
        document.forEach((key, value) -> node.set(key, toJson(value)));
        return node;
    }

    @SuppressWarnings("unchecked")
    private static JsonNode toJson(Object value) {
        if (value == null) {
            return Json.mapper().nullNode();
        }
        if (value instanceof Document nested) {
            return toJson(nested);
        }
        if (value instanceof Map<?, ?> map) {
            ObjectNode node = Json.newObject();
            map.forEach((key, entry) -> node.set(String.valueOf(key), toJson(entry)));
            return node;
        }
        if (value instanceof List<?> list) {
            ArrayNode array = Json.mapper().createArrayNode();
            list.forEach(element -> array.add(toJson(element)));
            return array;
        }
        if (value instanceof ObjectId objectId) {
            return Json.mapper().getNodeFactory().textNode(OBJECT_ID_PREFIX + objectId.toHexString());
        }
        if (value instanceof Decimal128 decimal) {
            // BigDecimal, never double. This is the whole reason Decimal128 exists in BSON.
            return Json.mapper().getNodeFactory().numberNode(decimal.bigDecimalValue());
        }
        if (value instanceof Date date) {
            return Json.mapper().getNodeFactory().textNode(date.toInstant().toString());
        }
        if (value instanceof Binary binary) {
            return Json.mapper().getNodeFactory()
                    .textNode(Base64.getEncoder().encodeToString(binary.getData()));
        }
        if (value instanceof byte[] bytes) {
            return Json.mapper().getNodeFactory()
                    .textNode(Base64.getEncoder().encodeToString(bytes));
        }
        if (value instanceof BigDecimal decimal) {
            return Json.mapper().getNodeFactory().numberNode(decimal);
        }
        // Each width mapped explicitly. Jackson's generic valueToTree collapses these, and the
        // result is a field that silently changes BSON type on every migration — an int32 becoming
        // an int64, a double becoming a decimal. A strict schema validator on the target rejects
        // that, and a consumer reading the field gets a different shape than it expects.
        if (value instanceof Integer integer) {
            return Json.mapper().getNodeFactory().numberNode(integer);
        }
        if (value instanceof Long longValue) {
            return Json.mapper().getNodeFactory().numberNode(longValue);
        }
        if (value instanceof Double doubleValue) {
            return Json.mapper().getNodeFactory().numberNode(doubleValue);
        }
        if (value instanceof Float floatValue) {
            return Json.mapper().getNodeFactory().numberNode(floatValue);
        }
        if (value instanceof Short shortValue) {
            return Json.mapper().getNodeFactory().numberNode(shortValue.intValue());
        }
        if (value instanceof Number number) {
            return Json.mapper().getNodeFactory().numberNode(number.doubleValue());
        }
        if (value instanceof Boolean flag) {
            return Json.mapper().getNodeFactory().booleanNode(flag);
        }
        return Json.mapper().getNodeFactory().textNode(String.valueOf(value));
    }

    static Document toDocument(JsonNode node) {
        Document document = new Document();
        node.fields().forEachRemaining(entry ->
                document.put(entry.getKey(), toBson(entry.getValue())));
        return document;
    }

    private static Object toBson(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isObject()) {
            return toDocument(node);
        }
        if (node.isArray()) {
            List<Object> values = new java.util.ArrayList<>(node.size());
            node.forEach(element -> values.add(toBson(element)));
            return values;
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        // Mirrors the read exactly, so a field's BSON type is the same on both sides. Mapping
        // every float to Decimal128 "for safety" would be a silent type change on every numeric
        // field the source stored as a double.
        if (node.isBigDecimal()) {
            return new Decimal128(node.decimalValue());
        }
        if (node.isDouble() || node.isFloat()) {
            return node.asDouble();
        }
        if (node.isInt() || node.isShort()) {
            return node.asInt();
        }
        if (node.isLong() || node.isBigInteger()) {
            return node.asLong();
        }
        if (node.isIntegralNumber()) {
            long value = node.asLong();
            return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE ? (int) value : value;
        }

        String text = node.asText();
        if (text.startsWith(OBJECT_ID_PREFIX)) {
            String hex = text.substring(OBJECT_ID_PREFIX.length());
            return ObjectId.isValid(hex) ? new ObjectId(hex) : hex;
        }
        Instant instant = asInstant(text);
        return instant != null ? Date.from(instant) : text;
    }

    /** Recognises ISO-8601 so a timestamp is written as a BSON date rather than as text. */
    private static Instant asInstant(String text) {
        if (text.length() < 20 || !text.endsWith("Z")) {
            return null;
        }
        try {
            return Instant.parse(text);
        } catch (Exception notATimestamp) {
            return null;
        }
    }

    /** Renders a split boundary so it can go back into a query as the right BSON type. */
    static Object boundary(JsonNode value) {
        return toBson(value);
    }

    /** Wraps an ObjectId hex string with the marker the write path recognises. */
    static String markObjectId(ObjectId objectId) {
        return OBJECT_ID_PREFIX + objectId.toHexString();
    }
}
