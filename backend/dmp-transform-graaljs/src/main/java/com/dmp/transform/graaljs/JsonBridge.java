package com.dmp.transform.graaljs;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

import java.math.BigDecimal;
import java.util.List;

/**
 * Moves data between Jackson and JavaScript without granting the script any host access.
 *
 * <p>The obvious implementation — hand the script a {@code JsonNode} and let it call methods on it
 * — would require {@code HostAccess}, which is exactly what ADR-0008 turns off. So values cross as
 * JSON text and are re-parsed on each side. That is not free, and it is the price of a sandbox
 * that cannot reach into the JVM at all.
 *
 * <p>Numeric width is preserved deliberately in both directions. JavaScript has one number type,
 * so a naive round trip turns an {@code int} into a {@code double} and writes {@code 3.0} where the
 * source had {@code 3} — the same class of corruption that turned a MongoDB {@code Number} into a
 * {@code Long} earlier in this project. Whole values that fit an {@code int} or {@code long} come
 * back as integers.
 */
final class JsonBridge {

    private JsonBridge() {
    }

    /** Parses a JSON string inside the guest, so the script receives a native JS object. */
    static Value toGuest(Context context, JsonNode node) {
        Value parse = context.eval("js", "JSON.parse");
        return parse.execute(node == null ? "null" : node.toString());
    }

    static Value arrayToGuest(Context context, List<JsonNode> nodes) {
        ArrayNode array = Json.mapper().createArrayNode();
        nodes.forEach(array::add);
        return toGuest(context, array);
    }

    /**
     * Converts a value the script returned back into Jackson.
     *
     * <p>Returns null for JS {@code null} and {@code undefined} alike. A function with no explicit
     * return statement yields {@code undefined}, and treating that as "drop this record" rather
     * than as an error is what lets a filter be written as a plain {@code if}.
     */
    static JsonNode fromGuest(Context context, Value value) {
        if (value == null || value.isNull()) {
            return null;
        }
        Value stringify = context.eval("js", "JSON.stringify");
        Value json = stringify.execute(value);
        if (json.isNull()) {
            // JSON.stringify(undefined) is undefined, not the string "undefined".
            return null;
        }
        try {
            return normalise(Json.mapper().readTree(json.asString()));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "The script returned something that is not JSON: " + e.getMessage(), e);
        }
    }

    /**
     * Restores integer types that a JavaScript round trip would have widened to floating point.
     *
     * <p>{@code JSON.stringify(3)} produces {@code "3"}, which Jackson reads back as an int — so
     * most of the work is already done. What this catches is the case where a script computed a
     * whole number through arithmetic and Jackson chose {@code double} for a value like
     * {@code 250.0}: a quantity of 3 must not reach the destination as 3.0.
     */
    private static JsonNode normalise(JsonNode node) {
        if (node == null) {
            return null;
        }
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            object.fieldNames().forEachRemaining(field -> {
                JsonNode normalised = normalise(object.get(field));
                if (normalised != null) {
                    object.set(field, normalised);
                }
            });
            return object;
        }
        if (node.isArray()) {
            ArrayNode array = (ArrayNode) node;
            for (int i = 0; i < array.size(); i++) {
                JsonNode normalised = normalise(array.get(i));
                if (normalised != null) {
                    array.set(i, normalised);
                }
            }
            return array;
        }
        if (node.isDouble() || node.isFloat()) {
            BigDecimal value = node.decimalValue();
            if (value.stripTrailingZeros().scale() <= 0) {
                try {
                    long whole = value.longValueExact();
                    return whole == (int) whole
                            ? Json.mapper().getNodeFactory().numberNode((int) whole)
                            : Json.mapper().getNodeFactory().numberNode(whole);
                } catch (ArithmeticException tooLarge) {
                    // Beyond a long. Leaving it as a double is the honest outcome; there is no
                    // integer type here that would hold it.
                    return node;
                }
            }
        }
        return node;
    }
}
