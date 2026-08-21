package com.dmp.common.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * The platform's single JSON configuration.
 *
 * <p>Per ADR-0003 {@code JsonNode} is the in-flight payload model, which makes this
 * configuration a correctness concern rather than a formatting preference. Two settings in
 * particular are load-bearing:
 *
 * <ul>
 *   <li>{@code USE_BIG_DECIMAL_FOR_FLOATS} — without it, {@code 1234567890123456789.01} from a
 *       source database silently becomes a lossy {@code double} on the way to the sink. ADR-0003
 *       accepts reduced type fidelity as a trade-off; it does not accept silent corruption of
 *       values that arrive with full precision.</li>
 *   <li>{@code FAIL_ON_UNKNOWN_PROPERTIES} disabled — connector payloads routinely carry fields
 *       the platform has no interest in, and rejecting a record for carrying extra data would be
 *       hostile in a migration tool.</li>
 * </ul>
 *
 * <p>Every {@code ObjectMapper} in the platform is built here. Spring's auto-configured mapper is
 * replaced with this one so that a record serialised by the web layer and a record serialised by
 * the engine cannot diverge.
 */
public final class Json {

    private static final ObjectMapper SHARED = newMapper();

    private Json() {
    }

    /**
     * Builds a mapper configured to platform conventions.
     *
     * <p>Nulls are written, not omitted — deliberately, and for two independent reasons.
     *
     * <p><b>Data integrity.</b> A null column is a value, not an absence. Dropping it would mean a
     * row whose {@code email} is null arrives at the destination with no {@code email} field at
     * all, and an upsert would then leave whatever was there before instead of clearing it. The
     * migration would silently fail to migrate.
     *
     * <p><b>API contract.</b> A typed client declaring {@code publishedVersion: number | null}
     * receives {@code undefined} when the field is omitted, and {@code undefined !== null} is true
     * — so an unpublished pipeline reads as published. Omitting nulls saves a few bytes and costs
     * correctness at every consumer.
     */
    public static ObjectMapper newMapper() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .serializationInclusion(JsonInclude.Include.ALWAYS)
                .build();
    }

    /**
     * The shared, immutable-by-convention mapper.
     *
     * <p>{@code ObjectMapper} is thread-safe once configured. Callers must not reconfigure it;
     * anything needing different settings calls {@link #newMapper()} and adjusts its own copy.
     */
    public static ObjectMapper mapper() {
        return SHARED;
    }

    public static ObjectNode newObject() {
        return SHARED.createObjectNode();
    }

    /** An empty object node. Used as the null-object for optional configuration blocks. */
    public static JsonNode emptyObject() {
        return SHARED.createObjectNode();
    }

    /** Null-safe normalisation: {@code null} becomes an empty object rather than a null node. */
    public static JsonNode orEmpty(JsonNode node) {
        return node == null || node.isNull() ? emptyObject() : node;
    }
}
