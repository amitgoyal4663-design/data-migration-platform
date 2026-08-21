package com.dmp.connector.api;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;
import java.util.Objects;

/**
 * One record in flight.
 *
 * <p>The payload is a Jackson {@code JsonNode} — the platform's in-flight data model. The
 * surrounding fields are engine coordinates, and they exist outside the payload deliberately: a
 * user's transformation may rewrite the payload entirely, and the engine still needs to know which
 * chunk this record came from and where it sat in that chunk. Idempotent sink writes depend on
 * that surviving.
 *
 * <p>{@code seq} and {@code ordinal} together identify a record within its chunk. {@code seq}
 * alone does not: a transform that turns one input into several gives every output the input's
 * sequence number, and a natural key does not either — a source is free to hold the same key
 * twice, and an audit trail that collapses those two rows reports fewer records than the run
 * moved.
 *
 * @param payload the user-visible document
 * @param key     natural key, used for partitioning and upsert; may be null
 * @param headers lineage and connector metadata, never business data
 * @param seq     position within the chunk, monotonic from 1
 * @param ordinal which output of that position this is, from 0
 * @param bytes   approximate serialised size, used to enforce the batch byte ceiling
 */
public record DataRecord(
        JsonNode payload,
        String key,
        Map<String, String> headers,
        long seq,
        int ordinal,
        long bytes) {

    public DataRecord {
        payload = Json.orEmpty(payload);
        headers = Map.copyOf(headers == null ? Map.of() : headers);
    }

    public static DataRecord of(JsonNode payload, long seq) {
        return new DataRecord(payload, null, Map.of(), seq, 0, estimateBytes(payload));
    }

    public static DataRecord of(JsonNode payload, String key, long seq) {
        return new DataRecord(payload, key, Map.of(), seq, 0, estimateBytes(payload));
    }

    public DataRecord withPayload(JsonNode newPayload) {
        return new DataRecord(newPayload, key, headers, seq, ordinal, estimateBytes(newPayload));
    }

    public DataRecord withHeader(String name, String value) {
        Map<String, String> merged = new java.util.LinkedHashMap<>(headers);
        merged.put(name, value);
        return new DataRecord(payload, key, merged, seq, ordinal, bytes);
    }

    /**
     * The same record at a different output position.
     *
     * <p>Set only where one input becomes several. {@code seq} cannot distinguish them — every
     * record a transform produces from one input deliberately keeps that input's sequence number,
     * because {@code seq} is where the checkpoint resumes and renumbering would move the resume
     * position onto a record that does not exist in the source.
     */
    public DataRecord withOrdinal(int newOrdinal) {
        return newOrdinal == ordinal
                ? this
                : new DataRecord(payload, key, headers, seq, newOrdinal, bytes);
    }

    /**
     * Approximate serialised size.
     *
     * <p>Approximate on purpose. The byte ceiling on a batch is a memory guard, not an accounting
     * figure, and serialising every record twice to measure it precisely would cost more than the
     * guard is worth. Overestimating slightly is the safe direction.
     */
    public static long estimateBytes(JsonNode node) {
        if (node == null || node.isNull()) {
            return 0;
        }
        return Objects.toString(node, "").length();
    }
}
