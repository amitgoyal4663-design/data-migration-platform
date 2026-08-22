package com.dmp.persistence.postgres.mapper;

import com.dmp.common.json.Json;
import com.dmp.domain.connector.RateLimitPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;

/**
 * The rate limit as a column, and back.
 *
 * <p>Written by hand rather than by Jackson's record support because the stored shape has to
 * outlive the record's field names. What is persisted is four plain values — two numbers and two
 * ISO-8601 durations — and a rename in the domain must not silently orphan every row.
 *
 * <p>Null in either direction means no limit, which is what the overwhelming majority of connectors
 * have and what every connector stored before this column existed has.
 */
public final class RateLimitPolicyJson {

    private static final String RECORDS = "records";
    private static final String RECORDS_WINDOW = "recordsWindow";
    private static final String CALLS = "calls";
    private static final String CALLS_WINDOW = "callsWindow";
    private static final String PACING = "pacing";

    private RateLimitPolicyJson() {
    }

    public static JsonNode toJson(RateLimitPolicy policy) {
        if (policy == null || policy.isUnlimited()) {
            // Stored as null rather than as an object full of zeroes, so "has no limit" is one
            // state in the database rather than two that have to be kept in agreement.
            return null;
        }
        ObjectNode node = Json.newObject();
        if (policy.limitsRecords()) {
            node.put(RECORDS, policy.records());
            node.put(RECORDS_WINDOW, policy.recordsWindow().toString());
        }
        if (policy.limitsCalls()) {
            node.put(CALLS, policy.calls());
            node.put(CALLS_WINDOW, policy.callsWindow().toString());
        }
        node.put(PACING, policy.pacing().name());
        return node;
    }

    public static RateLimitPolicy fromJson(JsonNode node) {
        if (node == null || node.isNull() || node.isEmpty()) {
            return RateLimitPolicy.NONE;
        }
        return new RateLimitPolicy(
                node.path(RECORDS).asLong(0),
                window(node.path(RECORDS_WINDOW)),
                node.path(CALLS).asLong(0),
                window(node.path(CALLS_WINDOW)),
                pacing(node.path(PACING)));
    }

    /**
     * Rows written before pacing was a choice mean BURST, which is what they have been doing.
     * An unrecognised value means the same rather than failing the load: a connector that cannot
     * be read is a pipeline that cannot run.
     */
    private static RateLimitPolicy.Pacing pacing(JsonNode node) {
        if (!node.isTextual()) {
            return RateLimitPolicy.Pacing.BURST;
        }
        try {
            return RateLimitPolicy.Pacing.valueOf(node.asText());
        } catch (IllegalArgumentException e) {
            return RateLimitPolicy.Pacing.BURST;
        }
    }

    private static Duration window(JsonNode node) {
        return node.isMissingNode() || node.isNull() || !node.isTextual()
                ? null
                : Duration.parse(node.asText());
    }
}
