package com.dmp.connector.api;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Several named ways for one connector to find records.
 *
 * <p>A connector instance has always described exactly one selection — a SQL statement, a Mongo
 * filter, a SOQL query. That is right for the job it was built for and useless for the question
 * people actually arrive with: <em>what happened to policy POL-44219?</em> A date-range filter
 * cannot answer it, and nobody knows the date, which is why they are asking.
 *
 * <p>So an instance may declare a map of them:
 *
 * <pre>
 * "queries": {
 *   "By date range":    { "sql": "SELECT * FROM policies WHERE updated_at >= :from AND ..." },
 *   "By policy number": { "sql": "SELECT * FROM policies WHERE policy_no IN (:policyNos)" }
 * }
 * </pre>
 *
 * <p><b>Applied before the connector sees the config, not inside it.</b> A variant's fields are
 * merged over the instance's own, so a connector receives a configuration in exactly the shape it
 * always received and never learns this feature exists. That is what makes it work for connectors
 * built outside this repository, and what keeps chunking, resume, parameter binding and the call
 * log identical for a targeted run and a scheduled one.
 *
 * <p><b>The variants are written by whoever configures the connector.</b> They are queries against
 * production in the source's own language. Support picks one from a named list and fills in a box;
 * they never type a query. The distance between those two things is the distance between a safe
 * operation and an arbitrary query tool.
 */
public final class QueryVariants {

    /** Where the map lives in a connector instance's configuration. */
    public static final String FIELD = "queries";

    private QueryVariants() {
    }

    /**
     * The names on offer, in declaration order.
     *
     * <p>Empty for an instance that declares none, which is every instance written before this
     * existed — and they keep working untouched, because no variant means the config is already
     * the query.
     */
    public static List<String> names(JsonNode config) {
        JsonNode queries = config == null ? null : config.get(FIELD);
        if (queries == null || !queries.isObject() || queries.isEmpty()) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        queries.fieldNames().forEachRemaining(names::add);
        return List.copyOf(names);
    }

    /**
     * The configuration a connector should see when this variant is chosen.
     *
     * <p>The variant's fields win; everything else — host, credentials, collection, chunk mode —
     * comes from the instance. A variant therefore says only what differs, which is the query, and
     * cannot accidentally repoint a run at another database.
     *
     * @param name null, blank, or unknown yields the configuration unchanged. Unknown is not an
     *             error here: the caller that lets somebody choose a name is the one that can say
     *             which names exist, and failing in a merge helper would put that message in the
     *             wrong place.
     */
    public static JsonNode apply(JsonNode config, String name) {
        if (config == null || name == null || name.isBlank()) {
            return config;
        }
        JsonNode variant = config.path(FIELD).path(name);
        if (!variant.isObject()) {
            return config;
        }

        ObjectNode merged = config.deepCopy();
        variant.fields().forEachRemaining(field -> merged.set(field.getKey(), field.getValue()));

        // Removed after merging, so a connector never sees the other variants. It has no use for
        // them, and a connector that started reading them would make this a feature it depends on
        // rather than one applied to it.
        merged.remove(FIELD);
        return merged;
    }

    /** Whether this instance offers a choice at all. */
    public static boolean any(JsonNode config) {
        return !names(config).isEmpty();
    }

    /**
     * The name a run should use when none was chosen.
     *
     * <p>The first declared, so an instance that offers variants always has a working default and
     * a schedule written before they existed keeps doing what it did. Declaration order is the
     * author's own ordering, which makes "the first one" a decision somebody made rather than
     * whichever name sorts earliest.
     */
    public static String defaultName(JsonNode config) {
        List<String> names = names(config);
        return names.isEmpty() ? null : names.get(0);
    }

    /** An empty object, for a caller that needs a config and has none. */
    public static JsonNode none() {
        return Json.emptyObject();
    }
}
