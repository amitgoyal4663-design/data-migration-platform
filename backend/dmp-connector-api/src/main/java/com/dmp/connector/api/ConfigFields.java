package com.dmp.connector.api;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Helpers for declaring configuration fields, including which pipeline role each one belongs to.
 *
 * <p>Most connectors are symmetric enough to share one configuration object, but individual fields
 * within it rarely are: a Kafka sink has no opinion on where to start reading, and a MongoDB source
 * has nothing to write. Rendering every field for every instance asks the user to answer questions
 * that do not apply to what they are building, and — worse — invites them to set a value that will
 * be silently ignored.
 *
 * <p>The role is carried as a JSON Schema extension keyword. Schema validators ignore keywords they
 * do not recognise, so the schema stays a valid, usable JSON Schema while the console gains enough
 * information to show only the fields that matter.
 */
public final class ConfigFields {

    /** Extension keyword naming the pipeline role a field applies to. Absent means both. */
    public static final String ROLE = "x-dmp-role";

    /**
     * Marks a field whose value belongs to the deployment rather than to the pipeline.
     *
     * <p>A connection string, a broker list, a workspace URL: these are owned by whoever runs the
     * cluster, differ in every environment, and are the reason a connection built in one place has
     * to be rebuilt by hand in the next. The console renders these as a reference by default, so
     * naming the variable is the ordinary path and typing a literal is the deliberate one.
     *
     * <p>Not enforcement. A literal is still accepted, because {@code mongodb://localhost:27017} is
     * the right answer on a laptop and a rule that forbade it would only teach people to work
     * around this.
     */
    public static final String ENVIRONMENT = "x-dmp-environment";

    /**
     * Marks a field that has a sensible default and rarely needs changing.
     *
     * <p>Collapsed by default in the console. The point is not to hide these — every one of them
     * still matters, and one of them decides whether a retry duplicates data — but a form that asks
     * eight questions to connect to a database buries the two that are actually about this
     * connection among six that are not.
     */
    public static final String ADVANCED = "x-dmp-advanced";

    /** Marks a field as supplied by the deployment. See {@link #ENVIRONMENT}. */
    public static ObjectNode fromEnvironment(ObjectNode field) {
        field.put(ENVIRONMENT, true);
        return field;
    }

    /** Marks a field as advanced, with the default it takes when left alone. */
    public static ObjectNode advanced(ObjectNode field) {
        field.put(ADVANCED, true);
        return field;
    }

    /**
     * Marks the field a source reads each record's identity from.
     *
     * <p>Declared by the connector rather than inferred by the platform, because only the connector
     * knows which of its settings produces the key it puts on a {@code DataRecord} — and a platform
     * that guessed from field names would be wrong the first time a connector named one differently.
     *
     * <p>Read at publish time to warn when a pipeline is set to index its records but its source has
     * no identity configured. That combination runs, completes, reports success and indexes nothing,
     * which is the worst way for a feature to be unavailable.
     */
    public static final String RECORD_KEY = "x-dmp-record-key";

    /**
     * Marks a field as the source's record identity, with the default the connector applies.
     *
     * <p>A connector may mark more than one — JDBC reads {@code keyColumns} and falls back to
     * {@code splitColumn} — and any one of them being set means the source can identify a record.
     * The alternative was a platform that knew about connectors' internal fallbacks, which is
     * exactly the knowledge this marker exists to avoid.
     */
    public static ObjectNode recordKeyField(ObjectNode field, String defaultValue) {
        field.put(RECORD_KEY, true);
        if (defaultValue != null) {
            field.put("default", defaultValue);
        }
        return field;
    }

    private ConfigFields() {
    }

    /** A field that means the same thing whether the instance reads or writes. */
    public static ObjectNode field(String type, String description) {
        return Json.newObject().put("type", type).put("description", description);
    }

    /** A field that only applies when the instance is used as a source. */
    public static ObjectNode sourceField(String type, String description) {
        return field(type, description).put(ROLE, ConnectorSpec.Direction.SOURCE.name());
    }

    /** A field that only applies when the instance is used as a sink. */
    public static ObjectNode sinkField(String type, String description) {
        return field(type, description).put(ROLE, ConnectorSpec.Direction.SINK.name());
    }

    public static ObjectNode enumField(String description, String... values) {
        return withValues(field("string", description), values);
    }

    public static ObjectNode sourceEnumField(String description, String... values) {
        return withValues(sourceField("string", description), values);
    }

    public static ObjectNode sinkEnumField(String description, String... values) {
        return withValues(sinkField("string", description), values);
    }

    private static ObjectNode withValues(ObjectNode node, String... values) {
        var allowed = Json.mapper().createArrayNode();
        for (String value : values) {
            allowed.add(value);
        }
        node.set("enum", allowed);
        return node;
    }
}
