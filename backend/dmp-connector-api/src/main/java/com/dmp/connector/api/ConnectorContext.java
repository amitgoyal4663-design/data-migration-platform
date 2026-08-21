package com.dmp.connector.api;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;

/**
 * Everything a connector is given when it opens a session.
 *
 * <p>Secrets are resolved here rather than being present in {@code config}. A connector asks for a
 * credential by name and receives the value; it never sees, and cannot log, the reference or the
 * store it came from. That separation is what will let vault or SSO-backed credentials be
 * substituted later without touching a single connector.
 */
public interface ConnectorContext {

    /** Configuration for this connector instance, validated against its declared JSON Schema. */
    JsonNode config();

    /**
     * Resolves a secret by the logical name declared in {@code ConnectorSpec.secretFields}.
     *
     * @return the value, or empty if the connector instance did not configure it
     */
    Optional<String> secret(String name);

    /** Required variant. Fails fast at session open rather than at first use. */
    default String requireSecret(String name) {
        return secret(name).orElseThrow(() -> new ConnectorException(
                ConnectorException.Kind.CONFIGURATION,
                "Required secret '" + name + "' is not configured for this connector instance"));
    }

    /**
     * Identifies the worker holding this session.
     *
     * <p>Connectors that create external resources should include it in whatever name or tag the
     * external system allows, so an orphaned Salesforce job or Databricks statement can be traced
     * back to the process that created it.
     */
    String workerId();

    /** Identifies the run, for the same reason. */
    String runId();

    /**
     * Values this run was started with, for a source whose query is parameterised.
     *
     * <p>The mechanism exists because the interesting question is rarely "move this table" but
     * "move what changed since yesterday" — and the answer differs on every run, so it cannot live
     * in a connector instance that is written once.
     *
     * <p>Decided before the run starts and stored on it, so every chunk of a run sees the same
     * values and a retry hours later repeats the original range rather than a newly computed one.
     * A connector reads them; it never decides them.
     *
     * <p>Empty for a pipeline whose query has no placeholders, which is why this has a default:
     * the overwhelming majority of connectors never look.
     */
    default JsonNode parameters() {
        return com.dmp.common.json.Json.emptyObject();
    }

    /**
     * Index of the chunk this session serves, or {@link #NO_CHUNK} outside chunk execution — during
     * a connection test or run planning, where no chunk exists yet.
     *
     * <p>A sink that writes to a resource it names itself <b>must</b> include this. Sessions are
     * opened per chunk, so a name built only from the run and the worker collides across every
     * chunk of the run: each session then overwrites its predecessor's output while every write
     * reports success. Naming per chunk also means a chunk retried on a different worker returns to
     * the same resource rather than creating a second one beside it.
     */
    default int chunkIndex() {
        return NO_CHUNK;
    }

    /**
     * Whether this chunk already has committed progress, and is being picked up rather than started.
     *
     * <p>Happens on the ordinary path, not only on an explicit retry: a pod that dies mid-chunk
     * leaves a checkpoint another pod resumes from. A sink that overwrites its target on open has
     * to distinguish the two — overwriting on a resume discards what the earlier attempt durably
     * wrote, which the checkpoint has already accounted for and nothing will read again.
     */
    default boolean isResuming() {
        return false;
    }

    /** A logger already tagged with the connector type and run, so connector logs are attributable. */
    org.slf4j.Logger log();

    /** {@link #chunkIndex()} outside chunk execution. */
    int NO_CHUNK = -1;
}
