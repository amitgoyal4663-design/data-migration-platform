package com.dmp.connector.runtime;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Builds the {@link ConnectorContext} handed to a connector when a session opens.
 *
 * <p>Secret resolution happens here and nowhere else. A connector asks for a credential by name and
 * receives the value; it never sees the reference, the scheme, or which store answered. That is
 * what keeps a connector unable to log a vault path or leak a reference into an error message.
 *
 * <p><b>Ordinary configuration is resolved too, not only credentials.</b> The thing that varies
 * between a developer's laptop and a production cluster is rarely just the password — it is the
 * MongoDB connection string, the Kafka bootstrap servers, the JDBC URL, the Databricks workspace.
 * Those are owned by whoever runs the cluster, arrive as environment variables, and differ per
 * environment, so storing them as literals means a connector instance that only works in the place
 * it was created and has to be rebuilt by hand for every other one. Any configuration value may
 * therefore be written as a reference, and it is resolved here, on the worker, at session open.
 */
@Component
public class ConnectorContexts {

    private final Map<String, SecretsProvider> providersByScheme;

    public ConnectorContexts(List<SecretsProvider> providers) {
        this.providersByScheme = providers.stream()
                .collect(Collectors.toMap(SecretsProvider::scheme, Function.identity()));
    }

    /**
     * A context outside chunk execution — a connection test, or run planning. Reports
     * {@link ConnectorContext#NO_CHUNK}, because there is no chunk to name.
     */
    public ConnectorContext forInstance(ConnectorInstance instance, String runId, String workerId) {
        return forChunk(instance, runId, workerId, ConnectorContext.NO_CHUNK, false);
    }

    /** A context outside chunk execution, carrying the run's query parameters. */
    public ConnectorContext forInstance(ConnectorInstance instance, String runId, String workerId,
                                        JsonNode parameters) {
        return forInstance(instance, runId, workerId, parameters, null);
    }

    /** Outside a chunk — a connection test, a preview — using one of the named queries. */
    public ConnectorContext forInstance(ConnectorInstance instance, String runId, String workerId,
                                        JsonNode parameters, String queryName) {
        return forChunk(instance, runId, workerId, ConnectorContext.NO_CHUNK, false, parameters,
                queryName);
    }

    /**
     * The context a chunk's source and sink sessions are opened with.
     *
     * @param chunkIndex index within the run, so a sink can name what it creates per chunk instead
     *                   of colliding with every other chunk of the same run
     * @param resuming   whether this chunk already has committed progress
     */
    public ConnectorContext forChunk(ConnectorInstance instance, String runId, String workerId,
                                     int chunkIndex, boolean resuming) {
        return forChunk(instance, runId, workerId, chunkIndex, resuming, Json.emptyObject());
    }

    public ConnectorContext forChunk(ConnectorInstance instance, String runId, String workerId,
                                     int chunkIndex, boolean resuming, JsonNode parameters) {
        return forChunk(instance, runId, workerId, chunkIndex, resuming, parameters, null);
    }

    /**
     * The context a session is opened with, using one of the instance's named queries.
     *
     * <p>The variant is merged over the configuration <em>here</em>, before any connector sees it,
     * which is the whole reason this feature costs no connector changes: a connector receives a
     * configuration in exactly the shape it always received, with its {@code sql} or {@code filter}
     * being whichever one the run chose. It never learns that alternatives existed.
     */
    public ConnectorContext forChunk(ConnectorInstance instance, String runId, String workerId,
                                     int chunkIndex, boolean resuming, JsonNode parameters,
                                     String queryName) {
        Logger log = LoggerFactory.getLogger(
                "connector." + instance.connectorType() + "." + instance.name());

        // Resolved once, at session open, rather than on each config() call. A missing environment
        // variable then fails when the session opens — before a chunk claims work — instead of
        // halfway through whatever the connector happened to read first.
        JsonNode resolvedConfig = com.dmp.connector.api.QueryVariants.apply(
                resolveConfig(instance, log), queryName);

        return new ConnectorContext() {

            @Override
            public int chunkIndex() {
                return chunkIndex;
            }

            @Override
            public boolean isResuming() {
                return resuming;
            }

            @Override
            public JsonNode config() {
                return resolvedConfig;
            }

            @Override
            public JsonNode parameters() {
                return Json.orEmpty(parameters);
            }

            @Override
            public Optional<String> secret(String name) {
                JsonNode reference = instance.secretRefs().get(name);
                if (reference == null || !reference.isTextual()) {
                    return Optional.empty();
                }
                return resolve(instance.tenantId(), reference.asText());
            }

            @Override
            public String workerId() {
                return workerId;
            }

            @Override
            public String runId() {
                return runId;
            }

            @Override
            public org.slf4j.Logger log() {
                return log;
            }
        };
    }

    /**
     * Resolves {@code scheme:value}, defaulting to {@code env} when no scheme is given.
     *
     * <p>Deliberately returns empty rather than throwing when a scheme is unknown. The connector
     * decides whether that credential was required, and its error message names the field the user
     * actually configured — which is more useful than one naming a scheme they never typed.
     */
    /**
     * The scheme prefix of a value, if it has one — {@code env} in {@code env:MONGO_URI}.
     *
     * <p>Deliberately narrow. A value is treated as a reference only when its scheme names a
     * provider that is actually registered, which is what lets
     * {@code mongodb://localhost:27017/orders} and {@code https://example.com} pass through
     * untouched: nothing answers to {@code mongodb} or {@code https}, so they are literals.
     */
    private static final Pattern SCHEME = Pattern.compile("^([a-z][a-z0-9]*):(.+)$", Pattern.DOTALL);

    /**
     * A reference embedded inside a larger string, as in
     * {@code mongodb://${MONGO_HOST}:27017/orders}.
     *
     * <p>Offered alongside whole-value references because a great deal of real configuration is a
     * template with one moving part. Being unable to express that forces either a whole extra
     * environment variable for a string that is 90% constant, or giving up and hard-coding.
     */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_.-]*)}");

    /**
     * Replaces every reference in a connector instance's configuration with its value.
     *
     * <p>Walks the whole document, so it works for a field nested in an object or an array without
     * any connector knowing it happened — a connector reads {@code config().get("connectionString")}
     * and gets a connection string, exactly as it did when the value was stored literally.
     *
     * <p>What is <b>not</b> done here matters as much: an unresolved reference is never passed
     * through as its own literal text. Doing so would hand MongoDB the string
     * {@code "env:MONGO_URI"} and produce a parse error about an invalid connection string — an
     * error that sends whoever reads it looking at the connector instead of at the missing variable.
     */
    private JsonNode resolveConfig(ConnectorInstance instance, Logger log) {
        List<String> resolved = new ArrayList<>();
        JsonNode config = resolveNode(instance.tenantId(), instance.config(), "", resolved);

        if (!resolved.isEmpty()) {
            // Field names only. The values are the entire point of this mechanism and must not
            // reach a log line, and neither must the variable names, which map a deployment's
            // secret layout for anyone reading the logs.
            log.debug("Resolved {} configuration field(s) from references: {}",
                    resolved.size(), resolved);
        }
        return config;
    }

    private JsonNode resolveNode(TenantId tenantId, JsonNode node, String path, List<String> resolved) {
        if (node == null || node.isNull()) {
            return node;
        }
        if (node.isObject()) {
            ObjectNode copy = Json.newObject();
            for (Iterator<Map.Entry<String, JsonNode>> fields = node.fields(); fields.hasNext(); ) {
                Map.Entry<String, JsonNode> field = fields.next();
                copy.set(field.getKey(),
                        resolveNode(tenantId, field.getValue(),
                                path.isEmpty() ? field.getKey() : path + "." + field.getKey(),
                                resolved));
            }
            return copy;
        }
        if (node.isArray()) {
            ArrayNode copy = Json.mapper().createArrayNode();
            for (int i = 0; i < node.size(); i++) {
                copy.add(resolveNode(tenantId, node.get(i), path + "[" + i + "]", resolved));
            }
            return copy;
        }
        if (!node.isTextual()) {
            return node;
        }

        String original = node.asText();
        String value = resolveText(tenantId, original, path);
        if (!value.equals(original)) {
            resolved.add(path);
        }
        return Json.mapper().getNodeFactory().textNode(value);
    }

    private String resolveText(TenantId tenantId, String value, String path) {
        Matcher scheme = SCHEME.matcher(value);
        if (scheme.matches() && providersByScheme.containsKey(scheme.group(1))) {
            return require(tenantId, value, scheme.group(2), path);
        }

        Matcher placeholders = PLACEHOLDER.matcher(value);
        if (!placeholders.find()) {
            return value;
        }
        placeholders.reset();

        StringBuilder out = new StringBuilder();
        while (placeholders.find()) {
            String name = placeholders.group(1);
            placeholders.appendReplacement(out,
                    Matcher.quoteReplacement(require(tenantId, "env:" + name, name, path)));
        }
        placeholders.appendTail(out);
        return out.toString();
    }

    /**
     * Resolves a reference, or explains precisely what is missing and whose job it is.
     *
     * <p>The message names the field and the variable and says nothing about the value. On
     * Kubernetes this is the failure that actually happens — a deployment rolled without a
     * {@code secretRef}, or a variable renamed — and the person reading it needs to know whether to
     * fix the connector or to ask whoever manages the pod.
     */
    private String require(TenantId tenantId, String reference, String name, String path) {
        return resolve(tenantId, reference).orElseThrow(() -> new ConnectorException(
                ConnectorException.Kind.CONFIGURATION,
                "Configuration field '" + path + "' is set to the reference '" + reference
                        + "', but nothing in this deployment provides '" + name + "'. It is read "
                        + "from the process environment and from application configuration, so "
                        + "either it is not set on this pod — ask whoever manages the deployment's "
                        + "environment — or the name is misspelled here. The run is stopped rather "
                        + "than started with the reference itself as the value, which would fail "
                        + "later with an error about the connector rather than about this."));
    }

    private Optional<String> resolve(TenantId tenantId, String reference) {
        int separator = reference.indexOf(':');
        String scheme = separator > 0 ? reference.substring(0, separator) : "env";

        SecretsProvider provider = providersByScheme.get(scheme);
        if (provider == null) {
            LoggerFactory.getLogger(ConnectorContexts.class)
                    .warn("No secrets provider registered for scheme '{}'", scheme);
            return Optional.empty();
        }
        return provider.resolve(tenantId, reference);
    }
}
