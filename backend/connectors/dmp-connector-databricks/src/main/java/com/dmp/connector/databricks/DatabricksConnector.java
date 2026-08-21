package com.dmp.connector.databricks;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConfigFields;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Reads a Databricks SQL warehouse through the Statement Execution API.
 *
 * <p>The shape of this API is the reason the connector SPI has an asynchronous half. A statement is
 * submitted and returns immediately with an id; the warehouse may take minutes to start and hours
 * to run; the result is then fetched in chunks the warehouse decided on. Holding a worker thread
 * across that would waste a pod for the duration, so {@code prepare} submits, the engine polls
 * {@code checkPreparation}, and only then is anything read.
 *
 * <p><b>Chunks come from the warehouse, not from arithmetic.</b> Once a statement succeeds its
 * manifest describes exactly how the result set is divided and how many rows are in each piece.
 * Those pieces are grouped into the run's chunks, which means the division is deterministic, needs
 * no key column, needs no {@code COUNT(*)}, and cannot produce the wildly uneven chunks that
 * slicing a key range produces on skewed data. It is the best-informed split any source in this
 * platform gets.
 *
 * <p><b>Read-only, by design.</b> Writing to a warehouse means {@code INSERT} statements or a
 * {@code COPY INTO} over staged files, which is a different connector with different failure modes
 * and different idempotency guarantees. Declaring {@code SOURCE} is honest; declaring {@code BOTH}
 * and throwing from {@code openSink} would not be.
 */
public class DatabricksConnector implements Source {

    private static final String TYPE = "databricks";

    @Override
    public ConnectorSpec spec() {
        return new ConnectorSpec(
                TYPE,
                "Databricks SQL",
                "Runs a SQL query on a Databricks SQL warehouse and migrates its results. The "
                        + "statement is submitted asynchronously and polled, and the warehouse's "
                        + "own division of the result set becomes the run's chunks.",
                ConnectorSpec.Direction.SOURCE,
                configSchema(),
                Set.of("token", "clientId", "clientSecret"),
                "1.0.0");
    }

    /**
     * Reaches the workspace without running anything.
     *
     * <p>Fetching the warehouse proves the host resolves, the credential is accepted and the
     * warehouse id exists — the three things that are actually wrong when this fails. Running a
     * {@code SELECT 1} instead would prove marginally more and would start a stopped warehouse to
     * do it, so a button labelled "test connection" would silently begin billing.
     */
    @Override
    public void testConnection(ConnectorContext context) {
        DatabricksConfig config = DatabricksConfig.from(context);
        DatabricksSession session = open(config, context);

        JsonNode warehouse = session.getJson(config.warehouseUrl(), "read the SQL warehouse");
        String state = warehouse.path("state").asText("UNKNOWN");

        context.log().info("Databricks warehouse '{}' ({}) is {}",
                warehouse.path("name").asText(config.warehouseId()), config.warehouseId(), state);

        if ("DELETED".equalsIgnoreCase(state)) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "SQL warehouse " + config.warehouseId() + " exists but has been deleted. "
                            + "A run against it would fail.");
        }
        // A STOPPED warehouse is not an error. It starts on the first statement, which costs a few
        // minutes on the run rather than a failure here, and saying otherwise would push people
        // into leaving warehouses running to keep a test button green.
    }

    /** The {@code :placeholders} in the configured query, so the console can ask for them. */
    @Override
    public Set<String> parameterNames(JsonNode config) {
        JsonNode sql = config == null ? null : config.get("sql");
        return sql == null || sql.isNull()
                ? Set.of()
                : StatementParameters.referencedBy(sql.asText());
    }

    @Override
    public SourceSession openSource(ConnectorContext context) {
        DatabricksConfig config = DatabricksConfig.from(context);
        config.requireQuery();
        DatabricksSession session = open(config, context);

        return new SourceSession() {

            /**
             * Submits the statement and returns immediately.
             *
             * <p>{@code wait_timeout} is zero, which is what makes this asynchronous: the workspace
             * answers with an id rather than holding the connection open. The deadline is written
             * into the handle rather than kept in a field, because the handle is persisted and the
             * process that polls it may not be the process that submitted it.
             */
            @Override
            public Preparation prepare() {
                ObjectNode request = Json.newObject();
                request.put("statement", config.sql());
                request.put("warehouse_id", config.warehouseId());
                request.put("wait_timeout", "0s");
                request.put("disposition", config.disposition().name());
                request.put("format", "JSON_ARRAY");
                if (config.catalog() != null) {
                    request.put("catalog", config.catalog());
                }
                if (config.schema() != null) {
                    request.put("schema", config.schema());
                }
                if (config.rowLimit() > 0) {
                    request.put("row_limit", config.rowLimit());
                }

                // Values this run was started with, bound rather than pasted into the SQL. A query
                // with no :placeholders gets no parameters list and behaves exactly as before.
                ArrayNode parameters = StatementParameters.bind(config.sql(), context.parameters());
                if (parameters != null) {
                    request.set("parameters", parameters);
                    context.log().info("Databricks statement bound {} parameter(s): {}",
                            parameters.size(), StatementParameters.referencedBy(config.sql()));
                }

                JsonNode statement = session.postJson(config.statementsUrl(),
                        request.toString(), "submit the SQL statement");

                String statementId = statement.path("statement_id").asText(null);
                if (statementId == null) {
                    throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                            "Databricks accepted the statement but returned no statement_id: "
                                    + statement);
                }

                ObjectNode state = Json.newObject();
                state.put("statementId", statementId);
                state.put("deadline", Instant.now().plus(config.queryTimeout()).toEpochMilli());

                context.log().info("Databricks statement {} submitted to {}",
                        statementId, config.describe());
                return Preparation.of(state);
            }

            /**
             * Reports where the statement stands, and enforces the configured timeout.
             *
             * <p>The timeout is enforced here rather than being handed to Databricks. Its own
             * {@code wait_timeout} caps a <em>synchronous</em> wait at fifty seconds and has nothing
             * to say about a statement running asynchronously, so a query that will never finish
             * would otherwise be polled for as long as the run lived. Reaching the deadline cancels
             * the statement before failing, so the warehouse stops working on a result nobody will
             * read.
             */
            @Override
            public Preparation.Status checkPreparation(Preparation preparation) {
                String statementId = statementId(preparation.state());
                if (statementId == null) {
                    return Preparation.Status.ready();
                }

                JsonNode statement = session.getJson(
                        config.statementsUrl() + "/" + statementId, "check the SQL statement");
                String state = statement.path("status").path("state").asText("UNKNOWN");

                switch (state.toUpperCase(Locale.ROOT)) {
                    case "SUCCEEDED" -> {
                        return Preparation.Status.ready();
                    }
                    case "PENDING", "RUNNING" -> {
                        long deadline = preparation.state().path("deadline").asLong(0);
                        if (deadline > 0 && Instant.now().toEpochMilli() > deadline) {
                            cancel(session, config, statementId, context);
                            return Preparation.Status.failed(
                                    "The statement was still " + state.toLowerCase(Locale.ROOT)
                                            + " after " + config.queryTimeout().toSeconds()
                                            + "s, which is this connector instance's configured "
                                            + "query timeout, so it was cancelled. Either the query "
                                            + "needs longer than queryTimeoutSeconds allows, or the "
                                            + "warehouse is undersized for it.");
                        }
                        return Preparation.Status.pending(config.pollInterval());
                    }
                    default -> {
                        // FAILED, CANCELED, CLOSED. The workspace's own error is the useful part —
                        // it names the column that does not exist or the table that is not granted.
                        JsonNode error = statement.path("status").path("error");
                        String message = error.path("message").asText("");
                        String code = error.path("error_code").asText("");
                        return Preparation.Status.failed(
                                "Databricks statement " + statementId + " ended as " + state
                                        + (code.isBlank() ? "" : " (" + code + ")")
                                        + (message.isBlank() ? "" : ": " + message));
                    }
                }
            }

            /**
             * Turns the result manifest into chunks.
             *
             * <p>Databricks has already divided the result and says how many rows are in each
             * piece, so this groups consecutive pieces up to the engine's target size rather than
             * inventing boundaries. The spec carries <b>chunk indices, never links</b>: the
             * pre-signed URLs in an external-links result expire in minutes, and chunk four hundred
             * may be claimed long after planning. Indices are resolved to a fresh link at the
             * moment the chunk is read.
             */
            @Override
            public List<SplitSpec> plan(Preparation preparation, PlanRequest request) {
                String statementId = statementId(preparation.state());
                JsonNode manifest = session.getJson(
                        config.statementsUrl() + "/" + statementId, "read the result manifest")
                        .path("manifest");

                int totalChunks = manifest.path("total_chunk_count").asInt(0);
                long totalRows = manifest.path("total_row_count").asLong(-1);

                if (manifest.path("truncated").asBoolean(false)) {
                    context.log().warn("Databricks truncated the result of statement {} — the run "
                            + "will migrate {} row(s), which is not the whole query. This happens "
                            + "when rowLimit is set or the result exceeded the workspace's byte "
                            + "limit.", statementId, totalRows);
                }
                if (totalChunks <= 0 || totalRows == 0) {
                    context.log().info("Databricks statement {} returned no rows", statementId);
                    return List.of();
                }

                List<int[]> groups = group(manifest, totalChunks, totalRows, request);
                List<SplitSpec> specs = new ArrayList<>(groups.size());

                for (int i = 0; i < groups.size(); i++) {
                    int[] group = groups.get(i);
                    ObjectNode spec = Json.newObject();
                    spec.put("statementId", statementId);
                    spec.put("fromChunk", group[0]);
                    spec.put("toChunk", group[1]);
                    specs.add(new SplitSpec(i, spec, group[0] == group[1]
                            ? "result chunk " + group[0]
                            : "result chunks " + group[0] + "–" + group[1]));
                }

                context.log().info("Databricks statement {} returned {} row(s) in {} result "
                                + "chunk(s), planned into {} chunk(s)",
                        statementId, totalRows, totalChunks, specs.size());
                return specs;
            }

            @Override
            public RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize) {
                return new ResultStream(session, config, split.spec(), fromCursor);
            }

            /**
             * Cancels the statement so the warehouse stops working on a result nobody will read.
             *
             * <p>Note that the engine does not currently call this for sources — only for sinks —
             * so today it runs only from the timeout path above. It is implemented rather than
             * omitted because the cost is nothing and the alternative is a connector that becomes
             * wrong the moment that gap is closed.
             */
            @Override
            public void release(Preparation preparation) {
                cancel(session, config, statementId(preparation.state()), context);
            }
        };
    }

    /**
     * Groups the warehouse's result chunks into the run's chunks.
     *
     * <p>The target size is raised when honouring it would plan more chunks than the run allows,
     * because the ceiling is a safety limit and quietly planning a hundred thousand chunks to
     * respect a size hint gets that backwards.
     */
    private static List<int[]> group(JsonNode manifest, int totalChunks, long totalRows,
                                     Source.PlanRequest request) {
        long target = request.targetRowsPerChunk();
        if (totalRows > 0 && request.maxChunks() > 0) {
            long minimum = (totalRows + request.maxChunks() - 1) / request.maxChunks();
            target = Math.max(target, minimum);
        }

        JsonNode chunks = manifest.path("chunks");
        List<int[]> groups = new ArrayList<>();

        // Without per-chunk row counts there is nothing to group by, so each result chunk becomes
        // one chunk of the run. Correct, just less well balanced.
        if (!chunks.isArray() || chunks.isEmpty()) {
            for (int i = 0; i < totalChunks; i++) {
                groups.add(new int[]{i, i});
            }
            return groups;
        }

        int start = -1;
        long rows = 0;
        for (JsonNode chunk : chunks) {
            int index = chunk.path("chunk_index").asInt();
            if (start < 0) {
                start = index;
            }
            rows += chunk.path("row_count").asLong(0);

            if (rows >= target) {
                groups.add(new int[]{start, index});
                start = -1;
                rows = 0;
            }
        }
        if (start >= 0) {
            groups.add(new int[]{start, chunks.get(chunks.size() - 1).path("chunk_index").asInt()});
        }
        return groups;
    }

    /**
     * Walks one chunk's slice of the result.
     *
     * <p>Links are resolved one result chunk at a time, immediately before it is downloaded. The
     * temptation is to resolve them all at planning time and store them — and it would work in
     * every test, because in a test the read happens a second later. In production chunk four
     * hundred is claimed forty minutes after planning, by which time every stored link has expired.
     */
    private static final class ResultStream implements Source.RecordStream {

        private final DatabricksSession session;
        private final DatabricksConfig config;
        private final String statementId;
        private final int toChunk;

        private List<String> columns = List.of();
        private List<String> types = List.of();

        private Iterator<JsonNode> rows = List.<JsonNode>of().iterator();
        /** The result chunk the iterator belongs to; what a resume position names. */
        private int currentChunk;
        /** The next result chunk to download. */
        private int nextChunk;
        /** Rows consumed from {@link #currentChunk}, which is the second half of the position. */
        private long consumed;
        /** Rows to drop when the resumed chunk is first downloaded. */
        private long skip;
        private long emitted;

        ResultStream(DatabricksSession session, DatabricksConfig config, JsonNode spec,
                     JsonNode fromCursor) {
            this.session = session;
            this.config = config;
            this.statementId = spec.path("statementId").asText(null);
            this.toChunk = spec.path("toChunk").asInt();

            int fromChunk = spec.path("fromChunk").asInt();
            if (fromCursor != null && fromCursor.hasNonNull("chunk")) {
                this.nextChunk = fromCursor.get("chunk").asInt();
                this.skip = fromCursor.path("row").asLong(0);
                this.emitted = fromCursor.path("emitted").asLong(0);
            } else {
                this.nextChunk = fromChunk;
            }
            this.currentChunk = this.nextChunk;

            if (statementId == null) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "This chunk has no Databricks statement id in it, so there is nothing to "
                                + "read. It was planned by an older version of this connector, or "
                                + "the run's plan was written by a different source.");
            }
        }

        @Override
        public DataRecord next() {
            while (!rows.hasNext()) {
                if (nextChunk > toChunk) {
                    return null;
                }
                fetchChunk();
            }

            JsonNode row = rows.next();
            consumed++;
            emitted++;
            return toRecord(row);
        }

        private void fetchChunk() {
            currentChunk = nextChunk++;

            JsonNode chunk = session.getJson(config.statementsUrl() + "/" + statementId
                    + "/result/chunks/" + currentChunk, "read result chunk " + currentChunk);

            if (columns.isEmpty()) {
                loadSchema();
            }

            List<JsonNode> data = rowsOf(chunk);

            if (skip > 0) {
                if (skip >= data.size()) {
                    // The resume position sat at the end of this chunk, so all of it was already
                    // written. Nothing to emit; the next chunk continues.
                    skip -= data.size();
                    consumed = data.size();
                    rows = List.<JsonNode>of().iterator();
                    return;
                }
                consumed = skip;
                data = data.subList((int) skip, data.size());
                skip = 0;
            } else {
                consumed = 0;
            }
            rows = data.iterator();
        }

        /**
         * Reads the rows out of a chunk response, following the external link when there is one.
         *
         * <p>Inline results carry {@code data_array} directly. External-links results carry a
         * pre-signed URL to the same array in cloud storage, fetched without the workspace
         * credential.
         */
        private List<JsonNode> rowsOf(JsonNode chunk) {
            JsonNode inline = chunk.path("data_array");
            if (inline.isArray()) {
                return toList(inline);
            }

            JsonNode links = chunk.path("external_links");
            if (!links.isArray() || links.isEmpty()) {
                // Not an error worth failing on: a chunk with neither is an empty chunk.
                return List.of();
            }

            List<JsonNode> all = new ArrayList<>();
            for (JsonNode link : links) {
                String url = link.path("external_link").asText(null);
                if (url == null) {
                    continue;
                }
                JsonNode downloaded = session.getExternal(url,
                        "read result chunk " + currentChunk + " of statement " + statementId);
                if (downloaded.isArray()) {
                    all.addAll(toList(downloaded));
                }
            }
            return all;
        }

        private void loadSchema() {
            JsonNode schema = session.getJson(config.statementsUrl() + "/" + statementId,
                    "read the result schema").path("manifest").path("schema").path("columns");

            List<String> names = new ArrayList<>();
            List<String> typeNames = new ArrayList<>();
            for (JsonNode column : schema) {
                names.add(column.path("name").asText());
                typeNames.add(column.path("type_name").asText("STRING"));
            }
            this.columns = List.copyOf(names);
            this.types = List.copyOf(typeNames);
        }

        private DataRecord toRecord(JsonNode row) {
            ObjectNode payload = Json.newObject();
            for (int i = 0; i < columns.size(); i++) {
                JsonNode value = i < row.size() ? row.get(i) : null;
                String raw = value == null || value.isNull() ? null : value.asText();
                payload.set(columns.get(i), config.typedValues()
                        ? convert(raw, types.get(i))
                        : Json.mapper().getNodeFactory().textNode(raw));
            }

            String key = null;
            if (config.keyColumn() != null) {
                JsonNode value = payload.get(config.keyColumn());
                key = value == null || value.isNull() ? null : value.asText();
            }
            return DataRecord.of(payload, key, emitted);
        }

        /**
         * Applies the column's declared type to a value.
         *
         * <p>JSON_ARRAY hands back every value as a string, including numbers. Passing that through
         * would send {@code "5001.0"} where a number was meant and quietly turn every decimal into
         * text in the destination — which is exactly the class of bug that only shows up after the
         * migration, in the system nobody is watching.
         *
         * <p>{@code DECIMAL} becomes a {@code BigDecimal} rather than a double on purpose. A price
         * or a balance that has survived the warehouse intact must not lose its last digit here.
         *
         * <p>An unparseable value falls back to text rather than failing. If Databricks says a
         * column is an INT and a value is not one, the honest thing is to carry it through and let
         * the destination refuse it by name, not to abandon a chunk of a million rows.
         */
        private static JsonNode convert(String raw, String typeName) {
            var nodes = Json.mapper().getNodeFactory();
            if (raw == null) {
                return nodes.nullNode();
            }
            try {
                return switch (typeName == null ? "STRING" : typeName.toUpperCase(Locale.ROOT)) {
                    case "BOOLEAN" -> nodes.booleanNode(Boolean.parseBoolean(raw));
                    case "BYTE", "SHORT", "INT", "LONG" -> nodes.numberNode(Long.parseLong(raw));
                    case "FLOAT", "DOUBLE" -> nodes.numberNode(Double.parseDouble(raw));
                    case "DECIMAL" -> nodes.numberNode(new BigDecimal(raw));
                    // Complex types arrive as JSON text; parsing them keeps them addressable by a
                    // transform script instead of arriving as an opaque blob.
                    case "ARRAY", "MAP", "STRUCT" -> Json.mapper().readTree(raw);
                    default -> nodes.textNode(raw);
                };
            } catch (Exception e) {
                return nodes.textNode(raw);
            }
        }

        private static List<JsonNode> toList(JsonNode array) {
            List<JsonNode> values = new ArrayList<>(array.size());
            array.forEach(values::add);
            return values;
        }

        @Override
        public JsonNode cursor() {
            ObjectNode cursor = Json.newObject();
            cursor.put("chunk", currentChunk);
            cursor.put("row", consumed);
            cursor.put("emitted", emitted);
            return cursor;
        }

        @Override
        public void close() {
            // The JDK client pools connections itself; there is nothing per-stream to release.
        }
    }

    // ------------------------------------------------------------------ shared

    private static DatabricksSession open(DatabricksConfig config, ConnectorContext context) {
        DatabricksSession session = new DatabricksSession(config,
                context.secret("token").orElse(null),
                context.secret("clientId").orElse(null),
                context.secret("clientSecret").orElse(null));
        session.authenticate();
        return session;
    }

    private static String statementId(JsonNode state) {
        String id = state.path("statementId").asText(null);
        return id == null || id.isBlank() ? null : id;
    }

    /**
     * Asks the warehouse to stop, and never throws.
     *
     * <p>Called from cleanup paths, where the run has already decided what it is doing. A workspace
     * that will not accept the cancellation must not turn a clear failure into a confusing one.
     */
    private static void cancel(DatabricksSession session, DatabricksConfig config,
                               String statementId, ConnectorContext context) {
        if (statementId == null) {
            return;
        }
        try {
            session.postJson(config.statementsUrl() + "/" + statementId + "/cancel", "{}",
                    "cancel the SQL statement");
            context.log().info("Databricks statement {} cancelled", statementId);
        } catch (RuntimeException e) {
            context.log().debug("Could not cancel Databricks statement {}: {}",
                    statementId, e.getMessage());
        }
    }

    private static JsonNode configSchema() {
        ObjectNode properties = Json.newObject();
        properties.set("host", ConfigFields.fromEnvironment(ConfigFields.field("string",
                "Workspace URL, e.g. https://adb-1234567890.5.azuredatabricks.net. Different in "
                        + "every environment, so name the variable rather than typing it.")));
        properties.set("warehouseId", ConfigFields.fromEnvironment(ConfigFields.field("string",
                "SQL warehouse the statement runs on — the last path segment of its URL. Each "
                        + "environment has its own, so this is usually supplied alongside the "
                        + "host.")));
        properties.set("sql", ConfigFields.sourceField("string",
                "The query whose results are migrated, e.g. SELECT * FROM main.sales.orders."));

        properties.set("catalog", ConfigFields.advanced(ConfigFields.sourceField("string",
                "Catalog the query resolves unqualified names against. Optional.")));
        properties.set("schema", ConfigFields.advanced(ConfigFields.sourceField("string",
                "Schema within that catalog. Optional.")));
        properties.set("authMethod", ConfigFields.advanced(ConfigFields.enumField(
                "Personal access token, or a service principal's OAuth client credentials.",
                "TOKEN", "OAUTH")));
        properties.set("queryTimeoutSeconds", ConfigFields.advanced(ConfigFields.sourceField("integer",
                "How long to wait for the statement to finish before cancelling it and failing "
                        + "the run. Defaults to 3600. Include warehouse start-up time: a stopped "
                        + "warehouse takes minutes before the query begins.")));
        properties.set("pollSeconds", ConfigFields.advanced(ConfigFields.sourceField("integer",
                "How often the statement's status is checked while it runs. Defaults to 5.")));
        properties.set("disposition", ConfigFields.advanced(ConfigFields.sourceEnumField(
                "EXTERNAL_LINKS stages results to cloud storage and has no size ceiling. INLINE "
                        + "returns them in the response and is capped at 25 MiB in total.",
                "EXTERNAL_LINKS", "INLINE")));
        properties.set("rowLimit", ConfigFields.advanced(ConfigFields.sourceField("integer",
                "Cap on rows the query returns. 0 means no cap. Useful for a trial run.")));
        properties.set("typedValues", ConfigFields.advanced(ConfigFields.sourceField("boolean",
                "Convert values using the result's declared column types, so numbers arrive as "
                        + "numbers rather than as strings. Defaults to true.")));
        properties.set("keyColumn", ConfigFields.recordKeyField(
                ConfigFields.advanced(ConfigFields.sourceField("string",
                        "Column holding each record's natural key, used for indexing and upserts.")),
                null));

        ObjectNode schema = Json.newObject();
        schema.put("type", "object");
        schema.set("properties", properties);
        ArrayNode required = Json.mapper().createArrayNode();
        required.add("host");
        required.add("warehouseId");
        schema.set("required", required);
        return schema;
    }
}
