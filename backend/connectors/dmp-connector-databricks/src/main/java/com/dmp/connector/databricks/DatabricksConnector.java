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
    public java.util.Set<String> listParameterNames(JsonNode config) {
        // A placeholder inside IN (...) takes a list; anywhere else it takes one value. The query
        // is the only thing that knows, and it says so plainly.
        return com.dmp.connector.api.QueryParameters.listsIn(config.path("sql").asText(null));
    }

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
                // Nothing to prepare when each chunk runs its own query. There is no shared
                // statement to submit, nothing for the run to wait on before dividing the work,
                // and so no preparation phase at all — the run goes straight to planning.
                if (config.chunkMode() == DatabricksConfig.ChunkMode.OFFSET) {
                    return Preparation.none();
                }

                ObjectNode request = Json.newObject();
                // Expanded first: a list parameter becomes one marker per value, so the
                // statement submitted is not quite the statement configured.
                StatementParameters.Bound bound =
                        StatementParameters.expand(config.sql(), context.parameters());
                request.put("statement", bound.sql());
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
                ArrayNode parameters = bound.parameters();
                if (parameters != null) {
                    request.set("parameters", parameters);
                    context.log().info("Databricks statement bound {} parameter(s): {}",
                            parameters.size(), StatementParameters.referencedBy(bound.sql()));
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
                if (config.chunkMode() == DatabricksConfig.ChunkMode.OFFSET) {
                    return planByOffset(session, config, context, request);
                }
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
                    // The manifest counted these rows, so this chunk is not an estimate: the
                    // warehouse has already produced the result and said how big each piece of it
                    // is. That is what lets the engine make the batch the whole chunk instead of
                    // falling back to whatever the destination prefers.
                    specs.add(new SplitSpec(i, spec, group[0] == group[1]
                            ? "result chunk " + group[0]
                            : "result chunks " + group[0] + "–" + group[1],
                            rowsIn(manifest, group[0], group[1])));
                }

                context.log().info("Databricks statement {} returned {} row(s) in {} result "
                                + "chunk(s), planned into {} chunk(s)",
                        statementId, totalRows, totalChunks, specs.size());
                return specs;
            }

            @Override
            public RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize) {
                if (config.chunkMode() == DatabricksConfig.ChunkMode.OFFSET) {
                    return new OffsetStream(session, config, context, split.spec(), fromCursor);
                }
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
     * Rows the manifest attributes to a range of result chunks, or 0 if it does not say.
     *
     * <p>Zero rather than a guess. A chunk claiming a size it does not have would size the batch
     * wrongly, and "I do not know" already has a meaning the engine handles — it falls back to the
     * destination's preference, exactly as before this existed.
     */
    private static long rowsIn(JsonNode manifest, int fromChunk, int toChunk) {
        JsonNode chunks = manifest.path("chunks");
        if (!chunks.isArray()) {
            return 0;
        }
        long rows = 0;
        for (JsonNode chunk : chunks) {
            int index = chunk.path("chunk_index").asInt();
            if (index >= fromChunk && index <= toChunk) {
                if (!chunk.hasNonNull("row_count")) {
                    return 0;
                }
                rows += chunk.path("row_count").asLong(0);
            }
        }
        return rows;
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

        /**
         * Calls made since the engine last collected them.
         *
         * <p>A plain list, not a concurrent queue: a stream belongs to one chunk and one thread,
         * and the engine drains it between records rather than alongside them.
         */
        private final List<Source.Fetch> fetches = new ArrayList<>();

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

            String url = config.statementsUrl() + "/" + statementId
                    + "/result/chunks/" + currentChunk;
            String reason = "read result chunk " + currentChunk;
            Instant startedAt = Instant.now();
            JsonNode chunk;
            try {
                chunk = session.getJson(url, reason);
            } catch (RuntimeException e) {
                fetches.add(Source.Fetch.failed(reason, url, startedAt, millisSince(startedAt),
                        e instanceof ConnectorException connector
                                ? connector.kind().name()
                                : e.getClass().getSimpleName(),
                        e.getMessage()));
                throw e;
            }

            if (columns.isEmpty()) {
                loadSchema();
            }

            List<JsonNode> data = rowsOf(chunk);

            // One call, whatever the engine goes on to do with the rows. This is the whole reason
            // the SPI has drainFetches: the warehouse was asked once for this many rows, and no
            // amount of batching downstream changes that number.
            fetches.add(Source.Fetch.ok(reason, url, startedAt, millisSince(startedAt),
                    data.size(), 0));

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
                // Reported separately, and deliberately not by its URL: a pre-signed link carries
                // its own credential in the query string, and this string is written to a search
                // index. The chunk it belongs to is what somebody actually needs to see.
                Instant startedAt = Instant.now();
                JsonNode downloaded = session.getExternal(url,
                        "read result chunk " + currentChunk + " of statement " + statementId);
                int rows = downloaded.isArray() ? downloaded.size() : 0;
                fetches.add(Source.Fetch.ok(
                        "follow the external link for result chunk " + currentChunk,
                        // Deliberately not the URL: a pre-signed link carries its own credential
                        // in the query string, and this is written to a search index.
                        "external link for result chunk " + currentChunk,
                        startedAt, millisSince(startedAt), rows, 0));
                if (downloaded.isArray()) {
                    all.addAll(toList(downloaded));
                }
            }
            return all;
        }

        private void loadSchema() {
            String url = config.statementsUrl() + "/" + statementId;
            Instant startedAt = Instant.now();
            JsonNode schema = session.getJson(url, "read the result schema")
                    .path("manifest").path("schema").path("columns");
            // A call that returns no rows is still a call. Left in because a schema lookup that
            // starts taking two seconds is invisible in the read window it hides inside — and
            // because a reader seeing two fetches against one chunk deserves to know that one of
            // them fetched column names rather than rows.
            fetches.add(Source.Fetch.ok("read the column names and types", url,
                    startedAt, millisSince(startedAt), 0, 0));

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
            return DatabricksConnector.toRecord(row, columns, types, config, emitted);
        }

        private static List<JsonNode> toList(JsonNode array) {
            List<JsonNode> values = new ArrayList<>(array.size());
            array.forEach(values::add);
            return values;
        }

        @Override
        public List<Source.Fetch> drainFetches() {
            if (fetches.isEmpty()) {
                return List.of();
            }
            List<Source.Fetch> drained = List.copyOf(fetches);
            fetches.clear();
            return drained;
        }

        private static long millisSince(Instant startedAt) {
            return java.time.Duration.between(startedAt, Instant.now()).toMillis();
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



    /**
     * One chunk, one statement of its own.
     *
     * <p>Submits {@code … ORDER BY … LIMIT n OFFSET m}, waits for it, reads the rows, and is done.
     * Nothing survives the chunk: no shared result, nothing left running on the warehouse, nothing
     * for another chunk to depend on. A chunk retried an hour later simply runs its query again.
     *
     * <p>Every call it makes is reported through {@link #drainFetches()} — the submission, each
     * poll while the warehouse is still working, and the download. That is what puts "asked three
     * times, still PENDING, then a thousand rows" on the chunk's own timeline rather than in a
     * server log.
     */
    private static final class OffsetStream implements Source.RecordStream {

        private final DatabricksSession session;
        private final DatabricksConfig config;
        private final ConnectorContext context;
        private final long offset;
        private final long limit;
        private final List<Source.Fetch> fetches = new ArrayList<>();

        private List<String> columns = List.of();
        private List<String> types = List.of();
        private Iterator<JsonNode> rows;
        private long emitted;
        private String sql;

        OffsetStream(DatabricksSession session, DatabricksConfig config, ConnectorContext context,
                     JsonNode spec, JsonNode fromCursor) {
            this.session = session;
            this.config = config;
            this.context = context;
            this.offset = spec.path("offset").asLong(0);
            this.limit = spec.path("limit").asLong(0);
            // Rows already handed over on a previous attempt are skipped rather than re-read. The
            // query is the same either way; only how much of it is emitted changes.
            this.emitted = fromCursor == null ? 0 : fromCursor.path("emitted").asLong(0);
        }

        @Override
        public DataRecord next() {
            if (rows == null) {
                fetch();
            }
            if (!rows.hasNext()) {
                return null;
            }
            emitted++;
            return toRecord(rows.next(), columns, types, config, emitted);
        }

        /** Submits this chunk's query, waits for it, and keeps the rows. */
        private void fetch() {
            // ORDER BY inside the subquery would be discarded by most engines; it belongs on the
            // outer statement, where it decides what OFFSET actually means.
            StatementParameters.Bound bound =
                    StatementParameters.expand(config.sql(), context.parameters());
            sql = "SELECT * FROM (" + bound.sql() + ") ORDER BY " + config.orderBy()
                    + " LIMIT " + limit + " OFFSET " + offset;

            Instant submitted = Instant.now();
            // The submission waits for the query, so a slice of a table that is already there
            // comes back in this one request with its rows attached. CONTINUE rather than CANCEL:
            // a query that outlasts the wait keeps running and is polled, instead of being thrown
            // away and started again.
            boolean waits = !config.waitTimeout().isZero();
            JsonNode submitResponse;
            String statementId;
            try {
                ObjectNode request = Json.newObject();
                request.put("statement", sql);
                request.put("warehouse_id", config.warehouseId());
                request.put("wait_timeout", config.waitTimeout().toSeconds() + "s");
                if (waits) {
                    request.put("on_wait_timeout", "CONTINUE");
                }
                request.put("disposition", "INLINE");
                request.put("format", "JSON_ARRAY");
                if (config.catalog() != null) {
                    request.put("catalog", config.catalog());
                }
                if (config.schema() != null) {
                    request.put("schema", config.schema());
                }
                ArrayNode parameters = bound.parameters();
                if (parameters != null) {
                    request.set("parameters", parameters);
                }
                submitResponse = session.postJson(config.statementsUrl(), request.toString(),
                        "run this chunk's query");
                statementId = submitResponse.path("statement_id").asText(null);
            } catch (RuntimeException e) {
                fetches.add(Source.Fetch.failed("run this chunk's query", sql, submitted,
                        millisSince(submitted), kindOf(e), e.getMessage()));
                throw e;
            }

            String state = submitResponse.path("status").path("state").asText("UNKNOWN")
                    .toUpperCase(Locale.ROOT);
            JsonNode done;
            if ("SUCCEEDED".equals(state)) {
                // The whole thing in one call, which is the point of the wait.
                long rowCount = submitResponse.path("manifest").path("total_row_count").asLong(0);
                fetches.add(Source.Fetch.ok("run this chunk's query", sql, submitted,
                        millisSince(submitted), rowCount, 0));
                done = submitResponse;
            } else if (!"PENDING".equals(state) && !"RUNNING".equals(state)) {
                String message = submitResponse.path("status").path("error").path("message")
                        .asText("");
                fetches.add(Source.Fetch.failed("this chunk's query ended as " + state, sql,
                        submitted, millisSince(submitted), state, message));
                throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                        "Databricks statement " + statementId + " for this chunk ended as "
                                + state + ": " + message);
            } else {
                // Outlasted the wait. It is still running, so it is polled — the behaviour a
                // long query has always had, now reached only by long queries.
                fetches.add(Source.Fetch.ok(
                        waits
                                ? "the query outlasted the " + config.waitTimeout().toSeconds()
                                        + "s wait and is still " + state
                                : "submitted; the query will be polled",
                        sql, submitted, millisSince(submitted), 0, 0));
                done = awaitReportingEachPoll(statementId);
            }

            JsonNode manifest = done.path("manifest").path("schema").path("columns");
            List<String> names = new ArrayList<>();
            List<String> typeNames = new ArrayList<>();
            for (JsonNode column : manifest) {
                names.add(column.path("name").asText());
                typeNames.add(column.path("type_name").asText("STRING"));
            }
            this.columns = List.copyOf(names);
            this.types = List.copyOf(typeNames);

            List<JsonNode> data = new ArrayList<>();
            done.path("result").path("data_array").forEach(data::add);
            // What a resumed attempt already handed over. Skipped here rather than re-read,
            // because the destination has them.
            if (emitted > 0 && emitted < data.size()) {
                data = data.subList((int) emitted, data.size());
            } else if (emitted >= data.size()) {
                data = List.of();
            }
            this.rows = data.iterator();
        }

        /** Waits for this chunk's statement, recording every question it asks. */
        private JsonNode awaitReportingEachPoll(String statementId) {
            Instant deadline = Instant.now().plus(config.queryTimeout());
            String url = config.statementsUrl() + "/" + statementId;

            while (true) {
                Instant asked = Instant.now();
                JsonNode statement = session.getJson(url, "check this chunk's query");
                String state = statement.path("status").path("state").asText("UNKNOWN")
                        .toUpperCase(Locale.ROOT);

                if ("SUCCEEDED".equals(state)) {
                    long rowCount = statement.path("manifest").path("total_row_count").asLong(0);
                    fetches.add(Source.Fetch.ok("the query finished — reading its rows", url,
                            asked, millisSince(asked), rowCount, 0));
                    return statement;
                }
                if (!"PENDING".equals(state) && !"RUNNING".equals(state)) {
                    String message = statement.path("status").path("error").path("message")
                            .asText("");
                    fetches.add(Source.Fetch.failed("this chunk's query ended as " + state, url,
                            asked, millisSince(asked), state, message));
                    throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                            "Databricks statement " + statementId + " for this chunk ended as "
                                    + state + ": " + message);
                }

                fetches.add(Source.Fetch.ok(
                        "the query is " + state + " — the warehouse has not finished it",
                        url, asked, millisSince(asked), 0, 0));

                if (Instant.now().isAfter(deadline)) {
                    cancel(session, config, statementId, context);
                    throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                            "This chunk's query was still " + state.toLowerCase(Locale.ROOT)
                                    + " after " + config.queryTimeout().toSeconds()
                                    + "s and was cancelled.");
                }
                try {
                    Thread.sleep(config.pollInterval().toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                            "Interrupted while waiting for this chunk's query", e);
                }
            }
        }

        @Override
        public List<Source.Fetch> drainFetches() {
            if (fetches.isEmpty()) {
                return List.of();
            }
            List<Source.Fetch> drained = List.copyOf(fetches);
            fetches.clear();
            return drained;
        }

        @Override
        public JsonNode cursor() {
            ObjectNode cursor = Json.newObject();
            cursor.put("emitted", emitted);
            return cursor;
        }

        @Override
        public String describe() {
            return sql;
        }

        @Override
        public void close() {
        }

        private static String kindOf(RuntimeException e) {
            return e instanceof ConnectorException connector
                    ? connector.kind().name()
                    : e.getClass().getSimpleName();
        }

        private static long millisSince(Instant startedAt) {
            return java.time.Duration.between(startedAt, Instant.now()).toMillis();
        }
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
    private static JsonNode convertValue(String raw, String typeName) {
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

    /**
     * One row of a JSON_ARRAY result, as a record.
     *
     * <p>The wire format is positional — an array of values with no names — so the column list
     * from the manifest is what makes it a document. Shared by both chunking modes: a row is a row
     * whichever statement produced it.
     */
    private static DataRecord toRecord(JsonNode row, List<String> columns, List<String> types,
                                       DatabricksConfig config, long seq) {
        ObjectNode payload = Json.newObject();
        for (int i = 0; i < columns.size(); i++) {
            JsonNode value = i < row.size() ? row.get(i) : null;
            String raw = value == null || value.isNull() ? null : value.asText();
            payload.set(columns.get(i), config.typedValues()
                    ? convertValue(raw, types.get(i))
                    : Json.mapper().getNodeFactory().textNode(raw));
        }

        String key = null;
        if (config.keyColumn() != null) {
            JsonNode value = payload.get(config.keyColumn());
            key = value == null || value.isNull() ? null : value.asText();
        }
        return DataRecord.of(payload, key, seq);
    }

    // ------------------------------------------------------- a statement per chunk

    /**
     * Divides the work by row offset, without leaving anything running on the warehouse.
     *
     * <p>One {@code COUNT(*)} to learn how many rows there are, then arithmetic. That count is a
     * cheap aggregate rather than the migration's own query, and once it returns the warehouse is
     * holding nothing on this run's behalf — which is the whole point of this mode. A chunk claimed
     * an hour later runs its own query and neither knows nor cares what the others did.
     */
    private static List<Source.SplitSpec> planByOffset(DatabricksSession session,
                                                       DatabricksConfig config,
                                                       ConnectorContext context,
                                                       Source.PlanRequest request) {
        if (config.orderBy() == null || config.orderBy().isBlank()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "chunkMode OFFSET needs 'orderBy' — a column, or columns, that put the rows in "
                            + "a total order. An OFFSET without one is not reproducible: the same "
                            + "query may return rows in a different sequence each time it runs, so "
                            + "two chunks could contain the same row while another row is read by "
                            + "nobody.");
        }

        long total = runScalar(session, config, context,
                "SELECT COUNT(*) FROM (" + config.sql() + ")", "count the rows to be migrated");
        if (config.rowLimit() > 0) {
            total = Math.min(total, config.rowLimit());
        }
        if (total <= 0) {
            context.log().info("Databricks source matched no rows");
            return List.of();
        }

        long perChunk = Math.max(1, request.targetRowsPerChunk());
        // The ceiling is a safety limit, so a small chunk size raises the size rather than quietly
        // planning a hundred thousand chunks.
        long chunks = (total + perChunk - 1) / perChunk;
        if (request.maxChunks() > 0 && chunks > request.maxChunks()) {
            perChunk = (total + request.maxChunks() - 1) / request.maxChunks();
            chunks = (total + perChunk - 1) / perChunk;
        }

        List<Source.SplitSpec> specs = new ArrayList<>((int) chunks);
        for (int i = 0; i < chunks; i++) {
            long offset = i * perChunk;
            long limit = Math.min(perChunk, total - offset);
            ObjectNode spec = Json.newObject();
            spec.put("offset", offset);
            spec.put("limit", limit);
            specs.add(new Source.SplitSpec(i, spec,
                    "rows " + offset + "–" + (offset + limit - 1), limit));
        }

        context.log().info("Databricks source has {} row(s), planned into {} chunk(s) of {} — "
                        + "one statement each",
                total, specs.size(), perChunk);
        return specs;
    }

    /**
     * Runs a statement that returns exactly one number, and waits for it.
     *
     * <p>Blocking, unlike the migration's own query, and deliberately: this is a count on a
     * warehouse that is already awake, it happens once per run, and giving it the whole
     * asynchronous apparatus would buy nothing.
     */
    private static long runScalar(DatabricksSession session, DatabricksConfig config,
                                  ConnectorContext context, String sql, String purpose) {
        ObjectNode request = Json.newObject();
        request.put("warehouse_id", config.warehouseId());
        request.put("wait_timeout", "0s");
        request.put("disposition", "INLINE");
        request.put("format", "JSON_ARRAY");
        if (config.catalog() != null) {
            request.put("catalog", config.catalog());
        }
        if (config.schema() != null) {
            request.put("schema", config.schema());
        }
        // The caller has already wrapped config.sql(), so expansion happens on the whole
        // statement — the placeholders are inside the subquery either way.
        StatementParameters.Bound bound = StatementParameters.expand(sql, context.parameters());
        request.put("statement", bound.sql());
        ArrayNode parameters = bound.parameters();
        if (parameters != null) {
            request.set("parameters", parameters);
        }

        String statementId = session.postJson(config.statementsUrl(), request.toString(), purpose)
                .path("statement_id").asText(null);
        if (statementId == null) {
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Databricks accepted the statement to " + purpose + " but returned no id");
        }

        JsonNode done = awaitStatement(session, config, statementId, purpose);
        JsonNode rows = done.path("result").path("data_array");
        if (!rows.isArray() || rows.isEmpty() || !rows.get(0).isArray() || rows.get(0).isEmpty()) {
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Databricks returned no value for the statement to " + purpose);
        }
        return Long.parseLong(rows.get(0).get(0).asText("0"));
    }

    /**
     * Polls one statement until it succeeds, fails, or runs out of time.
     *
     * <p>Holds the calling thread, which is the one real cost of a statement per chunk: a chunk
     * whose query takes ten minutes occupies a worker for ten minutes. Acceptable for queries that
     * read data already sitting in a table — the case this mode exists for — and the reason the
     * other mode hands its waiting to the engine instead.
     */
    private static JsonNode awaitStatement(DatabricksSession session, DatabricksConfig config,
                                           String statementId, String purpose) {
        Instant deadline = Instant.now().plus(config.queryTimeout());
        String url = config.statementsUrl() + "/" + statementId;

        while (true) {
            JsonNode statement = session.getJson(url, purpose);
            String state = statement.path("status").path("state").asText("UNKNOWN")
                    .toUpperCase(Locale.ROOT);

            if ("SUCCEEDED".equals(state)) {
                return statement;
            }
            if (!"PENDING".equals(state) && !"RUNNING".equals(state)) {
                JsonNode error = statement.path("status").path("error");
                throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                        "Databricks statement " + statementId + " (" + purpose + ") ended as "
                                + state + ": " + error.path("message").asText(""));
            }
            if (Instant.now().isAfter(deadline)) {
                cancel(session, config, statementId, null);
                throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                        "Databricks statement " + statementId + " (" + purpose + ") was still "
                                + state.toLowerCase(Locale.ROOT) + " after "
                                + config.queryTimeout().toSeconds() + "s and was cancelled.");
            }
            try {
                Thread.sleep(config.pollInterval().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                        "Interrupted while waiting for Databricks statement " + statementId, e);
            }
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
        properties.set("chunkMode", ConfigFields.sourceEnumField(
                "How the work is divided, and therefore how many times the query runs. "
                        + "RESULT_CHUNKS runs the statement once and splits the result the "
                        + "warehouse returns — exact boundaries, one query, but a shared result "
                        + "that expires and takes every remaining chunk with it. OFFSET runs a "
                        + "separate query per chunk: more load on the warehouse, and nothing "
                        + "shared, so a chunk can be retried an hour later on its own.",
                "RESULT_CHUNKS", "OFFSET"));
        properties.set("orderBy", ConfigFields.sourceField("string",
                "Required by chunkMode OFFSET: the column or columns that put rows in a total "
                        + "order. An OFFSET without one is not reproducible — the same query may "
                        + "return rows in a different order each time, so two chunks could read "
                        + "the same row while another row is read by nobody."));
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
