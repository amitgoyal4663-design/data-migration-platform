package com.dmp.connector.jdbc;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConfigFields;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.api.QueryParameters;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.RecordBatch;
import com.dmp.connector.api.Sink;
import com.dmp.connector.api.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Reads and writes relational databases over JDBC.
 *
 * <p>Abstract because the SQL that differs between databases lives in {@link JdbcDialect}: upsert
 * has four incompatible spellings across PostgreSQL, MySQL, SQL Server and Oracle, and identifier
 * quoting differs in ways that matter the moment a column is called {@code order}. Everything else
 * — splitting, streaming, type conversion, error classification — is shared, so one implementation
 * serves every relational database rather than four near-copies drifting apart.
 *
 * <p>Concrete subclasses are registered by declaration in {@code META-INF/services}, so the
 * platform contains no reference to any of them.
 */
public abstract class JdbcConnector implements Source, Sink {

    /** Supplies the SQL that differs between databases. */
    protected abstract JdbcDialect dialect();

    @Override
    public ConnectorSpec spec() {
        JdbcDialect dialect = dialect();
        return new ConnectorSpec(
                dialect.connectorType(),
                dialect.displayName(),
                "Reads and writes " + dialect.displayName() + " tables. Splits a table by a key "
                        + "column so large migrations run in parallel and resume cleanly.",
                ConnectorSpec.Direction.BOTH,
                configSchema(dialect),
                java.util.Set.of("username", "password"),
                "1.0.0");
    }

    @Override
    public void testConnection(ConnectorContext context) {
        JdbcConfig config = JdbcConfig.from(context);
        JdbcDialect dialect = dialect();
        try (Connection connection = connect(config, context)) {
            // Runs a real query against the configured table rather than only opening a socket. A
            // test that passes while the table is missing produces confidence the first run
            // immediately contradicts.
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT 1 FROM " + config.qualifiedTable(dialect) + " WHERE 1 = 0")) {
                statement.execute();
            }
        } catch (SQLException e) {
            throw translate(e, "Could not reach " + config.qualifiedTable(dialect));
        }
    }

    // ------------------------------------------------------------------ source

    /**
     * The {@code :placeholders} in the configured predicate, so the console can ask for them.
     *
     * <p>They live in {@code where} because that is the part of a JDBC read that changes per run —
     * the table and the columns do not.
     */
    @Override
    public java.util.Set<String> listParameterNames(JsonNode config) {
        // A placeholder inside IN (...) takes a list; anywhere else it takes one value. The query
        // is the only thing that knows, and it says so plainly.
        return com.dmp.connector.api.QueryParameters.listsIn(
                config == null ? null : config.path("where").asText(null));
    }

    @Override
    public java.util.Set<String> parameterNames(JsonNode config) {
        JsonNode where = config == null ? null : config.get("where");
        return where == null || where.isNull()
                ? java.util.Set.of()
                : QueryParameters.referencedBy(where.asText());
    }

    @Override
    public SourceSession openSource(ConnectorContext context) {
        return new JdbcSourceSession(JdbcConfig.from(context), context, dialect());
    }

    private static final class JdbcSourceSession implements SourceSession {

        private final JdbcConfig config;
        private final ConnectorContext context;
        private final JdbcDialect dialect;

        JdbcSourceSession(JdbcConfig config, ConnectorContext context, JdbcDialect dialect) {
            this.config = config;
            this.context = context;
            this.dialect = dialect;
        }

        /**
         * Divides the table into contiguous ranges of the split column.
         *
         * <p>Ranges are derived from the observed minimum and maximum, so the same table produces
         * the same boundaries every time — a resumed run must not re-read a range it finished or
         * skip one it never started.
         *
         * <p>Boundaries are values, not offsets. {@code OFFSET} would make chunk 400 scan and
         * discard the 40 million rows before it; a key range seeks straight to its start.
         */
        @Override
        public List<SplitSpec> plan(com.dmp.connector.api.Preparation preparation, PlanRequest request) {
            if (!config.isSplittable()) {
                // Honest rather than convenient: a table with no usable key column runs as one
                // chunk. Faking parallelism here would produce overlapping reads.
                context.log().info("No splitColumn configured for {}; reading as a single chunk",
                        config.qualifiedTable(dialect));
                return List.of(SplitSpec.single());
            }

            try (Connection connection = connect(config, context)) {
                QueryParameters.Positional filter =
                        QueryParameters.toPositional(config.whereClause());
                String sql = "SELECT MIN(" + dialect.quote(config.splitColumn()) + "), MAX("
                        + dialect.quote(config.splitColumn()) + ")"
                        + " FROM " + config.qualifiedTable(dialect)
                        + (filter.sql() == null ? "" : " WHERE " + filter.sql());

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    bindFilter(statement, filter, context, 1);
                    ResultSet rs = statement.executeQuery();

                    if (!rs.next() || rs.getObject(1) == null) {
                        context.log().info("{} is empty; nothing to plan", config.qualifiedTable(dialect));
                        return List.of();
                    }

                    JsonNode min = JdbcValues.toJson(rs, rs.getMetaData()).elements().next();
                    ResultSet boundsRow = rs;
                    Object minValue = boundsRow.getObject(1);
                    Object maxValue = boundsRow.getObject(2);

                    if (!(minValue instanceof Number) || !(maxValue instanceof Number)) {
                        context.log().warn(
                                "Split column {} is not numeric; reading {} as a single chunk",
                                config.splitColumn(), config.qualifiedTable(dialect));
                        return List.of(SplitSpec.single());
                    }

                    return rangeSplits(((Number) minValue).longValue(),
                            ((Number) maxValue).longValue(), request);
                }
            } catch (SQLException e) {
                throw translate(e, "Could not plan chunks for " + config.qualifiedTable(dialect));
            }
        }

        /**
         * Divides the key range into chunks of the requested size.
         *
         * <p>Boundaries are key values, not offsets. {@code OFFSET 40000} makes the database scan
         * and discard forty thousand rows to reach the next page, so a job would get quadratically
         * slower as it progressed; a key range seeks straight to its start.
         *
         * <p>The range is divided by the key span rather than by row count, so a table with gaps in
         * its keys produces chunks that differ in row count. That is deliberate: computing exact
         * row boundaries would need a full scan at planning time, and the pull loop already
         * corrects for uneven chunks by letting fast pods take more of them.
         */
        private List<SplitSpec> rangeSplits(long min, long max, PlanRequest request) {
            long span = max - min + 1;
            long chunkSize = Math.max(1, request.targetRowsPerChunk());

            // Ceiling, so a badly configured size cannot plan a million chunk documents.
            long wanted = (span + chunkSize - 1) / chunkSize;
            if (wanted > request.maxChunks()) {
                chunkSize = (span + request.maxChunks() - 1) / request.maxChunks();
                context.log().warn(
                        "Requested {} rows per chunk would plan {} chunks for {}; "
                                + "raised to {} rows per chunk to stay under the {} chunk ceiling",
                        request.targetRowsPerChunk(), wanted, config.qualifiedTable(dialect),
                        chunkSize, request.maxChunks());
            }

            List<SplitSpec> splits = new ArrayList<>();
            long lower = min;
            int id = 0;

            while (lower <= max) {
                long upper = Math.min(max, lower + chunkSize - 1);

                ObjectNode spec = Json.newObject();
                spec.put("from", lower);
                spec.put("to", upper);
                splits.add(new SplitSpec(id, spec,
                        config.splitColumn() + " " + lower + "–" + upper));

                lower = upper + 1;
                id++;
            }
            context.log().info("Planned {} chunk(s) of ~{} rows each over {}",
                    splits.size(), chunkSize, config.qualifiedTable(dialect));
            return splits;
        }

        @Override
        public RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize) {
            return new JdbcRecordStream(config, context, dialect, split, fromCursor, fetchSize);
        }
    }

    /**
     * A streaming cursor over one chunk.
     *
     * <p>Holds its connection open and streams with a server-side cursor rather than materialising
     * the chunk. Autocommit is disabled because PostgreSQL's driver silently ignores
     * {@code setFetchSize} on an autocommit connection and buffers the entire result set — the
     * difference between a bounded footprint and an out-of-memory kill on a large chunk.
     */
    private static final class JdbcRecordStream implements RecordStream {

        private final Connection connection;
        private final PreparedStatement statement;
        private final ResultSet resultSet;
        private final ResultSetMetaData metaData;
        private final String splitColumn;
        private final List<String> keyColumns;

        private JsonNode cursor;
        private long emitted;

        JdbcRecordStream(JdbcConfig config, ConnectorContext context, JdbcDialect dialect,
                         SplitSpec split, JsonNode fromCursor, int fetchSize) {
            this.splitColumn = config.splitColumn();
            // The columns that identify a record, for the record index and the dead-letter queue.
            // Falls back to the split column, which is usually the primary key anyway — but only as
            // a fallback: a chunking column is chosen for how it divides the table, and the two
            // stop being the same the moment somebody splits on a timestamp.
            this.keyColumns = config.keyColumns().isEmpty()
                    ? (config.splitColumn() == null ? List.of() : List.of(config.splitColumn()))
                    : config.keyColumns();
            try {
                this.connection = connect(config, context);
                // PostgreSQL silently ignores setFetchSize on an autocommit connection and buffers
                // the whole result set. Other drivers do not need this, and asking for a
                // transaction they do not want is not free.
                this.connection.setAutoCommit(!dialect.requiresTransactionForStreaming());
                this.connection.setReadOnly(true);

                String sql = buildQuery(config, dialect, split, fromCursor);
                this.statement = connection.prepareStatement(sql);
                this.statement.setFetchSize(fetchSize);
                if (config.queryTimeoutSeconds() > 0) {
                    this.statement.setQueryTimeout(config.queryTimeoutSeconds());
                }
                bindQuery(statement, config, context, split, fromCursor);

                this.resultSet = statement.executeQuery();
                this.metaData = resultSet.getMetaData();
                this.cursor = fromCursor == null ? Json.emptyObject() : fromCursor;

            } catch (SQLException e) {
                throw translate(e, "Could not open chunk " + split.id());
            }
        }

        @Override
        public DataRecord next() {
            try {
                if (!resultSet.next()) {
                    return null;
                }
                ObjectNode row = JdbcValues.toJson(resultSet, metaData);
                emitted++;

                // The cursor advances with every row read, but the engine only persists it once the
                // sink has durably accepted the batch containing that row.
                if (splitColumn != null && row.hasNonNull(splitColumn)) {
                    ObjectNode next = Json.newObject();
                    next.set("after", row.get(splitColumn));
                    cursor = next;
                }
                return DataRecord.of(row, keyOf(row), emitted);

            } catch (SQLException e) {
                throw translate(e, "Failed while reading a row");
            }
        }

        /**
         * The record's identity, from the key columns.
         *
         * <p>A composite key is joined rather than reduced to its first column, because half a
         * composite key identifies a group of rows and not a row. Null when no key column is
         * configured and there is no split column to fall back on — a record with no identity is
         * left out of the index rather than filed under a placeholder.
         */
        private String keyOf(JsonNode row) {
            if (keyColumns.isEmpty()) {
                return null;
            }
            StringBuilder key = new StringBuilder();
            for (String column : keyColumns) {
                if (!row.hasNonNull(column)) {
                    return null;
                }
                if (!key.isEmpty()) {
                    key.append('|');
                }
                key.append(row.get(column).asText());
            }
            return key.toString();
        }

        @Override
        public JsonNode cursor() {
            return cursor;
        }

        @Override
        public void close() {
            closeQuietly(resultSet);
            closeQuietly(statement);
            closeQuietly(connection);
        }

        private static String buildQuery(JdbcConfig config, JdbcDialect dialect,
                                         SplitSpec split, JsonNode fromCursor) {
            StringBuilder sql = new StringBuilder("SELECT ")
                    .append(config.selectList(dialect))
                    .append(" FROM ").append(config.qualifiedTable(dialect))
                    .append(" WHERE 1 = 1");

            // The predicate's own placeholders become ? markers here, and are bound first below —
            // they appear before the chunk boundary markers in the statement, and a
            // PreparedStatement is bound by position.
            if (config.whereClause() != null) {
                sql.append(" AND (")
                        .append(QueryParameters.toPositional(config.whereClause()).sql())
                        .append(')');
            }
            if (config.isSplittable() && split.spec().hasNonNull("from")) {
                sql.append(" AND ").append(dialect.quote(config.splitColumn())).append(" >= ?")
                        .append(" AND ").append(dialect.quote(config.splitColumn())).append(" <= ?");
            }
            if (hasResumePoint(fromCursor)) {
                sql.append(" AND ").append(dialect.quote(config.splitColumn())).append(" > ?");
            }
            if (config.isSplittable()) {
                // Ordering is what makes the cursor meaningful. Without it, "everything after X"
                // has no relationship to what has already been read.
                sql.append(" ORDER BY ").append(dialect.quote(config.splitColumn())).append(" ASC");
            }
            return sql.toString();
        }

        private static void bindQuery(PreparedStatement statement, JdbcConfig config,
                                      ConnectorContext context, SplitSpec split, JsonNode fromCursor)
                throws SQLException {
            int index = 1;

            index = bindFilter(statement, QueryParameters.toPositional(config.whereClause()),
                    context, index);

            if (config.isSplittable() && split.spec().hasNonNull("from")) {
                statement.setObject(index++, JdbcValues.boundaryValue(split.spec().get("from")));
                statement.setObject(index++, JdbcValues.boundaryValue(split.spec().get("to")));
            }
            if (hasResumePoint(fromCursor)) {
                statement.setObject(index, JdbcValues.boundaryValue(fromCursor.get("after")));
            }
        }

        private static boolean hasResumePoint(JsonNode cursor) {
            return cursor != null && cursor.hasNonNull("after");
        }
    }

    // -------------------------------------------------------------------- sink

    @Override
    public SinkSession openSink(ConnectorContext context) {
        return new JdbcSinkSession(JdbcConfig.from(context), context, dialect());
    }

    private static final class JdbcSinkSession implements SinkSession {

        private final JdbcConfig config;
        private final ConnectorContext context;
        private final JdbcDialect dialect;
        private final Connection connection;

        JdbcSinkSession(JdbcConfig config, ConnectorContext context, JdbcDialect dialect) {
            this.config = config;
            this.context = context;
            this.dialect = dialect;
            try {
                this.connection = connect(config, context);
                this.connection.setAutoCommit(false);
            } catch (SQLException e) {
                throw translate(e, "Could not open a write connection");
            }
        }

        @Override
        public Capabilities capabilities() {
            boolean idempotent = config.writeMode() != JdbcConfig.WriteMode.INSERT;
            return new Capabilities(
                    idempotent,
                    idempotent ? null
                            : "This sink inserts. Each batch is atomic, but atomicity is not "
                            + "idempotence: rows already committed by an earlier batch are "
                            + "re-inserted if the chunk resumes behind them, and collide on the "
                            + "primary key. Set write mode to UPSERT and name the key column to "
                            + "make repeated writes harmless.",
                    true,               // one batch, one transaction
                    false,              // JDBC commits synchronously
                    false,
                    false,              // rows are written individually; no batch envelope
                    0,                  // no protocol ceiling
                    1_000);
        }

        /**
         * Writes a batch inside a single transaction.
         *
         * <p>All or nothing. A partial batch followed by a checkpoint would leave the source cursor
         * ahead of what actually landed, so on a plain INSERT the batch is rolled back and retried
         * whole. Duplicate-key violations under UPSERT cannot occur by construction.
         */
        @Override
        public WriteResult write(RecordBatch batch) {
            if (batch.isEmpty()) {
                return WriteResult.allWritten(0, 0);
            }

            List<String> columns = columnsOf(batch);
            String sql = dialect.writeStatement(
                    config.qualifiedTable(dialect), columns, config.keyColumns(), config.writeMode());

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (DataRecord record : batch.records()) {
                    for (int i = 0; i < columns.size(); i++) {
                        JdbcValues.bind(statement, i + 1, record.payload().get(columns.get(i)));
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();

                return WriteResult.allWritten(batch.size(), batch.totalBytes());

            } catch (SQLException e) {
                rollbackQuietly();
                throw translate(e, "Failed to write " + batch.size() + " record(s) to "
                        + config.qualifiedTable(dialect));
            }
        }

        @Override
        public void close() {
            closeQuietly(connection);
        }

        /**
         * Column list taken from the first record.
         *
         * <p>The whole batch is assumed to share a shape, which holds for a relational source and
         * for any transformation producing a consistent projection. A record missing a column binds
         * null rather than failing, so an optional field being absent is not an error.
         */
        private List<String> columnsOf(RecordBatch batch) {
            if (!config.columns().isEmpty()) {
                return config.columns();
            }
            List<String> columns = new ArrayList<>();
            batch.records().get(0).payload().fieldNames().forEachRemaining(columns::add);
            return columns;
        }

        private void rollbackQuietly() {
            try {
                connection.rollback();
            } catch (SQLException ignored) {
                // The write already failed; a rollback failure adds nothing actionable.
            }
        }

    }

    // ------------------------------------------------------------------ shared

    private static Connection connect(JdbcConfig config, ConnectorContext context) throws SQLException {
        Properties properties = new Properties();
        context.secret("username").ifPresent(value -> properties.setProperty("user", value));
        context.secret("password").ifPresent(value -> properties.setProperty("password", value));
        // Identifies the platform in the database's activity view, so a DBA investigating load can
        // see which run is responsible rather than an anonymous connection.
        properties.setProperty("ApplicationName", "dmp-run-" + context.runId());
        return DriverManager.getConnection(config.url(), properties);
    }

    private static String whereSuffix(JdbcConfig config) {
        return config.whereClause() == null ? "" : " WHERE " + config.whereClause();
    }

    /**
     * Binds the read filter's {@code :placeholders} from the values the run was started with.
     *
     * <p>Bound rather than pasted into the predicate. A value containing a quote would otherwise
     * either break the statement or change which rows it selects, and the entire point of supplying
     * a range per run is that the value did not come from whoever wrote the query.
     *
     * <p>Used by both the planning query and the per-chunk read, because the same predicate appears
     * in both — a filter that applied to one and not the other would plan chunk boundaries over
     * rows the read then refused to return.
     *
     * @param index the next free position, since the caller binds more markers after these
     * @return the next free position
     */
    private static int bindFilter(PreparedStatement statement, QueryParameters.Positional filter,
                                  ConnectorContext context, int index) throws SQLException {
        for (String name : filter.names()) {
            JsonNode value = context.parameters().get(name);

            if (value == null || value.isNull()
                    || (value.isTextual() && value.asText().isBlank())) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "This pipeline's read filter expects a value for ':" + name + "', but the "
                                + "run was started without one. Supply it when starting the run, "
                                + "or remove the placeholder from 'where'.");
            }
            statement.setObject(index++, JdbcValues.boundaryValue(value));
        }
        return index;
    }

    /**
     * Classifies a SQL failure so the engine knows whether retrying is worth anything.
     *
     * <p>SQLState classes 08 (connection) and 40 (transaction rollback, including deadlock and
     * serialisation failure) are transient and worth another attempt. A syntax error or a missing
     * table is not, and retrying it five times with backoff only delays the error the user needs
     * to see.
     */
    private static ConnectorException translate(SQLException e, String context) {
        String state = e.getSQLState() == null ? "" : e.getSQLState();
        String message = context + ": " + e.getMessage();

        if (e instanceof SQLTransientException || state.startsWith("08") || state.startsWith("40")) {
            return new ConnectorException(ConnectorException.Kind.UNAVAILABLE, message, e);
        }
        if (state.startsWith("28") || state.startsWith("42501")) {
            return new ConnectorException(ConnectorException.Kind.AUTHENTICATION, message, e);
        }
        if (state.startsWith("42") || state.startsWith("3D") || state.startsWith("3F")) {
            return new ConnectorException(ConnectorException.Kind.CONFIGURATION, message, e);
        }
        return new ConnectorException(ConnectorException.Kind.UNKNOWN, message, e);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Nothing useful to do while unwinding.
        }
    }

    /**
     * The JSON Schema the console renders this connector's configuration form from.
     *
     * <p>This is what makes the plugin system usable rather than merely present: a new connector
     * jar produces a complete, validated form with no frontend change.
     */
    private static JsonNode configSchema(JdbcDialect dialect) {
        ObjectNode properties = Json.newObject();
        properties.set("url", ConfigFields.fromEnvironment(field("string",
                "The whole connection, for example " + dialect.urlExample() + ". The host, port and "
                        + "database differ in every environment and belong to whoever runs them, so "
                        + "name the variable rather than typing it — a connection defined that way "
                        + "is promoted between environments rather than rebuilt.")));
        properties.set("table", field("string", "Table to read or write."));

        properties.set("schema", ConfigFields.advanced(field("string",
                "Schema name. Defaults to public.")));
        properties.set("splitColumn", ConfigFields.recordKeyField(
                ConfigFields.advanced(ConfigFields.sourceField("string",
                        "Numeric key column the table is divided into parallel chunks by. Without "
                                + "it the table is read as a single chunk, which is correct but "
                                + "serial. Also read as each row's identity when keyColumns is not "
                                + "set.")), null));
        properties.set("columns", ConfigFields.advanced(field("array",
                "Columns to read or write. Defaults to all.")));
        properties.set("where", ConfigFields.selectionField(
                ConfigFields.advanced(ConfigFields.sourceField("string",
                        "SQL predicate applied to reads, to migrate a subset rather than the whole "
                                + "table. Empty reads everything."))));
        properties.set("queryTimeoutSeconds", ConfigFields.advanced(field("integer",
                "0 means no timeout.")));

        // Left in plain sight for the same reason as MongoDB's: writeMode decides whether a retried
        // chunk duplicates what it already wrote, and keyColumns is what UPSERT matches on. Neither
        // belongs behind a collapsed heading.
        properties.set("writeMode", ConfigFields.sinkField("string",
                "INSERT is fastest and duplicates if a chunk is retried; UPSERT matches on the key "
                        + "columns and is safe to repeat; INSERT_IGNORE skips rows that already "
                        + "exist."));
        properties.set("keyColumns", ConfigFields.recordKeyField(field("array",
                "Columns that identify a record. A sink matches on them to update rather than "
                + "insert; a source reports them as the record's identity, which is what record "
                + "search and the dead-letter queue key on. Defaults to the split column."), null));

        ObjectNode schema = Json.newObject();
        schema.put("type", "object");
        schema.set("properties", properties);
        schema.set("required", Json.mapper().createArrayNode().add("url").add("table"));
        return schema;
    }

    private static ObjectNode field(String type, String description) {
        return Json.mapper().createObjectNode().put("type", type).put("description", description);
    }
}
