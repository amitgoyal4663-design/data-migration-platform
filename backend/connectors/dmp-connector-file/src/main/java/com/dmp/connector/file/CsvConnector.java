package com.dmp.connector.file;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConfigFields;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.RecordBatch;
import com.dmp.connector.api.Sink;
import com.dmp.connector.api.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Reads and writes delimited text files.
 *
 * <p>Splits by file rather than by byte range. A CSV cannot be safely cut at an arbitrary offset —
 * a quoted field may contain the delimiter or a newline, so a byte-range split would produce
 * corrupt rows at every boundary. One file per chunk is honest and still parallel whenever there is
 * more than one file, which is the usual shape of a bulk export.
 *
 * <p>Within a file, the resume cursor is a row number. Resuming re-reads and discards preceding
 * rows, which is unavoidable for a format with no index — but it is correct, which matters more.
 */
public class CsvConnector implements Source, Sink {

    private static final String TYPE = "file-csv";

    @Override
    public ConnectorSpec spec() {
        return new ConnectorSpec(
                TYPE,
                "Delimited files (CSV, TSV)",
                "Reads and writes CSV, TSV and other delimited files on a mounted filesystem. "
                        + "A directory of files migrates in parallel, one chunk per file.",
                ConnectorSpec.Direction.BOTH,
                configSchema(),
                Set.of(),
                "1.0.0");
    }

    @Override
    public void testConnection(ConnectorContext context) {
        FileConfig config = FileConfig.from(context);
        Path root = config.path();

        if (config.isWriteTarget()) {
            if (!Files.isDirectory(root) && !Files.isDirectory(root.getParent())) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "Neither " + root + " nor its parent directory exists");
            }
            return;
        }

        if (!Files.exists(root)) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Path does not exist: " + root
                            + ". Remember this is a path inside the worker container, not on your machine.");
        }
        if (matchingFiles(config).isEmpty()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "No files under " + root + " match '" + config.pattern() + "'");
        }
    }

    // ------------------------------------------------------------------ source

    @Override
    public SourceSession openSource(ConnectorContext context) {
        FileConfig config = FileConfig.from(context);

        return new SourceSession() {

            @Override
            public List<SplitSpec> plan(Preparation preparation, PlanRequest request) {
                List<Path> files = matchingFiles(config);
                if (files.isEmpty()) {
                    context.log().warn("No files under {} match '{}'", config.path(), config.pattern());
                    return List.of();
                }

                List<SplitSpec> splits = new ArrayList<>(files.size());
                for (int i = 0; i < files.size(); i++) {
                    ObjectNode spec = Json.newObject();
                    // The path, not a handle or a stream. A chunk may be claimed long after it was
                    // planned, so its spec has to be something that is still valid later.
                    spec.put("file", files.get(i).toString());
                    splits.add(new SplitSpec(i, spec, files.get(i).getFileName().toString()));
                }
                context.log().info("Planned {} file(s) under {}", splits.size(), config.path());
                return splits;
            }

            @Override
            public RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize) {
                return new CsvRecordStream(config, split, fromCursor);
            }
        };
    }

    private static final class CsvRecordStream implements RecordStream {

        private final String keyColumn;
        private final Reader reader;
        private final CSVParser parser;
        private final Iterator<CSVRecord> rows;
        private final List<String> headers;
        private long rowNumber;

        CsvRecordStream(FileConfig config, SplitSpec split, JsonNode fromCursor) {
            this.keyColumn = config.keyColumn();
            Path file = Path.of(split.spec().get("file").asText());
            try {
                this.reader = Files.newBufferedReader(file, config.charset());
                this.parser = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                        .setDelimiter(config.delimiter())
                        .setQuote(config.quote())
                        .setHeader()
                        .setSkipHeaderRecord(true)
                        .setIgnoreSurroundingSpaces(true)
                        .setIgnoreEmptyLines(true)
                        .get()
                        .parse(reader);

                this.headers = config.hasHeader()
                        ? new ArrayList<>(parser.getHeaderNames())
                        : List.of();
                this.rows = parser.iterator();

                // No index into a text file, so resuming means re-reading and discarding. Correct
                // rather than fast: skipping by byte offset would land mid-field on any file
                // containing a quoted newline.
                long resumeAfter = fromCursor != null && fromCursor.hasNonNull("row")
                        ? fromCursor.get("row").asLong() : 0;
                while (rowNumber < resumeAfter && rows.hasNext()) {
                    rows.next();
                    rowNumber++;
                }

            } catch (IOException e) {
                throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                        "Could not open " + file + ": " + e.getMessage(), e);
            }
        }

        @Override
        public DataRecord next() {
            if (!rows.hasNext()) {
                return null;
            }
            CSVRecord row = rows.next();
            rowNumber++;

            ObjectNode payload = Json.newObject();
            if (headers.isEmpty()) {
                for (int i = 0; i < row.size(); i++) {
                    payload.put("column_" + (i + 1), row.get(i));
                }
            } else {
                for (String header : headers) {
                    // A short row is normal in hand-edited exports; treating a missing trailing
                    // column as null beats failing the whole file.
                    payload.put(header, row.isMapped(header) ? row.get(header) : null);
                }
            }
            // Null when no key column is configured, and deliberately not the row number: a row
            // number identifies a position in one file rather than a record, so indexing under it
            // would answer "was record 4,271 transferred" with whatever happens to sit on that line
            // of whichever file was read last.
            JsonNode keyNode = keyColumn == null ? null : payload.get(keyColumn);
            return DataRecord.of(payload,
                    keyNode == null || keyNode.isNull() ? null : keyNode.asText(), rowNumber);
        }

        @Override
        public JsonNode cursor() {
            ObjectNode cursor = Json.newObject();
            cursor.put("row", rowNumber);
            return cursor;
        }

        @Override
        public void close() {
            try {
                parser.close();
                reader.close();
            } catch (IOException ignored) {
                // Nothing useful to do while unwinding a read.
            }
        }
    }

    // -------------------------------------------------------------------- sink

    @Override
    public SinkSession openSink(ConnectorContext context) {
        FileConfig config = FileConfig.from(context);
        return new CsvSinkSession(config, context);
    }

    /**
     * Writes one file per chunk.
     *
     * <p><b>Per chunk, not per worker.</b> Sessions are opened once per chunk, so a name built from
     * the run and the worker alone is the same name for every chunk of the run — and each session
     * opening it would overwrite the last one's rows while reporting every write as successful. A
     * run of twenty chunks would end with the last chunk's output and nothing else.
     *
     * <p>Excluding the worker from the name is deliberate too: a chunk that fails on one pod and is
     * retried on another returns to the same file rather than leaving a partial one orphaned beside
     * its replacement. The chunk index alone is unique within a run and stable across attempts,
     * which is exactly the property the name needs.
     *
     * <p>Files are ordered by chunk, so {@code cat prefix-<run>-*.csv} reassembles the run in
     * source order when a single file is wanted.
     */
    private static final class CsvSinkSession implements SinkSession {

        /** Zero-padded so a lexical sort of the directory is also a chunk-order sort. */
        private static final String CHUNK_FORMAT = "%05d";

        private final FileConfig config;
        private final ConnectorContext context;
        private final Path target;

        private BufferedWriter writer;
        private CSVPrinter printer;
        private List<String> headers;

        CsvSinkSession(FileConfig config, ConnectorContext context) {
            this.config = config;
            this.context = context;
            this.target = config.path().resolve(config.filePrefix() + "-" + context.runId()
                    + "-" + chunkSuffix(context) + ".csv");
        }

        /**
         * Outside chunk execution there is no chunk to name the file after, so the worker
         * distinguishes concurrent sessions instead. Chunk execution never takes this path.
         */
        private static String chunkSuffix(ConnectorContext context) {
            return context.chunkIndex() == ConnectorContext.NO_CHUNK
                    ? context.workerId()
                    : CHUNK_FORMAT.formatted(context.chunkIndex());
        }

        @Override
        public Capabilities capabilities() {
            // Not idempotent, and the reason is narrower than it looks. Reported honestly rather
            // than claimed otherwise: declaring idempotence here would widen the engine's
            // checkpoint interval, which is precisely what makes the remaining gap larger.
            return new Capabilities(
                    false,
                    "A CSV file has no key to write over, so rows can only be appended. Each chunk "
                            + "owns its own file, named for the run and the chunk, so a chunk "
                            + "retried from its start replaces its own output exactly and a "
                            + "retried run writes a fresh set of files. The gap is a chunk resumed "
                            + "part-way: batches flushed to the file but not yet checkpointed are "
                            + "written a second time, so a pod lost mid-chunk can leave up to one "
                            + "checkpoint interval of duplicate rows in that chunk's file.",
                    false, false, true, false, 0, 5_000);
        }

        @Override
        public WriteResult write(RecordBatch batch) {
            if (batch.isEmpty()) {
                return WriteResult.allWritten(0, 0);
            }
            try {
                ensureOpen(batch);
                for (DataRecord record : batch.records()) {
                    List<Object> values = new ArrayList<>(headers.size());
                    for (String header : headers) {
                        JsonNode value = record.payload().get(header);
                        values.add(value == null || value.isNull() ? "" : asText(value));
                    }
                    printer.printRecord(values);
                }
                // Flushed per batch so the checkpoint the engine writes next reflects bytes that
                // have actually left this process.
                printer.flush();
                return WriteResult.allWritten(batch.size(), batch.totalBytes());

            } catch (IOException e) {
                throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                        "Could not write to " + target + ": " + e.getMessage(), e);
            }
        }

        private void ensureOpen(RecordBatch batch) throws IOException {
            if (printer != null) {
                return;
            }
            Files.createDirectories(target.getParent());

            // Column order from the first record. A CSV has one header row, so the shape is fixed
            // by whatever arrives first; a later record with extra fields would silently misalign,
            // which is why the engine's projection is expected to be consistent.
            headers = config.columns().isEmpty()
                    ? new ArrayList<>(fieldNames(batch.records().get(0)))
                    : config.columns();

            // A resumed chunk continues the file its earlier attempt was writing. Truncating here
            // would discard rows the checkpoint has already counted as written and that the source
            // will not be read for again — the run would report a total the file cannot support.
            // A chunk starting fresh truncates, so a retry from the beginning replaces its own
            // output instead of doubling it.
            boolean resuming = context.isResuming() && Files.exists(target);
            writer = Files.newBufferedWriter(target, config.charset(),
                    StandardOpenOption.CREATE,
                    resuming ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING);

            CSVFormat.Builder format = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                    .setDelimiter(config.delimiter())
                    .setQuote(config.quote());
            // The header belongs to the file, not to the session. Writing it again on resume would
            // plant a row of column names in the middle of the data.
            if (config.hasHeader() && !(resuming && Files.size(target) > 0)) {
                format.setHeader(headers.toArray(String[]::new));
            }
            printer = new CSVPrinter(writer, format.get());
            context.log().info("{} {}", resuming ? "Resuming" : "Writing to", target);
        }

        @Override
        public void close() {
            try {
                if (printer != null) {
                    printer.close();
                }
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static Set<String> fieldNames(DataRecord record) {
            Set<String> names = new LinkedHashSet<>();
            record.payload().fieldNames().forEachRemaining(names::add);
            return names;
        }

        private static String asText(JsonNode value) {
            // Objects and arrays are written as JSON rather than as Jackson's toString, so a
            // nested field round-trips through the file instead of becoming unparseable.
            return value.isValueNode() ? value.asText() : value.toString();
        }
    }

    // ------------------------------------------------------------------ shared

    private static List<Path> matchingFiles(FileConfig config) {
        Path root = config.path();
        if (Files.isRegularFile(root)) {
            return List.of(root);
        }
        if (!Files.isDirectory(root)) {
            return List.of();
        }

        PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + config.pattern());
        try (Stream<Path> found = Files.list(root)) {
            return found
                    .filter(Files::isRegularFile)
                    .filter(path -> matcher.matches(path.getFileName()))
                    // Sorted so planning is deterministic: the same directory must produce the same
                    // chunk order every time, or a resumed run would read a different file per chunk.
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Could not list " + root + ": " + e.getMessage(), e);
        }
    }

    private static JsonNode configSchema() {
        ObjectNode properties = Json.newObject();
        // A mount path, which is a property of how the workload is deployed rather than of the
        // migration — the same pipeline reads /data locally and /mnt/exports in the cluster.
        properties.set("path", ConfigFields.fromEnvironment(field("string",
                "Directory or file path, as seen from inside the worker container. Where a volume "
                        + "is mounted is decided by the deployment, so name the variable rather "
                        + "than typing a path that only exists in one environment.")));
        properties.set("pattern", ConfigFields.sourceField("string",
                "Glob for files to read. Defaults to *.csv."));
        properties.set("keyColumn", ConfigFields.recordKeyField(ConfigFields.sourceField("string",
                "Column holding each row's identifier, used by record search and the dead-letter "
                + "queue to say which row a result is about. A CSV has no key of its own, so "
                + "without this a row can only be identified by its position in the file — and a "
                + "position stops meaning anything once the file is regenerated."), null));

        properties.set("delimiter", ConfigFields.advanced(
                field("string", "Field separator. Defaults to a comma.")));
        properties.set("quote", ConfigFields.advanced(
                field("string", "Quote character. Defaults to a double quote.")));
        properties.set("header", ConfigFields.advanced(
                field("boolean", "Whether the first row holds column names.")));
        properties.set("encoding", ConfigFields.advanced(
                field("string", "Character set. Defaults to UTF-8.")));
        properties.set("columns", ConfigFields.advanced(ConfigFields.sinkField("array",
                "Column order when writing. Defaults to the record's own.")));
        properties.set("filePrefix", ConfigFields.advanced(ConfigFields.sinkField("string",
                "Prefix for written files. Defaults to 'export'.")));

        ObjectNode schema = Json.newObject();
        schema.put("type", "object");
        schema.set("properties", properties);
        schema.set("required", Json.mapper().createArrayNode().add("path"));
        return schema;
    }

    private static ObjectNode field(String type, String description) {
        return Json.newObject().put("type", type).put("description", description);
    }

    /** Typed view over the file connector's configuration. */
    private record FileConfig(
            Path path,
            String pattern,
            char delimiter,
            char quote,
            boolean hasHeader,
            Charset charset,
            List<String> columns,
            String keyColumn,
            String filePrefix) {

        static FileConfig from(ConnectorContext context) {
            JsonNode config = context.config();

            JsonNode pathNode = config.get("path");
            if (pathNode == null || pathNode.asText().isBlank()) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "Configuration field 'path' is required");
            }

            List<String> columns = new ArrayList<>();
            if (config.has("columns") && config.get("columns").isArray()) {
                config.get("columns").forEach(node -> columns.add(node.asText()));
            }

            return new FileConfig(
                    Path.of(pathNode.asText().strip()),
                    text(config, "pattern", "*.csv"),
                    single(text(config, "delimiter", ","), ','),
                    single(text(config, "quote", "\""), '"'),
                    !config.has("header") || config.get("header").asBoolean(true),
                    Charset.forName(text(config, "encoding", "UTF-8")),
                    List.copyOf(columns),
                    text(config, "keyColumn", null),
                    text(config, "filePrefix", "export"));
        }

        boolean isWriteTarget() {
            return Files.isDirectory(path) || !Files.exists(path);
        }

        private static String text(JsonNode config, String field, String fallback) {
            JsonNode node = config.get(field);
            return node == null || node.isNull() || node.asText().isBlank()
                    ? fallback : node.asText();
        }

        private static char single(String value, char fallback) {
            if (value == null || value.isEmpty()) {
                return fallback;
            }
            // Named aliases, because a tab is not typeable into a text field.
            return switch (value) {
                case "\\t", "tab", "TAB" -> '\t';
                case "\\|", "pipe" -> '|';
                default -> value.charAt(0);
            };
        }
    }
}
