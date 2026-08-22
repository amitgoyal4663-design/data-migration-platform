package com.dmp.connector.salesforce;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConfigFields;
import com.dmp.connector.api.CallCost;
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
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Reads and writes Salesforce through Bulk API 2.0.
 *
 * <p>The connector the asynchronous half of the SPI exists for. A database sink writes a batch and
 * knows immediately whether it landed; Salesforce takes an upload, returns, and decides later —
 * sometimes minutes later — how many records it accepted. Modelling that as a synchronous write
 * would mean either blocking a worker on a poll loop or reporting success for records the org went
 * on to reject.
 *
 * <p>So one chunk is one bulk job, and it moves through the lifecycle the SPI already defines:
 *
 * <pre>
 *   prepare        create the job                     — Open
 *   write × N      stage records to a CSV on disk     — nothing has left this process yet
 *   commit         upload the file, close the job     — UploadComplete
 *   checkCommit    poll until Salesforce is done      — JobComplete / Failed
 *   harvest        download the failed-record rows    — per-record outcomes, not a count
 *   release        delete the job
 * </pre>
 *
 * <p><b>Staged to disk rather than held in memory.</b> A chunk of 150,000 records is a large CSV,
 * and Bulk 2.0 wants it as one upload — buffering that in heap would make the memory footprint a
 * function of the chunk size, which is the one thing the engine's design exists to prevent.
 *
 * <p><b>Failures are fetched per record.</b> Salesforce reports a job as "10,000 processed, 347
 * failed", and a count is not actionable. The job's {@code failedResults} file names each refused
 * record with the org's own error, so a rejection arrives in the dead-letter queue as the record it
 * was rather than as a share of a total.
 */
public class SalesforceConnector implements Source, Sink {

    private static final String TYPE = "salesforce";

    /** Bulk 2.0 accepts one upload of up to 150 MB per job; the engine's chunk size is the lever. */
    private static final long MAX_UPLOAD_BYTES = 150L * 1024 * 1024;

    /**
     * One chunk is one bulk job, so how the records reach the staged file is not a decision.
     *
     * <p>A write here appends to a CSV on local disk and the job is submitted once, at commit.
     * Delivering the same records one at a time produces the same file and the same single job —
     * the setting would appear to do something and do nothing, which is worse than being
     * unavailable. Chunk size is the lever for this sink; the 150 MB upload limit is what it moves.
     */
    @Override
    public boolean supportsPerRecordDelivery() {
        return false;
    }

    @Override
    public ConnectorSpec spec() {
        return new ConnectorSpec(
                TYPE,
                "Salesforce (Bulk API 2.0)",
                "Reads with a bulk query job and writes with a bulk ingest job. One chunk is one "
                        + "job: records are staged to disk, uploaded once, and polled to "
                        + "completion, with per-record failures fetched from the job itself.",
                ConnectorSpec.Direction.BOTH,
                configSchema(),
                Set.of("clientId", "clientSecret", "username", "password"),
                "1.0.0",
                // One chunk is one job, and a job is what a rate limit on this connector should
                // count. Underneath it is a create, an upload, an "upload complete", however many
                // status polls the org's queue happens to require, and a fetch of the counts — and
                // that number is a property of how busy Salesforce is, not of the migration. Billed
                // per request, the identical chunk would cost four times as much on a busy morning.
                //
                // A limit set here is therefore in jobs. An org's daily API allowance is in
                // requests, so divide: roughly fourteen requests per job.
                CallCost.PER_CHUNK);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Opens its own short-lived session rather than reusing a chunk's: the chunk finished, its
     * session closed, and this may be asked days later by someone reading a run report.
     *
     * <p>An expired or purged job is {@link Optional#empty()}, not an error. Salesforce keeps job
     * results for about a week and the platform kept the counts, so "the org no longer has the
     * file" is a true and ordinary answer — the console shows it as one rather than as a failure.
     */
    @Override
    public Optional<ResultFile> fetchResults(ConnectorContext context, Preparation job, String kind) {
        String jobId = job == null ? null : job.state().path("jobId").asText(null);
        if (jobId == null) {
            return Optional.empty();
        }

        String file = "successful".equalsIgnoreCase(kind) ? "successfulResults" : "failedResults";
        SalesforceConfig config = SalesforceConfig.from(context);

        try {
            SalesforceSession session = open(config, context);
            String csv = session.getCsv(
                    session.jobsUrl() + "/ingest/" + jobId + "/" + file + "/",
                    "fetch " + file + " for job " + jobId);

            if (csv == null || csv.isBlank()) {
                return Optional.empty();
            }
            return Optional.of(new ResultFile(
                    jobId + "-" + file + ".csv",
                    "text/csv",
                    csv.getBytes(java.nio.charset.StandardCharsets.UTF_8)));

        } catch (ConnectorException e) {
            // The job is gone, or the org will not serve it. Either way there is no file, and the
            // counts on the chunk remain the answer.
            context.log().info("Salesforce has no {} for job {}: {}", file, jobId, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public void testConnection(ConnectorContext context) {
        SalesforceConfig config = SalesforceConfig.from(context);
        SalesforceSession session = open(config, context);

        // Reaching the jobs endpoint proves the token works *and* that the API version exists,
        // which a bare login does not — a wrong apiVersion authenticates perfectly and then fails
        // at the first real call.
        //
        // No query parameters. Salesforce validates the ones it accepts here and answers
        // INVALID_QUERY_KEY for anything else, so a harmless-looking ?limit=1 turns a connection
        // test into a failure that has nothing to do with the connection.
        session.getJson(session.jobsUrl() + "/ingest", "list bulk jobs");
    }

    private static SalesforceSession open(SalesforceConfig config, ConnectorContext context) {
        SalesforceSession session = new SalesforceSession(
                config,
                context.requireSecret("clientId"),
                context.secret("clientSecret").orElse(null),
                context.secret("username").orElse(null),
                context.secret("password").orElse(null));
        session.authenticate();
        return session;
    }

    // ------------------------------------------------------------------ source

    @Override
    public SourceSession openSource(ConnectorContext context) {
        SalesforceConfig config = SalesforceConfig.from(context);
        SalesforceSession session = open(config, context);

        if (config.soql() == null || config.soql().isBlank()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "A Salesforce source needs a 'soql' query, for example "
                            + "SELECT Id, Name FROM Account.");
        }

        return new SourceSession() {

            /**
             * Submits the query job and returns immediately.
             *
             * <p>Salesforce decides when a query is ready. Waiting here would hold a worker thread
             * for however long the org takes, which on a large object is minutes — the engine polls
             * instead, and the worker does other chunks meanwhile.
             */
            @Override
            public Preparation prepare() {
                ObjectNode request = Json.newObject();
                request.put("operation", "query");
                request.put("query", config.soql());
                request.put("contentType", "CSV");
                request.put("lineEnding", config.lineEnding());

                JsonNode job = session.postJson(session.jobsUrl() + "/query",
                        request.toString(), "create the query job");

                ObjectNode state = Json.newObject();
                state.put("jobId", job.path("id").asText());
                context.log().info("Salesforce query job {} submitted for {}",
                        job.path("id").asText(), config.describe());
                return Preparation.of(state);
            }

            @Override
            public Preparation.Status checkPreparation(Preparation preparation) {
                String jobId = preparation.state().path("jobId").asText(null);
                if (jobId == null) {
                    return Preparation.Status.ready();
                }
                JsonNode job = session.getJson(session.jobsUrl() + "/query/" + jobId,
                        "check the query job");
                return statusOf(job, config, "query");
            }

            /**
             * One chunk. Bulk 2.0 pages a query's results with an opaque locator, which cannot be
             * divided or restarted from the middle, so there is nothing to split on — and saying so
             * is better than inventing chunks that would each re-run the whole query.
             */
            @Override
            public List<SplitSpec> plan(Preparation preparation, PlanRequest request) {
                return List.of(new SplitSpec(0, preparation.state(),
                        config.queryObject() + " query results"));
            }

            @Override
            public RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize) {
                return new QueryResultStream(session, config,
                        split.spec().path("jobId").asText(), fromCursor, fetchSize);
            }

            @Override
            public void release(Preparation preparation) {
                deleteJob(session, "query", preparation.state().path("jobId").asText(null), context);
            }
        };
    }

    /**
     * Pages a completed query job's results.
     *
     * <p>The resume position is the locator Salesforce hands back, not a row offset. That is the
     * only thing it will accept, and it is also the honest one: the org decides where a page ends.
     */
    private static final class QueryResultStream implements RecordStream {

        private final SalesforceSession session;
        private final SalesforceConfig config;
        private final String jobId;
        private final int pageSize;

        private Iterator<CSVRecord> rows = List.<CSVRecord>of().iterator();
        private List<String> headers = List.of();

        /** The locator that produced the page currently being read. */
        private String pageLocator;
        /** Where the next page starts; null once Salesforce says there is no next page. */
        private String nextLocator;

        private boolean exhausted;
        private long emitted;

        QueryResultStream(SalesforceSession session, SalesforceConfig config, String jobId,
                          JsonNode fromCursor, int pageSize) {
            this.session = session;
            this.config = config;
            this.jobId = jobId;
            this.pageSize = Math.max(1, pageSize);
            this.pageLocator = fromCursor == null ? null : fromCursor.path("locator").asText(null);
            this.nextLocator = this.pageLocator;
        }

        @Override
        public DataRecord next() {
            while (!rows.hasNext()) {
                if (exhausted || !fetchPage()) {
                    return null;
                }
            }
            CSVRecord row = rows.next();
            emitted++;

            ObjectNode payload = Json.newObject();
            for (String header : headers) {
                String value = row.isMapped(header) ? row.get(header) : null;
                // Salesforce writes an empty string for a null field. Preserving that as "" would
                // make every absent value look like a deliberate blank downstream.
                payload.put(header, value == null || value.isEmpty() ? null : value);
            }

            JsonNode id = payload.get("Id");
            return DataRecord.of(payload, id == null || id.isNull() ? null : id.asText(), emitted);
        }

        private boolean fetchPage() {
            String url = session.jobsUrl() + "/query/" + jobId + "/results?maxRecords=" + pageSize;

            // The locator that fetches this page is remembered separately from the one it returns,
            // because a checkpoint can land part-way through a page — see cursor().
            this.pageLocator = nextLocator;

            SalesforceSession.QueryPage page =
                    session.getQueryPage(url, pageLocator, "fetch query results");

            // The locator Salesforce hands back is the resume position for the next page and the
            // end-of-results signal in one: absent means there is no next page. Deciding that from
            // a short page instead would be wrong whenever the org returns fewer rows than asked
            // for while more remain, which it is entitled to do.
            this.nextLocator = page.nextLocator();
            String csv = page.csv();

            if (csv == null || csv.isBlank()) {
                exhausted = true;
                return false;
            }

            try (CSVParser parser = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                    .setHeader().setSkipHeaderRecord(true).get()
                    .parse(new StringReader(csv))) {

                this.headers = new ArrayList<>(parser.getHeaderNames());
                List<CSVRecord> records = parser.getRecords();
                this.rows = records.iterator();

                if (nextLocator == null) {
                    exhausted = true;
                }
                return !records.isEmpty();

            } catch (IOException e) {
                throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                        "Could not read the query results for job " + jobId + ": " + e.getMessage(), e);
            }
        }

        /**
         * Where a resumed read should start.
         *
         * <p>Not simply the next page's locator. The engine saves this after a batch is written,
         * and a batch is not a page — with a write size smaller than the read size a checkpoint
         * lands part-way through a page, and reporting the <em>next</em> page there would skip
         * every row of the current one that had not yet been handed out.
         *
         * <p>So a partly-read page reports the locator that fetched it, and resuming re-reads that
         * page from its start. Records already written are sent again, which is the at-least-once
         * behaviour the platform declares everywhere else — and the opposite mistake, skipping
         * them, is silent data loss.
         */
        @Override
        public JsonNode cursor() {
            ObjectNode cursor = Json.newObject();
            String resumeFrom = rows.hasNext() ? pageLocator : nextLocator;
            if (resumeFrom != null) {
                cursor.put("locator", resumeFrom);
            }
            return cursor;
        }

        @Override
        public void close() {
            rows = List.<CSVRecord>of().iterator();
        }
    }

    // -------------------------------------------------------------------- sink

    @Override
    public SinkSession openSink(ConnectorContext context) {
        SalesforceConfig config = SalesforceConfig.from(context);

        if (config.object() == null || config.object().isBlank()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "A Salesforce sink needs 'object' — the sObject to write, for example Account.");
        }
        return new IngestSession(open(config, context), config, context);
    }

    /**
     * One bulk ingest job: stage, upload, close, poll, harvest.
     *
     * <p>{@code write} deliberately does no network work. Records go to a CSV on disk and the job
     * is closed once at {@code commit}, because Bulk 2.0 takes a job's data as a single upload —
     * and because a per-batch upload would create one job per batch, multiplying the org's job
     * quota by the number of batches in a chunk.
     */
    private static final class IngestSession implements SinkSession {

        private final SalesforceSession session;
        private final SalesforceConfig config;
        private final ConnectorContext context;

        private Path staged;
        private BufferedWriter writer;
        private CSVPrinter printer;
        private List<String> headers;
        private long stagedRecords;

        IngestSession(SalesforceSession session, SalesforceConfig config, ConnectorContext context) {
            this.session = session;
            this.config = config;
            this.context = context;
        }

        @Override
        public Capabilities capabilities() {
            String advice = "Salesforce assigns its own record ids on insert, so a re-sent batch "
                    + "creates a second set of records rather than replacing the first. Use the "
                    + "upsert operation with an external id field — then Salesforce matches on a "
                    + "value your data owns and a repeat is absorbed.";

            return new Capabilities(
                    config.operation().idempotent,
                    config.operation().idempotent ? null : advice,
                    // Not transactional: a bulk job partially succeeds by design, which is the
                    // whole reason harvest exists.
                    false,
                    // The write returns long before the org has decided anything.
                    true,
                    // Staged to disk, so a large chunk costs file space rather than heap.
                    true,
                    false,
                    0,
                    10_000);
        }

        @Override
        public Preparation prepare() {
            ObjectNode request = Json.newObject();
            request.put("object", config.object());
            request.put("operation", config.operation().wireName);
            request.put("contentType", "CSV");
            request.put("lineEnding", config.lineEnding());
            if (config.externalIdField() != null) {
                request.put("externalIdFieldName", config.externalIdField());
            }

            JsonNode job = session.postJson(session.jobsUrl() + "/ingest",
                    request.toString(), "create the ingest job");

            String jobId = job.path("id").asText();
            this.currentJobId = jobId;
            ObjectNode state = Json.newObject();
            state.put("jobId", jobId);

            context.log().info("Salesforce ingest job {} open for {} {}",
                    jobId, config.operation().wireName, config.object());
            return Preparation.of(state);
        }

        /** The job these rows are being staged for, so the stage log can name it. */
        private String currentJobId;

        @Override
        public WriteResult write(RecordBatch batch) {
            if (batch.isEmpty()) {
                return WriteResult.allWritten(0, 0);
            }
            try {
                ensureStaged(batch);
                for (DataRecord record : batch.records()) {
                    List<Object> values = new ArrayList<>(headers.size());
                    for (String header : headers) {
                        JsonNode value = record.payload().get(header);
                        values.add(value == null || value.isNull() ? "" : asText(value));
                    }
                    printer.printRecord(values);
                    stagedRecords++;
                }
                printer.flush();

                long size = Files.size(staged);
                if (size > MAX_UPLOAD_BYTES) {
                    throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                            "This chunk has staged " + (size / (1024 * 1024)) + " MB, and a "
                                    + "Salesforce bulk job accepts at most 150 MB. Lower rows per "
                                    + "chunk on the pipeline so each job stays inside the limit.");
                }

                // Reported as written because they are staged durably and will be uploaded at
                // commit. What Salesforce makes of them is decided later, which is exactly what
                // commitIsAsynchronous tells the engine.
                //
                // The details say what actually happened, because "wrote 1000 records" is not what
                // happened: nothing has left this process. A stage log entry that read like an
                // ordinary write would send somebody looking in the org for records still sitting
                // in a local file — and the job id is the handle to everything they would need
                // once the upload does happen.
                ObjectNode details = Json.newObject();
                details.put("phase", "STAGED");
                details.put("jobId", currentJobId);
                details.put("object", config.object());
                details.put("operation", config.operation().wireName);
                details.put("stagedRecords", stagedRecords);
                details.put("stagedBytes", size);
                return WriteResult.allWritten(batch.size(), batch.totalBytes())
                        .withDetails(details);

            } catch (IOException e) {
                throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                        "Could not stage records for " + config.describe() + ": " + e.getMessage(), e);
            }
        }

        private void ensureStaged(RecordBatch batch) throws IOException {
            if (printer != null) {
                return;
            }
            this.staged = Files.createTempFile("dmp-sfdc-" + context.runId() + "-", ".csv");
            this.headers = config.operation() == SalesforceConfig.Operation.DELETE
                    || config.operation() == SalesforceConfig.Operation.HARD_DELETE
                    ? List.of("Id")
                    : new ArrayList<>(fieldNames(batch.records().get(0)));

            this.writer = Files.newBufferedWriter(staged, StandardCharsets.UTF_8);

            // The separator must match what the job was told to expect. commons-csv defaults to
            // CRLF, and declaring LF while writing CRLF makes Salesforce fail the whole job with
            // "LineEnding is invalid on user data" — after the upload, so a hundred records are
            // staged, sent and lost before anything says why.
            this.printer = new CSVPrinter(writer, CSVFormat.Builder.create(CSVFormat.DEFAULT)
                    .setRecordSeparator("CRLF".equalsIgnoreCase(config.lineEnding()) ? "\r\n" : "\n")
                    .setHeader(headers.toArray(String[]::new)).get());
        }

        /** Uploads everything staged and tells Salesforce to start processing. */
        @Override
        public Preparation commit(Preparation preparation) {
            String jobId = preparation.state().path("jobId").asText();

            if (stagedRecords == 0) {
                // Nothing to upload. Closing an empty job would leave Salesforce processing a job
                // with no data and reporting it as failed, which is not what happened.
                deleteJob(session, "ingest", jobId, context);
                return Preparation.none();
            }

            closeStagedFile();
            session.putCsv(session.jobsUrl() + "/ingest/" + jobId + "/batches", staged,
                    "upload " + stagedRecords + " record(s)");

            session.patchJson(session.jobsUrl() + "/ingest/" + jobId,
                    "{\"state\":\"UploadComplete\"}", "close the ingest job");

            context.log().info("Salesforce ingest job {} uploaded {} record(s) and is processing",
                    jobId, stagedRecords);
            return preparation;
        }

        @Override
        public Preparation.Status checkCommit(Preparation commit) {
            String jobId = commit.state().path("jobId").asText(null);
            if (jobId == null) {
                return Preparation.Status.ready();
            }
            JsonNode job = session.getJson(session.jobsUrl() + "/ingest/" + jobId,
                    "check the ingest job");
            return statusOf(job, config, "ingest");
        }

        /**
         * {@inheritDoc}
         *
         * <p><b>Counts, never records.</b> Salesforce reports {@code numberRecordsProcessed} and
         * {@code numberRecordsFailed} in the same status document the engine already polls, so the
         * run's totals cost nothing and are the org's own numbers — the most authoritative
         * available.
         *
         * <p>The rejected rows are deliberately <em>not</em> pulled into the platform. They are
         * available from the org, on demand, through the results endpoint, and fetching them here
         * instead would mean copying customer data into a second store that then needs its own
         * redaction, retention and erasure story — to answer a question nobody had asked yet. A
         * chunk of a hundred and fifty thousand records is a file of several megabytes, downloaded
         * and parsed on every run whether or not anyone ever looks at it.
         *
         * <p>So: no dead-letter entries for this sink, and nothing to replay from the platform.
         * What exists instead is the job id, kept on the chunk, and Salesforce's own file behind
         * it for as long as the org keeps it.
         */
        @Override
        public Harvest harvest(Preparation commit) {
            String jobId = commit.state().path("jobId").asText(null);
            if (jobId == null) {
                return Harvest.none();
            }

            JsonNode job = session.getJson(session.jobsUrl() + "/ingest/" + jobId,
                    "read the job's record counts");
            long failed = job.path("numberRecordsFailed").asLong(0);
            long processed = job.path("numberRecordsProcessed").asLong(0);

            context.log().info("Salesforce job {} processed {} record(s), {} refused. "
                            + "The refused rows stay in the org — download them from the run's "
                            + "chunk view while Salesforce still holds them.",
                    jobId, processed, failed);

            return Harvest.countOnly(failed);
        }

        /**
         * {@inheritDoc}
         *
         * <p><b>Deliberately does not delete the job.</b> Deleting it destroys the failed-records
         * file, which is the only copy of what the org refused — the platform keeps none. Holding
         * the job costs nothing: Salesforce ages job data out on its own, and the ingest limit is
         * a rolling twenty-four-hour rate rather than a standing quota.
         *
         * <p>The consequence is that the file outlives the run only as long as the org keeps it,
         * roughly a week. That is Salesforce's window, not the platform's, and the console says so
         * rather than offering a download that silently returns nothing.
         */
        @Override
        public void release(Preparation preparation) {
            discardStagedFile();
        }

        @Override
        public void close() {
            closeStagedFile();
            discardStagedFile();
        }

        private void closeStagedFile() {
            try {
                if (printer != null) {
                    printer.close();
                    printer = null;
                }
                if (writer != null) {
                    writer.close();
                    writer = null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void discardStagedFile() {
            if (staged == null) {
                return;
            }
            try {
                Files.deleteIfExists(staged);
            } catch (IOException e) {
                context.log().warn("Could not delete the staged upload file {}", staged, e);
            }
            staged = null;
        }
    }

    // ----------------------------------------------------------------- shared

    /**
     * Reads a job's state.
     *
     * <p>A failed job is reported as failed rather than retried. Salesforce fails a whole job for
     * reasons that do not change on a second attempt — an object that does not exist, a field the
     * integration user cannot write — and the message it gives is the one somebody needs to see.
     */
    private static Preparation.Status statusOf(JsonNode job, SalesforceConfig config, String kind) {
        String state = job.path("state").asText("");

        return switch (state) {
            case "JobComplete" -> Preparation.Status.ready();
            case "Failed", "Aborted" -> Preparation.Status.failed(
                    "Salesforce " + kind + " job " + job.path("id").asText() + " ended as " + state
                            + (job.path("errorMessage").isMissingNode() ? ""
                            : ": " + job.path("errorMessage").asText()));
            default -> Preparation.Status.pending(Duration.ofSeconds(config.pollSeconds()));
        };
    }

    /** Best effort: a job that cannot be deleted is a tidiness problem, not a data problem. */
    private static void deleteJob(SalesforceSession session, String kind, String jobId,
                                  ConnectorContext context) {
        if (jobId == null || jobId.isBlank()) {
            return;
        }
        try {
            session.delete(session.jobsUrl() + "/" + kind + "/" + jobId, "delete job " + jobId);
        } catch (RuntimeException e) {
            context.log().warn("Could not delete Salesforce {} job {}; it will age out of the org",
                    kind, jobId);
        }
    }

    private static Set<String> fieldNames(DataRecord record) {
        Set<String> names = new LinkedHashSet<>();
        record.payload().fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static String asText(JsonNode value) {
        return value.isValueNode() ? value.asText() : value.toString();
    }

    private static JsonNode configSchema() {
        ObjectNode properties = Json.newObject();
        // The field that decides whether a run touches the sandbox or production, which makes it
        // the single most valuable one to move out of the stored definition: name the variable and
        // the same pipeline is promoted between the two rather than edited — and edited wrongly is
        // how a test run lands in a live org.
        properties.set("loginUrl", ConfigFields.fromEnvironment(ConfigFields.field("string",
                "https://login.salesforce.com for production, or a sandbox host such as "
                        + "https://test.salesforce.com. Naming the variable is what keeps one "
                        + "pipeline usable against both.")));
        properties.set("apiVersion", ConfigFields.advanced(ConfigFields.field("string",
                "REST API version, e.g. v62.0. A version the org does not have authenticates "
                        + "fine and then fails at the first call, so it is checked by Test.")));
        properties.set("object", ConfigFields.field("string",
                "The sObject to write, e.g. Account. A source derives it from the query."));
        properties.set("soql", ConfigFields.recordKeyField(ConfigFields.sourceField("string",
                "The query to run, e.g. SELECT Id, Name FROM Account. Include Id — it is what "
                        + "record search and the dead-letter queue identify a record by."), "Id"));
        properties.set("operation", ConfigFields.sinkEnumField(
                "How records are applied. Only upsert is idempotent, and only because it matches "
                        + "on a field your data owns rather than on an id Salesforce assigns.",
                "insert", "update", "upsert", "delete", "hardDelete"));
        properties.set("externalIdField", ConfigFields.sinkField("string",
                "The field an upsert matches on. Required for upsert and ignored otherwise."));
        properties.set("lineEnding", ConfigFields.advanced(ConfigFields.enumField(
                "Line ending Salesforce should expect in the uploaded CSV.", "LF", "CRLF")));
        properties.set("pollSeconds", ConfigFields.advanced(ConfigFields.field("integer",
                "How often to ask Salesforce whether the job has finished. Defaults to 5.")));
        properties.set("collectRecordResults", ConfigFields.advanced(ConfigFields.sinkField("boolean",
                "Download the job's failed-records file so each rejection is captured and can be "
                        + "replayed. Off by default — the failure count comes free from the status "
                        + "poll, while this downloads every rejected row. Worth turning on when a "
                        + "few records fail among many; not when the whole job fails for one reason.")));

        ObjectNode schema = Json.newObject();
        schema.put("type", "object");
        schema.set("properties", properties);
        schema.putArray("required").add("loginUrl");
        return schema;
    }
}
