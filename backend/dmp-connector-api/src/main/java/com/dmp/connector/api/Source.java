package com.dmp.connector.api;

import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/** A connector that reads. */
public interface Source extends Connector {

    /**
     * Names of the values this configuration expects to be supplied per run.
     *
     * <p>Lets the console ask for exactly the right boxes when a run is started, without knowing
     * anything about the connector — a Databricks source reads {@code :from} out of its SQL, a
     * future one might read them from somewhere else entirely, and neither requires a frontend
     * change.
     *
     * <p>Deliberately static: it takes the stored configuration rather than a session, so answering
     * costs no connection, resolves no credential and can be asked while somebody is looking at a
     * page. Most sources have no parameters and inherit the empty answer.
     */
    default java.util.Set<String> parameterNames(JsonNode config) {
        return java.util.Set.of();
    }

    /**
     * Opens a read session.
     *
     * <p>Named distinctly from {@link Sink#openSink} because Java erases the parameter types and a
     * single class could not otherwise implement both. Most connectors are symmetric — a PostgreSQL
     * connection reads and writes equally well — so making that expressible in one class matters
     * more than the shorter name.
     */
    SourceSession openSource(ConnectorContext context);

    /**
     * A read session.
     *
     * <p>The lifecycle is: {@code prepare} → poll {@code checkPreparation} until ready →
     * {@code plan} → {@code read} each chunk → {@code release}. Connectors that can read
     * immediately inherit the defaults for the asynchronous parts and implement only {@code plan}
     * and {@code read}.
     */
    interface SourceSession extends AutoCloseable {

        /**
         * Whether this source can read "everything after key K, in order" with no upper bound.
         *
         * <p>Opt-in, because it is only true of sources that are one continuously ordered thing —
         * a collection, a table. A file source needs to be told which file and a topic source needs
         * to be told which partition; handing either an open-ended spec would give it nothing to
         * read from.
         *
         * <p>Where it is true, a sequential run can dispense with planning altogether: chunks are
         * generated one at a time as each finishes, so nothing is counted, chunk sizes follow the
         * data instead of arithmetic over a key range, and rows that arrive mid-run are picked up
         * rather than falling beyond a boundary frozen at planning time.
         */
        default boolean supportsCursorPagination() {
            return false;
        }

        /**
         * Divides the work into chunks that can be executed in parallel and resumed independently.
         *
         * <p>Two properties are required of the result and neither is optional.
         *
         * <p><b>Determinism.</b> Planning the same source twice must produce the same chunks, or a
         * resumed run will read ranges it already processed and miss ranges it never did.
         *
         * <p><b>Stable references only.</b> A chunk spec must contain a key range, an offset or an
         * index — never a resolved URL or an expiring locator. Chunk four hundred may be claimed
         * forty minutes after planning, by which time a pre-signed link has expired. That mistake
         * passes every test with three chunks and fails in production.
         *
         * @param request carries the target rows per chunk — a hint, not a requirement. A source
         *                that cannot be divided returns a single chunk and says so honestly.
         */
        List<SplitSpec> plan(Preparation preparation, PlanRequest request);

        /**
         * Opens a cursor over one chunk, resuming from {@code fromCursor} if it is non-empty.
         *
         * <p>{@code fetchSize} is the source round-trip size, which is deliberately separate from
         * the sink's batch size — a JDBC cursor and an Elasticsearch bulk request want very
         * different numbers, and one setting would be wrong for one of them.
         */
        RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize);

        /** Submits any external job. Returns immediately; never blocks. */
        default Preparation prepare() {
            return Preparation.none();
        }

        /** Polled by the engine between {@code prepare} and {@code plan}. */
        default Preparation.Status checkPreparation(Preparation preparation) {
            return Preparation.Status.ready();
        }

        /**
         * Releases external resources.
         *
         * <p>Must be idempotent: it is called on the happy path and again by a reaper that sweeps
         * runs which died holding resources. Treating an already-released handle as an error would
         * turn that safety net into a source of noise.
         */
        default void release(Preparation preparation) {
            // Nothing to release for most connectors.
        }

        @Override
        default void close() {
        }
    }

    /**
     * What the engine asks for when planning.
     *
     * <p>Carries a target chunk <em>size</em> rather than a chunk count. A count has to be guessed
     * before anything is known about how the data is distributed, and an unlucky guess produces
     * chunks that differ by an order of magnitude — one pod grinding long after the rest have
     * finished. A size produces however many chunks the data actually warrants, and the pull loop
     * balances them by itself.
     *
     * @param targetRowsPerChunk rows one chunk should cover
     * @param maxChunks          safety ceiling, so a badly configured size cannot plan a million chunks
     */
    record PlanRequest(int targetRowsPerChunk, int maxChunks) {


        public PlanRequest {
            targetRowsPerChunk = Math.max(1, targetRowsPerChunk);
            maxChunks = maxChunks <= 0 ? 100_000 : maxChunks;
        }
    }

    /**
     * A pull-based cursor over one chunk.
     *
     * <p>Pull rather than push so back-pressure needs no protocol: when the sink is slow the engine
     * stops calling {@code next()}, and the source stops reading. Nothing buffers without bound and
     * no connector has to implement a flow-control scheme.
     */
    interface RecordStream extends AutoCloseable {

        /** Advances the cursor. Returns null when the chunk is exhausted. */
        DataRecord next();

        /**
         * The resume position immediately after the last record returned.
         *
         * <p>Read by the engine only after a batch has been durably accepted by the sink. Returning
         * a position ahead of what has actually been handed out would silently skip records on
         * resume — the one failure this whole mechanism exists to prevent.
         */
        JsonNode cursor();

        /**
         * What this stream is actually asking the source for, in that source's own language.
         *
         * <p>A Mongo filter, a SQL statement, a SOQL query, a request URL. Recorded in the call log
         * when a pipeline switches source-fetch logging on, and shown to whoever is asking the
         * commonest question in any migration: <em>why did this run move nothing?</em> The answer
         * is almost always the query, and until this existed the query was assembled inside a
         * connector and then discarded — leaving the person who needed it to guess.
         *
         * <p><b>Never include values that could be secret or personal.</b> Bind parameters belong
         * as placeholders. This string is written to a search index, and a source's own query text
         * is not exempt from the redaction the rest of the platform obeys.
         *
         * <p>Defaults to null so a connector built outside this repository keeps working; it loses
         * one field of one optional log, not the ability to run.
         */
        default String describe() {
            return null;
        }

        /**
         * The real interactions with the source since this was last called, and clears them.
         *
         * <p><b>Why the connector has to say.</b> The engine cannot count these. It sees records
         * arriving from {@link #next()} and nothing else — paging, statement execution and link
         * following all happen inside the connector — so the only boundary it can observe is the
         * batch it is filling. That made the read log batch-shaped while everybody reading it took
         * it to be source-shaped: one thousand rows pulled in a single call, buffered into two
         * batches of five hundred, appeared as two read entries carrying the same query, and a
         * developer debugging it had no way to tell that from the query genuinely running twice.
         *
         * <p>Drained rather than pushed so a connector needs no reference to the engine and nothing
         * blocks: record a {@link Fetch} where the call is made, and the engine collects them at
         * each batch boundary. Draining must be cheap and must not perform I/O.
         *
         * <p>Defaults to nothing, so a connector that has not been taught this still reads normally
         * — it forfeits the fetch entries, not the ability to run.
         */
        default List<Fetch> drainFetches() {
            return List.of();
        }

        @Override
        void close();
    }

    /**
     * One real interaction with the source: a statement executed, a page pulled, a link followed.
     *
     * <p>Distinct from a read window, and the distinction is the point. A read window is the
     * engine's unit — however much reading it took to fill one batch. This is the source's unit,
     * and it is the one that answers how many times the remote system was actually asked.
     *
     * @param reason    why this call was made, in a few plain words — "read result chunk 5",
     *                  "read the result schema", "follow the external link". The field people
     *                  actually want and the one the log did not have: a URL says what was
     *                  requested and never why, so a chunk showing two fetches left a reader
     *                  guessing whether the second was a retry, a page, or a different resource
     *                  altogether. Most connectors already build this string for their error
     *                  messages and then discard it
     * @param describe  what was asked, in the source's own language — a URL, a SQL statement, a
     *                  filter. Bind values belong as placeholders: this is written to a search
     *                  index and is not exempt from redaction.
     * @param startedAt when the call was made, so the entry lands in the log where it happened
     *                  rather than where it was collected
     * @param durationMillis how long the remote system took, which is the number that distinguishes
     *                  a slow source from a slow script
     * @param rows      rows the call returned, before any of them were read
     * @param bytes     what it weighed on the wire, or 0 if the connector cannot tell
     * @param request   the request body, or null. Only recorded when the pipeline captures bodies
     * @param response  what came back, or null. Same condition
     * @param errorCode a short code when the call failed, or null when it succeeded
     * @param errorMessage why it failed, or null
     */
    record Fetch(
            String reason,
            String describe,
            java.time.Instant startedAt,
            long durationMillis,
            long rows,
            long bytes,
            JsonNode request,
            JsonNode response,
            String errorCode,
            String errorMessage) {

        /** The ordinary case: a call that worked. */
        public static Fetch ok(String reason, String describe, java.time.Instant startedAt,
                               long durationMillis, long rows, long bytes) {
            return new Fetch(reason, describe, startedAt, durationMillis, rows, bytes, null, null,
                    null, null);
        }

        /** A call that did not come back, which is the entry worth having most. */
        public static Fetch failed(String reason, String describe, java.time.Instant startedAt,
                                   long durationMillis, String errorCode, String errorMessage) {
            return new Fetch(reason, describe, startedAt, durationMillis, 0, 0, null, null,
                    errorCode, errorMessage);
        }

        public boolean succeeded() {
            return errorCode == null;
        }
    }

    /**
     * One unit of parallel work, defined by the connector that produced it.
     *
     * <p>The engine never interprets {@code spec}; it stores it, hands it back to {@code read},
     * and uses it for nothing else.
     *
     * @param id    stable within a run; the engine orders chunks by it
     * @param spec  a key range, a partition, a file path, a result-set index
     * @param label short human-readable description, shown in the console
     * @param rows  how many rows this chunk covers, or 0 when the connector cannot say
     */
    record SplitSpec(int id, JsonNode spec, String label, long rows) {

        public SplitSpec {
            spec = Json.orEmpty(spec);
            label = label == null || label.isBlank() ? "chunk " + id : label;
            rows = Math.max(0, rows);
        }

        /**
         * A chunk whose size the connector does not know.
         *
         * <p>Which is the honest answer for a key range over unknown data — "ids 1 to 1000" is
         * not a thousand rows if nine hundred were deleted — and the engine falls back to the
         * sink's preferred batch, as it always did.
         */
        public SplitSpec(int id, JsonNode spec, String label) {
            this(id, spec, label, 0);
        }

        /**
         * Whether this chunk knows its own size.
         *
         * <p>The reason it matters is the batch. A chunk that knows it holds a thousand rows is
         * one batch of a thousand; one that does not falls back to whatever the destination
         * prefers, which is how a thousand-row chunk came to be read and written twice in five
         * hundreds — with the log showing the source query twice and nothing saying it had run
         * once.
         */
        public boolean knowsItsSize() {
            return rows > 0;
        }

        /** For sources that cannot be divided — a cursor-paged REST API, a single small file. */
        public static SplitSpec single() {
            return new SplitSpec(0, Json.emptyObject(), "whole source");
        }

        /**
         * Everything after wherever the previous chunk stopped, with no upper bound.
         *
         * <p>Used when chunks are generated as the run proceeds rather than planned in advance.
         * Carries no range of its own: the position comes entirely from the checkpoint's cursor,
         * which is what makes each chunk pick up exactly where the last one left off — including
         * rows that arrived after the run started.
         */
        public static SplitSpec openEnded(int id) {
            return new SplitSpec(id, Json.emptyObject(), "from where chunk " + (id - 1) + " stopped");
        }
    }
}
