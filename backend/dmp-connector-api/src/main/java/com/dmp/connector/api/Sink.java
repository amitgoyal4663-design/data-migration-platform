package com.dmp.connector.api;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** A connector that writes. */
public interface Sink extends Connector {

    /** Opens a write session. See {@link Source#openSource} on the naming. */
    SinkSession openSink(ConnectorContext context);

    /**
     * Streams a result file the destination is holding for a completed job.
     *
     * <p>For a destination that decides asynchronously and keeps its own record of what it refused.
     * The platform does not copy that record: pulling every rejected row into a second store — with
     * its own redaction, retention and erasure story — to answer a question nobody has asked yet is
     * a cost paid on every run, by every pipeline, for the few that are ever investigated. This
     * fetches it when somebody actually asks.
     *
     * <p>The file belongs to the destination and so does its lifetime. Salesforce keeps job results
     * for about a week; after that this returns empty and that is a normal answer, not a failure —
     * the counts the platform stored are permanent, the file behind them is not.
     *
     * @param job  the handle the engine kept on the chunk, as returned by {@code prepare}
     * @param kind which file, in the destination's own vocabulary — {@code failed} or
     *             {@code successful}
     * @return the file as the destination serves it, or empty when it no longer has one
     */
    default java.util.Optional<ResultFile> fetchResults(ConnectorContext context, Preparation job,
                                                        String kind) {
        return java.util.Optional.empty();
    }

    /**
     * A result file as the destination serves it.
     *
     * @param filename what to call it when it is downloaded
     * @param mediaType its content type, so a browser does the right thing with it
     * @param content the bytes, unaltered — this is the destination's own record and reformatting
     *                it would make it something the platform had produced instead
     */
    record ResultFile(String filename, String mediaType, byte[] content) {
    }

    /**
     * A write session.
     *
     * <p>The lifecycle mirrors the source: {@code prepare} → {@code write} repeatedly →
     * {@code commit} → poll {@code checkCommit} → {@code release}. Synchronous sinks inherit the
     * defaults and implement only {@code write}.
     */
    /**
     * Whether dividing a batch into smaller calls means anything to this sink.
     *
     * <p>Most sinks say yes, and gain something real by it: a batch of a thousand rows that fails
     * as a unit tells you a thousand rows failed, while the same rows delivered singly tell you it
     * was row 417 and let the other 999 through. That is a dead-letter-queue capability rather than
     * a tuning knob.
     *
     * <p>A sink says no when its unit of work is the chunk rather than the call. One Salesforce
     * bulk job is one chunk however it is fed — records are appended to a local file and the job is
     * submitted once at commit — so delivering them one at a time changes nothing except to make
     * the setting look as though it did something.
     *
     * <p>Declared here rather than on {@link SinkSession} because publish-time validation asks it,
     * and validation must not open a connection: a pipeline has to be publishable while its
     * destination is down.
     *
     * <p>A default method rather than a component on {@link Capabilities}: connectors are built
     * outside this repository and loaded without a platform rebuild (ADR-0006), and adding a
     * component to a record breaks every one of them at the constructor. A default they can ignore
     * does not.
     */
    default boolean supportsPerRecordDelivery() {
        return true;
    }

    interface SinkSession extends AutoCloseable {

        Capabilities capabilities();

        /**
         * Writes one batch.
         *
         * <p>Partial success is normal and expected: a hundred rows land, three violate a
         * constraint. Report the failures in {@link WriteResult} rather than throwing — one
         * malformed record must not fail a migration of a million. Throw only when the batch as a
         * whole could not be attempted.
         */
        WriteResult write(RecordBatch batch);

        default Preparation prepare() {
            return Preparation.none();
        }

        /**
         * Signals that no more data is coming for this chunk.
         *
         * <p>For an asynchronous sink this begins remote processing rather than completing it — see
         * {@link Capabilities#commitIsAsynchronous()}.
         */
        default Preparation commit(Preparation preparation) {
            return preparation;
        }

        /** Polled after {@code commit} when the sink declares asynchronous commit. */
        default Preparation.Status checkCommit(Preparation commit) {
            return Preparation.Status.ready();
        }

        /**
         * Asks what the destination made of the work after accepting it.
         *
         * <p>Returns a count and, where the sink can supply them, the individual rejections. The two
         * are one answer rather than two calls because they must never disagree: a count taken from
         * one source and a list from another is a reconciliation problem waiting to be discovered
         * during an incident.
         *
         * <p><b>A sink may legitimately know the count but not the detail.</b> Salesforce reports
         * "5,000 records failed" in the status it is already polled with, and names <em>which</em>
         * five thousand only in a separate results file that costs a full download of every rejected
         * row. When a pipeline is not going to keep those payloads, fetching them to count them is
         * pure waste — so a sink is allowed to answer {@link Harvest#countOnly(long)}, and the
         * engine reports an accurate number while storing nothing.
         */
        default Harvest harvest(Preparation commit) {
            return Harvest.none();
        }

        /** Idempotent; called on the happy path and again by the reaper. */
        default void release(Preparation preparation) {
        }

        @Override
        default void close() {
        }
    }

    /**
     * What a sink can do, so the engine can adapt rather than assume.
     *
     * <p>Answered by the <em>session</em>, not the connector class, and therefore after the
     * instance's configuration is known. That matters: idempotency is usually a property of how a
     * sink was configured rather than of what it is. The same MongoDB connector is idempotent in
     * upsert mode and not in insert mode, and one Salesforce instance with an External ID field
     * configured is idempotent while another without one is not.
     *
     * @param writeIsIdempotent    writing the same record twice leaves the target as if it had been
     *                             written once. The single property the engine needs, deliberately
     *                             stated without reference to how any sink achieves it — an upsert
     *                             key, a deterministic object key, an idempotency header and a
     *                             compacted topic are four mechanisms for the same guarantee, and
     *                             the engine must not know which one it is talking to
     * @param idempotencyAdvice    when writes are not idempotent, how to make them so — or why they
     *                             cannot be. Required, because the engine can only report the
     *                             boolean and "switch to UPSERT" is meaningless advice to give the
     *                             owner of an S3 bucket or an append-only file. Only the connector
     *                             knows the remedy, so only the connector may word it. Null when
     *                             writes are already idempotent
     * @param transactional        a batch lands entirely or not at all. Not the same as idempotent
     *                             and never a substitute for it: an atomic batch of inserts, re-sent
     *                             after a crash, duplicates just as cleanly as a non-atomic one
     * @param commitIsAsynchronous a successful {@code write} does not mean the records landed
     * @param stagesToDisk         batches are streamed to disk rather than held in heap
     * @param maxBatchSize         protocol limit on records per write; 0 means none
     * @param preferredBatchSize   what this sink performs best with
     */
    record Capabilities(
            boolean writeIsIdempotent,
            String idempotencyAdvice,
            boolean transactional,
            boolean commitIsAsynchronous,
            boolean stagesToDisk,
            /**
             * Whether a whole batch leaves as one payload the user could shape.
             *
             * <p>True for an HTTP endpoint receiving one request body; false for a database or a
             * file, which writes each record on its own and has nothing to apply an envelope to.
             *
             * <p>The engine needs this to refuse a pipeline whose batch transform could never take
             * effect. Without it such a pipeline runs happily and changes nothing, which is
             * indistinguishable from a broken script.
             */
            boolean sendsBatchAsSinglePayload,
            int maxBatchSize,
            int preferredBatchSize) {

        private static final String NO_ADVICE_GIVEN =
                "A sink that declares writes are not idempotent must also say how to make them so, "
                        + "or why it cannot be done. The engine can report the flag but not the "
                        + "remedy — advice like \"use upsert mode\" is meaningless for an "
                        + "append-only file or an object store, and only this connector knows what "
                        + "its user should actually change.";

        public Capabilities {
            if (writeIsIdempotent) {
                // Advice describes a problem this sink does not have. Dropped rather than carried,
                // so nothing downstream can display a remedy for a sink that needs none.
                idempotencyAdvice = null;
            } else if (idempotencyAdvice == null || idempotencyAdvice.isBlank()) {
                throw new IllegalArgumentException(NO_ADVICE_GIVEN);
            }
        }

        /** Writes are already idempotent; no advice applies. */
        public static Capabilities idempotent(int preferredBatchSize) {
            return new Capabilities(true, null, true, false, false, false, 0, preferredBatchSize);
        }

        /** Writes are not idempotent, for the stated reason. */
        public static Capabilities atLeastOnce(String idempotencyAdvice, int preferredBatchSize) {
            return new Capabilities(false, idempotencyAdvice, false, false, false, false,
                    0, preferredBatchSize);
        }

        /**
         * Delivery guarantee this sink can support, reported to the user on the pipeline itself.
         *
         * <p>Worth surfacing because no competitor does: users otherwise read documentation and
         * guess whether a retry will duplicate their data.
         */
        public String deliveryGuarantee() {
            if (transactional && writeIsIdempotent) {
                return "Exactly-once — batches are atomic and repeating a write changes nothing";
            }
            if (writeIsIdempotent) {
                return "Effectively-once — repeating a write leaves the target as it was";
            }
            return "At-least-once — a retry after partial success may duplicate records";
        }

        /** How to make writes idempotent, or why it is not possible. Empty when they already are. */
        public Optional<String> advice() {
            return Optional.ofNullable(idempotencyAdvice);
        }
    }

    /**
     * The outcome of one batch.
     *
     * @param written      records the sink accepted
     * @param failed       records the sink rejected, detailed in {@code errors}
     * @param bytesWritten approximate, for throughput metrics
     */
    /**
     * What a destination made of the records it accepted.
     *
     * <p>{@code failedCount} is authoritative and {@code errors} may be shorter than it — empty,
     * even. That asymmetry is the point: the run's totals stay correct whether or not anybody is
     * keeping the rejected records.
     *
     * @param failedCount how many records the destination refused after accepting them
     * @param errors      those rejections individually, or empty if this sink was not asked for
     *                    them or cannot produce them
     */
    record Harvest(long failedCount, List<RecordError> errors) {

        public Harvest {
            errors = errors == null ? List.of() : List.copyOf(errors);
            if (failedCount < errors.size()) {
                // The list is evidence; a count below it would mean discarding rejections we
                // are holding in our hand.
                failedCount = errors.size();
            }
        }

        /** Nothing was refused. */
        public static Harvest none() {
            return new Harvest(0, List.of());
        }

        /** Every rejection, individually — the count follows from the list. */
        public static Harvest of(List<RecordError> errors) {
            return new Harvest(errors == null ? 0 : errors.size(), errors);
        }

        /** How many were refused, with no detail about which. */
        public static Harvest countOnly(long failedCount) {
            return new Harvest(failedCount, List.of());
        }

        /** Whether the destination named the records it refused. */
        public boolean hasDetail() {
            return !errors.isEmpty();
        }
    }

    record WriteResult(int written, int failed, long bytesWritten, List<RecordError> errors,
                       com.fasterxml.jackson.databind.JsonNode details) {

        public WriteResult {
            errors = List.copyOf(errors == null ? List.of() : errors);
        }

        /** Four-argument form, for a sink with nothing to report beyond the counts. */
        public WriteResult(int written, int failed, long bytesWritten, List<RecordError> errors) {
            this(written, failed, bytesWritten, errors, null);
        }

        public static WriteResult allWritten(int count, long bytes) {
            return new WriteResult(count, 0, bytes, List.of());
        }

        public static WriteResult partial(int written, long bytes, List<RecordError> errors) {
            return new WriteResult(written, errors.size(), bytes, errors);
        }

        /**
         * The same result, carrying what the destination said about the call.
         *
         * <p>A bulk job id, an HTTP status, a partition and offset, matched-and-modified counts.
         * Whatever a person debugging <em>this</em> destination would want, which is why it is a
         * free-form node rather than a schema: one wide enough for every target would describe
         * none of them.
         *
         * <p>Reaches the call log only where the pipeline asks for it, and nowhere else — it is
         * not part of any count and no control flow reads it. A connector may therefore report
         * generously without wondering what it costs.
         */
        public WriteResult withDetails(com.fasterxml.jackson.databind.JsonNode newDetails) {
            return new WriteResult(written, failed, bytesWritten, errors, newDetails);
        }

        public boolean hasFailures() {
            return failed > 0;
        }
    }

    /**
     * One rejected record.
     *
     * <p>Carries the payload so the dead-letter entry is actionable rather than a bare error code.
     * Payloads are redacted according to the pipeline's audit policy before being persisted.
     *
     * @param seq     position within the chunk, so the record can be located exactly
     * @param key     natural key, if the record had one
     * @param code    external system's error code, verbatim
     * @param message external system's message, verbatim
     * @param payload the record as the sink received it
     */
    record RecordError(long seq, String key, String code, String message,
                       com.fasterxml.jackson.databind.JsonNode payload) {
    }

    /** Delay before retrying a batch the sink rejected as retryable. */
    Duration DEFAULT_RETRY_DELAY = Duration.ofSeconds(30);
}
