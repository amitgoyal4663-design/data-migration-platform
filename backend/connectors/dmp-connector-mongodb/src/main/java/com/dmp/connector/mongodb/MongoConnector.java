package com.dmp.connector.mongodb;

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
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Reads and writes MongoDB collections.
 *
 * <p>Splits by ranges of {@code _id}. Ranges rather than {@code skip},
 * because {@code skip(400000)} makes the server walk and discard four hundred thousand documents to
 * reach the next page — a job would get quadratically slower as it progressed. A range seeks
 * straight to its start through the index.
 *
 * <p>{@code ObjectId} sorts by creation time, so ranges over it divide a collection along the axis
 * documents were actually written — which is usually the axis that matters.
 */
public class MongoConnector implements Source, Sink {

    private static final String TYPE = "mongodb";

    /** The only field a collection is ordered and resumed by. See {@link MongoConfig#splitField()}. */
    private static final String ID_FIELD = "_id";

    @Override
    public ConnectorSpec spec() {
        return new ConnectorSpec(
                TYPE,
                "MongoDB",
                "Reads and writes MongoDB collections. Splits a collection by _id so a large "
                        + "migration runs in parallel and resumes where it stopped.",
                ConnectorSpec.Direction.BOTH,
                configSchema(),
                Set.of("username", "password"),
                "1.0.0");
    }

    @Override
    public void testConnection(ConnectorContext context) {
        MongoConfig config = MongoConfig.from(context);
        try (MongoClient client = connect(config, context)) {
            // A real query against the configured collection, not just a ping. A test that passes
            // while the collection is missing produces confidence the first run contradicts.
            collection(client, config).find().limit(1).first();
        } catch (MongoException e) {
            throw translate(e, "Could not reach " + config.describe());
        }
    }

    // ------------------------------------------------------------------ source

    /**
     * The {@code ":name"} placeholders in the configured filter, so the console can ask for them.
     *
     * <p>They live in {@code filter} because that is the part of a Mongo read that changes per run
     * — the collection and the key field do not.
     */
    @Override
    public java.util.Set<String> listParameterNames(JsonNode config) {
        return FilterParameters.listsIn(filterJson(config));
    }

    @Override
    public java.util.Set<String> parameterNames(JsonNode config) {
        return FilterParameters.referencedBy(filterJson(config));
    }

    /**
     * The filter as JSON text, whether it was written as a string or as an object.
     *
     * <p>It was a string because the console renders a text box from the connector's schema. A
     * named query is written by hand, and written by hand nobody escapes a JSON document into a
     * string — so both are accepted, and the difference stops mattering anywhere else.
     */
    private static String filterJson(JsonNode config) {
        JsonNode filter = config == null ? null : config.get("filter");
        if (filter == null || filter.isNull()) {
            return null;
        }
        return filter.isObject() || filter.isArray() ? filter.toString() : filter.asText();
    }

    @Override
    public SourceSession openSource(ConnectorContext context) {
        MongoConfig config = MongoConfig.from(context);

        return new SourceSession() {

            @Override
            public boolean supportsCursorPagination() {
                // The read already filters on "$gt cursor" and sorts by the split field, so an
                // empty spec plus a cursor is exactly "everything after here, in order".
                return true;
            }

            @Override
            public List<SplitSpec> plan(Preparation preparation, PlanRequest request) {
                try (MongoClient client = connect(config, context)) {
                    MongoCollection<Document> source = collection(client, config);
                    Bson filter = config.filter(context);

                    Document lowest = source.find(filter)
                            .sort(Sorts.ascending(config.splitField())).limit(1).first();
                    Document highest = source.find(filter)
                            .sort(Sorts.descending(config.splitField())).limit(1).first();

                    if (lowest == null || highest == null) {
                        context.log().info("{} is empty; nothing to plan", config.describe());
                        return List.of();
                    }

                    Object min = lowest.get(config.splitField());
                    Object max = highest.get(config.splitField());

                    long total = source.countDocuments(filter);
                    int chunks = (int) Math.max(1,
                            Math.min(request.maxChunks(),
                                    (total + request.targetRowsPerChunk() - 1) / request.targetRowsPerChunk()));

                    List<SplitSpec> splits = planRanges(min, max, chunks, config, context);
                    context.log().info("Planned {} chunk(s) over {} document(s) in {}",
                            splits.size(), total, config.describe());
                    return splits;

                } catch (MongoException e) {
                    throw translate(e, "Could not plan chunks for " + config.describe());
                }
            }

            @Override
            public RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize) {
                return new MongoRecordStream(config, context, split, fromCursor, fetchSize);
            }
        };
    }

    /**
     * Divides the key range into chunks.
     *
     * <p>ObjectId and numbers are divided arithmetically; anything else falls back to a single
     * chunk. A string key could be split lexicographically, but only by assuming a distribution
     * that is usually wrong — and badly skewed chunks are worse than one honest chunk.
     */
    private static List<SplitSpec> planRanges(Object min, Object max, int chunks,
                                              MongoConfig config, ConnectorContext context) {
        if (chunks <= 1) {
            return List.of(SplitSpec.single());
        }

        if (min instanceof ObjectId lowest && max instanceof ObjectId highest) {
            return objectIdSplits(lowest, highest, chunks, config);
        }
        if (min instanceof Number lowest && max instanceof Number highest) {
            return numericSplits(lowest.longValue(), highest.longValue(), chunks, config);
        }

        context.log().info(
                "_id is a {} in {}, and only ObjectId and numeric ids can be divided into ranges "
                        + "arithmetically, so this reads as a single chunk. Correct, and slower "
                        + "than a parallel read — splitting strings would need a guess at their "
                        + "distribution, and badly skewed chunks are worse than one honest one.",
                min.getClass().getSimpleName(), config.describe());
        return List.of(SplitSpec.single());
    }

    /**
     * Splits the ObjectId range arithmetically, over all twelve bytes.
     *
     * <p>Deliberately not over the embedded timestamp alone. An ObjectId's first four bytes are a
     * Unix second, and splitting on those works only for a collection written gradually — a bulk
     * load puts every document in the same second, which collapses to a single chunk. Bulk-loaded
     * collections are precisely the ones people migrate, so that assumption failed on the common
     * case rather than an edge one.
     *
     * <p>Treating the whole id as a 96-bit integer divides evenly regardless of how the collection
     * was written. The counter bytes vary within a second, so the boundaries land in sensible
     * places either way.
     */
    private static List<SplitSpec> objectIdSplits(ObjectId min, ObjectId max, int chunks,
                                                  MongoConfig config) {
        BigInteger low = toBigInteger(min);
        BigInteger high = toBigInteger(max);
        BigInteger span = high.subtract(low).add(BigInteger.ONE);

        if (span.compareTo(BigInteger.valueOf(chunks)) <= 0) {
            // Fewer distinct ids than requested chunks. Genuinely nothing to divide.
            return List.of(SplitSpec.single());
        }

        BigInteger step = span.divide(BigInteger.valueOf(chunks)).max(BigInteger.ONE);

        List<SplitSpec> splits = new ArrayList<>();
        BigInteger lower = low;
        int id = 0;

        while (lower.compareTo(high) <= 0) {
            BigInteger upper = lower.add(step).subtract(BigInteger.ONE).min(high);

            ObjectNode spec = Json.newObject();
            spec.put("fromOid", BsonValues.markObjectId(toObjectId(lower)));
            spec.put("toOid", BsonValues.markObjectId(toObjectId(upper)));
            splits.add(new SplitSpec(id, spec, "_id range " + id));

            lower = upper.add(BigInteger.ONE);
            id++;
        }
        return splits;
    }

    /** An ObjectId's twelve bytes as an unsigned 96-bit integer. */
    private static BigInteger toBigInteger(ObjectId id) {
        return new BigInteger(1, id.toByteArray());
    }

    /** The inverse, left-padded so the value always occupies exactly twelve bytes. */
    private static ObjectId toObjectId(BigInteger value) {
        byte[] raw = value.toByteArray();
        byte[] twelve = new byte[12];

        // BigInteger.toByteArray may prepend a sign byte or return fewer than twelve; copy the
        // low-order bytes into the right-hand end so the numeric value is preserved exactly.
        int copy = Math.min(raw.length, 12);
        System.arraycopy(raw, raw.length - copy, twelve, 12 - copy, copy);
        return new ObjectId(twelve);
    }

    private static List<SplitSpec> numericSplits(long min, long max, int chunks, MongoConfig config) {
        long span = max - min + 1;
        long step = Math.max(1, span / chunks);

        List<SplitSpec> splits = new ArrayList<>();
        long lower = min;
        int id = 0;

        while (lower <= max) {
            long upper = Math.min(max, lower + step - 1);

            ObjectNode spec = Json.newObject();
            spec.put("from", lower);
            spec.put("to", upper);
            splits.add(new SplitSpec(id, spec, config.splitField() + " " + lower + "–" + upper));

            lower = upper + 1;
            id++;
        }
        return splits;
    }

    /** A cursor over one chunk, held open and streamed rather than materialised. */
    private static final class MongoRecordStream implements RecordStream {

        private final MongoClient client;
        private final MongoCursor<Document> cursor;
        private final String splitField;
        private final String description;

        private JsonNode resumePoint;
        private long emitted;

        MongoRecordStream(MongoConfig config, ConnectorContext context, SplitSpec split,
                          JsonNode fromCursor, int fetchSize) {
            this.splitField = config.splitField();
            this.resumePoint = fromCursor == null ? Json.emptyObject() : fromCursor;

            try {
                this.client = connect(config, context);
                List<Bson> conditions = new ArrayList<>();
                conditions.add(config.filter(context));

                if (split.spec().hasNonNull("fromOid")) {
                    conditions.add(Filters.gte(splitField,
                            BsonValues.boundary(split.spec().get("fromOid"))));
                    conditions.add(Filters.lte(splitField,
                            BsonValues.boundary(split.spec().get("toOid"))));
                } else if (split.spec().hasNonNull("from")) {
                    conditions.add(Filters.gte(splitField,
                            BsonValues.boundary(split.spec().get("from"))));
                    conditions.add(Filters.lte(splitField,
                            BsonValues.boundary(split.spec().get("to"))));
                }

                // Everything strictly after the last document the sink accepted.
                if (fromCursor != null && fromCursor.hasNonNull("after")) {
                    conditions.add(Filters.gt(splitField,
                            BsonValues.boundary(fromCursor.get("after"))));
                }

                Bson query = Filters.and(conditions);

                // Rendered once, here, while the filter is still assembled. This is the answer to
                // "why did this chunk return nothing?", and until the call log existed it was
                // built on this line and then thrown away.
                this.description = "db." + config.describe() + ".find("
                        + query.toBsonDocument(Document.class,
                                com.mongodb.MongoClientSettings.getDefaultCodecRegistry())
                                .toJson()
                        + ").sort({ " + splitField + ": 1 }).batchSize(" + fetchSize + ")";

                this.cursor = collection(client, config)
                        .find(query)
                        // Sorted, because without an order "everything after X" has no relationship
                        // to what has already been read.
                        .sort(Sorts.ascending(splitField))
                        .batchSize(fetchSize)
                        .noCursorTimeout(false)
                        .iterator();

            } catch (MongoException e) {
                throw translate(e, "Could not open chunk " + split.id());
            }
        }

        @Override
        public DataRecord next() {
            if (!cursor.hasNext()) {
                return null;
            }
            Document document = cursor.next();
            emitted++;

            ObjectNode payload = BsonValues.toJson(document);
            JsonNode keyNode = payload.get(splitField);

            if (keyNode != null && !keyNode.isNull()) {
                ObjectNode next = Json.newObject();
                next.set("after", keyNode);
                resumePoint = next;
            }
            return DataRecord.of(payload, keyNode == null ? null : keyNode.asText(), emitted);
        }

        @Override
        public JsonNode cursor() {
            return resumePoint;
        }

        @Override
        public String describe() {
            return description;
        }

        @Override
        public void close() {
            try {
                cursor.close();
            } finally {
                client.close();
            }
        }
    }

    // -------------------------------------------------------------------- sink

    @Override
    public SinkSession openSink(ConnectorContext context) {
        MongoConfig config = MongoConfig.from(context);
        return new MongoSinkSession(config, context);
    }

    private static final class MongoSinkSession implements SinkSession {

        private final MongoConfig config;
        private final MongoClient client;
        private final MongoCollection<Document> target;

        MongoSinkSession(MongoConfig config, ConnectorContext context) {
            this.config = config;
            try {
                this.client = connect(config, context);
                this.target = collection(client, config);
            } catch (MongoException e) {
                throw translate(e, "Could not open " + config.describe() + " for writing");
            }
        }

        @Override
        public Capabilities capabilities() {
            boolean idempotent = config.writeMode() != MongoConfig.WriteMode.INSERT;
            return new Capabilities(
                    idempotent,
                    idempotent ? null
                            : "This sink inserts. Re-writing a document it has already written "
                            + "fails on a duplicate key, so a retry after partial success is not "
                            + "safe. Set write mode to UPSERT and name the key field — '"
                            + config.keyField() + "' — to make repeated writes harmless.",
                    false,   // no multi-document transaction; each document lands independently
                    false,
                    false,
                    // A batch transform may return one object for the whole batch, and this sink
                    // stores it as one document. That is a real migration shape — an order and its
                    // lines becoming a single nested document — and refusing it forced the shape
                    // to be flattened into as many documents as there were source rows.
                    true,
                    // The driver caps a bulk write at 100,000 operations, and the request itself at
                    // 16 MB. Declaring the limit lets the engine clamp rather than fail mid-batch.
                    100_000,
                    1_000);
        }

        @Override
        public WriteResult write(RecordBatch batch) {
            if (batch.isEmpty()) {
                return WriteResult.allWritten(0, 0);
            }

            // An envelope is the batch as one document. The record count still governs the run's
            // arithmetic — five records went in, and five are reported written — because that is
            // what the source produced and what the checkpoint resumes past. Only the shape in the
            // destination changed.
            List<WriteModel<Document>> operations = batch.envelope()
                    .map(envelope -> List.of(toOperation(BsonValues.toDocument(envelope))))
                    .orElseGet(() -> {
                        List<WriteModel<Document>> perRecord = new ArrayList<>(batch.size());
                        for (DataRecord record : batch.records()) {
                            perRecord.add(toOperation(BsonValues.toDocument(record.payload())));
                        }
                        return perRecord;
                    });

            try {
                // Unordered, so one rejected document does not stop the rest of the batch. The
                // failures come back in the exception and go to the dead-letter queue individually.
                BulkWriteResult result = target.bulkWrite(operations,
                        new com.mongodb.client.model.BulkWriteOptions().ordered(false));

                int written = result.getInsertedCount() + result.getUpserts().size()
                        + result.getModifiedCount();
                return WriteResult.allWritten(Math.max(written, batch.size()), batch.totalBytes())
                        // What the counts alone cannot say: a run reporting "written" for every
                        // record while modifying none of them is an upsert matching documents that
                        // are already identical — a no-op migration that looks like a success.
                        .withDetails(bulkDetails(result));

            } catch (com.mongodb.MongoBulkWriteException e) {
                return partialResult(batch, e);
            } catch (MongoException e) {
                throw translate(e, "Failed to write " + batch.size() + " document(s) to "
                        + config.describe());
            }
        }

        /**
         * Separates the documents MongoDB rejected from the ones it accepted.
         *
         * <p>A duplicate key or a schema-validation failure is a property of one document, not of
         * the batch. Failing the whole chunk for it would stop a migration of a million over one
         * bad row.
         */
        private WriteResult partialResult(RecordBatch batch, com.mongodb.MongoBulkWriteException e) {
            List<RecordError> errors = new ArrayList<>();
            for (var failure : e.getWriteErrors()) {
                int index = failure.getIndex();
                DataRecord record = index < batch.records().size()
                        ? batch.records().get(index) : null;

                errors.add(new RecordError(
                        record == null ? index : record.seq(),
                        record == null ? null : record.key(),
                        String.valueOf(failure.getCode()),
                        failure.getMessage(),
                        record == null ? Json.emptyObject() : record.payload()));
            }
            int written = batch.size() - errors.size();
            return WriteResult.partial(written, batch.totalBytes(), errors)
                    .withDetails(bulkDetails(e.getWriteResult()));
        }

        /** What MongoDB says it did, which is not derivable from the record counts. */
        private com.fasterxml.jackson.databind.JsonNode bulkDetails(BulkWriteResult result) {
            com.fasterxml.jackson.databind.node.ObjectNode details = Json.newObject();
            details.put("operation", config.writeMode().name());
            details.put("collection", config.describe());
            if (result.wasAcknowledged()) {
                details.put("inserted", result.getInsertedCount());
                details.put("matched", result.getMatchedCount());
                details.put("modified", result.getModifiedCount());
                details.put("upserted", result.getUpserts().size());
                details.put("deleted", result.getDeletedCount());
            } else {
                details.put("acknowledged", false);
            }
            return details;
        }

        private WriteModel<Document> toOperation(Document document) {
            return switch (config.writeMode()) {
                case INSERT -> new InsertOneModel<>(document);
                case UPSERT -> new ReplaceOneModel<>(
                        matchOnKey(document),
                        document,
                        new ReplaceOptions().upsert(true));
                case REPLACE -> new ReplaceOneModel<>(
                        matchOnKey(document),
                        document,
                        new ReplaceOptions().upsert(false));
            };
        }

        /**
         * The filter an upsert matches on, refusing a record that has no value for the key field.
         *
         * <p>Without this check the filter became {@code {_id: null}} and every record in the
         * migration matched the same one document, replacing it in turn. The run reported
         * COMPLETED with the full record count written, and the collection held a single document
         * with a null key — a whole migration silently collapsed into one row, reported as a
         * success. It is the worst outcome this connector can produce and it took nothing more than
         * a transform that removed {@code _id} while the sink still matched on it.
         *
         * <p>Failed rather than sent to the dead-letter queue. A key field that is absent is a
         * configuration mistake — the wrong field name, or a transform that dropped it — and it is
         * absent from every record, so the alternative is a queue holding the entire migration and
         * a run that still has to be diagnosed.
         */
        private Bson matchOnKey(Document document) {
            return MongoConnector.matchOnKey(document, config.keyField());
        }

        @Override
        public void close() {
            client.close();
        }
    }

    // ------------------------------------------------------------------ shared

    /**
     * Opens a client, applying credentials from the secrets provider rather than from the URI.
     *
     * <p>Credentials are kept out of a <em>literal</em> {@code connectionString} deliberately. A URI
     * of the form {@code mongodb://user:password@host} typed into the field would put the password
     * in the definition database, in every API response describing the connector, and in any log
     * line that echoed the configuration. Supplying them separately means the stored URI is safe to
     * read.
     *
     * <p>A URI arriving through a reference — {@code env:MONGO_URI} — is a different case, and may
     * carry credentials safely: only the reference is stored, and resolution happens here on the
     * worker. That case is deliberately supported, because a single connection string is what most
     * teams are handed. The driver reads any credentials in the URI, and the fields below override
     * them when both are set, so either arrangement works and the more specific one wins.
     */
    private static MongoClient connect(MongoConfig config, ConnectorContext context) {
        String uri = config.connectionString();
        if (uri == null || uri.isBlank()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Configuration field 'connectionString' is required, "
                            + "for example mongodb://localhost:27017");
        }

        MongoClientSettings.Builder settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri));

        Optional<String> username = context.secret("username");
        Optional<String> password = context.secret("password");

        if (username.isPresent() && password.isPresent()) {
            settings.credential(MongoCredential.createCredential(
                    username.get(), authSource(uri), password.get().toCharArray()));
        }
        return MongoClients.create(settings.build());
    }

    /**
     * Resolves the collection, taking the database from the URI when it was not named separately.
     *
     * <p>A MongoDB URI already carries a database, so demanding it again in its own field asked for
     * the same fact twice and let the two disagree. The explicit field remains as an override for a
     * URI that names none, or names one this connection should not use.
     */
    /**
     * The filter an upsert matches on, refusing a record that has no value for the key field.
     *
     * <p>Without this check the filter became {@code {_id: null}} and every record in the migration
     * matched the same one document, replacing it in turn. Worse, MongoDB copies a filter's
     * equality fields into a document it inserts, so the survivor carried {@code _id: null} — and
     * the run reported COMPLETED with the full record count written. A whole migration collapsed
     * into a single row and called it a success. It took nothing more than a transform that removed
     * {@code _id} while the sink still matched on it.
     *
     * <p>Removing the field from the filter instead is not an option: an empty filter matches
     * <em>every</em> document rather than one. There is no correct filter for a record with no key,
     * so this refuses rather than choosing between two wrong answers.
     *
     * <p>Failed rather than sent to the dead-letter queue. An absent key field is a configuration
     * mistake — the wrong name, or a transform that dropped it — and it is absent from every
     * record, so the alternative is a queue holding the entire migration and a run that still has
     * to be diagnosed.
     */
    static Bson matchOnKey(Document document, String keyField) {
        Object key = document.get(keyField);

        if (key == null) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "This sink matches on '" + keyField + "' to decide whether a document already "
                            + "exists, but the records arriving here have no value for it. Every "
                            + "one of them would match the same document and overwrite it, leaving "
                            + "one record where the migration should have left all of them. Either "
                            + "point 'keyField' at a field the records do carry, or stop removing "
                            + "it in a transform. The fields on this record are: "
                            + document.keySet() + ".");
        }
        return Filters.eq(keyField, key);
    }

    /**
     * Where the separate username and password authenticate, read from the URI's {@code authSource}.
     *
     * <p>It had its own field, and a field is a second place for one answer to live: a URI saying
     * {@code ?authSource=admin} beside a box saying something else is a disagreement nothing
     * resolves out loud. Defaults to admin, which is where a MongoDB user is defined unless
     * somebody decided otherwise — and somebody who decided otherwise says so in the URI, next to
     * the host it applies to.
     */
    private static String authSource(String uri) {
        int query = uri.indexOf('?');
        if (query >= 0) {
            for (String pair : uri.substring(query + 1).split("&")) {
                int equals = pair.indexOf('=');
                if (equals > 0 && pair.substring(0, equals).trim().equalsIgnoreCase("authSource")) {
                    String value = pair.substring(equals + 1).trim();
                    if (!value.isBlank()) {
                        return value;
                    }
                }
            }
        }
        return "admin";
    }

    private static MongoCollection<Document> collection(MongoClient client, MongoConfig config) {
        String database = new ConnectionString(config.connectionString()).getDatabase();

        if (database == null || database.isBlank()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "This connection names no database. Put one in the connection string, after "
                            + "the host — mongodb://host:27017/orders.");
        }
        return client.getDatabase(database).getCollection(config.collection());
    }

    /**
     * Classifies a MongoDB failure so the engine knows whether retrying is worth anything.
     *
     * <p>A socket failure or a timeout is transient and worth another attempt; a missing collection
     * or a failed validator is not, and retrying it five times only delays the message the user
     * needs to read.
     */
    private static ConnectorException translate(MongoException e, String context) {
        String message = context + ": " + e.getMessage();

        if (e instanceof MongoSocketException || e instanceof MongoTimeoutException) {
            return new ConnectorException(ConnectorException.Kind.UNAVAILABLE, message, e);
        }
        // 13 Unauthorized, 18 AuthenticationFailed.
        if (e.getCode() == 13 || e.getCode() == 18) {
            return new ConnectorException(ConnectorException.Kind.AUTHENTICATION, message, e);
        }
        // 26 NamespaceNotFound, 121 DocumentValidationFailure.
        if (e.getCode() == 26 || e.getCode() == 121) {
            return new ConnectorException(ConnectorException.Kind.CONFIGURATION, message, e);
        }
        return new ConnectorException(ConnectorException.Kind.UNKNOWN, message, e);
    }

    private static JsonNode configSchema() {
        ObjectNode properties = Json.newObject();
        // The whole connection in one field. Everything else that used to sit beside it — the
        // database, the credentials, the auth source — is already expressible in a MongoDB URI, and
        // asking for them separately meant a connection defined in two places that could disagree.
        //
        // The distinction in this hint is a real one, not pedantry. A literal typed here is stored
        // in the definition database and returned by the API, so credentials must not be in it. A
        // reference is resolved on the worker and never stored, so a URI that does carry them is
        // perfectly safe behind one — which matters because a single URI is usually exactly what
        // the team running the cluster hands out.
        properties.set("connectionString", ConfigFields.fromEnvironment(field("string",
                "The whole connection, including the database and any credentials — for example "
                        + "mongodb://user:pw@host:27017/orders?authSource=admin. Supplied by "
                        + "whoever runs the cluster, so name the variable rather than typing it: "
                        + "behind a reference nothing is stored and credentials in the URI are "
                        + "safe. Typed in as a literal it is stored exactly as written, so it must "
                        + "not contain a password.")));
        properties.set("collection", field("string",
                "Collection to read or write. A URI cannot carry this, so it is named here."));

        properties.set("filter", ConfigFields.selectionField(
                ConfigFields.advanced(ConfigFields.sourceField("string",
                "MongoDB query as JSON, to migrate a subset rather than the whole collection — for "
                        + "example {\"status\": \"active\"}. A value written as \":from\" is "
                        + "supplied per run, so {\"updatedAt\": {\"$gt\": \":from\", "
                        + "\"$lte\": \":to\"}} reads a window a schedule or the Run dialog "
                        + "decides. Empty reads everything."))));

        // Deliberately not advanced. writeMode decides whether a retried chunk duplicates what it
        // already wrote, which is the most consequential answer on this form — burying it under a
        // collapsed heading would be optimising the wrong thing.
        properties.set("writeMode", ConfigFields.sinkEnumField(
                "How documents are written. INSERT is fastest and duplicates if a chunk is retried; "
                        + "UPSERT matches on the key field and is safe to repeat.",
                "INSERT", "UPSERT", "REPLACE"));
        properties.set("keyField", ConfigFields.sinkField("string",
                "Field matched on for UPSERT and REPLACE. Defaults to _id."));

        ObjectNode schema = Json.newObject();
        schema.put("type", "object");
        schema.set("properties", properties);
        schema.set("required", Json.mapper().createArrayNode()
                .add("connectionString").add("collection"));
        return schema;
    }

    private static ObjectNode field(String type, String description) {
        return Json.newObject().put("type", type).put("description", description);
    }

    private static ObjectNode enumField(String description, String... values) {
        ObjectNode node = Json.newObject().put("type", "string").put("description", description);
        var options = Json.mapper().createArrayNode();
        for (String value : values) {
            options.add(value);
        }
        node.set("enum", options);
        return node;
    }

    /** Typed view over the MongoDB connector's configuration. */
    private record MongoConfig(
            String connectionString,
            String collection,
            String filterJson,
            WriteMode writeMode,
            String keyField) {

        enum WriteMode {
            /** Plain insert. Fastest, and a retry duplicates. */
            INSERT,
            /** Replace matching the key, inserting when absent. Idempotent. */
            UPSERT,
            /** Replace matching the key, skipping when absent. Idempotent, never creates. */
            REPLACE
        }

        static MongoConfig from(ConnectorContext context) {
            JsonNode config = context.config();

            return new MongoConfig(
                    text(config, "connectionString", null),
                    required(config, "collection"),
                    MongoConnector.filterJson(config),
                    parseWriteMode(text(config, "writeMode", "INSERT")),
                    text(config, "keyField", "_id"));
        }

        /**
         * The field a collection is ordered, chunked and resumed by. Always {@code _id}.
         *
         * <p>Not configurable, and that is the point. It was, and pointing it at an ordinary field
         * cost a run three records: the resume cursor asks for {@code > lastValue}, so two
         * documents sharing that value at a chunk boundary both fall outside the next read and the
         * run completes reporting nothing wrong. {@code _id} is the one field MongoDB guarantees
         * exists, is unique, and is indexed — the three properties this mechanism silently assumed
         * of whatever field it was handed.
         *
         * <p>What that costs: a collection whose {@code _id} is a string cannot be divided into
         * parallel ranges, because strings can only be split by guessing a distribution. Such a
         * collection reads as one chunk, sequentially. That is a throughput ceiling, and a
         * throughput ceiling is worth more than a correctness hole.
         */
        String splitField() {
            return ID_FIELD;
        }

        Bson filter() {
            return filter(null);
        }

        /**
         * The configured filter, with any {@code ":name"} placeholders replaced by this run's values.
         *
         * <p>Substituted into the parsed document rather than into the JSON text. The structure —
         * which fields, which operators — is fixed the moment the JSON parses, so no value can
         * change what the filter asks for. It is the same guarantee a prepared statement gives,
         * reached differently because MongoDB has no prepared statements.
         */
        Bson filter(ConnectorContext context) {
            if (filterJson == null || filterJson.isBlank()) {
                return new Document();
            }
            Document parsed;
            try {
                parsed = Document.parse(filterJson);
            } catch (Exception e) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "'filter' is not valid MongoDB query JSON: " + filterJson, e);
            }
            return context == null
                    ? parsed
                    : FilterParameters.bind(parsed, context.parameters());
        }

        /**
         * Names the collection as the connection actually resolves it.
         *
         * <p>Says so explicitly when the URI names no database, because every log line about such a
         * connection otherwise said "null.orders" — which reads like a bug in the connector, in the
         * place you go looking when there is one.
         */
        String describe() {
            String named = null;
            if (connectionString != null) {
                try {
                    named = new ConnectionString(connectionString).getDatabase();
                } catch (RuntimeException e) {
                    named = null;
                }
            }
            return (named == null || named.isBlank() ? "(no database in the connection string)" : named)
                    + "." + collection;
        }

        private static WriteMode parseWriteMode(String value) {
            try {
                return WriteMode.valueOf(value.toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "Unknown writeMode '" + value + "'. Use INSERT, UPSERT or REPLACE.");
            }
        }

        private static String required(JsonNode config, String field) {
            String value = text(config, field, null);
            if (value == null) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "Configuration field '" + field + "' is required");
            }
            return value;
        }

        private static String text(JsonNode config, String field, String fallback) {
            JsonNode node = config.get(field);
            return node == null || node.isNull() || node.asText().isBlank()
                    ? fallback : node.asText().strip();
        }
    }
}
