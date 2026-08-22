package com.dmp.connector.rest;

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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Reads paginated JSON APIs and writes batches to HTTP endpoints.
 *
 * <p>Cannot be split. A cursor-paged API has no way to start from the middle — page 40 is only
 * reachable by walking pages 1 to 39 — so this reports a single chunk rather than faking
 * parallelism. Overlapping reads would be far worse than a slower one, and the platform surfaces
 * the honest answer to the user instead of hiding it.
 *
 * <p>Resumption works within that single chunk: the cursor holds the page number or the API's own
 * continuation token, so a restart picks up where it stopped rather than at page one.
 */
public class RestConnector implements Source, Sink {

    private static final String TYPE = "rest";
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    /**
     * How much of a destination's reply is worth keeping on the call log.
     *
     * <p>Generous, because a rejection list for a five-hundred-record batch is the reply worth
     * having and it is not small. Bounded, because this ends up in a search index and an API that
     * echoes the whole request back would otherwise store every record twice.
     */
    private static final int MAX_REPORTED_RESPONSE_CHARS = 64_000;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            // Followed automatically: an API that 301s to https is common, and failing on it would
            // be a confusing first experience.
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    @Override
    public ConnectorSpec spec() {
        return new ConnectorSpec(
                TYPE,
                "REST API (JSON)",
                "Reads paginated JSON endpoints and posts batches to HTTP APIs. Supports page, "
                        + "offset and cursor pagination, with bearer, basic or header auth.",
                ConnectorSpec.Direction.BOTH,
                configSchema(),
                Set.of("token", "username", "password", "apiKey"),
                "1.0.0");
    }

    @Override
    public void testConnection(ConnectorContext context) {
        RestConfig config = RestConfig.from(context);
        HttpResponse<String> response = send(request(config, context, config.firstPageUri(), null));

        if (response.statusCode() >= 400) {
            throw classify(response, "Test request to " + config.firstPageUri() + " failed");
        }
    }

    // ------------------------------------------------------------------ source

    @Override
    public SourceSession openSource(ConnectorContext context) {
        RestConfig config = RestConfig.from(context);

        return new SourceSession() {

            @Override
            public List<SplitSpec> plan(Preparation preparation, PlanRequest request) {
                // One chunk, always. See the class comment: page N is not addressable without
                // walking to it, so splitting would mean reading the same pages repeatedly.
                context.log().info(
                        "REST sources are read as a single chunk — pages are not independently addressable");
                return List.of(SplitSpec.single());
            }

            @Override
            public RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize) {
                return new RestRecordStream(config, context, fromCursor, fetchSize);
            }
        };
    }

    /**
     * Walks pages, emitting records as they are consumed.
     *
     * <p>One page is held at a time. The engine pulls, so a slow sink stops the pulls and the next
     * page is simply not requested — back-pressure with no protocol.
     */
    private final class RestRecordStream implements RecordStream {

        private final RestConfig config;
        private final ConnectorContext context;
        private final int pageSize;

        private Iterator<JsonNode> currentPage = List.<JsonNode>of().iterator();
        private String nextCursor;
        private int pageNumber;
        private long emitted;
        private boolean exhausted;

        RestRecordStream(RestConfig config, ConnectorContext context, JsonNode fromCursor, int pageSize) {
            this.config = config;
            this.context = context;
            this.pageSize = pageSize;

            if (fromCursor != null && fromCursor.hasNonNull("page")) {
                this.pageNumber = fromCursor.get("page").asInt();
                this.emitted = fromCursor.hasNonNull("emitted") ? fromCursor.get("emitted").asLong() : 0;
            }
            if (fromCursor != null && fromCursor.hasNonNull("next")) {
                this.nextCursor = fromCursor.get("next").asText();
            }
        }

        @Override
        public DataRecord next() {
            while (!currentPage.hasNext()) {
                if (exhausted || !fetchNextPage()) {
                    return null;
                }
            }
            emitted++;
            JsonNode payload = currentPage.next();
            return DataRecord.of(payload, keyOf(payload), emitted);
        }

        /** @return true if the page contained anything */
        private boolean fetchNextPage() {
            URI uri = config.pageUri(pageNumber, pageSize, nextCursor);
            HttpResponse<String> response = send(request(config, context, uri, null));

            if (response.statusCode() >= 400) {
                throw classify(response, "GET " + uri + " failed");
            }

            JsonNode body = parse(response.body(), uri);
            JsonNode data = config.dataPath().isEmpty() ? body : body.at(config.dataPath());

            if (!data.isArray()) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "Expected an array at '" + config.dataPath() + "' in the response from " + uri
                                + ", but found " + data.getNodeType()
                                + ". Set 'dataPath' to the JSON pointer of the array, e.g. /data/items.");
            }

            List<JsonNode> records = new ArrayList<>(data.size());
            data.forEach(records::add);
            currentPage = records.iterator();
            pageNumber++;

            // Advance the continuation before deciding whether to stop, so the cursor persisted
            // after this page points at the next one rather than at the one just read.
            nextCursor = config.nextCursor(body, response).orElse(null);
            exhausted = records.isEmpty()
                    || (config.pagination() == RestConfig.Pagination.CURSOR && nextCursor == null);

            context.log().debug("Fetched {} record(s) from {}", records.size(), uri);
            return !records.isEmpty();
        }

        private String keyOf(JsonNode payload) {
            if (config.keyField().isEmpty()) {
                return null;
            }
            JsonNode key = payload.at(config.keyField());
            return key.isMissingNode() || key.isNull() ? null : key.asText();
        }

        @Override
        public JsonNode cursor() {
            ObjectNode cursor = Json.newObject();
            cursor.put("page", pageNumber);
            cursor.put("emitted", emitted);
            if (nextCursor != null) {
                cursor.put("next", nextCursor);
            }
            return cursor;
        }

        @Override
        public void close() {
            // The JDK client pools connections itself; there is nothing per-stream to release.
        }
    }

    // -------------------------------------------------------------------- sink

    @Override
    public SinkSession openSink(ConnectorContext context) {
        RestConfig config = RestConfig.from(context);

        return new SinkSession() {

            @Override
            public Capabilities capabilities() {
                // Whether a retry duplicates depends entirely on the remote API, which this
                // connector cannot know. Reported as at-least-once, which is the safe claim.
                return new Capabilities(
                        false,
                        "Whether repeating a request duplicates data is a property of the remote "
                                + "API, and this connector has no way to find out. It is therefore "
                                + "assumed unsafe. If the endpoint accepts an idempotency key "
                                + "header, or addresses records individually so the same call "
                                + "overwrites rather than appends, repeated writes are already "
                                + "harmless and this can be treated as a conservative warning.",
                        false, false, false, true, config.maxBatchSize(), 500);
            }

            @Override
            public WriteResult write(RecordBatch batch) {
                if (batch.isEmpty()) {
                    return WriteResult.allWritten(0, 0);
                }

                // A batch transform's output wins over any wrapping this connector would do. The
                // user wrote a script precisely because the shape their API wants is not one a
                // configuration flag can describe.
                String body = batch.envelope()
                        .map(Object::toString)
                        .orElseGet(() -> config.wrapBatch()
                                ? wrapped(batch)
                                : Json.mapper().createArrayNode().addAll(
                                        batch.records().stream().map(DataRecord::payload).toList())
                                        .toString());

                HttpResponse<String> response = send(request(config, context, config.writeUri(), body));

                if (response.statusCode() >= 400) {
                    throw classify(response, "POST " + config.writeUri() + " failed");
                }

                // What the API said, kept whether or not it refused anything. A 200 that quietly
                // dropped half a batch and a 200 that took all of it are the same line in every
                // count this platform holds; the reply is the only thing that tells them apart,
                // and it used to be read for its status code and then discarded.
                JsonNode parsed = parseOrNull(response.body());
                ObjectNode details = Json.newObject();
                details.put("status", response.statusCode());
                if (parsed != null) {
                    details.set("response", parsed);
                } else if (!response.body().isBlank()) {
                    // Not JSON. Still worth keeping — "OK" or an HTML error page is an answer, and
                    // an empty details node would read as the API having said nothing.
                    details.put("response", truncated(response.body()));
                }

                List<Sink.RecordError> rejected = rejections(config, batch, parsed);
                if (rejected.isEmpty()) {
                    return WriteResult.allWritten(batch.size(), batch.totalBytes())
                            .withDetails(details);
                }
                return WriteResult.partial(batch.size() - rejected.size(), batch.totalBytes(),
                        rejected).withDetails(details);
            }

            /**
             * The records the API said it would not take, from wherever it says so.
             *
             * <p>Off unless {@code rejectedPath} is configured, because there is no convention
             * here: an API that returns 200 and a list of failures is common, and no two spell it
             * the same way. Without this the connector reported every 2xx as a clean batch — a
             * destination could refuse four hundred records out of five hundred and the run would
             * call all five hundred written, which is the worst answer a migration can give.
             *
             * <p>A rejection names its record by {@code index} — its position in the batch as
             * sent. Anything the API returns that cannot be tied to a record is skipped rather
             * than guessed at: attributing a failure to the wrong row would put a healthy record
             * in the dead-letter queue and let the broken one through as written.
             */
            private List<Sink.RecordError> rejections(RestConfig config, RecordBatch batch,
                                                      JsonNode response) {
                if (config.rejectedPath().isEmpty() || response == null) {
                    return List.of();
                }
                JsonNode rejected = response.at(config.rejectedPath());
                if (!rejected.isArray() || rejected.isEmpty()) {
                    return List.of();
                }

                List<DataRecord> records = batch.records();
                List<Sink.RecordError> errors = new ArrayList<>();
                for (JsonNode entry : rejected) {
                    int index = entry.path("index").asInt(-1);
                    if (index < 0 || index >= records.size()) {
                        context.log().warn("The destination rejected an entry this connector "
                                + "cannot match to a record — it has no usable 'index' within the "
                                + "{} record(s) sent. It is reported on the call, not against a "
                                + "record: {}", records.size(), entry);
                        continue;
                    }
                    DataRecord record = records.get(index);
                    errors.add(new Sink.RecordError(record.seq(), record.key(),
                            entry.path("code").asText("REJECTED"),
                            entry.path("message").asText("The destination refused this record"),
                            record.payload()));
                }
                return errors;
            }

            private String wrapped(RecordBatch batch) {
                ArrayNode records = Json.mapper().createArrayNode();
                batch.records().forEach(record -> records.add(record.payload()));
                ObjectNode envelope = Json.newObject();
                envelope.set(config.batchField(), records);
                return envelope.toString();
            }
        };
    }

    // ------------------------------------------------------------------ shared

    private HttpRequest request(RestConfig config, ConnectorContext context, URI uri, String body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(TIMEOUT)
                .header("Accept", "application/json");

        config.headers().forEach(builder::header);

        // Credentials are resolved from the secrets provider, never read from config, so nothing
        // secret is ever stored in the definition database or returned by the API.
        context.secret("token").ifPresent(token ->
                builder.header("Authorization", "Bearer " + token));
        context.secret("apiKey").ifPresent(key ->
                builder.header(config.apiKeyHeader(), key));

        Optional<String> username = context.secret("username");
        Optional<String> password = context.secret("password");
        if (username.isPresent() && password.isPresent()) {
            String credentials = username.get() + ":" + password.get();
            builder.header("Authorization", "Basic " + Base64.getEncoder()
                    .encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
        }

        if (body == null) {
            return builder.GET().build();
        }
        return builder
                .header("Content-Type", "application/json")
                .method(config.writeMethod(), HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            // A network failure is transient by default. Saying otherwise would abandon a chunk
            // that a retry thirty seconds later would complete.
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    request.method() + " " + request.uri() + " failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE, "Request interrupted", e);
        }
    }

    /**
     * Maps an HTTP status onto the engine's retry decision.
     *
     * <p>The distinction earns its keep: 401 retried five times is five more failures and a delayed
     * error message, while 503 retried five times usually succeeds. 429 is retryable and the engine
     * backs off, which is what the header is asking for.
     */
    private static ConnectorException classify(HttpResponse<String> response, String context) {
        int status = response.statusCode();
        String detail = context + ": HTTP " + status + " — " + truncate(response.body());

        if (status == 401 || status == 403) {
            return new ConnectorException(ConnectorException.Kind.AUTHENTICATION, detail);
        }
        if (status == 429) {
            return new ConnectorException(ConnectorException.Kind.RATE_LIMITED, detail);
        }
        if (status >= 500) {
            return new ConnectorException(ConnectorException.Kind.UNAVAILABLE, detail);
        }
        return new ConnectorException(ConnectorException.Kind.CONFIGURATION, detail);
    }

    private static JsonNode parse(String body, URI uri) {
        try {
            return Json.mapper().readTree(body);
        } catch (Exception e) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Response from " + uri + " was not JSON: " + truncate(body), e);
        }
    }

    /**
     * The reply as JSON, or null when it is not JSON at all.
     *
     * <p>Unlike {@link #parse(String, URI)} this does not fail. A destination that accepted the
     * batch and answered {@code OK} has done nothing wrong, and turning a successful write into a
     * chunk failure over the content type of a reply nobody reads would be absurd.
     */
    private static JsonNode parseOrNull(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            return Json.mapper().readTree(body);
        } catch (Exception e) {
            return null;
        }
    }

    /** The reply, cut to something a search index can hold. */
    private static String truncated(String body) {
        return body.length() <= MAX_REPORTED_RESPONSE_CHARS
                ? body
                : body.substring(0, MAX_REPORTED_RESPONSE_CHARS) + "…";
    }

    /** Error bodies can be entire HTML pages; a log line is not the place for one. */
    private static String truncate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 500 ? body : body.substring(0, 500) + "…";
    }

    private static JsonNode configSchema() {
        ObjectNode properties = Json.newObject();
        properties.set("url", ConfigFields.fromEnvironment(field("string",
                "Full URL of the endpoint to read or write. The host differs between environments, "
                        + "so name the variable — or substitute part of it, as in "
                        + "${API_BASE}/v2/customers.")));
        properties.set("pagination", ConfigFields.sourceEnumField(
                "How the API paginates.", "NONE", "PAGE", "OFFSET", "CURSOR"));
        properties.set("dataPath", ConfigFields.sourceField("string",
                "JSON pointer to the array of records, e.g. /data. Leave blank if the body is the array."));
        properties.set("writeMethod", ConfigFields.sinkEnumField(
                "HTTP method for writes.", "POST", "PUT", "PATCH"));

        properties.set("pageParam", ConfigFields.advanced(ConfigFields.sourceField("string",
                "Query parameter for the page or offset. Defaults to 'page'.")));
        properties.set("sizeParam", ConfigFields.advanced(ConfigFields.sourceField("string",
                "Query parameter for the page size. Defaults to 'size'.")));
        properties.set("cursorParam", ConfigFields.advanced(ConfigFields.sourceField("string",
                "Query parameter carrying the continuation token.")));
        properties.set("cursorPath", ConfigFields.advanced(ConfigFields.sourceField("string",
                "JSON pointer to the next cursor in the response, e.g. /meta/nextCursor.")));
        properties.set("keyField", ConfigFields.advanced(ConfigFields.sinkField("string",
                "JSON pointer to each record's natural key, e.g. /id.")));
        properties.set("headers", ConfigFields.advanced(field("object", "Extra request headers.")));
        properties.set("apiKeyHeader", ConfigFields.advanced(field("string",
                "Header the API key is sent in. Defaults to X-API-Key.")));
        properties.set("batchField", ConfigFields.advanced(ConfigFields.sinkField("string",
                "Wrap records in an object under this field. Leave blank to post a bare array.")));
        properties.set("rejectedPath", ConfigFields.advanced(ConfigFields.sinkField("string",
                "JSON pointer to an array of per-record rejections in a successful response, for "
                        + "an API that answers 200 and lists what it would not take — for example "
                        + "/rejected. Each entry needs an 'index' giving the record's position in "
                        + "the batch, and may carry 'code' and 'message'. Left empty, a 2xx is "
                        + "taken to mean every record was accepted.")));
        properties.set("maxBatchSize", ConfigFields.advanced(ConfigFields.sinkField("integer",
                "Records per request the API accepts. 0 means no limit.")));

        ObjectNode schema = Json.newObject();
        schema.put("type", "object");
        schema.set("properties", properties);
        schema.set("required", Json.mapper().createArrayNode().add("url"));
        return schema;
    }

    private static ObjectNode field(String type, String description) {
        return Json.newObject().put("type", type).put("description", description);
    }

    private static ObjectNode enumField(String description, String... values) {
        ObjectNode node = Json.newObject().put("type", "string").put("description", description);
        ArrayNode options = Json.mapper().createArrayNode();
        for (String value : values) {
            options.add(value);
        }
        node.set("enum", options);
        return node;
    }

    /** Typed view over the REST connector's configuration. */
    private record RestConfig(
            String url,
            Pagination pagination,
            String pageParam,
            String sizeParam,
            String cursorParam,
            String cursorPath,
            String dataPath,
            String keyField,
            Map<String, String> headers,
            String apiKeyHeader,
            String writeMethod,
            String batchField,
            String rejectedPath,
            int maxBatchSize) {

        enum Pagination {
            /** A single request returns everything. */
            NONE,
            /** {@code ?page=1&size=100}, incrementing until a page comes back empty. */
            PAGE,
            /** {@code ?offset=0&limit=100}, advancing by the page size. */
            OFFSET,
            /** The response carries a token for the next request. Stops when it is absent. */
            CURSOR
        }

        static RestConfig from(ConnectorContext context) {
            JsonNode config = context.config();

            JsonNode urlNode = config.get("url");
            if (urlNode == null || urlNode.asText().isBlank()) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "Configuration field 'url' is required");
            }

            Map<String, String> headers = new LinkedHashMap<>();
            if (config.has("headers") && config.get("headers").isObject()) {
                config.get("headers").fields()
                        .forEachRemaining(entry -> headers.put(entry.getKey(), entry.getValue().asText()));
            }

            return new RestConfig(
                    urlNode.asText().strip(),
                    parsePagination(text(config, "pagination", "NONE")),
                    text(config, "pageParam", "page"),
                    text(config, "sizeParam", "size"),
                    text(config, "cursorParam", "cursor"),
                    pointer(text(config, "cursorPath", "")),
                    pointer(text(config, "dataPath", "")),
                    pointer(text(config, "keyField", "")),
                    Map.copyOf(headers),
                    text(config, "apiKeyHeader", "X-API-Key"),
                    text(config, "writeMethod", "POST"),
                    text(config, "batchField", ""),
                    pointer(text(config, "rejectedPath", "")),
                    config.hasNonNull("maxBatchSize") ? config.get("maxBatchSize").asInt() : 0);
        }

        URI firstPageUri() {
            return pageUri(0, 1, null);
        }

        URI writeUri() {
            return URI.create(url);
        }

        boolean wrapBatch() {
            return !batchField.isEmpty();
        }

        URI pageUri(int pageNumber, int pageSize, String cursor) {
            StringBuilder uri = new StringBuilder(url);
            char separator = url.contains("?") ? '&' : '?';

            switch (pagination) {
                case NONE -> {
                    return URI.create(url);
                }
                case PAGE -> uri.append(separator).append(encode(pageParam)).append('=').append(pageNumber)
                        .append('&').append(encode(sizeParam)).append('=').append(pageSize);
                case OFFSET -> uri.append(separator).append(encode(pageParam)).append('=')
                        .append((long) pageNumber * pageSize)
                        .append('&').append(encode(sizeParam)).append('=').append(pageSize);
                case CURSOR -> {
                    uri.append(separator).append(encode(sizeParam)).append('=').append(pageSize);
                    if (cursor != null) {
                        uri.append('&').append(encode(cursorParam)).append('=').append(encode(cursor));
                    }
                }
            }
            return URI.create(uri.toString());
        }

        Optional<String> nextCursor(JsonNode body, HttpResponse<String> response) {
            if (pagination != Pagination.CURSOR) {
                return Optional.empty();
            }
            if (!cursorPath.isEmpty()) {
                JsonNode cursor = body.at(cursorPath);
                return cursor.isMissingNode() || cursor.isNull()
                        ? Optional.empty() : Optional.of(cursor.asText());
            }
            // Falls back to the RFC 8288 Link header, which is what GitHub and many others use
            // instead of putting the cursor in the body.
            return response.headers().firstValue("Link").flatMap(RestConfig::nextFromLinkHeader);
        }

        private static Optional<String> nextFromLinkHeader(String header) {
            for (String link : header.split(",")) {
                if (link.contains("rel=\"next\"")) {
                    int open = link.indexOf('<');
                    int close = link.indexOf('>');
                    if (open >= 0 && close > open) {
                        return Optional.of(link.substring(open + 1, close));
                    }
                }
            }
            return Optional.empty();
        }

        private static Pagination parsePagination(String value) {
            try {
                return Pagination.valueOf(value.toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "Unknown pagination '" + value + "'. Use NONE, PAGE, OFFSET or CURSOR.");
            }
        }

        /** Accepts {@code data} or {@code /data} — a JSON pointer needs the leading slash. */
        private static String pointer(String value) {
            if (value == null || value.isBlank()) {
                return "";
            }
            String trimmed = value.strip();
            return trimmed.startsWith("/") ? trimmed : "/" + trimmed;
        }

        private static String text(JsonNode config, String field, String fallback) {
            JsonNode node = config.get(field);
            return node == null || node.isNull() || node.asText().isBlank() ? fallback : node.asText();
        }

        private static String encode(String value) {
            return URLEncoder.encode(value, StandardCharsets.UTF_8);
        }
    }
}
