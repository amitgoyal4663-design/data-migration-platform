package com.dmp.recordlog.opensearch;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.StageLogPort;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * The stage log, in OpenSearch.
 *
 * <p>One document per thing the platform did — a window of reading, a transform stage over a batch,
 * a call to a destination. Small: one entry per batch rather than per record, so at a thousand
 * records to a batch this index is roughly a thousandth the size of the record index. That is what
 * lets it carry the query, the timings and the destination's own answer without anyone having to
 * think about the cost.
 *
 * <p>Writes go through {@link AsyncBulkIndexer}, so they never block a migration and are dropped
 * rather than retried when the cluster is unwell. That is the correct trade here and the wrong one
 * for {@link OpenSearchRecordIndex} — see {@link StageLogPort} for why the two ports differ.
 */
@Component
@ConditionalOnProperty(prefix = "dmp.recordlog.opensearch", name = "enabled", havingValue = "true")
public class OpenSearchStageLog implements StageLogPort {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchStageLog.class);

    /**
     * The fields this version of the platform expects, declared once.
     *
     * <p>Used both to create the index and to add fields to one that already exists, because those
     * are the same list and keeping two copies of it is how they drift — silently, since a field
     * missing from the update still appears in every document while every exact-match search on it
     * returns nothing.
     *
     * <p>{@code request} and {@code response} are {@code text} holding the body as one JSON
     * string. They are bodies — read whole, by a person, when something has gone wrong — and
     * storing them as a subtree meant a viewer flattened one call's five records into
     * {@code request.seq: 2800, 2801, 2802, 2803, 2804}: the batch transposed into a column per
     * field, which is not how anybody reads a request. As text it renders as what was sent, and
     * full-text search still finds a call by something inside it.
     *
     * <p>{@code details}, {@code cursorIn} and {@code cursorOut} stay {@code flat_object}. Those
     * are small, bounded sets of engine and connector facts — {@code inserted}, {@code matched},
     * {@code modified} — and being able to filter on {@code details.inserted: 0} is worth having.
     * The rule is the shape of the thing: structured facts stay structured, bodies stay bodies.
     *
     * <p>Neither shape lets arbitrary customer field names become real mapping fields, which is
     * the constraint that matters: twenty source tables of fifty columns would otherwise reach the
     * thousand-field ceiling, after which the cluster rejects every later document.
     */
    private static final String MAPPING_PROPERTIES = """
            {
              "properties": {
                "tenantId":      { "type": "keyword" },
                "pipelineId":    { "type": "keyword" },
                "runId":         { "type": "keyword" },
                "chunkId":       { "type": "keyword" },
                "traceId":       { "type": "keyword" },
                "stage":         { "type": "keyword" },
                "nodeId":        { "type": "keyword" },
                "nodeName":      { "type": "keyword" },
                "connectorType": { "type": "keyword" },
                "sequence":      { "type": "integer" },
                "position":      { "type": "integer" },
                "attempt":       { "type": "integer" },
                "recordsIn":     { "type": "integer" },
                "recordsOut":    { "type": "integer" },
                "bytes":         { "type": "long" },
                "durationMs":    { "type": "long" },
                "outcome":       { "type": "keyword" },
                "errorCode":     { "type": "keyword" },
                "errorMessage":  { "type": "text" },
                "query":         { "type": "text" },
                "cursorIn":      { "type": "flat_object" },
                "cursorOut":     { "type": "flat_object" },
                "details":       { "type": "flat_object" },
                "request":       { "type": "text" },
                "response":      { "type": "text" },
                "occurredAt":    { "type": "date" },
                "expiresAt":     { "type": "date" }
              }
            }
            """;

    private final OpenSearchProperties properties;
    private final HttpClient http;
    private final AsyncBulkIndexer<StageEntry> indexer;
    private final String index;

    public OpenSearchStageLog(OpenSearchProperties properties) {
        this.properties = properties;
        this.index = properties.stageIndexName();
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.indexer = new AsyncBulkIndexer<>("stage-log", properties,
                entry -> index, OpenSearchStageLog::document);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        ensureIndex();
        indexer.start();
        log.info("Stage log indexing to {} (index '{}')", properties.url(), index);
    }

    @PreDestroy
    public void stop() {
        indexer.stop();
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void log(List<StageEntry> entries) {
        indexer.submit(entries);
    }

    @Override
    public Page<StageEntry> find(TenantId tenantId, RunId runId, SplitId splitId, Stage stage,
                                 PageQuery pageQuery) {
        ArrayNode filters = Json.mapper().createArrayNode();
        filters.add(term("tenantId", tenantId.value().toString()));
        filters.add(term("runId", runId.value().toString()));
        if (splitId != null) {
            filters.add(term("chunkId", splitId.value().toString()));
        }
        if (stage != null) {
            filters.add(term("stage", stage.name()));
        }

        ObjectNode body = Json.newObject();
        body.put("from", pageQuery.page() * pageQuery.size());
        body.put("size", pageQuery.size());
        body.putObject("query").putObject("bool").set("filter", filters);
        // Chronological, not newest-first. A stage log is read as a sequence — this read, then the
        // transform, then the write — and reversing it turns a story into a list. The tiebreak on
        // sequence matters because a fast chunk can put several stages in the same millisecond.
        ArrayNode sort = body.putArray("sort");
        sort.addObject().putObject("occurredAt").put("order", "asc");
        // Position, not sequence. Sequence counts within a stage, so using it here interleaved
        // the stages of any chunk fast enough to put several in one millisecond — a fetch sorted
        // after the read it fed, and the log read as fetch, read, fetch.
        sort.addObject().putObject("position").put("order", "asc");
        body.put("track_total_hits", true);

        HttpResponse<String> response = send(HttpRequest
                .newBuilder(URI.create(properties.url() + "/" + index + "/_search"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8)));

        if (response == null || response.statusCode() >= 300) {
            throw new IllegalStateException("Stage log search failed"
                    + (response == null ? "" : ": HTTP " + response.statusCode()));
        }
        return toPage(response.body(), pageQuery);
    }

    // ------------------------------------------------------------------ mapping

    /**
     * The mapping, created once.
     *
     * <p>{@code details}, {@code request} and {@code response} are {@code flat_object} for the same
     * reason the record index's payload is: they hold whatever a connector or a customer put there,
     * and letting arbitrary field names become real mapping fields is how a cluster reaches its
     * thousand-field ceiling and starts rejecting documents.
     */
    private void ensureIndex() {
        String settings = """
                {
                  "settings": { "number_of_shards": %d, "number_of_replicas": %d },
                  "mappings": %s
                }
                """.formatted(properties.shards(), properties.replicas(), MAPPING_PROPERTIES);

        HttpResponse<String> created = send(HttpRequest
                .newBuilder(URI.create(properties.url() + "/" + index))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(settings, StandardCharsets.UTF_8)));

        if (created != null && created.statusCode() >= 300
                && !created.body().contains("resource_already_exists")) {
            log.warn("Could not create the stage log index '{}': HTTP {}",
                    index, created.statusCode());
            return;
        }

        // Additive, for an index created by an earlier version. See MAPPING_PROPERTIES.
        HttpResponse<String> updated = send(HttpRequest
                .newBuilder(URI.create(properties.url() + "/" + index + "/_mapping"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(MAPPING_PROPERTIES, StandardCharsets.UTF_8)));

        if (updated != null && updated.statusCode() >= 300) {
            log.warn("Stage log index '{}' has fields mapped differently from this version of the "
                            + "platform, and OpenSearch will not change a field's type in place. "
                            + "Exact-match searches on those fields will return nothing until the "
                            + "index is reindexed. HTTP {}", index, updated.statusCode());
        }
    }

    // ------------------------------------------------------------------ serialisation

    /**
     * One entry as a document.
     *
     * <p>No document id, unlike the record index. A stage is an event that happened at a moment;
     * re-running a chunk does the work <em>again</em>, and collapsing the retry onto the original
     * would hide exactly the thing somebody debugging a retry wants to see. The record index has
     * the opposite requirement and therefore the opposite scheme.
     */
    private static String document(StageEntry entry) {
        ObjectNode document = Json.newObject();
        document.put("tenantId", entry.tenantId().value().toString());
        document.put("pipelineId", entry.pipelineId().value().toString());
        document.put("runId", entry.runId().value().toString());
        document.put("chunkId", entry.splitId().value().toString());
        document.put("traceId", entry.traceId());
        document.put("stage", entry.stage().name());
        document.put("nodeId", entry.nodeId());
        document.put("nodeName", entry.nodeName());
        document.put("connectorType", entry.connectorType());
        document.put("sequence", entry.sequence());
        document.put("position", entry.position());
        document.put("attempt", entry.attempt());
        document.put("recordsIn", entry.recordsIn());
        document.put("recordsOut", entry.recordsOut());
        document.put("bytes", entry.bytes());
        document.put("durationMs", entry.durationMs());
        document.put("outcome", entry.outcome().name());
        document.put("errorCode", entry.errorCode());
        document.put("errorMessage", entry.errorMessage());
        document.put("query", entry.query());
        document.put("occurredAt", entry.occurredAt().toString());
        document.put("expiresAt", entry.expiresAt().toString());

        putIfPresent(document, "cursorIn", entry.cursorIn());
        putIfPresent(document, "cursorOut", entry.cursorOut());
        putIfPresent(document, "details", entry.details());
        putBodyIfPresent(document, "request", entry.request());
        putBodyIfPresent(document, "response", entry.response());

        return document.toString();
    }

    private static void putIfPresent(ObjectNode document, String field, JsonNode value) {
        if (value != null && !value.isNull() && !value.isEmpty()) {
            document.set(field, value);
        }
    }

    /**
     * A body as one string, pretty-printed.
     *
     * <p>Printed rather than compacted because the only thing that ever reads it is a person, and
     * they are reading it because something went wrong. The few bytes of indentation are the
     * cheapest part of this document.
     */
    private static void putBodyIfPresent(ObjectNode document, String field, JsonNode value) {
        if (value == null || value.isNull() || value.isEmpty()) {
            return;
        }
        document.put(field, value.toPrettyString());
    }

    private Page<StageEntry> toPage(String body, PageQuery pageQuery) {
        JsonNode root;
        try {
            root = Json.mapper().readTree(body);
        } catch (IOException e) {
            throw new IllegalStateException("Stage log search returned a response that is not JSON", e);
        }
        JsonNode hits = root.path("hits");

        List<StageEntry> content = new ArrayList<>();
        for (JsonNode hit : hits.path("hits")) {
            JsonNode s = hit.path("_source");
            content.add(new StageEntry(
                    TenantId.of(UUID.fromString(s.path("tenantId").asText())),
                    PipelineId.of(UUID.fromString(s.path("pipelineId").asText())),
                    RunId.of(UUID.fromString(s.path("runId").asText())),
                    SplitId.of(UUID.fromString(s.path("chunkId").asText())),
                    text(s, "traceId"),
                    Stage.valueOf(s.path("stage").asText()),
                    text(s, "nodeId"),
                    text(s, "nodeName"),
                    text(s, "connectorType"),
                    s.path("sequence").asInt(),
                    s.path("position").asInt(),
                    s.path("attempt").asInt(),
                    s.path("recordsIn").asInt(),
                    s.path("recordsOut").asInt(),
                    s.path("bytes").asLong(),
                    s.path("durationMs").asLong(),
                    Outcome.valueOf(s.path("outcome").asText()),
                    text(s, "errorCode"),
                    text(s, "errorMessage"),
                    text(s, "query"),
                    node(s, "cursorIn"),
                    node(s, "cursorOut"),
                    node(s, "details"),
                    body(s, "request"),
                    body(s, "response"),
                    Instant.parse(s.path("occurredAt").asText()),
                    Instant.parse(s.path("expiresAt").asText())));
        }
        return Page.of(content, pageQuery, hits.path("total").path("value").asLong());
    }

    private static String text(JsonNode source, String field) {
        JsonNode value = source.path(field);
        return value.isNull() || value.isMissingNode() ? null : value.asText(null);
    }

    private static JsonNode node(JsonNode source, String field) {
        return source.has(field) ? source.get(field) : null;
    }

    /**
     * A body back out of the string it was stored as.
     *
     * <p>Falls through to the raw node for entries written before bodies became strings, so an
     * index holding both shapes reads correctly rather than half-failing. Unparseable text is
     * returned as text: whatever a body turned out to be, showing it beats discarding it.
     */
    private static JsonNode body(JsonNode source, String field) {
        JsonNode value = node(source, field);
        if (value == null || !value.isTextual()) {
            return value;
        }
        try {
            return Json.mapper().readTree(value.asText());
        } catch (IOException e) {
            return value;
        }
    }

    private HttpResponse<String> send(HttpRequest.Builder request) {
        properties.credentials().ifPresent(credentials -> request.header("Authorization",
                "Basic " + Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8))));
        try {
            return http.send(request.timeout(Duration.ofSeconds(15)).build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            log.warn("Stage log request to {} failed: {}", properties.url(), e.getMessage());
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static ObjectNode term(String field, String value) {
        ObjectNode node = Json.newObject();
        node.putObject("term").put(field, value);
        return node;
    }
}
