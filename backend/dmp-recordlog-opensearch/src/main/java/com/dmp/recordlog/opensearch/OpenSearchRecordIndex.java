package com.dmp.recordlog.opensearch;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.RecordIndexPort;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * The record index, in OpenSearch or Elasticsearch.
 *
 * <p><b>Deliberately not the same thing as {@link OpenSearchRecordLog}</b>, which shares this
 * module and this cluster. That one is observability: it queues events, drains them on a background
 * thread, and drops them when the queue fills or the cluster is unreachable, because a migration
 * must never wait on its own logging. This one is evidence. A dropped entry here does not degrade
 * the answer, it inverts it — the search reports "not transferred" for a record that was, and
 * somebody re-migrates data or tells a customer their records are missing. So it writes
 * synchronously, in the chunk's own thread, and throws when it cannot; the chunk then fails and is
 * retried, which is the correct outcome.
 *
 * <p>Speaks the bulk and search APIs over plain HTTP rather than a client library. The two engines
 * diverged after Elasticsearch 7.10 and their official clients are mutually incompatible, but these
 * endpoints are identical in both, so one implementation serves whichever the organisation runs.
 */
@Component
@ConditionalOnProperty(prefix = "dmp.recordindex", name = "enabled", havingValue = "true")
public class OpenSearchRecordIndex implements RecordIndexPort {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchRecordIndex.class);

    private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration SEARCH_TIMEOUT = Duration.ofSeconds(15);

    /**
     * The fields this version of the platform expects, declared once.
     *
     * <p>Used both to create the index and to add fields to one that already exists, because those
     * are the same list and keeping two copies of it is how they drift. A field present in the
     * create block and missing from the update block reaches new deployments and no existing one —
     * and the failure is silent, since the field still appears in every document while every
     * exact-match search on it returns nothing.
     *
     * <p>{@code record} is {@code flat_object}: the whole payload is <b>one</b> mapping field
     * however many distinct keys it contains. Mapping customer data as an ordinary object would
     * make every field in every tenant's data a real mapping field, and twenty source tables of
     * fifty columns reaches the thousand-field ceiling after which the cluster rejects documents.
     */
    private static final String MAPPING_PROPERTIES = """
            {
              "properties": {
                "tenantId":   { "type": "keyword" },
                "pipelineId": { "type": "keyword" },
                "runId":      { "type": "keyword" },
                "chunkId":    { "type": "keyword" },
                "traceId":    { "type": "keyword" },
                "seq":        { "type": "long" },
                "ordinal":    { "type": "integer" },
                "recordKey":  { "type": "keyword" },
                "outcome":    { "type": "keyword" },
                "errorCode":  { "type": "keyword" },
                "occurredAt": { "type": "date" },
                "expiresAt":  { "type": "date" },
                "record":     { "type": "flat_object" },
                "sourceRecord": { "type": "flat_object" },
                "errorMessage": { "type": "text" }
              }
            }
            """;

    /**
     * Connection settings are shared with {@link OpenSearchRecordLog} — same cluster, same
     * credentials — while the two features are switched on separately. They are enabled
     * independently because they are unrelated decisions: a deployment may want searchable record
     * lineage without debug event logging, or the reverse.
     */
    private final HttpClient http;
    private final OpenSearchProperties properties;
    private final String index;

    public OpenSearchRecordIndex(OpenSearchProperties properties) {
        this.properties = properties;
        // The prefix already names this platform; the log module appends its own date suffix to the
        // same prefix, so this only needs to distinguish the index from those daily log indices.
        this.index = properties.indexPrefix().replaceAll("-records$", "") + "-record-index";
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void createIndex() {
        // The mapping matters more here than anywhere else in the platform. Customer payloads have
        // no knowable shape, and mapping them as ordinary objects makes every distinct field in
        // every tenant's data a real mapping field — twenty source tables of fifty columns reaches
        // the default thousand-field ceiling, after which the cluster rejects every later document.
        // flat_object (OpenSearch) / flattened (Elasticsearch) maps the whole subtree as one field,
        // which keeps it searchable without letting the mapping grow without bound.
        String mapping = """
                {
                  "settings": { "number_of_shards": %d, "number_of_replicas": %d },
                  "mappings": %s
                }
                """.formatted(properties.shards(), properties.replicas(), MAPPING_PROPERTIES);

        HttpResponse<String> response = send(
                request(index).header("Content-Type", "application/json").PUT(body(mapping)),
                "create index");
        if (response != null && response.statusCode() >= 300
                && !response.body().contains("resource_already_exists")) {
            log.warn("Could not create the record index '{}': HTTP {} — {}",
                    index, response.statusCode(), truncate(response.body()));
            return;
        }

        applyMappingUpdates();
        log.info("Record index '{}' ready at {}", index, properties.url());
    }

    /**
     * Adds fields the mapping has gained since the index was created.
     *
     * <p>Creating an index is a no-op once it exists, so a field added to the mapping above reaches
     * only deployments that start from empty. Everywhere else the first document carrying it gets
     * the field <em>dynamically</em> mapped — and a keyword mapped dynamically becomes {@code text},
     * which is analysed, which means an exact-match query on it silently returns nothing. Silently
     * is the problem: the field is visibly present in every document while every search for it
     * comes back empty.
     *
     * <p>This is additive only. OpenSearch refuses to change an existing field's type, which is the
     * correct behaviour and is why the failure is logged rather than retried — an index that has
     * already dynamically mapped a field needs reindexing, and that is an operator's decision.
     */
    private void applyMappingUpdates() {
        HttpResponse<String> response = send(
                request(index + "/_mapping").header("Content-Type", "application/json")
                        .PUT(body(MAPPING_PROPERTIES)),
                "update mapping");

        if (response != null && response.statusCode() >= 300) {
            log.warn("Record index '{}' has fields mapped differently from this version of the "
                            + "platform, and OpenSearch will not change a field's type in place. "
                            + "Exact-match searches on those fields will return nothing until the "
                            + "index is reindexed. HTTP {} — {}",
                    index, response.statusCode(), truncate(response.body()));
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Document ids are {@code chunkId:seq:ordinal}, so re-indexing the same record overwrites
     * rather than duplicating. Entries are written before the checkpoint advances, so a chunk
     * resumed after a crash re-indexes what it had already indexed; without a deterministic id
     * every such resume would inflate the index and every count taken from it.
     *
     * <p>The id was once {@code runId:recordKey}, which was deterministic but not unique. A source
     * holding the same key twice had its two rows filed as one, and a run that moved forty records
     * left thirty entries — the index disagreeing with the run it was meant to explain. The
     * engine's own coordinates are unique whatever the data looks like.
     */
    @Override
    public void indexAll(List<RecordIndexEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        StringBuilder ndjson = new StringBuilder(entries.size() * 256);
        for (RecordIndexEntry entry : entries) {
            ndjson.append("{\"index\":{\"_index\":\"").append(index)
                    .append("\",\"_id\":\"").append(escape(idOf(entry))).append("\"}}\n");
            ndjson.append(document(entry)).append('\n');
        }

        HttpResponse<String> response = send(
                request(index + "/_bulk").POST(body(ndjson.toString()))
                        .header("Content-Type", "application/x-ndjson"),
                "index records");

        if (response == null || response.statusCode() >= 300) {
            throw new IllegalStateException(
                    "Could not write " + entries.size() + " record index entries to "
                            + properties.url() + (response == null ? "" : ": HTTP "
                            + response.statusCode() + " — " + truncate(response.body())));
        }
        // A bulk request answers 200 even when individual documents failed, so the body decides.
        if (response.body().contains("\"errors\":true")) {
            throw new IllegalStateException(
                    "Some record index entries were rejected: " + truncate(response.body()));
        }
    }

    @Override
    public Page<RecordIndexEntry> findByKey(TenantId tenantId, PipelineId pipelineId,
                                            String recordKey, PageQuery pageQuery) {
        return search(tenantId,
                new Query(pipelineId, recordKey, null, null, null, null, null, null), pageQuery);
    }

    @Override
    public Page<RecordIndexEntry> findByRun(TenantId tenantId, RunId runId, Outcome outcome,
                                            PageQuery pageQuery) {
        return search(tenantId, new Query(null, null, null, null, runId, outcome, null, null), pageQuery);
    }

    @Override
    public long countByRun(TenantId tenantId, RunId runId) {
        return findByRun(tenantId, runId, null, new PageQuery(0, 1, null, false)).totalElements();
    }

    @Override
    public boolean supportsContentSearch() {
        return true;
    }

    @Override
    public Page<RecordIndexEntry> search(TenantId tenantId, Query query, PageQuery pageQuery) {
        ArrayNode filters = Json.mapper().createArrayNode();
        filters.add(term("tenantId", tenantId.value().toString()));

        if (query.pipelineId() != null) {
            filters.add(term("pipelineId", query.pipelineId().value().toString()));
        }
        if (notBlank(query.recordKey())) {
            filters.add(term("recordKey", query.recordKey()));
        }
        if (query.runId() != null) {
            filters.add(term("runId", query.runId().value().toString()));
        }
        if (query.outcome() != null) {
            filters.add(term("outcome", query.outcome().name()));
        }
        if (query.after() != null || query.before() != null) {
            ObjectNode range = Json.newObject();
            ObjectNode bounds = range.putObject("range").putObject("occurredAt");
            if (query.after() != null) {
                bounds.put("gte", query.after().toString());
            }
            if (query.before() != null) {
                bounds.put("lte", query.before().toString());
            }
            filters.add(range);
        }
        if (notBlank(query.text())) {
            // Across both ends of the pipeline, not only what was sent.
            //
            // A transform is free to remove a field, and a support desk is asked about the record
            // as the customer knows it — an email address the mapping strips is exactly the value
            // somebody will type. Searching only `record` finds what the destination received and
            // silently cannot find what the source produced, which is the same misleading empty
            // answer this screen exists to avoid. Both are flat_object, so `record.email` addresses
            // one field of either without the mapping having had to know that field would exist.
            ObjectNode match = Json.newObject();
            ObjectNode queryString = match.putObject("query_string").put("query", query.text());

            com.fasterxml.jackson.databind.node.ArrayNode fields = queryString.putArray("fields");
            if (notBlank(query.field())) {
                fields.add("record." + query.field());
                fields.add("sourceRecord." + query.field());
            } else {
                fields.add("record.*");
                fields.add("sourceRecord.*");
            }
            filters.add(match);
        }

        ObjectNode body = Json.newObject();
        body.put("from", pageQuery.page() * pageQuery.size());
        body.put("size", pageQuery.size());
        body.putObject("query").putObject("bool").set("filter", filters);
        body.putArray("sort").addObject().putObject("occurredAt").put("order", "desc");
        body.put("track_total_hits", true);

        HttpResponse<String> response = send(
                request(index + "/_search").POST(body(body.toString()))
                        .header("Content-Type", "application/json"),
                "search records");

        if (response == null || response.statusCode() >= 300) {
            throw new IllegalStateException("Record search failed"
                    + (response == null ? "" : ": HTTP " + response.statusCode()
                    + " — " + truncate(response.body())));
        }
        return toPage(response.body(), pageQuery);
    }

    private Page<RecordIndexEntry> toPage(String body, PageQuery pageQuery) {
        JsonNode root;
        try {
            root = Json.mapper().readTree(body);
        } catch (IOException e) {
            throw new IllegalStateException("Record search returned a response that is not JSON", e);
        }
        JsonNode hits = root.path("hits");

        List<RecordIndexEntry> content = new ArrayList<>();
        for (JsonNode hit : hits.path("hits")) {
            JsonNode source = hit.path("_source");
            content.add(new RecordIndexEntry(
                    TenantId.of(UUID.fromString(source.path("tenantId").asText())),
                    PipelineId.of(UUID.fromString(source.path("pipelineId").asText())),
                    RunId.of(UUID.fromString(source.path("runId").asText())),
                    SplitId.of(UUID.fromString(source.path("chunkId").asText())),
                    source.path("traceId").isNull() ? null
                            : source.path("traceId").asText(null),
                    source.path("seq").asLong(),
                    source.path("ordinal").asInt(),
                    source.path("recordKey").isNull() ? null
                            : source.path("recordKey").asText(null),
                    Outcome.valueOf(source.path("outcome").asText()),
                    source.path("errorCode").isNull() ? null : source.path("errorCode").asText(null),
                    source.has("record") ? source.get("record") : null,
                    source.has("sourceRecord") ? source.get("sourceRecord") : null,
                    source.path("errorMessage").isNull() ? null
                            : source.path("errorMessage").asText(null),
                    java.time.Instant.parse(source.path("occurredAt").asText()),
                    java.time.Instant.parse(source.path("expiresAt").asText())));
        }

        return Page.of(content, pageQuery, hits.path("total").path("value").asLong());
    }

    private String document(RecordIndexEntry entry) {
        ObjectNode document = Json.newObject();
        document.put("tenantId", entry.tenantId().value().toString());
        document.put("pipelineId", entry.pipelineId().value().toString());
        document.put("runId", entry.runId().value().toString());
        document.put("chunkId", entry.splitId().value().toString());
        document.put("traceId", entry.traceId());
        document.put("seq", entry.seq());
        document.put("ordinal", entry.ordinal());
        document.put("recordKey", entry.recordKey());
        document.put("outcome", entry.outcome().name());
        document.put("errorCode", entry.errorCode());
        document.put("errorMessage", entry.errorMessage());
        document.put("occurredAt", entry.occurredAt().toString());
        document.put("expiresAt", entry.expiresAt().toString());

        // Nested under `record` so a customer field called `outcome` or `runId` cannot collide with
        // the platform's own and corrupt what a search means.
        if (entry.payload() != null) {
            document.set("record", entry.payload());
        }
        // Only when a transform actually changed something. An identical copy beside every record
        // would double the index to say nothing.
        if (entry.sourcePayload() != null && !entry.sourcePayload().equals(entry.payload())) {
            document.set("sourceRecord", entry.sourcePayload());
        }
        return document.toString();
    }

    /**
     * The engine's coordinates for the record, not the record's own key.
     *
     * <p>A chunk id is unique on its own, so the run is not repeated here — it is a field, and at
     * roughly a hundred bytes an entry, kept for the life of a migration, another thirty-seven in
     * every id is not free.
     */
    private static String idOf(RecordIndexEntry entry) {
        return entry.splitId().value() + ":" + entry.seq() + ":" + entry.ordinal();
    }

    private static ObjectNode term(String field, String value) {
        ObjectNode node = Json.newObject();
        node.putObject("term").put(field, value);
        return node;
    }

    private HttpRequest.Builder request(String path) {
        // Content-Type is deliberately not set here. The bulk endpoint needs x-ndjson and the
        // others need json, and adding one at each call site on top of a default set here sends
        // two Content-Type headers — which OpenSearch rejects outright.
        HttpRequest.Builder builder = HttpRequest.newBuilder(
                        URI.create(properties.url() + "/" + path))
                .timeout(path.contains("_search") ? SEARCH_TIMEOUT : WRITE_TIMEOUT);

        properties.credentials().ifPresent(credentials -> builder.header("Authorization",
                "Basic " + Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8))));
        return builder;
    }

    private static HttpRequest.BodyPublisher body(String json) {
        return HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8);
    }

    private HttpResponse<String> send(HttpRequest.Builder request, String what) {
        try {
            return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            log.warn("Could not {} at {}: {}", what, properties.url(), e.getMessage());
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String truncate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 400 ? body : body.substring(0, 400) + "…";
    }
}
