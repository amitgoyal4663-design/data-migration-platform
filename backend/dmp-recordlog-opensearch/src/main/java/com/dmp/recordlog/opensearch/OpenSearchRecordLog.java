package com.dmp.recordlog.opensearch;

import com.dmp.application.port.out.RecordLogPort;
import com.dmp.common.json.Json;
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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Indexes per-record events into OpenSearch or Elasticsearch.
 *
 * <p>Speaks the bulk API over plain HTTP rather than using a client library. The two engines
 * diverged after Elasticsearch 7.10 and their official clients are mutually incompatible, but the
 * bulk endpoint is identical in both — so one implementation covers whichever the organisation
 * runs, and adds no transitive dependencies to a classpath that connectors share.
 *
 * <p><b>This never blocks and never fails a run.</b> Events go onto a bounded queue that a
 * background thread drains in bulk. If the queue is full, or the cluster is unreachable, events are
 * dropped and counted. A migration must not slow down — let alone fail — because its logging
 * backend is having a bad day.
 */
@Component
@ConditionalOnProperty(prefix = "dmp.recordlog.opensearch", name = "enabled", havingValue = "true")
public class OpenSearchRecordLog implements RecordLogPort {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchRecordLog.class);

    private static final DateTimeFormatter INDEX_SUFFIX =
            DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);

    private final HttpClient http;
    private final OpenSearchProperties properties;
    private final BlockingQueue<RecordEvent> queue;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong indexed = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private Thread flusher;

    public OpenSearchRecordLog(OpenSearchProperties properties) {
        this.properties = properties;
        this.queue = new ArrayBlockingQueue<>(properties.queueCapacity());
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        ensureIndexTemplate();

        this.flusher = Thread.ofPlatform()
                .name("dmp-recordlog-flusher")
                .daemon(true)
                .start(this::drainForever);

        log.info("Record log indexing to {} (index prefix '{}', queue {})",
                properties.url(), properties.indexPrefix(), properties.queueCapacity());
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (flusher != null) {
            flusher.interrupt();
            try {
                // A brief join so the last partial bulk is sent. Records that arrived seconds
                // before shutdown are usually the interesting ones.
                flusher.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("Record log stopped: {} indexed, {} dropped", indexed.get(), dropped.get());
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Uses {@code offer}, never {@code put}. Blocking here would apply back-pressure from the
     * logging backend onto the data path, which inverts the whole point: the migration is the work,
     * the log is a description of it.
     */
    @Override
    public void log(List<RecordEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        for (RecordEvent event : events) {
            if (!queue.offer(event)) {
                long total = dropped.incrementAndGet();
                // Logged sparsely: a saturated queue would otherwise produce one warning per
                // record, turning a logging problem into a second logging problem.
                if (total % 10_000 == 1) {
                    log.warn("Record log queue is full; {} event(s) dropped so far. "
                            + "The migration is unaffected.", total);
                }
            }
        }
    }

    private void drainForever() {
        List<RecordEvent> buffer = new ArrayList<>(properties.bulkSize());

        while (running.get() || !queue.isEmpty()) {
            try {
                // Waits for the first event, then takes whatever else is already queued. This
                // batches naturally under load and stays responsive when idle, without a timer.
                RecordEvent first = queue.poll(properties.flushInterval().toMillis(),
                        TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                buffer.add(first);
                queue.drainTo(buffer, properties.bulkSize() - 1);

                flush(buffer);
                buffer.clear();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Drain what is left rather than discarding it on the way out.
                queue.drainTo(buffer, properties.bulkSize());
                if (!buffer.isEmpty()) {
                    flush(buffer);
                }
                return;
            } catch (Exception e) {
                log.warn("Record log flush failed; {} event(s) lost. The migration is unaffected.",
                        buffer.size(), e);
                dropped.addAndGet(buffer.size());
                buffer.clear();
            }
        }
    }

    private void flush(List<RecordEvent> events) {
        if (events.isEmpty()) {
            return;
        }

        StringBuilder body = new StringBuilder(events.size() * 512);
        for (RecordEvent event : events) {
            // Bulk NDJSON: an action line, then the document, each newline-terminated. The trailing
            // newline on the last pair is required — omitting it makes the whole request fail.
            body.append("{\"index\":{\"_index\":\"").append(indexFor(event)).append("\"}}\n");
            body.append(toDocument(event)).append('\n');
        }

        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(properties.url() + "/_bulk"))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/x-ndjson")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));

        properties.credentials().ifPresent(credentials -> request.header("Authorization",
                "Basic " + Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8))));

        try {
            HttpResponse<String> response = http.send(request.build(),
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 300) {
                log.warn("Record log bulk rejected with HTTP {}: {}",
                        response.statusCode(), truncate(response.body()));
                dropped.addAndGet(events.size());
                return;
            }
            indexed.addAndGet(events.size());

        } catch (IOException e) {
            log.warn("Record log unreachable at {}; {} event(s) lost",
                    properties.url(), events.size(), e);
            dropped.addAndGet(events.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Time-based index names, one per day.
     *
     * <p>Retention then becomes dropping whole indices, which is close to free — as opposed to
     * deleting documents by query, which rewrites segments and is the classic way to make a
     * search cluster miserable.
     */
    private String indexFor(RecordEvent event) {
        return properties.indexPrefix() + "-" + INDEX_SUFFIX.format(event.occurredAt());
    }

    private String toDocument(RecordEvent event) {
        ObjectNode doc = Json.newObject();
        doc.put("@timestamp", event.occurredAt().toString());
        doc.put("tenantId", event.tenantId().toString());
        doc.put("runId", event.runId().toString());
        doc.put("chunkId", event.splitId().toString());
        doc.put("pipeline", event.pipelineName());
        doc.put("nodeId", event.nodeId());
        doc.put("connectorType", event.connectorType());
        doc.put("seq", event.seq());
        doc.put("recordKey", event.key());
        doc.put("outcome", event.outcome().name());
        doc.put("errorCode", event.errorCode());
        doc.put("errorMessage", event.errorMessage());
        doc.put("durationMs", event.durationMs());

        // Nested under `record` so a field called `outcome` or `runId` in the customer's own data
        // cannot collide with the platform's fields and corrupt the mapping.
        doc.set("record", event.payload());
        return doc.toString();
    }

    /**
     * Installs an index template so every daily index maps consistently.
     *
     * <p>Without one, the first document creates the mapping by inference, and a field that arrives
     * as a number one day and a string the next causes every later document to be rejected. The
     * customer payload is mapped as {@code flattened}-style dynamic content precisely because its
     * shape is not knowable in advance.
     *
     * <p>Best effort. A cluster that refuses the template still accepts documents, and refusing to
     * start over it would be the logging backend deciding whether migrations may run.
     */
    private void ensureIndexTemplate() {
        String template = """
                {
                  "index_patterns": ["%s-*"],
                  "template": {
                    "settings": {
                      "number_of_shards": %d,
                      "number_of_replicas": %d,
                      "refresh_interval": "5s"
                    },
                    "mappings": {
                      "properties": {
                        "@timestamp":    { "type": "date" },
                        "tenantId":      { "type": "keyword" },
                        "runId":         { "type": "keyword" },
                        "chunkId":       { "type": "keyword" },
                        "pipeline":      { "type": "keyword" },
                        "nodeId":        { "type": "keyword" },
                        "connectorType": { "type": "keyword" },
                        "seq":           { "type": "long" },
                        "recordKey":     { "type": "keyword" },
                        "outcome":       { "type": "keyword" },
                        "errorCode":     { "type": "keyword" },
                        "errorMessage":  { "type": "text" },
                        "durationMs":    { "type": "long" },
                        "record":        { "type": "object", "dynamic": true }
                      }
                    }
                  }
                }
                """.formatted(properties.indexPrefix(), properties.shards(), properties.replicas());

        HttpRequest.Builder request = HttpRequest.newBuilder(
                        URI.create(properties.url() + "/_index_template/" + properties.indexPrefix()))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(template));

        properties.credentials().ifPresent(credentials -> request.header("Authorization",
                "Basic " + Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8))));

        try {
            HttpResponse<String> response = http.send(request.build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                log.warn("Could not install the record-log index template: HTTP {} — {}",
                        response.statusCode(), truncate(response.body()));
            } else {
                log.info("Record log index template '{}' installed", properties.indexPrefix());
            }
        } catch (Exception e) {
            log.warn("Could not reach {} to install the record-log index template. "
                    + "Indexing will still be attempted.", properties.url(), e);
        }
    }

    /** Indexed and dropped counts, for the metrics endpoint. */
    public long indexedCount() {
        return indexed.get();
    }

    public long droppedCount() {
        return dropped.get();
    }

    private static String truncate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 400 ? body : body.substring(0, 400) + "…";
    }
}
