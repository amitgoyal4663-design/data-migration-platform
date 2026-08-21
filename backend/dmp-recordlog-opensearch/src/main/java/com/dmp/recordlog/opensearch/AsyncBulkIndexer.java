package com.dmp.recordlog.opensearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Sends documents to OpenSearch on a background thread, and drops them rather than waiting.
 *
 * <p>Shared by every log the platform writes for observability rather than for record-keeping. The
 * distinction matters: {@link OpenSearchRecordIndex} does <em>not</em> use this, because a missing
 * index entry inverts an answer — the search reports "not transferred" for a record that was — and
 * that port has to fail loudly enough for the chunk to be retried. Everything routed through here
 * is a description of the work, and a description that arrives late or not at all is a smaller
 * problem than a migration that waited for it.
 *
 * <p>Three properties, none negotiable:
 *
 * <ul>
 *   <li><b>Bounded queue.</b> An unbounded one in front of a struggling cluster does not avoid a
 *       problem, it converts a logging outage into an out-of-memory kill of the worker — taking the
 *       migration with it.</li>
 *   <li><b>{@code offer}, never {@code put}.</b> Blocking would apply the search cluster's
 *       back-pressure to the data path.</li>
 *   <li><b>Every failure is counted, and warned about sparsely.</b> One warning per dropped
 *       document turns a logging problem into a second logging problem.</li>
 * </ul>
 *
 * @param <T> the event type; the caller supplies how to name its index and serialise it
 */
final class AsyncBulkIndexer<T> {

    private static final Logger log = LoggerFactory.getLogger(AsyncBulkIndexer.class);

    /** One warning per this many drops. Enough to notice, too few to become the problem. */
    private static final long WARN_EVERY = 10_000;

    private final String name;
    private final OpenSearchProperties properties;
    private final HttpClient http;
    private final BlockingQueue<T> queue;
    private final Function<T, String> indexOf;
    private final Function<T, String> documentOf;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong indexed = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private Thread flusher;

    /**
     * @param name       used for the thread name and log lines, e.g. {@code call-log}
     * @param indexOf    which index a given event belongs in
     * @param documentOf the event as a single-line JSON document
     */
    AsyncBulkIndexer(String name, OpenSearchProperties properties,
                     Function<T, String> indexOf, Function<T, String> documentOf) {
        this.name = name;
        this.properties = properties;
        this.indexOf = indexOf;
        this.documentOf = documentOf;
        this.queue = new ArrayBlockingQueue<>(properties.queueCapacity());
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    void start() {
        this.flusher = Thread.ofPlatform()
                .name("dmp-" + name + "-flusher")
                .daemon(true)
                .start(this::drainForever);
    }

    void stop() {
        running.set(false);
        if (flusher != null) {
            flusher.interrupt();
            try {
                // A brief join so the last partial bulk is sent. Events from the seconds before a
                // shutdown are usually the ones somebody is about to go looking for.
                flusher.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("{} stopped: {} indexed, {} dropped", name, indexed.get(), dropped.get());
    }

    void submit(List<T> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        for (T event : events) {
            if (!queue.offer(event)) {
                long total = dropped.incrementAndGet();
                if (total % WARN_EVERY == 1) {
                    log.warn("{} queue is full; {} event(s) dropped so far. "
                            + "The migration is unaffected.", name, total);
                }
            }
        }
    }

    long indexedCount() {
        return indexed.get();
    }

    long droppedCount() {
        return dropped.get();
    }

    private void drainForever() {
        List<T> buffer = new ArrayList<>(properties.bulkSize());

        while (running.get() || !queue.isEmpty()) {
            try {
                // Waits for the first event, then takes whatever else is already queued. This
                // batches naturally under load and stays responsive when idle, without a timer.
                T first = queue.poll(properties.flushInterval().toMillis(), TimeUnit.MILLISECONDS);
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
                log.warn("{} flush failed; {} event(s) lost. The migration is unaffected.",
                        name, buffer.size(), e);
                dropped.addAndGet(buffer.size());
                buffer.clear();
            }
        }
    }

    private void flush(List<T> events) {
        if (events.isEmpty()) {
            return;
        }

        StringBuilder body = new StringBuilder(events.size() * 512);
        for (T event : events) {
            // Bulk NDJSON: an action line, then the document, each newline-terminated. The trailing
            // newline on the last pair is required — omitting it fails the whole request.
            body.append("{\"index\":{\"_index\":\"").append(indexOf.apply(event)).append("\"}}\n");
            body.append(documentOf.apply(event)).append('\n');
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
                log.warn("{} bulk rejected with HTTP {}: {}",
                        name, response.statusCode(), truncate(response.body()));
                dropped.addAndGet(events.size());
                return;
            }
            indexed.addAndGet(events.size());

        } catch (IOException e) {
            log.warn("{} unreachable at {}; {} event(s) lost", name, properties.url(),
                    events.size(), e);
            dropped.addAndGet(events.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Cluster errors can be enormous; the first part is where the cause is. */
    private static String truncate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 500 ? body : body.substring(0, 500) + "…";
    }
}
