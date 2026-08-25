package com.dmp.events.kafka;

import com.dmp.application.port.out.RecordIndexPort;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Takes record-index entries off Kafka and puts them in OpenSearch.
 *
 * <p>The other half of {@link KafkaRecordIndex}. Everything that made the write path fast lands
 * here as the obligation not to lose any of it, and the whole design is one rule:
 *
 * <p><b>The offset is committed after OpenSearch has accepted the batch, never before.</b> Commit
 * first and a failure between the two loses evidence permanently and silently — the run's counters
 * would say five thousand written and the search would find four thousand, with nothing anywhere
 * saying why. Commit after, and the same failure re-delivers the batch instead.
 *
 * <p>Re-delivery is therefore normal, not exceptional, and safe because a record-index document's
 * id is derived from the run, chunk and sequence it describes. Indexing the same entry twice writes
 * the same document to the same id, which is what makes at-least-once delivery behave as exactly
 * once where it matters.
 *
 * <p><b>A failing OpenSearch stalls this consumer and nothing else.</b> The batch is retried, the
 * offset stays where it is, and the queue grows — which is visible as consumer lag and recoverable
 * the moment the cluster returns. Migrations keep running throughout; the index is behind, and
 * being behind is a state it can come back from.
 */
public class RecordIndexConsumer {

    private static final Logger log = LoggerFactory.getLogger(RecordIndexConsumer.class);

    private static final Duration POLL = Duration.ofSeconds(1);

    /** How long to wait before retrying a batch OpenSearch refused. Long enough not to spin. */
    private static final Duration RETRY_PAUSE = Duration.ofSeconds(5);

    /** One line per this many entries, so a healthy consumer is quiet. */
    private static final long LOG_EVERY = 50_000;

    private final RecordIndexPort openSearch;
    private final KafkaEventProperties properties;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private Thread worker;
    private long indexed;
    private long loggedAt;

    public RecordIndexConsumer(RecordIndexPort openSearch, KafkaEventProperties properties) {
        this.openSearch = openSearch;
        this.properties = properties;
    }

    /**
     * Starts consuming, or refuses to start and says why.
     *
     * <p>Unlike the work nudge, a missing topic here is fatal to this consumer and loud about it.
     * The nudge is an optimisation and the platform is correct without it; this carries the
     * evidence every migration produces, and running without it would mean writes acknowledged into
     * a topic nobody drains — a record index that quietly stops being written while every run
     * reports success.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        worker = new Thread(this::consume, "dmp-record-indexer");
        worker.setDaemon(true);
        worker.start();
    }

    private void consume() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, properties.recordIndexGroupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Never automatic. The whole guarantee is that the commit follows the indexing, and an
        // auto-commit on a timer would move the offset past entries OpenSearch had not accepted.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // From the beginning, because a group with no committed offset has not read these yet and
        // they are evidence somebody is waiting for. 'latest' would silently skip the backlog.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2_000);

        // Generous, because a slow OpenSearch makes a poll interval long. Too short and the broker
        // decides this consumer is dead mid-batch and hands its partitions to another, which then
        // indexes the same entries again — harmless, but pointless work during an incident.
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            var partitions = consumer.partitionsFor(properties.recordIndexTopic());
            if (partitions == null || partitions.isEmpty()) {
                throw new IllegalStateException("the topic has no partitions");
            }
            consumer.subscribe(List.of(properties.recordIndexTopic()));
            log.info("Indexing record entries from '{}' as group '{}'",
                    properties.recordIndexTopic(), properties.recordIndexGroupId());

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(POLL);
                if (records.isEmpty()) {
                    continue;
                }
                if (indexBatch(records)) {
                    consumer.commitSync();
                }
            }
        } catch (org.apache.kafka.common.errors.InterruptException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("The record index consumer stopped: topic '{}' on {} could not be consumed "
                            + "({}). Record-index entries are being written to that topic and "
                            + "nothing is draining it, so the record search will fall behind and "
                            + "stay behind until this is fixed. The topic must exist — this "
                            + "platform never creates one.",
                    properties.recordIndexTopic(), properties.bootstrapServers(), e.getMessage(), e);
        }
    }

    /**
     * Indexes one poll's worth, retrying until it lands or the consumer is shutting down.
     *
     * @return whether the batch is safely in OpenSearch and its offset may be committed
     */
    private boolean indexBatch(ConsumerRecords<String, String> records) {
        List<RecordIndexPort.RecordIndexEntry> entries = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
            try {
                entries.add(parse(record.value()));
            } catch (Exception e) {
                // One unreadable message must not stop the queue for every readable one behind it.
                // Dropped and named, because it cannot become valid by being retried.
                log.error("Skipping an unreadable record index message at {}-{} offset {}: {}",
                        record.topic(), record.partition(), record.offset(), e.getMessage());
            }
        }
        if (entries.isEmpty()) {
            return true;
        }

        while (running.get()) {
            try {
                openSearch.indexAll(entries);
                indexed += entries.size();
                if (indexed - loggedAt >= LOG_EVERY) {
                    loggedAt = indexed;
                    log.info("Record index: {} entries indexed", indexed);
                }
                return true;
            } catch (Exception e) {
                log.warn("OpenSearch would not take {} record index entries ({}). Retrying in {}s; "
                                + "the offset stays where it is, so nothing is lost — the index is "
                                + "behind until this clears.",
                        entries.size(), e.getMessage(), RETRY_PAUSE.toSeconds());
                try {
                    Thread.sleep(RETRY_PAUSE.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    /** One message back into the entry it was made from. */
    private static RecordIndexPort.RecordIndexEntry parse(String message) throws Exception {
        JsonNode node = Json.mapper().readTree(message);
        return new RecordIndexPort.RecordIndexEntry(
                text(node, "tenantId") == null ? null : TenantId.of(java.util.UUID.fromString(text(node, "tenantId"))),
                text(node, "pipelineId") == null ? null : PipelineId.of(java.util.UUID.fromString(text(node, "pipelineId"))),
                text(node, "runId") == null ? null : RunId.of(java.util.UUID.fromString(text(node, "runId"))),
                text(node, "splitId") == null ? null : SplitId.of(java.util.UUID.fromString(text(node, "splitId"))),
                text(node, "traceId"),
                node.path("seq").asLong(),
                node.path("ordinal").asInt(),
                text(node, "recordKey"),
                text(node, "outcome") == null
                        ? null : RecordIndexPort.Outcome.valueOf(text(node, "outcome")),
                text(node, "errorCode"),
                node.hasNonNull("payload") ? node.get("payload") : null,
                node.hasNonNull("sourcePayload") ? node.get("sourcePayload") : null,
                text(node, "errorMessage"),
                instant(node, "occurredAt"),
                instant(node, "expiresAt"));
    }

    private static String text(JsonNode node, String field) {
        return node.hasNonNull(field) ? node.get(field).asText() : null;
    }

    private static Instant instant(JsonNode node, String field) {
        String value = text(node, field);
        return value == null ? null : Instant.parse(value);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (worker != null) {
            worker.interrupt();
        }
    }
}
