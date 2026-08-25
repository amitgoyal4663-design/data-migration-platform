package com.dmp.events.kafka;

import com.dmp.application.port.out.RecordIndexPort;
import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.run.RunId;
import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Record-index writes go to Kafka; a consumer puts them in OpenSearch.
 *
 * <p>The index is the evidence a migration produces — it is what answers "did policy POL-44219
 * move". Writing it straight to OpenSearch made every chunk wait for a search cluster, and made a
 * search cluster having a bad afternoon into failed migrations. Writing it fire-and-forget would
 * have been worse: a dropped entry does not lose a log line, it inverts an answer, and the search
 * reports "not transferred" about a record that was.
 *
 * <p>A durable queue is the arrangement that gives up neither. The chunk waits for Kafka to
 * acknowledge the batch and no longer, which is milliseconds against a local broker; the entries
 * are then durably somebody's problem, and {@link RecordIndexConsumer} indexes them at whatever
 * pace OpenSearch can take. OpenSearch being down stops the index catching up. It no longer stops
 * the migration, and nothing is lost while it is away.
 *
 * <p><b>The send is acknowledged before the chunk completes, deliberately.</b> {@code acks=all} and
 * a blocking wait on the batch. Anything less and a chunk could report success while its evidence
 * was still in a buffer on a pod about to be killed — which is the same silent hole as dropping,
 * arrived at more slowly.
 *
 * <p><b>Reads still go to OpenSearch.</b> Only the write path moved. Every search, count and
 * lookup is delegated to the port this decorates, which means a read sees whatever the consumer
 * has indexed so far — the index is now eventually consistent, and a search immediately after a
 * run finishes may briefly be behind it.
 */
public class KafkaRecordIndex implements RecordIndexPort {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordIndex.class);

    /** The shape of a message on the topic. A change that readers cannot ignore becomes v2. */
    static final int SCHEMA = 1;

    /** How long a batch may wait for its acknowledgement before the chunk is told it failed. */
    private static final Duration ACK_TIMEOUT = Duration.ofSeconds(30);

    private final RecordIndexPort reads;
    private final KafkaEventProperties properties;
    private final Producer<String, String> producer;

    public KafkaRecordIndex(RecordIndexPort reads, KafkaEventProperties properties) {
        this(reads, properties, defaultProducer(properties));
    }

    KafkaRecordIndex(RecordIndexPort reads, KafkaEventProperties properties,
                     Producer<String, String> producer) {
        this.reads = reads;
        this.properties = properties;
        this.producer = producer;
        log.info("Record index writes go to '{}' on {}; a consumer indexes them into OpenSearch",
                properties.recordIndexTopic(), properties.bootstrapServers());
    }

    private static Producer<String, String> defaultProducer(KafkaEventProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Every one of these is the difference between a queue and a hope.
        //
        // acks=all: the batch is on every in-sync replica before the chunk is told it landed. acks=1
        // acknowledges a leader that can still lose the write to an unreplicated failover, and the
        // chunk would have already moved on.
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) ACK_TIMEOUT.toMillis());
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        // Batched, unlike the doorbell. These are bulk writes and a few milliseconds of gathering
        // turns a thousand small requests into a handful of large ones.
        config.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024);

        return new KafkaProducer<>(config);
    }

    /**
     * Publishes the batch and waits for every message to be acknowledged.
     *
     * <p>Throws when the topic is missing or the broker will not take the batch, which is what
     * makes the chunk retry rather than complete without its evidence. The platform does not create
     * the topic — that authority is not assumed here or anywhere — so a missing one is a
     * deployment that is not finished, and it says so in those words.
     */
    @Override
    public void indexAll(List<RecordIndexEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        List<Future<?>> sent = new java.util.ArrayList<>(entries.size());
        try {
            for (RecordIndexEntry entry : entries) {
                // Keyed by run, so one run's entries stay in order on one partition and a replay
                // of that partition replays that run's evidence in the order it was produced.
                sent.add(producer.send(new ProducerRecord<>(
                        properties.recordIndexTopic(),
                        entry.runId() == null ? null : entry.runId().toString(),
                        toJson(entry))));
            }
            for (Future<?> future : sent) {
                future.get(ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while recording " + entries.size()
                    + " record index entries", e);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Could not record " + entries.size() + " record index entries on topic '"
                            + properties.recordIndexTopic() + "'. The chunk is failed rather than "
                            + "completed, because a chunk that reports success without its "
                            + "evidence makes the record search answer 'not transferred' about "
                            + "records that were. If the topic does not exist, it has to be "
                            + "created — this platform never creates one. Cause: " + e.getMessage(),
                    e);
        }
    }

    /**
     * One entry as a single-line JSON document.
     *
     * <p>Written out field by field rather than handed to a mapper, so the message shape is a
     * decision recorded here and not a consequence of how a domain record happens to be declared.
     * A field renamed in the domain would otherwise silently rename itself on a topic other systems
     * may be reading.
     */
    private static String toJson(RecordIndexEntry entry) {
        ObjectNode node = Json.newObject();
        node.put("schema", SCHEMA);
        put(node, "tenantId", entry.tenantId());
        put(node, "pipelineId", entry.pipelineId());
        put(node, "runId", entry.runId());
        put(node, "splitId", entry.splitId());
        node.put("traceId", entry.traceId());
        node.put("seq", entry.seq());
        node.put("ordinal", entry.ordinal());
        node.put("recordKey", entry.recordKey());
        node.put("outcome", entry.outcome() == null ? null : entry.outcome().name());
        node.put("errorCode", entry.errorCode());
        node.put("errorMessage", entry.errorMessage());
        node.set("payload", entry.payload());
        node.set("sourcePayload", entry.sourcePayload());
        node.put("occurredAt", entry.occurredAt() == null ? null : entry.occurredAt().toString());
        node.put("expiresAt", entry.expiresAt() == null ? null : entry.expiresAt().toString());
        return node.toString();
    }

    private static void put(ObjectNode node, String field, Object id) {
        node.put(field, id == null ? null : id.toString());
    }

    @PreDestroy
    public void close() {
        try {
            // Flushed before closing: anything still buffered belongs to a chunk that was told it
            // landed, and a shutdown is not a reason to make that untrue.
            producer.flush();
            producer.close(Duration.ofSeconds(10));
        } catch (Exception e) {
            log.warn("Closing the record index producer failed: {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------ reads, unchanged

    @Override
    public Page<RecordIndexEntry> findByKey(TenantId tenantId, PipelineId pipelineId,
                                            String recordKey, PageQuery pageQuery) {
        return reads.findByKey(tenantId, pipelineId, recordKey, pageQuery);
    }

    @Override
    public Page<RecordIndexEntry> findByRun(TenantId tenantId, RunId runId, Outcome outcome,
                                            PageQuery pageQuery) {
        return reads.findByRun(tenantId, runId, outcome, pageQuery);
    }

    @Override
    public long countByRun(TenantId tenantId, RunId runId) {
        return reads.countByRun(tenantId, runId);
    }

    @Override
    public java.util.Map<String, Long> countByOutcome(TenantId tenantId, RunId runId) {
        return reads.countByOutcome(tenantId, runId);
    }

    @Override
    public boolean supportsContentSearch() {
        return reads.supportsContentSearch();
    }

    @Override
    public Page<RecordIndexEntry> search(TenantId tenantId, Query query, PageQuery pageQuery) {
        return reads.search(tenantId, query, pageQuery);
    }
}
