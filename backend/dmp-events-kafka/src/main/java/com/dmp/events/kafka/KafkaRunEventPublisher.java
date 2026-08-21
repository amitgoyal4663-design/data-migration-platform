package com.dmp.events.kafka;

import com.dmp.application.port.out.RunEventPublisher;
import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes run lifecycle events to a Kafka topic.
 *
 * <p>Events only. A migration of fifty million records produces a few hundred messages here — one
 * per run transition and one per chunk — because the records themselves never leave the pod that
 * read them. Routing data through a broker would double the network cost and make a cluster shared
 * with other teams the ceiling on every migration.
 *
 * <p><b>Never creates a topic.</b> The platform's service account is not assumed to have that
 * authority, and a connector that quietly creates topics in development produces a permission
 * failure in production at the worst possible moment. A missing topic is reported loudly at startup
 * and then tolerated — publishing degrades, migrations do not.
 */
@Component
@ConditionalOnProperty(prefix = "dmp.events.kafka", name = "enabled", havingValue = "true")
public class KafkaRunEventPublisher implements RunEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaRunEventPublisher.class);

    private final KafkaEventProperties properties;
    private final Producer<String, String> producer;
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    public KafkaRunEventPublisher(KafkaEventProperties properties) {
        this.properties = properties;

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "dmp-events");

        // acks=1 rather than all. These are observability events: waiting for every replica would
        // add latency to the data path for messages whose loss is an inconvenience, not a defect.
        config.put(ProducerConfig.ACKS_CONFIG, "1");

        // Bounded so a broker outage cannot stall a worker. Once the buffer fills, send() would
        // block for at most this long — hence the explicit cap rather than the 60s default.
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        this.producer = new KafkaProducer<>(config);
    }

    /**
     * Checks the topic exists, and says so plainly if it does not.
     *
     * <p>Uses {@code partitionsFor}, which needs only metadata access — not the admin rights the
     * platform deliberately does not require. A missing topic is a message to hand the platform
     * team, not a reason to refuse to start.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void verifyTopic() {
        try {
            var partitions = producer.partitionsFor(properties.topic());
            log.info("Run events publishing to '{}' ({} partition(s)) on {}",
                    properties.topic(), partitions.size(), properties.bootstrapServers());
        } catch (UnknownTopicOrPartitionException e) {
            log.error("""
                    Topic '{}' does not exist on {}.
                    Run events will not be published until your platform team creates it.
                    Suggested: 12 partitions, 30 day retention, cleanup.policy=delete.
                    Migrations are unaffected.""",
                    properties.topic(), properties.bootstrapServers());
        } catch (TimeoutException e) {
            log.error("Could not reach Kafka at {} to check topic '{}'. "
                            + "Run events will not be published. Migrations are unaffected.",
                    properties.bootstrapServers(), properties.topic());
        }
    }

    @PreDestroy
    public void close() {
        try {
            producer.flush();
            producer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("Kafka producer did not close cleanly", e);
        }
        log.info("Run events stopped: {} published, {} failed", published.get(), failed.get());
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Asynchronous, with the callback only counting outcomes. Waiting on the future would put
     * broker latency directly into the chunk execution path — so a slow broker would slow every
     * migration, which inverts the relationship between the work and the description of it.
     *
     * <p>Keyed by run id, so one run's events land on one partition and stay in order. A consumer
     * seeing CHUNK_COMPLETED after RUN_COMPLETED would have to reorder them itself.
     */
    @Override
    public void publish(RunEvent event) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    properties.topic(), event.runId().toString(), toJson(event));

            record.headers().add("eventType", event.type().name().getBytes());
            record.headers().add("tenantId", event.tenantId().toString().getBytes());

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    published.incrementAndGet();
                } else {
                    long total = failed.incrementAndGet();
                    if (total % 100 == 1) {
                        log.warn("Run event publish failed ({} so far): {}. Migrations unaffected.",
                                total, exception.getMessage());
                    }
                }
            });
        } catch (Exception e) {
            // Includes the buffer-full timeout. Counted and dropped: an event bus under pressure
            // must not become back-pressure on the migration.
            failed.incrementAndGet();
            log.debug("Could not queue run event", e);
        }
    }

    private String toJson(RunEvent event) {
        ObjectNode node = Json.newObject();
        node.put("type", event.type().name());
        node.put("occurredAt", event.occurredAt().toString());
        node.put("tenantId", event.tenantId().toString());
        node.put("runId", event.runId().toString());
        node.put("pipelineId", event.pipelineId());
        node.put("pipelineName", event.pipelineName());
        node.put("versionNumber", event.versionNumber());

        ObjectNode details = Json.newObject();
        for (Map.Entry<String, Object> entry : event.details().entrySet()) {
            details.set(entry.getKey(), Json.mapper().valueToTree(entry.getValue()));
        }
        node.set("details", details);
        return node.toString();
    }

    public long publishedCount() {
        return published.get();
    }

    public long failedCount() {
        return failed.get();
    }
}
