package com.dmp.app.config;

import com.dmp.application.port.out.RecordIndexPort;
import com.dmp.events.kafka.KafkaEventProperties;
import com.dmp.events.kafka.KafkaRecordIndex;
import com.dmp.events.kafka.RecordIndexConsumer;
import com.dmp.recordlog.opensearch.OpenSearchRecordIndex;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

/**
 * Wires the record index to run through Kafka rather than straight into OpenSearch.
 *
 * <p>Assembled here rather than by component scanning, because the two halves have to be told apart
 * and Spring cannot do it from the type alone: both are a {@link RecordIndexPort}, and one of them
 * wraps the other.
 *
 * <p><b>Where each half runs is a deployment decision, not a code one.</b> That is the point of
 * moving the write path onto a topic: the seam between producing evidence and indexing it is now a
 * Kafka topic, so whether the consumer shares a JVM with the workers or runs as its own fleet is
 * settled by a profile and a container, with no code aware of the difference.
 *
 * <ul>
 *   <li>{@code worker} — produces. Any pod executing chunks needs somewhere to put its evidence.</li>
 *   <li>{@code indexer} — consumes. Scale this with the size of the backlog, which has nothing to
 *       do with how many migrations are running.</li>
 *   <li>{@code all} / {@code default} — both, which is one container and the right answer on a
 *       laptop and for most deployments.</li>
 * </ul>
 */
@Configuration
@ConditionalOnProperty(prefix = "dmp.recordindex", name = "queue", havingValue = "kafka")
public class RecordIndexQueueConfig {

    /**
     * The write path: entries go to Kafka, reads still go to OpenSearch.
     *
     * <p>{@code @Primary} so everything injecting a {@link RecordIndexPort} gets this one. The
     * OpenSearch implementation stays a bean, and stays reachable — the consumer needs it by its
     * concrete type, which is the only place in the platform that knows there are two.
     */
    @Bean
    @Primary
    @Profile({"worker", "control-plane", "all", "default"})
    public RecordIndexPort kafkaRecordIndex(OpenSearchRecordIndex openSearch,
                                            KafkaEventProperties properties) {
        return new KafkaRecordIndex(openSearch, properties);
    }

    /**
     * The read path: takes entries off the topic and puts them in OpenSearch.
     *
     * <p>Given the OpenSearch implementation directly and never the primary bean, which would make
     * it publish back to the topic it is draining.
     */
    @Bean
    @Profile({"indexer", "all", "default"})
    public RecordIndexConsumer recordIndexConsumer(OpenSearchRecordIndex openSearch,
                                                   KafkaEventProperties properties) {
        return new RecordIndexConsumer(openSearch, properties);
    }
}
