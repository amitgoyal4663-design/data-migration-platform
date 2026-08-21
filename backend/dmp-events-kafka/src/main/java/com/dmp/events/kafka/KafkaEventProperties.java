package com.dmp.events.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configuration for run event publishing.
 *
 * <p>Off unless {@code dmp.events.kafka.enabled=true}. Kafka is not a requirement of the engine —
 * work distribution and checkpointing use MongoDB — so a deployment with no bus simply does not
 * publish.
 *
 * @param topic must already exist; the platform never creates one
 */
@ConfigurationProperties(prefix = "dmp.events.kafka")
public record KafkaEventProperties(
        @DefaultValue("localhost:9092") String bootstrapServers,

        /*
         * Versioned in the name so an incompatible change to the message shape becomes a new topic
         * requested alongside the old one, rather than a coordinated stop-the-world cutover of
         * every consumer.
         */
        @DefaultValue("dmp.run.events.v1") String topic) {
}
