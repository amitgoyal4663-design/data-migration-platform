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
        @DefaultValue("dmp.run.events.v1") String topic,

        /**
         * Topic carrying the "there is claimable work" nudge.
         *
         * <p>Separate from the event topic, and not merely for tidiness. The event topic is an
         * outbound record other systems consume and keep; this one is a doorbell whose messages are
         * worthless a second after they are sent. Sharing a topic would mean either retaining
         * doorbells or discarding events, and would put a run's audit trail behind the retention
         * policy of an internal optimisation.
         *
         * <p>One partition is enough while execution is sequential: there is one slot in the fleet,
         * so ordering and parallelism across partitions buy nothing.
         */
        @DefaultValue("dmp.work.available.v1") String nudgeTopic) {
}
