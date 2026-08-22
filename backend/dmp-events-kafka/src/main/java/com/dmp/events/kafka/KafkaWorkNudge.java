package com.dmp.events.kafka;

import com.dmp.application.port.out.WorkNudge;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wakes a worker the moment work appears, over Kafka.
 *
 * <p>Built for one purpose: the poll interval, not the work, was the cost of a run. Fifty-one
 * chunks, 17.7 seconds of execution, 261.7 seconds of wall clock — the difference being a worker
 * asleep beside work it had already finished. The worker loop is otherwise unchanged; only its
 * sleep became interruptible.
 *
 * <p><b>The poll is the mechanism; this is the optimisation.</b> Every guarantee a migration needs
 * — that a chunk runs once, that a failure retries, that a dead pod's work is reclaimed — still
 * comes from the atomic claim and the lease. A nudge carries no chunk and grants nothing, so it may
 * be duplicated, lost, delayed or delivered to a pod with no free slot without any of those
 * mattering. That is why Kafka's own guarantees are not load-bearing here, and why none of the
 * usual objections to a log as a work queue apply.
 *
 * <p><b>A group per pod, deliberately.</b> A shared consumer group would assign each partition to
 * one pod, so a nudge would wake whichever pod owns that partition — possibly a busy one, while a
 * free pod owning a different partition heard nothing. Every pod hearing every nudge and racing for
 * the claim is work-conserving: whoever is free takes it, and the losers spend one cheap query.
 *
 * <p><b>Never replays.</b> The consumer seeks to the end of the log on assignment and commits
 * nothing. A doorbell is worthless a second after it rings, and a pod restarting into a backlog of
 * ten thousand stale nudges would spin through them for no reason.
 */
@Component
@ConditionalOnProperty(prefix = "dmp.events.kafka", name = "enabled", havingValue = "true")
public class KafkaWorkNudge implements WorkNudge {

    private static final Logger log = LoggerFactory.getLogger(KafkaWorkNudge.class);

    /**
     * How long the consumer blocks in one poll.
     *
     * <p>Bounds how quickly the thread notices it has been asked to stop, and nothing else — a
     * nudge arriving mid-poll returns immediately.
     */
    private static final Duration CONSUMER_POLL = Duration.ofMillis(500);

    private final KafkaEventProperties properties;
    private final Producer<String, String> producer;

    /**
     * The signal between the consumer thread and whichever thread is waiting.
     *
     * <p>A semaphore rather than a queue because the content is never read: the only fact being
     * communicated is that something happened. Permits are drained after a successful wait, so a
     * burst of nudges collapses into one wake rather than queueing up loops that find nothing.
     */
    private final Semaphore signal = new Semaphore(0);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile boolean usable;
    private Thread listener;

    public KafkaWorkNudge(KafkaEventProperties properties) {
        this.properties = properties;

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "dmp-nudge");

        // acks=0 and no retries. A lost nudge costs one poll interval; a nudge waited on costs the
        // chunk that just finished. There is no version of this worth blocking the data path for.
        config.put(ProducerConfig.ACKS_CONFIG, "0");
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2_000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1_000);
        // No linger: batching a doorbell defeats it.
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        this.producer = new KafkaProducer<>(config);
    }

    /**
     * Confirms the topic exists and starts listening, or says why it is not.
     *
     * <p>A missing topic does not fail startup, which is the one place this differs from the run
     * event topic. That one carries the audit trail somebody asked for; this one is an internal
     * optimisation, and refusing to start because a doorbell is unwired would take a platform down
     * to protect a five-second saving. The platform falls back to polling and says so once.
     *
     * <p>The topic is still never created. That authority is not assumed, here or anywhere.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        try {
            var partitions = producer.partitionsFor(properties.nudgeTopic());
            if (partitions == null || partitions.isEmpty()) {
                throw new IllegalStateException("no partitions");
            }
        } catch (Exception e) {
            log.warn("Work nudges are off: topic '{}' is not available on {} ({}). Workers will "
                            + "poll instead, which is correct but slower — a run spends the poll "
                            + "interval between chunks. Ask for the topic to be created to turn "
                            + "this on.",
                    properties.nudgeTopic(), properties.bootstrapServers(), e.getMessage());
            return;
        }

        usable = true;
        listener = new Thread(this::listen, "dmp-work-nudge");
        listener.setDaemon(true);
        listener.start();
        log.info("Work nudges on via '{}': workers wake as soon as a chunk is claimable",
                properties.nudgeTopic());
    }

    @Override
    public void publish() {
        if (!usable) {
            return;
        }
        try {
            // Fire and forget, with no callback. The send is already non-blocking; waiting on the
            // future would put a broker round trip on the path that just finished a chunk.
            producer.send(new ProducerRecord<>(properties.nudgeTopic(), null, ""));
        } catch (Exception e) {
            // Including the case where the buffer is full because the broker is gone. A worker
            // whose migration failed because it could not ring a doorbell would be absurd.
            log.debug("Could not publish a work nudge: {}", e.getMessage());
        }
    }

    @Override
    public boolean await(Duration timeout) {
        try {
            boolean nudged = signal.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (nudged) {
                // Everything that arrived while this thread was waking is the same news.
                signal.drainPermits();
            }
            return nudged;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean isEnabled() {
        return usable;
    }

    /**
     * Consumes nudges and does nothing with them but signal.
     *
     * <p>A group id unique to this process, so every pod hears every nudge. Offsets are never
     * committed and the group is transient — Kafka expires it on its own once the pod is gone.
     */
    private void listen() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "dmp-nudge-" + UUID.randomUUID());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Only what arrives from now on. A restart must not walk a backlog of stale doorbells.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "dmp-nudge-consumer");
        // One at a time is plenty: the payload is discarded and many nudges collapse into one wake.
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(properties.nudgeTopic()));

            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(CONSUMER_POLL);
                    if (!records.isEmpty()) {
                        // One permit however many arrived. The waiting thread re-queries either
                        // way, so a count would be a number nobody could act on.
                        signal.release();
                    }
                } catch (org.apache.kafka.common.errors.InterruptException
                         | org.apache.kafka.common.errors.WakeupException e) {
                    // Asked to stop. Kafka wraps the interrupt in its own type.
                    return;
                } catch (Exception e) {
                    // A broker restart lands here. Logged quietly and retried, because the poll
                    // fallback means an unavailable doorbell is slow rather than broken.
                    log.debug("Work nudge listener recovering: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Work nudge listener stopped; workers will poll instead: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        usable = false;
        if (listener != null) {
            listener.interrupt();
        }
        try {
            producer.close(Duration.ofSeconds(2));
        } catch (Exception e) {
            log.debug("Nudge producer did not close cleanly: {}", e.getMessage());
        }
    }
}
