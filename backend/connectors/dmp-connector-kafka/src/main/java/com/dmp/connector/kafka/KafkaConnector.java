package com.dmp.connector.kafka;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConfigFields;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.RecordBatch;
import com.dmp.connector.api.Sink;
import com.dmp.connector.api.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Reads and writes Kafka topics.
 *
 * <p><b>Never creates a topic, and has no setting to.</b> The platform's service account is not
 * assumed to hold that authority — in many organisations topics are provisioned by a platform team
 * on request, and an application that quietly creates one in development produces an authorisation
 * failure in production at the worst possible moment. A missing topic stops the run immediately
 * with a message naming the topic and the cluster, which is a message that can be forwarded to
 * whoever provisions them.
 *
 * <p><b>One chunk per partition.</b> That is not a convenience — a partition is the only unit Kafka
 * offers that can be read independently and resumed exactly, and its offset is a perfect resume
 * cursor. A twelve-partition topic therefore parallelises twelve ways across the fleet with no
 * coordination, and each chunk resumes at the precise record it reached.
 *
 * <p><b>Assigned, not subscribed.</b> Consumer groups exist to divide partitions among members and
 * commit offsets on their behalf. This engine already does both, through chunk claiming and
 * checkpoints, and running both would mean two mechanisms disagreeing about who owns what — with a
 * rebalance able to move a partition out from under a chunk mid-write. {@code assign()} takes the
 * partition the engine gave this chunk and nothing else.
 */
public class KafkaConnector implements Source, Sink {

    private static final String TYPE = "kafka";

    /** How long one poll waits before returning empty. Short: the stream must notice its end. */
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(15);

    @Override
    public ConnectorSpec spec() {
        return new ConnectorSpec(
                TYPE,
                "Apache Kafka",
                "Reads and writes Kafka topics. Each partition becomes one chunk, resumed by "
                        + "offset. Topics are never created — a missing topic stops the run.",
                ConnectorSpec.Direction.BOTH,
                configSchema(),
                Set.of("username", "password"),
                "1.0.0");
    }

    @Override
    public void testConnection(ConnectorContext context) {
        KafkaConfig config = KafkaConfig.from(context);
        try (KafkaConsumer<String, String> consumer = consumer(config)) {
            requirePartitions(consumer, config);
        }
    }

    /**
     * Looks the topic up, and refuses clearly when it is not there.
     *
     * <p>{@code partitionsFor} needs only metadata access, not the admin rights the platform
     * deliberately does not require. Kafka returns an empty list rather than raising for an unknown
     * topic when auto-creation is off, so the emptiness is what is checked.
     */
    private static List<PartitionInfo> requirePartitions(KafkaConsumer<String, String> consumer,
                                                         KafkaConfig config) {
        List<PartitionInfo> partitions;
        try {
            partitions = consumer.partitionsFor(config.topic(), ADMIN_TIMEOUT);
        } catch (TimeoutException e) {
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Could not reach Kafka at " + config.bootstrapServers() + " within "
                            + ADMIN_TIMEOUT.toSeconds() + "s to look up topic '" + config.topic()
                            + "'.", e);
        } catch (AuthorizationException e) {
            throw new ConnectorException(ConnectorException.Kind.AUTHENTICATION,
                    "Not authorised to read metadata for topic '" + config.topic() + "' on "
                            + config.bootstrapServers() + ".", e);
        }

        if (partitions == null || partitions.isEmpty()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Topic '" + config.topic() + "' does not exist on " + config.bootstrapServers()
                            + ". This platform never creates topics — ask whoever provisions them "
                            + "on your cluster to create it, then run this again.");
        }
        return partitions;
    }

    /**
     * Turns a rejected send into the message the user needs.
     *
     * <p>A topic deleted while a run is in flight is the case worth singling out. The broker
     * answers {@code UnknownTopicOrPartitionException}, which as a bare message says only that a
     * partition was not found — and being classed as retryable, the chunk would be attempted again,
     * hit the guard at session open, and only then produce the sentence explaining what actually
     * happened. The first failure is the one somebody reads, so it should be the one that explains
     * itself.
     *
     * <p>Classified as CONFIGURATION rather than UNAVAILABLE for the same reason the guard is:
     * retrying cannot bring a topic back, and this platform never creates one. Anything else keeps
     * its retryable classification, because most write failures genuinely are transient.
     */
    private static ConnectorException translateWriteFailure(KafkaConfig config, Throwable cause) {
        if (cause instanceof UnknownTopicOrPartitionException) {
            return new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Topic '" + config.topic() + "' disappeared from " + config.bootstrapServers()
                            + " while this run was writing to it. Records already acknowledged are "
                            + "in the topic that existed; the rest are not. This platform never "
                            + "creates topics — ask whoever provisions them to restore it, then "
                            + "retry the failed chunks.", cause);
        }
        if (cause instanceof AuthorizationException) {
            return new ConnectorException(ConnectorException.Kind.AUTHENTICATION,
                    "Not authorised to write to topic '" + config.topic() + "' on "
                            + config.bootstrapServers() + ".", cause);
        }
        return new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                "Kafka rejected a batch for " + config.describe() + ": " + cause.getMessage(),
                cause);
    }

    // ------------------------------------------------------------------ source

    @Override
    public SourceSession openSource(ConnectorContext context) {
        KafkaConfig config = KafkaConfig.from(context);

        return new SourceSession() {

            /**
             * One chunk per partition, each bounded by the end offset at planning time.
             *
             * <p>The upper bound is what makes a migration from Kafka finite. Without it a chunk
             * would follow the topic forever and the run would never complete — correct for a
             * streaming pipeline, wrong for "copy what is in this topic now", which is what a
             * migration means. Records arriving after planning belong to the next run.
             */
            @Override
            public List<SplitSpec> plan(Preparation preparation, PlanRequest request) {
                try (KafkaConsumer<String, String> consumer = consumer(config)) {
                    List<PartitionInfo> partitions = requirePartitions(consumer, config);

                    List<TopicPartition> assignable = partitions.stream()
                            .map(info -> new TopicPartition(info.topic(), info.partition()))
                            .toList();

                    Map<TopicPartition, Long> starts = config.startFrom() == KafkaConfig.StartFrom.LATEST
                            ? consumer.endOffsets(assignable)
                            : consumer.beginningOffsets(assignable);
                    Map<TopicPartition, Long> ends = consumer.endOffsets(assignable);

                    List<SplitSpec> splits = new ArrayList<>();
                    long total = 0;

                    for (TopicPartition partition : assignable) {
                        long from = starts.getOrDefault(partition, 0L);
                        long to = ends.getOrDefault(partition, 0L);
                        if (to <= from) {
                            // Nothing to read. Planning it anyway would hand a worker a chunk whose
                            // only job is to discover it is empty.
                            continue;
                        }
                        total += to - from;

                        ObjectNode spec = Json.newObject();
                        spec.put("partition", partition.partition());
                        spec.put("fromOffset", from);
                        spec.put("toOffset", to);

                        splits.add(new SplitSpec(partition.partition(), spec,
                                "partition " + partition.partition() + " [" + from + "–" + to + ")"));
                    }

                    context.log().info("Planned {} chunk(s) over {} record(s) in {}",
                            splits.size(), total, config.describe());
                    return splits;
                }
            }

            @Override
            public RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize) {
                return new KafkaRecordStream(config, context, split, fromCursor, fetchSize);
            }
        };
    }

    /**
     * Reads one partition from its checkpoint to the end offset fixed at planning time.
     *
     * <p>Polls in batches and hands them out one at a time, so the engine's pull model still
     * governs the pace: a slow sink stops the calls to {@code next()}, the buffer drains, and only
     * then is the broker asked for more.
     */
    private static final class KafkaRecordStream implements RecordStream {

        private final KafkaConfig config;
        private final ConnectorContext context;
        private final KafkaConsumer<String, String> consumer;
        private final TopicPartition partition;
        private final long endOffset;
        private final int fetchSize;

        private final Deque<ConsumerRecord<String, String>> buffered = new ArrayDeque<>();
        private long nextOffset;
        private long emitted;
        private boolean exhausted;

        KafkaRecordStream(KafkaConfig config, ConnectorContext context, SplitSpec split,
                          JsonNode fromCursor, int fetchSize) {
            this.config = config;
            this.context = context;
            this.fetchSize = fetchSize;

            JsonNode spec = split.spec();
            this.partition = new TopicPartition(config.topic(), spec.path("partition").asInt());
            this.endOffset = spec.path("toOffset").asLong();

            // The checkpoint wins over the plan: it is where this chunk actually got to.
            this.nextOffset = fromCursor != null && fromCursor.hasNonNull("offset")
                    ? fromCursor.get("offset").asLong()
                    : spec.path("fromOffset").asLong();
            this.emitted = fromCursor != null && fromCursor.hasNonNull("emitted")
                    ? fromCursor.get("emitted").asLong() : 0;

            this.consumer = consumer(config, fetchSize);
            consumer.assign(List.of(partition));
            consumer.seek(partition, nextOffset);
        }

        @Override
        public DataRecord next() {
            while (buffered.isEmpty()) {
                if (exhausted || nextOffset >= endOffset || !poll()) {
                    return null;
                }
            }

            ConsumerRecord<String, String> record = buffered.poll();
            nextOffset = record.offset() + 1;
            emitted++;

            return new DataRecord(payloadOf(record), record.key(), headersOf(record), emitted, 0,
                    record.serializedValueSize() < 0 ? 0 : record.serializedValueSize());
        }

        /** @return true if anything was buffered */
        private boolean poll() {
            ConsumerRecords<String, String> polled = consumer.poll(POLL_TIMEOUT);

            for (ConsumerRecord<String, String> record : polled.records(partition)) {
                // The plan fixed where this chunk ends. Anything produced since belongs to a later
                // run, and taking it would make the same run read a different amount each time.
                if (record.offset() >= endOffset) {
                    exhausted = true;
                    break;
                }
                buffered.add(record);
            }

            if (buffered.isEmpty() && polled.isEmpty()) {
                // An empty poll before the end offset means the records were removed by retention
                // between planning and reading. Stopping is correct and worth saying out loud.
                context.log().warn("Partition {} returned nothing before its planned end offset {} "
                                + "(at {}). Records were most likely aged out by retention.",
                        partition.partition(), endOffset, nextOffset);
                exhausted = true;
            }
            return !buffered.isEmpty();
        }

        /**
         * The message body as JSON, or wrapped when it is not JSON at all.
         *
         * <p>A topic of plain strings is ordinary, and failing the whole chunk over one would be a
         * poor trade. The raw text is preserved under a field so nothing is lost and the shape
         * stays predictable for a transform.
         */
        private JsonNode payloadOf(ConsumerRecord<String, String> record) {
            if (record.value() == null) {
                // A tombstone. Represented rather than dropped: in a compacted topic the deletion
                // is the information.
                ObjectNode tombstone = Json.newObject();
                tombstone.putNull("value");
                return tombstone;
            }
            try {
                JsonNode parsed = Json.mapper().readTree(record.value());
                return parsed.isObject() || parsed.isArray() ? parsed : wrap(record.value());
            } catch (Exception notJson) {
                return wrap(record.value());
            }
        }

        private JsonNode wrap(String raw) {
            ObjectNode node = Json.newObject();
            node.put("value", raw);
            return node;
        }

        private Map<String, String> headersOf(ConsumerRecord<String, String> record) {
            Map<String, String> headers = new HashMap<>();
            headers.put("kafka.topic", record.topic());
            headers.put("kafka.partition", String.valueOf(record.partition()));
            headers.put("kafka.offset", String.valueOf(record.offset()));
            headers.put("kafka.timestamp", String.valueOf(record.timestamp()));
            record.headers().forEach(header -> headers.put(
                    "kafka.header." + header.key(),
                    header.value() == null ? "" : new String(header.value())));
            return headers;
        }

        @Override
        public JsonNode cursor() {
            ObjectNode cursor = Json.newObject();
            cursor.put("offset", nextOffset);
            cursor.put("emitted", emitted);
            return cursor;
        }

        @Override
        public void close() {
            consumer.close(Duration.ofSeconds(5));
        }
    }

    // -------------------------------------------------------------------- sink

    @Override
    public SinkSession openSink(ConnectorContext context) {
        KafkaConfig config = KafkaConfig.from(context);

        // Verified before a single record is read, not on the first write. Discovering a missing
        // topic after a source has streamed half a table wastes the whole read.
        try (KafkaConsumer<String, String> probe = consumer(config)) {
            requirePartitions(probe, config);
        }

        Producer<String, String> producer = producer(config);

        return new SinkSession() {

            @Override
            public Capabilities capabilities() {
                return new Capabilities(
                        false,      // a topic is an append-only log, not a keyed store
                        "A topic is an append-only log: sending the same record twice produces two "
                                + "messages, and no setting on this sink can change that. Give "
                                + "records a stable key and let consumers deduplicate on it, or "
                                + "publish to a compacted topic so only the last value per key "
                                + "survives.",
                        false,      // no transaction spanning the batch
                        false,
                        false,
                        // A batch transform may return one object for the whole batch, and this
                        // sink publishes it as one message. That is a normal shape for a topic —
                        // an event carrying a set of orders rather than one event per row — and
                        // refusing it forced every batch to become as many messages as rows.
                        true,
                        0,          // no protocol ceiling on how many can be sent
                        1_000);
            }

            /**
             * Sends the batch and waits for every acknowledgement.
             *
             * <p>Waiting is the point. The engine advances the checkpoint once this returns, so
             * returning before the broker has the records would let a crash resume past messages
             * that were never durably stored — exactly the loss the checkpoint order prevents.
             */
            @Override
            public WriteResult write(RecordBatch batch) {
                if (batch.isEmpty()) {
                    return WriteResult.allWritten(0, 0);
                }

                List<java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>>
                        pending = new ArrayList<>(batch.size());

                // An envelope is the batch as one message. Keyed by the first record, so a batch
                // and the records it was built from land on the same partition — otherwise a
                // consumer reading a keyed topic would see the summary and its detail diverge.
                if (batch.envelope().isPresent()) {
                    pending.add(producer.send(new ProducerRecord<>(
                            config.topic(),
                            batch.records().isEmpty() ? null : keyOf(batch.records().get(0)),
                            batch.envelope().get().toString())));
                } else {
                    for (DataRecord record : batch.records()) {
                        pending.add(producer.send(new ProducerRecord<>(
                                config.topic(), keyOf(record), record.payload().toString())));
                    }
                }
                producer.flush();

                for (var future : pending) {
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                                "Interrupted while waiting for Kafka to acknowledge a batch", e);
                    } catch (ExecutionException e) {
                        throw translateWriteFailure(config, e.getCause());
                    }
                }
                return WriteResult.allWritten(batch.size(), batch.totalBytes());
            }

            /**
             * The partition key, which decides ordering.
             *
             * <p>Records sharing a key land on one partition and stay in order relative to each
             * other. Without a key Kafka spreads them, and two updates to the same entity can be
             * consumed out of order — a silent corruption that only appears under load.
             */
            private String keyOf(DataRecord record) {
                return config.keyFieldName()
                        .map(field -> {
                            JsonNode value = record.payload().get(field);
                            return value == null || value.isNull() ? null : value.asText();
                        })
                        .orElse(record.key());
            }

            @Override
            public void close() {
                producer.flush();
                producer.close(Duration.ofSeconds(10));
            }
        };
    }

    // ----------------------------------------------------------------- clients

    private static KafkaConsumer<String, String> consumer(KafkaConfig config) {
        return consumer(config, 500);
    }

    private static KafkaConsumer<String, String> consumer(KafkaConfig config, int fetchSize) {
        Properties properties = new Properties();
        properties.putAll(config.commonProperties());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.max(1, fetchSize));

        // Off, and it must stay off: the engine's checkpoint is the only record of progress, and a
        // background commit would let the two disagree after a crash.
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Required by the client even when assigning partitions directly and never joining a
        // group. Named after the platform so it is identifiable in broker logs.
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dmp-reader");

        // Every read seeks explicitly, so this only matters if a seek were ever skipped — in which
        // case failing is far better than silently starting somewhere arbitrary.
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        return new KafkaConsumer<>(properties);
    }

    private static Producer<String, String> producer(KafkaConfig config) {
        Properties properties = new Properties();
        properties.putAll(config.commonProperties());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks=all, unlike the event publisher's acks=1. These are the user's records: a broker
        // failure losing an acknowledged write would be data loss in a migration, not a missed
        // notification.
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new KafkaProducer<>(properties);
    }

    // ------------------------------------------------------------------ schema

    private static JsonNode configSchema() {
        ObjectNode schema = Json.newObject();
        schema.put("type", "object");

        ObjectNode properties = Json.newObject();
        properties.set("bootstrapServers", ConfigFields.fromEnvironment(field("string",
                "Broker list, e.g. broker1:9092,broker2:9092. Different in every environment and "
                        + "owned by whoever runs the cluster, so name the variable rather than "
                        + "typing it.")));
        properties.set("topic", field("string",
                "Topic to read or write. It must already exist — this platform never creates one."));
        properties.set("keyField", ConfigFields.sinkField("string",
                "Field whose value becomes the message key. Records sharing a key keep their "
                        + "order. Leave blank to let Kafka spread them."));

        properties.set("startFrom", ConfigFields.advanced(ConfigFields.sourceEnumField(
                "Where a read starts when there is no saved position.", "EARLIEST", "LATEST")));
        properties.set("securityProtocol", ConfigFields.advanced(
                enumField("How the client authenticates to the cluster.",
                        "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")));
        properties.set("saslMechanism", ConfigFields.advanced(
                enumField("SASL mechanism, when the protocol uses SASL.",
                        "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")));

        schema.set("properties", properties);
        schema.set("required", Json.mapper().createArrayNode().add("bootstrapServers").add("topic"));
        return schema;
    }

    private static ObjectNode field(String type, String description) {
        ObjectNode node = Json.newObject();
        node.put("type", type);
        node.put("description", description);
        return node;
    }

    private static ObjectNode enumField(String description, String... values) {
        ObjectNode node = field("string", description);
        var allowed = Json.mapper().createArrayNode();
        for (String value : values) {
            allowed.add(value);
        }
        node.set("enum", allowed);
        node.put("default", values[0]);
        return node;
    }
}
