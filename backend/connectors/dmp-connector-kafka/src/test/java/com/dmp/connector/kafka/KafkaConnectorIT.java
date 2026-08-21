package com.dmp.connector.kafka;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.RecordBatch;
import com.dmp.connector.api.Sink;
import com.dmp.connector.api.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Kafka connector against a real broker.
 *
 * <p>The first test is the one that matters most operationally. This platform is deployed where
 * topics are provisioned by a platform team and the application's account cannot create them, so a
 * missing topic must stop the run with a message naming it — not be auto-created in development and
 * fail on permissions in production.
 */
class KafkaConnectorIT {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectorIT.class);

    private static KafkaContainer kafka;
    private static final KafkaConnector connector = new KafkaConnector();

    @BeforeAll
    static void startBroker() {
        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"))
                // The whole point of this connector's contract. With auto-creation on, a missing
                // topic would silently spring into existence and the first test would pass for
                // entirely the wrong reason.
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        kafka.start();
    }

    @AfterAll
    static void stopBroker() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    private static void createTopic(String name, int partitions) {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("Could not create test topic " + name, e);
        }
    }

    private static ConnectorContext context(String topic) {
        ObjectNode config = Json.newObject();
        config.put("bootstrapServers", kafka.getBootstrapServers());
        config.put("topic", topic);
        config.put("startFrom", "EARLIEST");
        config.put("keyField", "orderId");

        return new ConnectorContext() {
            @Override
            public JsonNode config() {
                return config;
            }

            @Override
            public Optional<String> secret(String name) {
                return Optional.empty();
            }

            @Override
            public String workerId() {
                return "test-worker";
            }

            @Override
            public String runId() {
                return "test-run";
            }

            @Override
            public Logger log() {
                return log;
            }
        };
    }

    private static DataRecord order(String id, int quantity) {
        ObjectNode node = Json.newObject();
        node.put("orderId", id);
        node.put("quantity", quantity);
        return DataRecord.of(node, id, quantity);
    }

    @Test
    @DisplayName("a missing topic stops the run and names it")
    void missingTopicIsRefused() {
        ConnectorContext context = context("topic-that-does-not-exist");

        assertThatThrownBy(() -> connector.testConnection(context))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("topic-that-does-not-exist")
                .hasMessageContaining("does not exist")
                .hasMessageContaining("never creates topics");
    }

    @Test
    @DisplayName("a sink refuses a missing topic before any data is read")
    void sinkChecksTheTopicUpFront() {
        // Checked when the session opens rather than on the first write. Discovering it after a
        // source has streamed half a table wastes the entire read.
        assertThatThrownBy(() -> connector.openSink(context("sink-topic-missing")))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    @DisplayName("plans one chunk per partition that holds records")
    void plansOneChunkPerPartition() {
        createTopic("orders-planning", 4);
        writeRecords("orders-planning", 40);

        try (Source.SourceSession session = connector.openSource(context("orders-planning"))) {
            List<Source.SplitSpec> splits =
                    session.plan(Preparation.none(), new Source.PlanRequest(1_000, 100));

            // A partition is the only unit Kafka can read independently and resume exactly, so it
            // is the only honest chunk boundary. Empty partitions are not planned — a chunk whose
            // only job is to discover it has nothing to do is wasted work.
            assertThat(splits).isNotEmpty().hasSizeLessThanOrEqualTo(4);
            assertThat(splits).allSatisfy(split -> {
                assertThat(split.spec().get("toOffset").asLong())
                        .isGreaterThan(split.spec().get("fromOffset").asLong());
                assertThat(split.label()).contains("partition");
            });
        }
    }

    @Test
    @DisplayName("reads back exactly what was written")
    void roundTrip() {
        createTopic("orders-roundtrip", 3);
        writeRecords("orders-roundtrip", 30);

        List<JsonNode> read = readAll("orders-roundtrip");

        assertThat(read).hasSize(30);
        assertThat(read).extracting(node -> node.get("orderId").asText())
                .containsExactlyInAnyOrderElementsOf(
                        java.util.stream.IntStream.range(0, 30).mapToObj(i -> "order-" + i).toList());
    }

    @Test
    @DisplayName("resumes from a saved offset rather than re-reading")
    void resumesFromCursor() {
        createTopic("orders-resume", 1);
        writeRecords("orders-resume", 10);

        ConnectorContext context = context("orders-resume");
        try (Source.SourceSession session = connector.openSource(context)) {
            Source.SplitSpec split =
                    session.plan(Preparation.none(), new Source.PlanRequest(1_000, 100)).get(0);

            JsonNode cursor;
            try (Source.RecordStream stream = session.read(split, null, 100)) {
                for (int i = 0; i < 4; i++) {
                    assertThat(stream.next()).isNotNull();
                }
                cursor = stream.cursor();
            }

            // Resuming must not replay what was already handed out. That is the entire reason the
            // cursor is read only after the sink has accepted a batch.
            List<JsonNode> remaining = new ArrayList<>();
            try (Source.RecordStream stream = session.read(split, cursor, 100)) {
                DataRecord record;
                while ((record = stream.next()) != null) {
                    remaining.add(record.payload());
                }
            }
            assertThat(remaining).hasSize(6);
        }
    }

    @Test
    @DisplayName("stops at the end offset fixed when the chunk was planned")
    void doesNotFollowTheTopicForever() {
        createTopic("orders-bounded", 1);
        writeRecords("orders-bounded", 5);

        ConnectorContext context = context("orders-bounded");
        try (Source.SourceSession session = connector.openSource(context)) {
            Source.SplitSpec split =
                    session.plan(Preparation.none(), new Source.PlanRequest(1_000, 100)).get(0);

            // Produced after planning. A migration copies what was in the topic when it started;
            // without an upper bound the chunk would never end and the run would never complete.
            writeRecords("orders-bounded", 5);

            int count = 0;
            try (Source.RecordStream stream = session.read(split, null, 100)) {
                while (stream.next() != null) {
                    count++;
                }
            }
            assertThat(count).isEqualTo(5);
        }
    }

    @Test
    @DisplayName("a non-JSON message is preserved rather than failing the chunk")
    void plainTextIsWrapped() {
        createTopic("orders-plaintext", 1);

        try (Sink.SinkSession sink = connector.openSink(context("orders-plaintext"))) {
            ObjectNode raw = Json.newObject();
            raw.put("value", "not json at all");
            sink.write(RecordBatch.of(List.of(DataRecord.of(raw, 1))));
        }

        List<JsonNode> read = readAll("orders-plaintext");
        assertThat(read).hasSize(1);
        assertThat(read.get(0).get("value").asText()).isEqualTo("not json at all");
    }

    @Test
    @DisplayName("a topic is a log, so writes are not idempotent and carry advice saying why")
    void capabilitiesAreHonest() {
        createTopic("orders-capabilities", 1);

        try (Sink.SinkSession sink = connector.openSink(context("orders-capabilities"))) {
            Sink.Capabilities capabilities = sink.capabilities();

            assertThat(capabilities.writeIsIdempotent()).isFalse();
            // Declaring the flag without the remedy is what makes a warning useless. Nothing in
            // the engine can word this — only the connector knows a topic has no key to overwrite.
            assertThat(capabilities.advice()).isPresent();
            // Each record is its own message; there is no single payload for a batch script to
            // shape, and claiming otherwise would let a batch transform silently do nothing.
            assertThat(capabilities.sendsBatchAsSinglePayload()).isFalse();
        }
    }

    private static void writeRecords(String topic, int count) {
        try (Sink.SinkSession sink = connector.openSink(context(topic))) {
            List<DataRecord> records = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                records.add(order("order-" + i, i));
            }
            Sink.WriteResult result = sink.write(RecordBatch.of(records));
            assertThat(result.written()).isEqualTo(count);
        }
    }

    private static List<JsonNode> readAll(String topic) {
        List<JsonNode> payloads = new ArrayList<>();
        try (Source.SourceSession session = connector.openSource(context(topic))) {
            for (Source.SplitSpec split :
                    session.plan(Preparation.none(), new Source.PlanRequest(1_000, 100))) {
                try (Source.RecordStream stream = session.read(split, null, 100)) {
                    DataRecord record;
                    while ((record = stream.next()) != null) {
                        payloads.add(record.payload());
                    }
                }
            }
        }
        return payloads;
    }
}
