package com.dmp.connector.databricks;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The statement lifecycle, driven end to end against a local stand-in.
 *
 * <p>These assertions are about <em>sequence</em> and about <em>position</em>, because those are
 * what an asynchronous source gets wrong. Planning before the statement has succeeded finds no
 * manifest; treating the first PENDING as an answer reads a result that does not exist yet; storing
 * a pre-signed link in the chunk works in every test and expires in production; and a resume that
 * is off by one row either duplicates a record or loses one, silently, in a system whose entire
 * purpose is not doing that.
 *
 * <p>What this cannot show is whether Databricks really behaves this way — see {@link FakeDatabricks}.
 */
class DatabricksLifecycleTest {

    @Test
    void theStatementIsSubmittedOnceAndPolledUntilItSucceeds() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks()
                .pollsBeforeSuccess(3)
                .chunks(List.of(List.of(List.of("1", "Acme"))))) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> { }))) {

                Preparation prepared = source.prepare();
                assertThat(prepared.state().path("statementId").asText()).startsWith("stmt-");
                assertThat(prepared.state().path("deadline").asLong())
                        .as("the deadline travels in the handle, because the process that polls "
                                + "may not be the process that submitted")
                        .isGreaterThan(0);

                assertThat(source.checkPreparation(prepared).isReady()).isFalse();
                assertThat(source.checkPreparation(prepared).isReady()).isFalse();
                assertThat(source.checkPreparation(prepared).isReady()).isFalse();
                assertThat(source.checkPreparation(prepared).isReady()).isTrue();

                assertThat(databricks.calls())
                        .filteredOn(call -> call.equals("POST /api/2.0/sql/statements"))
                        .as("one statement for the run, not one per poll")
                        .hasSize(1);
            }
        }
    }

    @Test
    void chunksComeFromTheManifestAndCarryIndicesRatherThanLinks() throws Exception {
        // Four result chunks of 100 rows. A target of 250 groups them 3 + 1: a group closes as soon
        // as it reaches the target, which is the honest reading of a size hint.
        try (FakeDatabricks databricks = new FakeDatabricks()
                .chunks(List.of(rows(0, 100), rows(100, 100), rows(200, 100), rows(300, 100)))) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> { }))) {

                Preparation prepared = ready(source);
                List<Source.SplitSpec> specs = source.plan(prepared,
                        new Source.PlanRequest(250, 1000));

                assertThat(specs).hasSize(2);
                assertThat(specs.get(0).spec().path("fromChunk").asInt()).isZero();
                assertThat(specs.get(0).spec().path("toChunk").asInt()).isEqualTo(2);
                assertThat(specs.get(1).spec().path("fromChunk").asInt()).isEqualTo(3);
                assertThat(specs.get(1).spec().path("toChunk").asInt()).isEqualTo(3);

                assertThat(specs.get(0).spec().toString())
                        .as("a chunk spec must survive being claimed forty minutes later, so it "
                                + "may not contain a pre-signed link")
                        .doesNotContain("http");

                assertThat(databricks.calls())
                        .as("planning reads the manifest; it does not touch the result")
                        .noneMatch(call -> call.contains("/result/chunks/"));
            }
        }
    }

    @Test
    void theChunkCeilingWinsOverTheSizeHint() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks()
                .chunks(List.of(rows(0, 10), rows(10, 10), rows(20, 10), rows(30, 10)))) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> { }))) {

                // One row per chunk would plan four; the run allows two, and the ceiling is a
                // safety limit rather than a suggestion.
                List<Source.SplitSpec> specs = source.plan(ready(source),
                        new Source.PlanRequest(1, 2));

                assertThat(specs).hasSize(2);
            }
        }
    }

    @Test
    void readingWalksTheChunksAndAppliesTheDeclaredColumnTypes() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks()
                .columns("id:LONG", "name:STRING", "price:DECIMAL", "active:BOOLEAN")
                .chunks(List.of(
                        List.of(List.of("1", "Acme", "10.50", "true")),
                        List.of(List.of("2", "Globex", "0.10", "false"),
                                java.util.Arrays.asList("3", null, "99.99", "true"))))) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> node.put("keyColumn", "id")))) {

                Source.SplitSpec split = source.plan(ready(source),
                        new Source.PlanRequest(1000, 100)).get(0);

                List<DataRecord> records = drain(source.read(split, null, 100));

                assertThat(records).hasSize(3);
                assertThat(records.get(0).key()).isEqualTo("1");

                JsonNode first = records.get(0).payload();
                assertThat(first.get("id").isNumber())
                        .as("JSON_ARRAY hands every value back as a string; a LONG must not reach "
                                + "the destination as one")
                        .isTrue();
                assertThat(first.get("id").asLong()).isEqualTo(1);
                assertThat(first.get("name").asText()).isEqualTo("Acme");
                assertThat(first.get("active").isBoolean()).isTrue();

                assertThat(first.get("price").decimalValue())
                        .as("a price may not lose its last digit on the way through")
                        .isEqualByComparingTo(new java.math.BigDecimal("10.50"));

                assertThat(records.get(2).payload().get("name").isNull())
                        .as("a null stays a null rather than becoming an empty string")
                        .isTrue();
            }
        }
    }

    @Test
    void aResumedReadSkipsExactlyWhatWasAlreadyWritten() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks()
                .chunks(List.of(rows(0, 5), rows(5, 5)))) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> node.put("keyColumn", "id")))) {

                Source.SplitSpec split = source.plan(ready(source),
                        new Source.PlanRequest(1000, 100)).get(0);

                // Read three, then take the cursor exactly where the engine takes it: after a batch
                // the sink has durably accepted.
                JsonNode cursor;
                try (Source.RecordStream stream = source.read(split, null, 100)) {
                    stream.next();
                    stream.next();
                    stream.next();
                    cursor = stream.cursor();
                }
                assertThat(cursor.path("chunk").asInt()).isZero();
                assertThat(cursor.path("row").asLong()).isEqualTo(3);

                List<DataRecord> rest = drain(source.read(split, cursor, 100));

                assertThat(rest).hasSize(7);
                assertThat(rest.get(0).key())
                        .as("the resume starts at the fourth row, not the third and not the fifth")
                        .isEqualTo("3");
                assertThat(rest.get(rest.size() - 1).key()).isEqualTo("9");
            }
        }
    }

    @Test
    void aResumePositionAtTheEndOfAChunkContinuesIntoTheNextOne() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks()
                .chunks(List.of(rows(0, 4), rows(4, 4)))) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> node.put("keyColumn", "id")))) {

                Source.SplitSpec split = source.plan(ready(source),
                        new Source.PlanRequest(1000, 100)).get(0);

                // The boundary case: everything in chunk 0 was written and nothing in chunk 1 was.
                ObjectNode cursor = Json.newObject();
                cursor.put("chunk", 0);
                cursor.put("row", 4);

                List<DataRecord> rest = drain(source.read(split, cursor, 100));

                assertThat(rest).hasSize(4);
                assertThat(rest.get(0).key()).isEqualTo("4");
            }
        }
    }

    @Test
    void externalLinksAreResolvedAtReadTimeAndFetchedWithoutTheWorkspaceCredential() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks()
                .externalLinks()
                .chunks(List.of(rows(0, 3)))) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> {
                        node.put("keyColumn", "id");
                        node.put("disposition", "EXTERNAL_LINKS");
                    }))) {

                Source.SplitSpec split = source.plan(ready(source),
                        new Source.PlanRequest(1000, 100)).get(0);

                assertThat(databricks.calls())
                        .as("the link must not be resolved until the chunk is actually read")
                        .noneMatch(call -> call.startsWith("GET /external/"));

                assertThat(drain(source.read(split, null, 100))).hasSize(3);

                assertThat(databricks.externalAuthHeaders())
                        .as("cloud storage gets the pre-signed URL and nothing else; presenting a "
                                + "bearer token as well is rejected with an opaque 400")
                        .containsExactly("null");
            }
        }
    }

    @Test
    void aFailedStatementReportsTheWorkspacesOwnError() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks()
                .pollsBeforeSuccess(0)
                .failWith("FAILED", "[UNRESOLVED_COLUMN] A column with name `ordr_id` cannot be resolved")) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> { }))) {

                Preparation prepared = source.prepare();
                Preparation.Status status = source.checkPreparation(prepared);

                assertThat(status.isFailed()).isTrue();
                assertThat(status.message())
                        .as("the workspace's message names what to fix; ours would not")
                        .contains("UNRESOLVED_COLUMN", "ordr_id")
                        .contains("BAD_REQUEST");
            }
        }
    }

    @Test
    void aStatementThatOverrunsItsTimeoutIsCancelledRatherThanPolledForever() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks().pollsBeforeSuccess(1000)) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> node.put("queryTimeoutSeconds", 1)))) {

                Preparation prepared = source.prepare();
                assertThat(source.checkPreparation(prepared).isReady()).isFalse();

                Thread.sleep(1100);

                Preparation.Status status = source.checkPreparation(prepared);

                assertThat(status.isFailed()).isTrue();
                assertThat(status.message()).contains("query timeout");
                assertThat(databricks.wasCancelled())
                        .as("the warehouse must stop working on a result nobody will read")
                        .isTrue();
            }
        }
    }

    @Test
    void anEmptyResultPlansNothingRatherThanAnEmptyChunk() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks().chunks(List.of())) {

            try (Source.SourceSession source = new DatabricksConnector()
                    .openSource(context(databricks, node -> { }))) {

                assertThat(source.plan(ready(source), new Source.PlanRequest(1000, 100))).isEmpty();
            }
        }
    }

    @Test
    void aSourceWithNoQueryIsRefusedAtSessionOpenRatherThanAtFirstRead() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks()) {

            assertThatThrownBy(() -> new DatabricksConnector()
                    .openSource(context(databricks, node -> node.remove("sql"))))
                    .isInstanceOf(ConnectorException.class)
                    .hasMessageContaining("'sql'");
        }
    }

    @Test
    void theConnectionTestReachesTheWarehouseWithoutRunningAnything() throws Exception {
        try (FakeDatabricks databricks = new FakeDatabricks()) {

            new DatabricksConnector().testConnection(context(databricks, node -> { }));

            assertThat(databricks.calls())
                    .as("a test button must not start a warehouse and begin billing")
                    .noneMatch(call -> call.equals("POST /api/2.0/sql/statements"));
            assertThat(databricks.calls()).anyMatch(call -> call.contains("/sql/warehouses/"));
        }
    }

    // ------------------------------------------------------------------ helpers

    /** Polls until the statement is readable, as the planner does. */
    private static Preparation ready(Source.SourceSession source) {
        Preparation prepared = source.prepare();
        while (!source.checkPreparation(prepared).isReady()) {
            // The fake succeeds after a fixed number of polls; no sleep is needed or wanted.
        }
        return prepared;
    }

    private static List<DataRecord> drain(Source.RecordStream stream) {
        List<DataRecord> records = new ArrayList<>();
        try (stream) {
            for (DataRecord record = stream.next(); record != null; record = stream.next()) {
                records.add(record);
            }
        }
        return records;
    }

    /** {@code count} rows numbered from {@code first}, as JSON_ARRAY delivers them: strings. */
    private static List<List<String>> rows(int first, int count) {
        List<List<String>> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(List.of(String.valueOf(first + i), "row-" + (first + i)));
        }
        return rows;
    }

    private static ConnectorContext context(FakeDatabricks databricks,
                                            java.util.function.Consumer<ObjectNode> customise) {
        ObjectNode config = Json.newObject();
        config.put("host", databricks.url());
        config.put("warehouseId", "fake-warehouse");
        config.put("sql", "SELECT id, name FROM main.sales.orders");
        config.put("pollSeconds", 1);
        config.put("disposition", "INLINE");
        customise.accept(config);

        return new ConnectorContext() {
            @Override
            public JsonNode config() {
                return config;
            }

            @Override
            public Optional<String> secret(String name) {
                return "token".equals(name) ? Optional.of("fake-pat") : Optional.empty();
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
            public org.slf4j.Logger log() {
                return LoggerFactory.getLogger(DatabricksLifecycleTest.class);
            }
        };
    }
}
