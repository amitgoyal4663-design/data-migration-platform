package com.dmp.transform.graaljs;

import com.dmp.common.json.Json;
import com.dmp.connector.api.DataRecord;
import com.dmp.transform.api.BatchResult;
import com.dmp.transform.api.RecordTransform;
import com.dmp.transform.api.TransformException;
import com.dmp.transform.api.TransformSpec;
import com.dmp.transform.api.TransformStage;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GraalJsTransformTest {

    private static GraalJsTransformFactory factory;

    @BeforeAll
    static void startFactory() {
        factory = new GraalJsTransformFactory();
    }

    @AfterAll
    static void stopFactory() {
        factory.shutdown();
    }

    private static TransformSpec record(String script) {
        return new TransformSpec("tx", "Test transform", TransformStage.RECORD, script,
                Duration.ofSeconds(2));
    }

    private static DataRecord order(int quantity, double price, String status) {
        var node = Json.mapper().createObjectNode();
        node.put("quantity", quantity);
        node.put("price", price);
        node.put("status", status);
        return DataRecord.of(node, 7);
    }

    @Nested
    @DisplayName("what a script can do")
    class Capabilities {

        @Test
        void addsAField() {
            try (RecordTransform transform = factory.compile(List.of(record("""
                    function transform(record) {
                      record.total = record.quantity * record.price;
                      return record;
                    }
                    """)))) {

                List<DataRecord> out = transform.applyRecord(order(3, 250.5, "NEW"));

                assertThat(out).hasSize(1);
                assertThat(out.get(0).payload().get("total").asDouble()).isEqualTo(751.5);
            }
        }

        @Test
        void dropsARecordByReturningNull() {
            try (RecordTransform transform = factory.compile(List.of(record("""
                    function transform(record) {
                      if (record.status === 'CANCELLED') return null;
                      return record;
                    }
                    """)))) {

                assertThat(transform.applyRecord(order(1, 10, "CANCELLED"))).isEmpty();
                assertThat(transform.applyRecord(order(1, 10, "NEW"))).hasSize(1);
            }
        }

        @Test
        void dropsARecordByReturningNothingAtAll() {
            // A function with no return yields undefined. Treating that as a drop is what lets a
            // filter be written as a plain if-statement, which is how people will write one.
            try (RecordTransform transform = factory.compile(List.of(record("""
                    function transform(record) {
                      if (record.status !== 'CANCELLED') return record;
                    }
                    """)))) {

                assertThat(transform.applyRecord(order(1, 10, "CANCELLED"))).isEmpty();
            }
        }

        @Test
        void fansOneRecordIntoMany() {
            try (RecordTransform transform = factory.compile(List.of(record("""
                    function transform(record) {
                      return [1, 2, 3].map(n => ({ line: n, status: record.status }));
                    }
                    """)))) {

                List<DataRecord> out = transform.applyRecord(order(1, 10, "NEW"));

                assertThat(out).hasSize(3);
                assertThat(out).extracting(r -> r.payload().get("line").asInt())
                        .containsExactly(1, 2, 3);
            }
        }

        @Test
        void fannedRecordsKeepTheirSourceSequence() {
            // The sequence number is the checkpoint's resume coordinate. Renumbering records
            // produced from one input would make the resume position point at nothing real.
            try (RecordTransform transform = factory.compile(List.of(record("""
                    function transform(record) { return [record, record]; }
                    """)))) {

                assertThat(transform.applyRecord(order(1, 10, "NEW")))
                        .extracting(DataRecord::seq)
                        .containsExactly(7L, 7L);
            }
        }

        @Test
        void chainsNodesInOrder() {
            var addTotal = record("""
                    function transform(r) { r.total = r.quantity * r.price; return r; }
                    """);
            var dropSmall = new TransformSpec("tx2", "Drop small", TransformStage.RECORD, """
                    function transform(r) { return r.total >= 100 ? r : null; }
                    """, Duration.ofSeconds(2));

            try (RecordTransform transform = factory.compile(List.of(addTotal, dropSmall))) {
                assertThat(transform.applyRecord(order(1, 10, "NEW"))).isEmpty();
                assertThat(transform.applyRecord(order(4, 250, "NEW"))).hasSize(1);
            }
        }

        @Test
        void twoNodesMayBothDefineTransform() {
            // Each node's function is captured into its own binding. Without that, the second
            // node's declaration would silently replace the first and one of them would vanish.
            var first = record("function transform(r) { r.a = 1; return r; }");
            var second = new TransformSpec("tx2", "Second", TransformStage.RECORD,
                    "function transform(r) { r.b = 2; return r; }", Duration.ofSeconds(2));

            try (RecordTransform transform = factory.compile(List.of(first, second))) {
                JsonNode out = transform.applyRecord(order(1, 10, "NEW")).get(0).payload();

                assertThat(out.get("a").asInt()).isEqualTo(1);
                assertThat(out.get("b").asInt()).isEqualTo(2);
            }
        }

        @Test
        void anObjectBecomesTheOutgoingPayload() {
            var spec = new TransformSpec("bx", "Wrap", TransformStage.BATCH, """
                    function transformBatch(records) {
                      return { items: records, count: records.length };
                    }
                    """, Duration.ofSeconds(2));

            try (RecordTransform transform = factory.compile(List.of(spec))) {
                BatchResult result = transform.applyBatch(
                        List.of(order(1, 10, "NEW"), order(2, 20, "NEW")));

                assertThat(result.hasEnvelope()).isTrue();
                assertThat(result.replacesRecords()).isFalse();
                assertThat(result.envelope().get("count").asInt()).isEqualTo(2);
                assertThat(result.envelope().get("items")).hasSize(2);
                assertThat(transform.isIdentity()).isTrue();
                assertThat(transform.hasBatchStage()).isTrue();
            }
        }

        @Test
        void anArrayRewritesTheRecords() {
            // The only way to put a value that exists at batch scope onto every record in it. A
            // per-record script cannot: records are transformed before they are grouped, so at
            // that point there is no batch to take a value from.
            var spec = new TransformSpec("bx", "Stamp", TransformStage.BATCH, """
                    function transformBatch(records) {
                      var batchId = 'batch-' + records.length;
                      return records.map(function (r) {
                        r.batchId = batchId;
                        return r;
                      });
                    }
                    """, Duration.ofSeconds(2));

            try (RecordTransform transform = factory.compile(List.of(spec))) {
                BatchResult result = transform.applyBatch(
                        List.of(order(1, 10, "NEW"), order(2, 20, "NEW")));

                assertThat(result.replacesRecords()).isTrue();
                assertThat(result.hasEnvelope()).isFalse();
                assertThat(result.replacements()).hasSize(2);
                assertThat(result.replacements())
                        .allSatisfy(node -> assertThat(node.get("batchId").asText())
                                .isEqualTo("batch-2"));
            }
        }

        @Test
        void returningADifferentNumberOfRecordsIsRejected() {
            // Returning fewer would lose records the run has already counted as read, while the
            // counters still balanced — the exact failure this platform exists to make impossible.
            var spec = new TransformSpec("bx", "Shrink", TransformStage.BATCH,
                    "function transformBatch(records) { return [records[0]]; }",
                    Duration.ofSeconds(2));

            try (RecordTransform transform = factory.compile(List.of(spec))) {
                assertThatThrownBy(() -> transform.applyBatch(
                        List.of(order(1, 10, "NEW"), order(2, 20, "NEW"))))
                        .isInstanceOf(TransformException.class)
                        .hasMessageContaining("given 2 record(s) and returned 1");
            }
        }
    }

    @Nested
    @DisplayName("numeric fidelity")
    class Numbers {

        @Test
        void wholeNumbersDoNotBecomeFloatingPoint() {
            // JavaScript has one number type. A naive round trip writes 3.0 where the source had
            // 3 — the same corruption that turned a MongoDB Number into a Long earlier in this
            // project, and just as invisible until it reaches the destination schema.
            try (RecordTransform transform = factory.compile(List.of(record("""
                    function transform(r) { r.doubled = r.quantity * 2; return r; }
                    """)))) {

                JsonNode out = transform.applyRecord(order(3, 250.5, "NEW")).get(0).payload();

                assertThat(out.get("doubled").isIntegralNumber()).isTrue();
                assertThat(out.get("doubled").asText()).isEqualTo("6");
            }
        }

        @Test
        void genuineDecimalsSurvive() {
            try (RecordTransform transform = factory.compile(List.of(record("""
                    function transform(r) { return r; }
                    """)))) {

                JsonNode out = transform.applyRecord(order(3, 250.5, "NEW")).get(0).payload();

                assertThat(out.get("price").asDouble()).isEqualTo(250.5);
                assertThat(out.get("quantity").isIntegralNumber()).isTrue();
            }
        }
    }

    @Nested
    @DisplayName("the sandbox")
    class Sandbox {

        @Test
        void cannotReachJavaClasses() {
            assertSandboxed("var f = Java.type('java.io.File');");
        }

        @Test
        void cannotReadTheFilesystem() {
            assertSandboxed("var fs = require('fs');");
        }

        @Test
        void cannotMakeNetworkCalls() {
            assertSandboxed("fetch('http://example.com');");
        }

        @Test
        void cannotStartThreads() {
            assertSandboxed("new Worker('x');");
        }

        @Test
        void cannotReadEnvironmentVariables() {
            assertSandboxed("var p = process.env.DMP_POSTGRES_PASSWORD;");
        }

        @Test
        void cannotReachOtherLanguages() {
            assertSandboxed("Polyglot.eval('python', '1');");
        }

        private void assertSandboxed(String hostile) {
            try (RecordTransform transform = factory.compile(List.of(record(
                    "function transform(r) { " + hostile + " return r; }")))) {

                assertThatThrownBy(() -> transform.applyRecord(order(1, 10, "NEW")))
                        .isInstanceOf(TransformException.class);
            }
        }
    }

    @Nested
    @DisplayName("failure handling")
    class Failures {

        @Test
        void aRunawayLoopIsInterrupted() {
            // Without the watchdog this holds a worker thread until the pod is killed, taking that
            // worker's whole chunk capacity with it.
            var spec = new TransformSpec("tx", "Runaway", TransformStage.RECORD,
                    "function transform(r) { while (true) {} }", Duration.ofMillis(500));

            try (RecordTransform transform = factory.compile(List.of(spec))) {
                assertThatThrownBy(() -> transform.applyRecord(order(1, 10, "NEW")))
                        .isInstanceOf(TransformException.class)
                        .hasMessageContaining("Runaway");
            }
        }

        @Test
        void aSyntaxErrorIsReportedAtCompileTimeNotOnTheFirstRecord() {
            // The difference between a run that fails immediately and one that fails after
            // writing half a table.
            assertThatThrownBy(() -> factory.compile(List.of(record("function transform(r) {"))))
                    .isInstanceOf(TransformException.class)
                    .hasMessageContaining("did not compile");
        }

        @Test
        void aScriptWithoutTheExpectedFunctionSaysSo() {
            assertThatThrownBy(() -> factory.compile(List.of(record("var x = 1;"))))
                    .isInstanceOf(TransformException.class)
                    .hasMessageContaining("function transform(record)");
        }

        @Test
        void aThrownErrorNamesTheNode() {
            try (RecordTransform transform = factory.compile(List.of(record("""
                    function transform(r) { throw new Error('bad row'); }
                    """)))) {

                assertThatThrownBy(() -> transform.applyRecord(order(1, 10, "NEW")))
                        .isInstanceOf(TransformException.class)
                        .hasMessageContaining("Test transform")
                        .hasMessageContaining("bad row");
            }
        }

        @Test
        void theFailingRecordIsIdentifiable() {
            try (RecordTransform transform = factory.compile(List.of(record("""
                    function transform(r) { throw new Error('bad row'); }
                    """)))) {

                assertThatThrownBy(() -> transform.applyRecord(order(1, 10, "NEW")))
                        .isInstanceOfSatisfying(TransformException.class,
                                e -> assertThat(e.seq()).isEqualTo(7));
            }
        }

        @Test
        void aBatchScriptReturningNothingIsRejected() {
            // Returning null would write nothing while the records still count as written.
            var spec = new TransformSpec("bx", "Empty", TransformStage.BATCH,
                    "function transformBatch(records) { return null; }", Duration.ofSeconds(2));

            try (RecordTransform transform = factory.compile(List.of(spec))) {
                assertThatThrownBy(() -> transform.applyBatch(List.of(order(1, 10, "NEW"))))
                        .isInstanceOf(TransformException.class)
                        .hasMessageContaining("must return either the records or the payload");
            }
        }
    }

    @Nested
    @DisplayName("no transforms configured")
    class Identity {

        @Test
        void anEmptyListCostsNothing() {
            RecordTransform transform = factory.compile(List.of());

            assertThat(transform).isSameAs(RecordTransform.IDENTITY);
            assertThat(transform.isIdentity()).isTrue();
            assertThat(transform.hasBatchStage()).isFalse();
        }
    }

    @Nested
    @DisplayName("the console's test button")
    class TestRun {

        @Test
        void reportsWhatAScriptProduced() {
            var result = factory.test(record("""
                    function transform(r) { r.total = r.quantity * r.price; return r; }
                    """), order(2, 100, "NEW").payload());

            assertThat(result.ok()).isTrue();
            assertThat(result.output().get("total").asInt()).isEqualTo(200);
        }

        @Test
        void reportsAFailureInsteadOfThrowing() {
            var result = factory.test(record("function transform(r) { throw new Error('nope'); }"),
                    order(1, 1, "NEW").payload());

            assertThat(result.ok()).isFalse();
            assertThat(result.message()).contains("nope");
        }
    }
}
