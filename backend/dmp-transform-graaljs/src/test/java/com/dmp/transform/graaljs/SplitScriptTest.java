package com.dmp.transform.graaljs;

import com.dmp.common.json.Json;
import com.dmp.connector.api.DataRecord;
import com.dmp.transform.api.RecordTransform;
import com.dmp.transform.api.TransformException;
import com.dmp.transform.api.TransformSpec;
import com.dmp.transform.api.TransformStage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The script that decides how a batch is divided into calls on the sink.
 *
 * <p>It takes the whole batch, which is the point — a running total, a comparison with the previous
 * record, a size-based cut are all expressible and none of them are from a per-record function.
 * What it returns is a <em>label per record</em> rather than groups of records, and that choice is
 * what these tests are mostly about.
 *
 * <p>Records cross into the sandbox as payloads; the engine keeps their sequence number and key on
 * its side and pairs results back by position. A script returning nested lists would have
 * rearranged them, so position would identify nothing — and the sequence number is the checkpoint's
 * resume coordinate while the key drives idempotent writes and the audit index. Returning labels
 * makes losing a record's identity, dropping one, or duplicating one unrepresentable rather than
 * merely detected.
 */
class SplitScriptTest {

    private static GraalJsTransformFactory factory;

    @BeforeAll
    static void startFactory() {
        factory = new GraalJsTransformFactory();
    }

    @AfterAll
    static void stopFactory() {
        factory.shutdown();
    }

    private static TransformSpec split(String script) {
        return new TransformSpec("delivery-split", "Split", TransformStage.SPLIT, script,
                Duration.ofSeconds(2));
    }

    private static DataRecord row(String region, int amount, long seq) {
        var node = Json.mapper().createObjectNode();
        node.put("region", region);
        node.put("amount", amount);
        return DataRecord.of(node, seq);
    }

    private static List<DataRecord> batch() {
        return List.of(
                row("EU", 10, 1),
                row("US", 20, 2),
                row("EU", 30, 3),
                row("APAC", 40, 4),
                row("US", 50, 5));
    }

    @Nested
    @DisplayName("what it can express")
    class WhatItCanExpress {

        @Test
        @DisplayName("groups by a field, one call per distinct value")
        void groupsByField() {
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) {
                      return records.map(r => r.region);
                    }
                    """)))) {

                assertThat(transform.split(batch()))
                        .containsExactly("EU", "US", "EU", "APAC", "US");
            }
        }

        @Test
        @DisplayName("cuts on a running total, which no per-record function could do")
        void cutsOnARunningTotal() {
            // The case that justifies passing the whole batch. A per-record function sees one
            // record and cannot know what came before it.
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) {
                      let total = 0, group = 0;
                      return records.map(r => {
                        total += r.amount;
                        if (total > 50) { group++; total = r.amount; }
                        return String(group);
                      });
                    }
                    """)))) {

                // 10, 30 → 0 ; 60 exceeds → 1 starting at 30 ; 70 exceeds → 2 ; 90 exceeds → 3
                assertThat(transform.split(batch())).containsExactly("0", "0", "1", "2", "3");
            }
        }

        @Test
        @DisplayName("compares a record with the one before it")
        void comparesWithTheNeighbour() {
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) {
                      let group = 0;
                      return records.map((r, i) => {
                        if (i > 0 && r.region !== records[i - 1].region) group++;
                        return String(group);
                      });
                    }
                    """)))) {

                assertThat(transform.split(batch())).containsExactly("0", "1", "2", "3", "4");
            }
        }

        @Test
        @DisplayName("a numeric label is usable as a label")
        void numbersBecomeLabels() {
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) {
                      return records.map((r, i) => Math.floor(i / 2));
                    }
                    """)))) {

                assertThat(transform.split(batch())).containsExactly("0", "0", "1", "1", "2");
            }
        }
    }

    @Nested
    @DisplayName("what it cannot do")
    class WhatItCannotDo {

        @Test
        @DisplayName("returning fewer labels than records is refused, by count")
        void refusesTooFewLabels() {
            // The failure a nested-list design would have had to detect after the fact, and could
            // not have detected reliably: a script that quietly drops records. Here it is a length
            // mismatch, caught before anything is written.
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) {
                      return records.filter(r => r.region === 'EU').map(r => r.region);
                    }
                    """)))) {

                assertThatThrownBy(() -> transform.split(batch()))
                        .isInstanceOf(TransformException.class)
                        .hasMessageContaining("5 record(s) and returned 2 label(s)")
                        .hasMessageContaining("matched to records by position");
            }
        }

        @Test
        @DisplayName("returning more labels than records is refused too")
        void refusesTooManyLabels() {
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) {
                      return records.map(r => r.region).concat(['extra']);
                    }
                    """)))) {

                assertThatThrownBy(() -> transform.split(batch()))
                        .isInstanceOf(TransformException.class)
                        .hasMessageContaining("returned 6 label(s)");
            }
        }

        @Test
        @DisplayName("returning something that is not an array is refused")
        void refusesANonArray() {
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) {
                      return { groups: records.map(r => r.region) };
                    }
                    """)))) {

                assertThatThrownBy(() -> transform.split(batch()))
                        .isInstanceOf(TransformException.class)
                        .hasMessageContaining("must return an array of group labels");
            }
        }

        @Test
        @DisplayName("a script that does not define split is rejected when it compiles")
        void refusesAScriptWithoutTheFunction() {
            assertThatThrownBy(() -> factory.compile(List.of(split("""
                    function groupKey(record) { return record.region; }
                    """))))
                    .isInstanceOf(TransformException.class)
                    .hasMessageContaining("function split(records)");
        }
    }

    @Nested
    @DisplayName("the console's Try button")
    class Preview {

        @Test
        @DisplayName("shows the groups, not the labels — the grouping is the decision")
        void previewShowsGroups() {
            var result = factory.test(split("""
                    function split(records) {
                      return records.map(r => r.region);
                    }
                    """), sampleArray());

            assertThat(result.ok()).isTrue();
            assertThat(result.output().size()).as("EU, US, APAC").isEqualTo(3);

            assertThat(result.output().get(0).path("label").asText()).isEqualTo("EU");
            assertThat(result.output().get(0).path("records").asInt()).isEqualTo(2);
            assertThat(result.output().get(1).path("label").asText())
                    .as("call order follows the first record of each group, not the alphabet")
                    .isEqualTo("US");
        }

        @Test
        @DisplayName("runs against several records, because one cannot demonstrate a grouping")
        void previewTakesAWholeBatch() {
            var result = factory.test(split("""
                    function split(records) {
                      let total = 0, group = 0;
                      return records.map(r => {
                        total += r.amount;
                        if (total > 50) { group++; total = r.amount; }
                        return String(group);
                      });
                    }
                    """), sampleArray());

            assertThat(result.ok()).isTrue();
            assertThat(result.output().size())
                    .as("a running total is meaningless against a single record")
                    .isGreaterThan(1);
        }

        @Test
        @DisplayName("a broken script is reported rather than thrown")
        void previewReportsFailure() {
            var result = factory.test(split("""
                    function split(records) {
                      return records.slice(1).map(r => r.region);
                    }
                    """), sampleArray());

            assertThat(result.ok()).isFalse();
            assertThat(result.message()).contains("label");
        }

        private com.fasterxml.jackson.databind.JsonNode sampleArray() {
            var array = Json.mapper().createArrayNode();
            for (DataRecord record : batch()) {
                array.add(record.payload());
            }
            return array;
        }
    }

    @Nested
    @DisplayName("edges")
    class Edges {

        @Test
        @DisplayName("records the script has no label for are grouped together, not one call each")
        void unlabelledRecordsShareAGroup() {
            // A script written as a lookup returns undefined for anything the lookup misses. Those
            // records still have to go somewhere, and one call per unmatched record would turn a
            // typo in a lookup table into a thousand requests.
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) {
                      const known = { EU: 'europe', US: 'america' };
                      return records.map(r => known[r.region]);
                    }
                    """)))) {

                assertThat(transform.split(batch()))
                        .containsExactly("europe", "america", "europe", "", "america");
            }
        }

        @Test
        @DisplayName("an empty batch produces no labels rather than failing")
        void emptyBatch() {
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) {
                      return records.map(r => r.region);
                    }
                    """)))) {

                assertThat(transform.split(List.of())).isEmpty();
            }
        }

        @Test
        @DisplayName("no split node means no labels, and the caller writes the batch whole")
        void noSplitNode() {
            try (RecordTransform transform = factory.compile(List.of())) {
                assertThat(transform.hasSplitStage()).isFalse();
                assertThat(transform.split(batch())).isEmpty();
            }
        }

        @Test
        @DisplayName("a split script does not make the transform non-identity per record")
        void splitIsNotAPerRecordStage() {
            // isIdentity() lets the executor skip the per-record call entirely. A pipeline whose
            // only script is a split still transforms nothing per record, and paying for a call
            // per record to discover that would cost more than the split saves.
            try (RecordTransform transform = factory.compile(List.of(split("""
                    function split(records) { return records.map(r => r.region); }
                    """)))) {

                assertThat(transform.isIdentity()).isTrue();
                assertThat(transform.hasSplitStage()).isTrue();
            }
        }
    }
}
