package com.dmp.engine;

import com.dmp.common.error.DmpException;
import com.dmp.common.json.Json;
import com.dmp.connector.api.DataRecord;
import com.dmp.domain.pipeline.NodeDefinition;
import com.dmp.domain.pipeline.NodeType;
import com.dmp.transform.api.RecordTransform;
import com.dmp.transform.api.TransformException;
import com.dmp.transform.api.TransformSpec;
import com.dmp.transform.api.TransformStage;
import com.dmp.transform.graaljs.GraalJsTransformFactory;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The mapper and validation nodes, run through the sandbox that will actually run them.
 *
 * <p>Asserting on the generated source would test the string this produces rather than what it
 * does, and the two come apart the first time the generator is refactored. These compile and
 * execute, which is the only thing that says the node works.
 */
@DisplayName("Declarative nodes")
class DeclarativeNodesTest {

    private static final GraalJsTransformFactory FACTORY = new GraalJsTransformFactory();

    @Nested
    @DisplayName("the mapper")
    class MapperTest {

        @Test
        @DisplayName("renames fields and drops everything not asked for")
        void renamesAndDrops() {
            JsonNode out = map("""
                    {"mappings": [
                      {"from": "order_id", "to": "Order_Number__c"},
                      {"from": "amount", "to": "Amount__c"}
                    ]}
                    """, """
                    {"order_id": "DBX-1", "amount": 12.5, "internal_note": "do not send"}
                    """);

            assertThat(out.path("Order_Number__c").asText()).isEqualTo("DBX-1");
            assertThat(out.path("Amount__c").asDouble()).isEqualTo(12.5);
            // The safer direction, and the point of a mapper: a source column added months from now
            // does not silently start arriving somewhere nobody declared it.
            assertThat(out.has("internal_note")).isFalse();
        }

        @Test
        @DisplayName("carries everything through when asked to")
        void keepsUnmapped() {
            JsonNode out = map("""
                    {"keepUnmapped": true, "mappings": [{"from": "a", "to": "b"}]}
                    """, """
                    {"a": 1, "untouched": "still here"}
                    """);

            assertThat(out.path("b").asInt()).isEqualTo(1);
            assertThat(out.path("untouched").asText()).isEqualTo("still here");
        }

        @Test
        @DisplayName("converts a string the source typed loosely into the number the sink wants")
        void coercesTypes() {
            // The exact case the source preview surfaced: a warehouse returning amount as "50.0".
            JsonNode out = map("""
                    {"mappings": [
                      {"from": "amount", "to": "Amount__c", "type": "NUMBER"},
                      {"from": "qty", "to": "Quantity__c", "type": "INTEGER"},
                      {"from": "active", "to": "Active__c", "type": "BOOLEAN"},
                      {"from": "id", "to": "Ref__c", "type": "STRING"}
                    ]}
                    """, """
                    {"amount": "50.0", "qty": "3", "active": "yes", "id": 99}
                    """);

            assertThat(out.path("Amount__c").isNumber()).isTrue();
            assertThat(out.path("Amount__c").asDouble()).isEqualTo(50.0);
            assertThat(out.path("Quantity__c").asInt()).isEqualTo(3);
            assertThat(out.path("Active__c").asBoolean()).isTrue();
            assertThat(out.path("Ref__c").asText()).isEqualTo("99");
        }

        @Test
        @DisplayName("throws rather than writing NaN when a value cannot be converted")
        void refusesToCoerceNonsense() {
            // Silent coercion is how a destination ends up holding rows that look right and are
            // not. The record is far more useful in the dead-letter queue with a reason attached.
            assertThatThrownBy(() -> map("""
                    {"mappings": [{"from": "amount", "to": "Amount__c", "type": "NUMBER"}]}
                    """, """
                    {"amount": "about fifty"}
                    """))
                    .isInstanceOf(TransformException.class)
                    .hasMessageContaining("not a number");
        }

        @Test
        @DisplayName("reads and writes nested paths without throwing on a missing parent")
        void handlesNestedPaths() {
            JsonNode out = map("""
                    {"mappings": [
                      {"from": "customer.address.city", "to": "Billing.City"},
                      {"from": "customer.address.zip", "to": "Billing.Zip"}
                    ]}
                    """, """
                    {"customer": {"address": {"city": "Pune"}}}
                    """);

            assertThat(out.path("Billing").path("City").asText()).isEqualTo("Pune");
            // Absent, not an exception. A hand-written record.customer.address.zip on a record
            // without an address is a TypeError, which is one of the two commonest ways a
            // hand-written transform fails in production.
            assertThat(out.path("Billing").has("Zip")).isFalse();
        }

        @Test
        @DisplayName("fills in a default before deciding a required field is missing")
        void defaultsThenRequires() {
            JsonNode out = map("""
                    {"mappings": [
                      {"from": "region", "to": "Region__c", "default": "UNKNOWN", "required": true}
                    ]}
                    """, "{}");

            assertThat(out.path("Region__c").asText()).isEqualTo("UNKNOWN");
        }

        @Test
        @DisplayName("writes a constant when a mapping has a default and no source")
        void constantMapping() {
            // One of the commonest things a migration needs: stamp every record with where it came
            // from, a record type, a load id. Requiring a source field would force a script for the
            // simplest possible operation.
            JsonNode out = map("""
                    {"mappings": [
                      {"to": "Source__c", "default": "MIGRATION"},
                      {"to": "Attempt__c", "default": "1", "type": "INTEGER"},
                      {"from": "order_id", "to": "Ref__c"}
                    ]}
                    """, """
                    {"order_id": "DBX-1"}
                    """);

            assertThat(out.path("Source__c").asText()).isEqualTo("MIGRATION");
            // The declared type applies to a default as well, so "1" typed into a form arrives as a
            // number rather than as the string the form gave us.
            assertThat(out.path("Attempt__c").isNumber()).isTrue();
            assertThat(out.path("Ref__c").asText()).isEqualTo("DBX-1");
        }

        @Test
        @DisplayName("treats an empty string as missing, so a default fills it")
        void blankCountsAsMissing() {
            // A warehouse returning '' for an absent value is at least as common as returning null,
            // and a default that only fired on null would leave the blank to travel onwards.
            JsonNode out = map("""
                    {"mappings": [{"from": "region", "to": "Region__c", "default": "UNKNOWN"}]}
                    """, """
                    {"region": ""}
                    """);

            assertThat(out.path("Region__c").asText()).isEqualTo("UNKNOWN");
        }

        @Test
        @DisplayName("refuses a mapping with neither a source nor a default")
        void refusesAnEmptyMapping() {
            assertThatThrownBy(() -> DeclarativeNodes.mapperScript(node(NodeType.MAPPER,
                    "{\"mappings\": [{\"to\": \"Nowhere__c\"}]}")))
                    .isInstanceOf(DmpException.class)
                    .hasMessageContaining("nothing to put there");
        }

        @Test
        @DisplayName("names the field when a required one is missing, so failures group by field")
        void namesTheMissingField() {
            // The message becomes the failure signature. "A transform threw" would group every
            // missing field in the pipeline under one heading.
            assertThatThrownBy(() -> map("""
                    {"mappings": [{"from": "email", "to": "Email__c", "required": true}]}
                    """, """
                    {"name": "no email here"}
                    """))
                    .isInstanceOf(TransformException.class)
                    // Both ends: the requirement is the destination's, the fix is in the source.
                    .hasMessageContaining("Email__c is required")
                    .hasMessageContaining("from email");
        }

        @Test
        @DisplayName("treats a field name that looks like code as a name")
        void quotesFieldNames() {
            // Inside a sandbox, so the blast radius is one record — but a mapping that silently
            // becomes a different mapping is a correctness problem whatever it can reach. A real
            // value is carried through, so the assertion is that the name survived as a name
            // rather than that nothing happened.
            String dangerous = "a\");throw new Error('injected');(\"x";
            JsonNode out = map("{\"mappings\": [{\"from\": \"ok\", \"to\": \""
                    + dangerous.replace("\\", "\\\\").replace("\"", "\\\"")
                    + "\"}]}", "{\"ok\": 1}");

            assertThat(out.path(dangerous).asInt()).isEqualTo(1);
        }

        @Test
        @DisplayName("translates the source's vocabulary into the destination's")
        void translatesValues() {
            // The single most common thing after a rename: two systems with different words for the
            // same state. Static, so it belongs here rather than in a lookup against anything.
            JsonNode out = map("""
                    {"mappings": [{"from": "status", "to": "Stage__c",
                      "values": {"NEW": "Open", "SHIPPED": "Closed Won"},
                      "otherwise": "Unknown"}]}
                    """, """
                    {"status": "SHIPPED"}
                    """);
            assertThat(out.path("Stage__c").asText()).isEqualTo("Closed Won");

            // A value not in the table takes the fallback. Without "otherwise" it passes through
            // unchanged, which is the right default — a partial table should not blank the rest.
            JsonNode unlisted = map("""
                    {"mappings": [{"from": "status", "to": "Stage__c",
                      "values": {"NEW": "Open"}, "otherwise": "Unknown"}]}
                    """, "{\"status\": \"CANCELLED\"}");
            assertThat(unlisted.path("Stage__c").asText()).isEqualTo("Unknown");

            JsonNode passthrough = map("""
                    {"mappings": [{"from": "status", "to": "Stage__c", "values": {"NEW": "Open"}}]}
                    """, "{\"status\": \"CANCELLED\"}");
            assertThat(passthrough.path("Stage__c").asText()).isEqualTo("CANCELLED");
        }

        @Test
        @DisplayName("trims, cases, affixes, replaces and truncates in a fixed order")
        void tidiesText() {
            JsonNode out = map("""
                    {"mappings": [
                      {"from": "code", "to": "Code__c", "trim": true, "case": "UPPER",
                       "prefix": "MIG-"},
                      {"from": "order_id", "to": "Ref__c",
                       "replace": {"find": "DBX-", "with": ""}},
                      {"from": "note", "to": "Note__c", "maxLength": 8}
                    ]}
                    """, """
                    {"code": "  ab-9 ", "order_id": "DBX-100042",
                     "note": "far too long for the destination"}
                    """);

            // Trim before case before prefix: the prefix keeps the case it was typed in, and the
            // length limit applies to what is actually sent rather than to the raw value.
            assertThat(out.path("Code__c").asText()).isEqualTo("MIG-AB-9");
            assertThat(out.path("Ref__c").asText()).isEqualTo("100042");
            assertThat(out.path("Note__c").asText()).isEqualTo("far too ");
        }

        @Test
        @DisplayName("joins several source fields into one")
        void joinsFields() {
            // "First name and last name into one field" is not logic anybody should open a script
            // for, and it is asked for on nearly every migration.
            JsonNode out = map("""
                    {"mappings": [{"from": ["first", "last"], "to": "Name", "join": " "}]}
                    """, """
                    {"first": "Asha", "last": "Rao"}
                    """);
            assertThat(out.path("Name").asText()).isEqualTo("Asha Rao");

            // A missing part is skipped rather than leaving a dangling separator.
            JsonNode partial = map("""
                    {"mappings": [{"from": ["first", "last"], "to": "Name", "join": " "}]}
                    """, "{\"first\": \"Asha\"}");
            assertThat(partial.path("Name").asText()).isEqualTo("Asha");
        }

        @Test
        @DisplayName("omits an absent field rather than writing null, unless told otherwise")
        void omitsRatherThanNulling() {
            // Not interchangeable at the destination: a Salesforce update sent an explicit null
            // clears the field, while an absent key leaves what is there. Writing null by default
            // would silently erase data on an upsert.
            JsonNode omitted = map("""
                    {"mappings": [{"from": "missing", "to": "Region__c"}]}
                    """, "{}");
            assertThat(omitted.has("Region__c")).isFalse();

            JsonNode nulled = map("""
                    {"mappings": [{"from": "missing", "to": "Region__c", "onMissing": "NULL"}]}
                    """, "{}");
            assertThat(nulled.has("Region__c")).isTrue();
            assertThat(nulled.path("Region__c").isNull()).isTrue();
        }

        @Test
        @DisplayName("names the target when a required field ends up with no value")
        void requiredAfterEverythingElse() {
            // Checked after the default, the translation and the trim, so "required" means "ended
            // up with a value" rather than "was present in the source".
            assertThatThrownBy(() -> map("""
                    {"mappings": [{"from": "region", "to": "Region__c", "trim": true,
                                   "required": true}]}
                    """, "{\"region\": \"   \"}"))
                    .isInstanceOf(TransformException.class)
                    .hasMessageContaining("Region__c is required");
        }

        @Test
        @DisplayName("is refused at resolution when it maps nothing")
        void refusesAnEmptyMapper() {
            assertThatThrownBy(() -> DeclarativeNodes.mapperScript(
                    node(NodeType.MAPPER, "{\"mappings\": []}")))
                    .isInstanceOf(DmpException.class)
                    .hasMessageContaining("no mappings");
        }
    }

    @Nested
    @DisplayName("validation")
    class ValidationTest {

        @Test
        @DisplayName("passes a record that satisfies every rule")
        void passes() {
            JsonNode out = validate("""
                    {"rules": [
                      {"name": "email must be present", "field": "email", "check": "REQUIRED"},
                      {"name": "amount must not be negative", "field": "amount",
                       "check": "MIN", "value": 0}
                    ]}
                    """, """
                    {"email": "a@b.io", "amount": 5}
                    """);

            assertThat(out.path("email").asText()).isEqualTo("a@b.io");
        }

        @Test
        @DisplayName("throws the rule's own name, which is what the console groups failures by")
        void throwsTheRuleName() {
            // The whole reason this is not a script node. A hand-written validation throws whatever
            // its author typed, and a TypeError on line 4 groups every failing record under a stack
            // frame — where "email must be present, 3,402 records" is a sentence somebody can act on.
            assertThatThrownBy(() -> validate("""
                    {"rules": [{"name": "email must be present", "field": "email",
                                "check": "REQUIRED"}]}
                    """, """
                    {"amount": 5}
                    """))
                    .isInstanceOf(TransformException.class)
                    .hasMessageContaining("email must be present");
        }

        @Test
        @DisplayName("drops rather than rejects when told to, so the record counts as filtered")
        void dropsWhenAsked() {
            // Filtered and failed are different things on a run's balance sheet, and the difference
            // is exactly what somebody reading it wants to know.
            List<DataRecord> out = run(DeclarativeNodes.validationScript(node(NodeType.VALIDATION, """
                    {"onFail": "DROP",
                     "rules": [{"name": "EU only", "field": "region", "check": "ONE_OF",
                                "value": ["EU", "UK"]}]}
                    """)), "{\"region\": \"APAC\"}");

            assertThat(out).isEmpty();
        }

        @Test
        @DisplayName("checks numbers, lengths, patterns and membership")
        void everyCheck() {
            assertThatThrownBy(() -> validate("""
                    {"rules": [{"name": "at most 10 characters", "field": "code",
                                "check": "MAX_LENGTH", "value": 10}]}
                    """, "{\"code\": \"far too long to fit\"}"))
                    .hasMessageContaining("at most 10 characters");

            assertThatThrownBy(() -> validate("""
                    {"rules": [{"name": "looks like an email", "field": "email",
                                "check": "MATCHES", "value": "^[^@]+@[^@]+$"}]}
                    """, "{\"email\": \"not-an-email\"}"))
                    .hasMessageContaining("looks like an email");

            assertThatThrownBy(() -> validate("""
                    {"rules": [{"name": "amount is a number", "field": "amount",
                                "check": "IS_NUMBER"}]}
                    """, "{\"amount\": \"n/a\"}"))
                    .hasMessageContaining("amount is a number");
        }

        @Test
        @DisplayName("is refused at resolution when the check is not one it has")
        void refusesAnUnknownCheck() {
            assertThatThrownBy(() -> DeclarativeNodes.validationScript(node(NodeType.VALIDATION, """
                    {"rules": [{"field": "a", "check": "SOUNDS_RIGHT"}]}
                    """)))
                    .isInstanceOf(DmpException.class)
                    .hasMessageContaining("SOUNDS_RIGHT");
        }
    }

    // ------------------------------------------------------------------ setup

    private static JsonNode map(String config, String record) {
        List<DataRecord> out = run(DeclarativeNodes.mapperScript(node(NodeType.MAPPER, config)),
                record);
        assertThat(out).hasSize(1);
        return out.get(0).payload();
    }

    private static JsonNode validate(String config, String record) {
        List<DataRecord> out =
                run(DeclarativeNodes.validationScript(node(NodeType.VALIDATION, config)), record);
        assertThat(out).hasSize(1);
        return out.get(0).payload();
    }

    /** Compiles and runs, in the same sandbox the engine uses. */
    private static List<DataRecord> run(String script, String record) {
        TransformSpec spec = new TransformSpec("n1", "Node", TransformStage.RECORD, script,
                Duration.ofSeconds(5));
        try (RecordTransform transform = FACTORY.compile(List.of(spec))) {
            return transform.applyRecord(DataRecord.of(json(record), 1));
        }
    }

    private static JsonNode json(String raw) {
        try {
            return Json.mapper().readTree(raw);
        } catch (Exception e) {
            throw new IllegalArgumentException("Test fixture is not JSON: " + raw, e);
        }
    }

    private static NodeDefinition node(NodeType type, String config) {
        return new NodeDefinition("n1", type, "Node", null, json(config));
    }
}
