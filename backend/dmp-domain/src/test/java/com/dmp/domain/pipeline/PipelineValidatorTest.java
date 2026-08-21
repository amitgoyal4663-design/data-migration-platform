package com.dmp.domain.pipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for structural pipeline validation.
 *
 * <p>Each case asserts on the issue <em>code</em> rather than the message, so wording can be
 * improved without breaking tests — the code is the contract, the message is for humans.
 */
class PipelineValidatorTest {

    private final PipelineValidator validator = new PipelineValidator();

    private static final UUID CONNECTOR = UUID.randomUUID();

    private static NodeDefinition source(String id) {
        return new NodeDefinition(id, NodeType.SOURCE, "Source", CONNECTOR, null);
    }

    private static NodeDefinition sink(String id) {
        return new NodeDefinition(id, NodeType.SINK, "Sink", CONNECTOR, null);
    }

    private static NodeDefinition transform(String id) {
        return new NodeDefinition(id, NodeType.TRANSFORM, "Transform", null, null);
    }

    private static NodeDefinition batchTransform(String id) {
        return new NodeDefinition(id, NodeType.BATCH_TRANSFORM, "Batch transform", null, null);
    }

    private List<String> codes(ValidationResult result) {
        return result.issues().stream().map(ValidationIssue::code).toList();
    }

    @Nested
    @DisplayName("valid pipelines")
    class Valid {

        @Test
        @DisplayName("accepts the minimal source to sink pipeline")
        void minimalPipeline() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "dst")));

            assertThat(validator.validate(definition).isValid()).isTrue();
        }

        @Test
        @DisplayName("accepts an error handler as a terminal branch that never reaches a sink")
        void errorHandlerIsNotADeadEnd() {
            // An ERROR_HANDLER routing rejected records to a dead-letter destination is a
            // legitimate end of a branch, not an accidental one.
            var definition = new PipelineDefinition(
                    List.of(source("src"),
                            new NodeDefinition("check", NodeType.VALIDATION, "Validate", null, null),
                            sink("dst"),
                            new NodeDefinition("dlq", NodeType.ERROR_HANDLER, "Rejects", null, null)),
                    List.of(EdgeDefinition.of("e1", "src", "check"),
                            new EdgeDefinition("e2", "check", "dst", "valid"),
                            new EdgeDefinition("e3", "check", "dlq", "!valid")));

            assertThat(validator.validate(definition).isValid()).isTrue();
        }

        @Test
        @DisplayName("accepts two conditional edges between the same pair of nodes")
        void conditionalParallelEdgesAreAllowed() {
            // Mutually exclusive routing between the same nodes is meaningful; only an
            // unconditional duplicate is ambiguous.
            var definition = new PipelineDefinition(
                    List.of(source("src"), sink("dst")),
                    List.of(new EdgeDefinition("e1", "src", "dst", "amount > 100"),
                            new EdgeDefinition("e2", "src", "dst", "amount <= 100")));

            assertThat(validator.validate(definition).isValid()).isTrue();
        }
    }

    @Nested
    @DisplayName("structural errors")
    class Structural {

        @Test
        @DisplayName("rejects an empty pipeline")
        void empty() {
            var result = validator.validate(PipelineDefinition.empty());
            assertThat(codes(result)).containsExactly(PipelineValidator.EMPTY_PIPELINE);
        }

        @Test
        @DisplayName("rejects duplicate node ids")
        void duplicateNodeId() {
            var definition = new PipelineDefinition(
                    List.of(source("dup"), sink("dup")),
                    List.of());

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.DUPLICATE_NODE_ID);
        }

        @Test
        @DisplayName("rejects an edge pointing at a node that does not exist")
        void unknownEdgeTarget() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "ghost")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.EDGE_UNKNOWN_NODE);
        }

        @Test
        @DisplayName("rejects a self-loop")
        void selfLoop() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), transform("tx"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "tx"),
                            EdgeDefinition.of("e2", "tx", "tx"),
                            EdgeDefinition.of("e3", "tx", "dst")));

            assertThat(codes(validator.validate(definition))).contains(PipelineValidator.SELF_LOOP);
        }

        @Test
        @DisplayName("rejects a duplicate unconditional edge")
        void parallelEdge() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "dst"),
                            EdgeDefinition.of("e2", "src", "dst")));

            assertThat(codes(validator.validate(definition))).contains(PipelineValidator.PARALLEL_EDGE);
        }

        @Test
        @DisplayName("reports structural problems without also reporting graph problems they cause")
        void structuralErrorsShortCircuitGraphAnalysis() {
            // An edge to a non-existent node would otherwise produce a cascade of unreachable and
            // dead-end findings about nodes that are fine. Reporting the cause alone is more useful.
            var definition = new PipelineDefinition(
                    List.of(source("src"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "ghost")));

            assertThat(codes(validator.validate(definition)))
                    .containsOnly(PipelineValidator.EDGE_UNKNOWN_NODE);
        }
    }

    @Nested
    @DisplayName("topology errors")
    class Topology {

        @Test
        @DisplayName("requires at least one source and one sink")
        void requiresEndpoints() {
            var definition = new PipelineDefinition(List.of(transform("tx")), List.of());

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.NO_SOURCE, PipelineValidator.NO_SINK);
        }

        @Test
        @DisplayName("refuses a fan-out to two sinks at design time rather than at run time")
        void twoSinksAreRefused() {
            // This graph used to validate, publish, create a run, and only then fail inside the
            // engine — which is the worst possible place to learn it. The executor's rule and the
            // validator's rule have to be the same rule.
            var definition = new PipelineDefinition(
                    List.of(source("src"), transform("tx"), sink("a"), sink("b")),
                    List.of(EdgeDefinition.of("e1", "src", "tx"),
                            EdgeDefinition.of("e2", "tx", "a"),
                            EdgeDefinition.of("e3", "tx", "b")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.MULTIPLE_SINKS);
        }

        @Test
        @DisplayName("refuses two sources")
        void twoSourcesAreRefused() {
            var definition = new PipelineDefinition(
                    List.of(source("a"), source("b"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "a", "dst"),
                            EdgeDefinition.of("e2", "b", "dst")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.MULTIPLE_SOURCES);
        }

        @Test
        @DisplayName("refuses a second batch transform, which would silently never run")
        void twoBatchTransformsAreRefused() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), batchTransform("b1"), batchTransform("b2"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "b1"),
                            EdgeDefinition.of("e2", "b1", "b2"),
                            EdgeDefinition.of("e3", "b2", "dst")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.MULTIPLE_BATCH_TRANSFORMS);
        }

        @Test
        @DisplayName("still accepts several record transforms, which do chain")
        void severalRecordTransformsRemainValid() {
            // The point of the rules above is that they match what the executor does. Record
            // transforms genuinely chain in wiring order, so nothing here may refuse them.
            var definition = new PipelineDefinition(
                    List.of(source("src"), transform("t1"), transform("t2"), transform("t3"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "t1"),
                            EdgeDefinition.of("e2", "t1", "t2"),
                            EdgeDefinition.of("e3", "t2", "t3"),
                            EdgeDefinition.of("e4", "t3", "dst")));

            assertThat(validator.validate(definition).isValid()).isTrue();
        }

        @Test
        @DisplayName("detects a cycle")
        void cycle() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), transform("a"), transform("b"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "a"),
                            EdgeDefinition.of("e2", "a", "b"),
                            EdgeDefinition.of("e3", "b", "a"),
                            EdgeDefinition.of("e4", "b", "dst")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.CYCLE_DETECTED);
        }

        @Test
        @DisplayName("detects a long cycle without overflowing the stack")
        void deepGraphIsIterative() {
            // The validator traverses iteratively rather than recursively, because definitions
            // arrive over HTTP and a deep graph must fail validation, not the request thread.
            int depth = 10_000;
            var nodes = new java.util.ArrayList<NodeDefinition>();
            var edges = new java.util.ArrayList<EdgeDefinition>();

            nodes.add(source("src"));
            for (int i = 0; i < depth; i++) {
                nodes.add(transform("t" + i));
            }
            nodes.add(sink("dst"));

            edges.add(EdgeDefinition.of("e-start", "src", "t0"));
            for (int i = 0; i < depth - 1; i++) {
                edges.add(EdgeDefinition.of("e" + i, "t" + i, "t" + (i + 1)));
            }
            edges.add(EdgeDefinition.of("e-end", "t" + (depth - 1), "dst"));

            assertThat(validator.validate(new PipelineDefinition(nodes, edges)).isValid()).isTrue();
        }

        @Test
        @DisplayName("rejects a node unreachable from any source")
        void unreachableNode() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), sink("dst"), transform("orphanA"), sink("orphanB")),
                    List.of(EdgeDefinition.of("e1", "src", "dst"),
                            EdgeDefinition.of("e2", "orphanA", "orphanB")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.UNREACHABLE_NODE);
        }

        @Test
        @DisplayName("rejects a node whose output reaches no sink")
        void deadEnd() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), sink("dst"), transform("nowhere")),
                    List.of(EdgeDefinition.of("e1", "src", "dst"),
                            EdgeDefinition.of("e2", "src", "nowhere")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.DEAD_END_NODE);
        }

        @Test
        @DisplayName("rejects a node connected to nothing")
        void isolatedNode() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), sink("dst"), transform("lonely")),
                    List.of(EdgeDefinition.of("e1", "src", "dst")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.ISOLATED_NODE);
        }
    }

    @Nested
    @DisplayName("node rules")
    class NodeRules {

        @Test
        @DisplayName("rejects an inbound edge into a source")
        void sourceCannotHaveInbound() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), transform("tx"), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "tx"),
                            EdgeDefinition.of("e2", "tx", "dst"),
                            EdgeDefinition.of("e3", "tx", "src")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.INBOUND_NOT_ALLOWED);
        }

        @Test
        @DisplayName("rejects an outbound edge from a sink")
        void sinkCannotHaveOutbound() {
            var definition = new PipelineDefinition(
                    List.of(source("src"), sink("dst"), sink("second")),
                    List.of(EdgeDefinition.of("e1", "src", "dst"),
                            EdgeDefinition.of("e2", "dst", "second")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.OUTBOUND_NOT_ALLOWED);
        }

        @Test
        @DisplayName("rejects a source without a connector instance")
        void sourceRequiresConnector() {
            var definition = new PipelineDefinition(
                    List.of(new NodeDefinition("src", NodeType.SOURCE, "Source", null, null), sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "dst")));

            assertThat(codes(validator.validate(definition)))
                    .contains(PipelineValidator.MISSING_CONNECTOR);
        }

        @Test
        @DisplayName("warns, but does not fail, when a merger has one input")
        void singleInputMergerIsAWarning() {
            var definition = new PipelineDefinition(
                    List.of(source("src"),
                            new NodeDefinition("merge", NodeType.MERGER, "Merge", null, null),
                            sink("dst")),
                    List.of(EdgeDefinition.of("e1", "src", "merge"),
                            EdgeDefinition.of("e2", "merge", "dst")));

            var result = validator.validate(definition);

            assertThat(result.isValid()).isTrue();
            assertThat(result.warnings()).extracting(ValidationIssue::code)
                    .contains(PipelineValidator.MERGER_SINGLE_INPUT);
        }
    }
}
