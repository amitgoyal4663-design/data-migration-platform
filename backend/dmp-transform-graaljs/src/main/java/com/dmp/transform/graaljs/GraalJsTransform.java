package com.dmp.transform.graaljs;

import com.dmp.connector.api.DataRecord;
import com.dmp.transform.api.BatchResult;
import com.dmp.transform.api.RecordTransform;
import com.dmp.transform.api.TransformException;
import com.dmp.transform.api.TransformSpec;
import com.dmp.transform.api.TransformStage;
import com.fasterxml.jackson.databind.JsonNode;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * A pipeline's scripts, compiled into one guest context, for the life of one chunk.
 *
 * <p>All of a pipeline's nodes share a single context rather than getting one each. A context is
 * the expensive object here; the isolation that matters is between <em>pipelines</em>, and every
 * node in one pipeline was written by the same person for the same job. Each node's function is
 * bound to a distinct generated name so two nodes both defining {@code transform} do not collide.
 *
 * <p>Not thread-safe, by construction: a polyglot context may only be entered by one thread at a
 * time. One instance belongs to one chunk on one worker thread.
 */
final class GraalJsTransform implements RecordTransform {

    /**
     * Wrapper that turns the user's script into a uniquely named function.
     *
     * <p>The user writes {@code function transform(record) {...}} — the name they would expect —
     * and it is captured into a per-node binding. Without this, a second transform node in the
     * same pipeline would silently redefine the first.
     */
    private static final String RECORD_WRAPPER = """
            (function() {
              %s
              if (typeof transform !== 'function') {
                throw new Error("Your script must define: function transform(record) { ... }");
              }
              return transform;
            })()
            """;

    private static final String SPLIT_WRAPPER = """
            (function() {
              %s
              if (typeof split !== 'function') {
                throw new Error("Your script must define: function split(records) { ... }");
              }
              return split;
            })()
            """;

    private static final String BATCH_WRAPPER = """
            (function() {
              %s
              if (typeof transformBatch !== 'function') {
                throw new Error("Your script must define: function transformBatch(records) { ... }");
              }
              return transformBatch;
            })()
            """;

    private final Context context;
    private final ScriptWatchdog watchdog;
    private final List<CompiledNode> recordNodes;
    private final CompiledNode batchNode;
    private final CompiledNode splitNode;

    GraalJsTransform(Context context, ScriptWatchdog watchdog, List<TransformSpec> specs) {
        this.context = context;
        this.watchdog = watchdog;

        List<CompiledNode> records = new ArrayList<>();
        CompiledNode batch = null;
        CompiledNode split = null;

        for (TransformSpec spec : specs) {
            Value function = compile(spec);
            switch (spec.stage()) {
                case BATCH -> batch = new CompiledNode(spec, function);
                case SPLIT -> split = new CompiledNode(spec, function);
                default -> records.add(new CompiledNode(spec, function));
            }
        }

        this.recordNodes = List.copyOf(records);
        this.batchNode = batch;
        this.splitNode = split;
    }

    /**
     * Compiles one node's script.
     *
     * <p>Done at construction, so a syntax error surfaces when the chunk starts rather than on the
     * first record — the difference between a run that fails immediately and one that fails after
     * writing half a table.
     */
    private Value compile(TransformSpec spec) {
        String wrapper = switch (spec.stage()) {
            case BATCH -> BATCH_WRAPPER;
            case SPLIT -> SPLIT_WRAPPER;
            default -> RECORD_WRAPPER;
        };
        try {
            return context.eval("js", wrapper.formatted(spec.script()));
        } catch (PolyglotException e) {
            throw new TransformException(spec.nodeId(), spec.name(),
                    "Transform '" + spec.name() + "' did not compile: " + e.getMessage(), e);
        }
    }

    @Override
    public List<DataRecord> applyRecord(DataRecord record) {
        List<DataRecord> current = List.of(record);

        for (CompiledNode node : recordNodes) {
            if (current.isEmpty()) {
                // Already dropped by an earlier node. Running the rest would be work with no
                // possible effect.
                return List.of();
            }
            List<DataRecord> next = new ArrayList<>(current.size());
            for (DataRecord input : current) {
                next.addAll(applyOne(node, input));
            }
            current = next;
        }

        // Numbered once, at the end of the chain, rather than by each node that fans out. Two
        // nodes each turning one record into two produce four records all sharing the input's
        // seq, and numbering per node would give two of them ordinal 0. The pair (seq, ordinal)
        // has to be unique or the audit index silently keeps one row of the several a record
        // became.
        if (current.size() > 1) {
            List<DataRecord> numbered = new ArrayList<>(current.size());
            for (int i = 0; i < current.size(); i++) {
                numbered.add(current.get(i).withOrdinal(i));
            }
            return numbered;
        }
        return current;
    }

    /**
     * Runs one node against one record and interprets what it returned.
     *
     * <p>Three return shapes, each meaning something a data pipeline needs: null or undefined
     * drops the record, an array fans it out, anything else replaces it. A filter is then an
     * ordinary {@code if}, and a splitter an ordinary {@code map}, with no separate node type or
     * configuration syntax to learn.
     */
    private List<DataRecord> applyOne(CompiledNode node, DataRecord input) {
        Value result = invoke(node, input.seq(),
                () -> node.function.execute(JsonBridge.toGuest(context, input.payload())));

        if (result == null || result.isNull()) {
            return List.of();
        }

        if (result.hasArrayElements()) {
            long size = result.getArraySize();
            List<DataRecord> fanned = new ArrayList<>((int) size);
            for (long i = 0; i < size; i++) {
                JsonNode payload = convert(node, input.seq(), result.getArrayElement(i));
                if (payload != null) {
                    // Sequence numbers are the checkpoint's resume coordinate, so every record
                    // produced from one input keeps that input's seq. Renumbering them would make
                    // the resume position meaningless.
                    fanned.add(input.withPayload(payload));
                }
            }
            return fanned;
        }

        JsonNode payload = convert(node, input.seq(), result);
        return payload == null ? List.of() : List.of(input.withPayload(payload));
    }

    @Override
    public BatchResult applyBatch(List<DataRecord> records) {
        if (batchNode == null) {
            return BatchResult.none();
        }
        Value result = invoke(batchNode, -1, () -> batchNode.function.execute(
                JsonBridge.arrayToGuest(context, records.stream().map(DataRecord::payload).toList())));

        JsonNode payload = convert(batchNode, -1, result);
        if (payload == null) {
            throw new TransformException(batchNode.spec.nodeId(), batchNode.spec.name(),
                    "Transform '" + batchNode.spec.name() + "' returned nothing. A batch transform "
                            + "must return either the records or the payload to send; returning "
                            + "null would mean writing nothing while the records count as written.",
                    null);
        }

        // An array means "these are the records now" — one payload per record, in order. That is
        // how a value which only exists at batch scope reaches every record in the batch.
        if (payload.isArray()) {
            if (payload.size() != records.size()) {
                throw new TransformException(batchNode.spec.nodeId(), batchNode.spec.name(),
                        "Transform '" + batchNode.spec.name() + "' was given " + records.size()
                                + " record(s) and returned " + payload.size() + ". A batch "
                                + "transform may change records but not how many there are — "
                                + "returning fewer would lose records the run had already counted "
                                + "as read. Use a Transform to drop or multiply records.", null);
            }
            List<JsonNode> replacements = new ArrayList<>(payload.size());
            payload.forEach(replacements::add);
            return BatchResult.replacing(replacements);
        }

        return BatchResult.enveloping(payload);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The length check is the whole safety story. A script sees every record and may compute a
     * label from any of them, so it is as expressive as returning groups directly — but because it
     * returns labels rather than records, it cannot drop one, duplicate one, reorder one, or strip
     * the sequence number the checkpoint resumes from. Those failures are not caught here; they are
     * unrepresentable.
     */
    @Override
    public List<String> split(List<DataRecord> records) {
        if (splitNode == null) {
            return List.of();
        }
        Value result = invoke(splitNode, -1, () -> splitNode.function.execute(
                JsonBridge.arrayToGuest(context, records.stream().map(DataRecord::payload).toList())));

        JsonNode labels = convert(splitNode, -1, result);
        if (labels == null || !labels.isArray()) {
            throw new TransformException(splitNode.spec.nodeId(), splitNode.spec.name(),
                    "Split '" + splitNode.spec.name() + "' must return an array of group labels — "
                            + "one per record, in the same order. Records sharing a label are "
                            + "written together, and each distinct label is one call on the sink.",
                    null);
        }
        if (labels.size() != records.size()) {
            throw new TransformException(splitNode.spec.nodeId(), splitNode.spec.name(),
                    "Split '" + splitNode.spec.name() + "' was given " + records.size()
                            + " record(s) and returned " + labels.size() + " label(s). It must "
                            + "return exactly one label per record, in the same order — the labels "
                            + "are matched to records by position, so a different count would "
                            + "attach labels to the wrong records or leave some with none.", null);
        }

        List<String> groups = new ArrayList<>(labels.size());
        for (JsonNode label : labels) {
            // A missing label is a group, not an error. A script written as a lookup will return
            // undefined for anything the lookup does not cover, and those records still have to be
            // written somewhere — together, and predictably, rather than one call each.
            groups.add(label == null || label.isNull() ? UNLABELLED : label.asText());
        }
        return groups;
    }

    /** Where records go when the script returns nothing for them. */
    private static final String UNLABELLED = "";

    /** Runs a script under the watchdog, translating guest failures into named ones. */
    private Value invoke(CompiledNode node, long seq, java.util.function.Supplier<Value> call) {
        watchdog.enter(context, node.spec.timeout(), node.spec.name());
        try {
            return call.get();
        } catch (PolyglotException e) {
            throw new TransformException(node.spec.nodeId(), node.spec.name(), seq,
                    describe(node, e), e);
        } catch (IllegalStateException e) {
            // Raised when the watchdog closed the context out from under this thread.
            throw new TransformException(node.spec.nodeId(), node.spec.name(), seq,
                    "Transform '" + node.spec.name() + "' was interrupted after exceeding "
                            + node.spec.timeout().toMillis() + "ms", e);
        } finally {
            watchdog.exit();
        }
    }

    private String describe(CompiledNode node, PolyglotException e) {
        if (e.isCancelled() || e.isInterrupted()) {
            return "Transform '" + node.spec.name() + "' exceeded its "
                    + node.spec.timeout().toMillis() + "ms limit and was interrupted";
        }
        return "Transform '" + node.spec.name() + "' threw: " + e.getMessage();
    }

    private JsonNode convert(CompiledNode node, long seq, Value value) {
        try {
            return JsonBridge.fromGuest(context, value);
        } catch (RuntimeException e) {
            throw new TransformException(node.spec.nodeId(), node.spec.name(), seq,
                    "Transform '" + node.spec.name() + "' returned a value that is not JSON: "
                            + e.getMessage(), e);
        }
    }

    @Override
    public boolean isIdentity() {
        return recordNodes.isEmpty();
    }

    @Override
    public boolean hasBatchStage() {
        return batchNode != null;
    }

    @Override
    public boolean hasSplitStage() {
        return splitNode != null;
    }

    @Override
    public void close() {
        try {
            context.close(true);
        } catch (Exception e) {
            // Already closed by the watchdog, most likely. Nothing here is worth failing a chunk
            // that has otherwise finished.
        }
    }

    private record CompiledNode(TransformSpec spec, Value function) {
    }
}
