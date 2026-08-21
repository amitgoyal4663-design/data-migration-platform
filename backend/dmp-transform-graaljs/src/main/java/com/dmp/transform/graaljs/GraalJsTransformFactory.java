package com.dmp.transform.graaljs;

import com.dmp.transform.api.RecordTransform;
import com.dmp.transform.api.TransformException;
import com.dmp.transform.api.TransformFactory;
import com.dmp.transform.api.TransformSpec;
import com.dmp.transform.api.TransformStage;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PreDestroy;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.EnvironmentAccess;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.io.IOAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Builds locked-down JavaScript contexts (ADR-0008).
 *
 * <p>Every capability that could reach outside the payload is off: no host classes, no filesystem,
 * no network, no threads, no processes, no environment, no other languages. What remains is enough
 * to manipulate JSON and nothing more — which is the entire job.
 *
 * <p>Denying IO is not only a security measure. A chunk resumes by re-reading from its checkpoint
 * and re-transforming, so a script with side effects would produce them twice. Removing the
 * capability makes that impossible rather than merely discouraged.
 *
 * <p>One shared {@link Engine} across all contexts. That is what makes per-chunk compilation
 * affordable: the engine caches parsed ASTs, so the fortieth chunk of a migration reuses the
 * parse from the first while still getting a clean global scope.
 */
@Component
public class GraalJsTransformFactory implements TransformFactory {

    private static final Logger log = LoggerFactory.getLogger(GraalJsTransformFactory.class);

    private final Engine engine;
    private final ScriptWatchdog watchdog = new ScriptWatchdog();

    public GraalJsTransformFactory() {
        this.engine = Engine.newBuilder("js")
                // The interpreter-only warning fires on a stock JVM, where GraalJS cannot use the
                // optimising compiler. It is accurate and it is also not actionable for anyone
                // running this on a normal JDK, so it is silenced rather than printed on every
                // worker start.
                .option("engine.WarnInterpreterOnly", "false")
                .build();
        log.info("JavaScript transforms enabled ({})", engine.getImplementationName());
    }

    @Override
    public RecordTransform compile(List<TransformSpec> specs) {
        if (specs == null || specs.isEmpty()) {
            return RecordTransform.IDENTITY;
        }
        Context context = newContext();
        try {
            return new GraalJsTransform(context, watchdog, specs);
        } catch (RuntimeException e) {
            // A script that does not compile must not leak the context it was being compiled into.
            context.close(true);
            throw e;
        }
    }

    @Override
    public TestResult test(TransformSpec spec, JsonNode sample) {
        try (Context context = newContext()) {
            GraalJsTransform transform = new GraalJsTransform(context, watchdog, List.of(spec));

            if (sample == null) {
                return TestResult.success(null, 0);
            }

            long startedAt = System.nanoTime();
            JsonNode output = switch (spec.stage()) {
                case BATCH -> previewBatch(transform, asRecords(sample));
                case SPLIT -> previewSplit(transform, asRecords(sample));
                default -> firstOrNull(transform, sample);
            };
            long elapsed = (System.nanoTime() - startedAt) / 1_000_000;

            return TestResult.success(output, elapsed);

        } catch (TransformException e) {
            return TestResult.failure(e.getMessage());
        } catch (Exception e) {
            return TestResult.failure(e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }

    /**
     * The sample as the list a batch-scoped script will actually be given.
     *
     * <p>Accepts either an array or a single object, because both are reasonable things to paste
     * and refusing one of them teaches nothing. A batch script run against one record is legal but
     * nearly useless — a script computing a total over the batch returns that record's own value —
     * and a split script run against one record cannot demonstrate a grouping at all.
     */
    private static List<com.dmp.connector.api.DataRecord> asRecords(JsonNode sample) {
        List<com.dmp.connector.api.DataRecord> records = new java.util.ArrayList<>();
        if (sample != null && sample.isArray()) {
            long seq = 1;
            for (JsonNode node : sample) {
                records.add(com.dmp.connector.api.DataRecord.of(node, seq++));
            }
        } else if (sample != null) {
            records.add(com.dmp.connector.api.DataRecord.of(sample, 1));
        }
        return records;
    }

    /**
     * Runs a split script and shows the groups it produced, not the labels it returned.
     *
     * <p>The labels are the mechanism; the grouping is the decision. An author checking a split
     * wants to see which records ended up in the same call and in what order the calls go out —
     * so the preview does the grouping the engine would do and returns that.
     */
    private JsonNode previewSplit(GraalJsTransform transform,
                                  List<com.dmp.connector.api.DataRecord> records) {
        List<String> labels = transform.split(records);

        java.util.Map<String, com.fasterxml.jackson.databind.node.ArrayNode> byLabel =
                new java.util.LinkedHashMap<>();
        for (int i = 0; i < records.size(); i++) {
            byLabel.computeIfAbsent(labels.get(i),
                    key -> com.dmp.common.json.Json.mapper().createArrayNode())
                    .add(records.get(i).payload());
        }

        var groups = com.dmp.common.json.Json.mapper().createArrayNode();
        byLabel.forEach((label, members) -> {
            var group = com.dmp.common.json.Json.newObject();
            group.put("label", label);
            group.put("records", members.size());
            group.set("payloads", members);
            groups.add(group);
        });
        return groups;
    }

    /**
     * Runs a batch script against the sample batch for the console's preview.
     *
     * <p>Shows what the script produced either way: the rewritten records when it returned an
     * array, or the envelope when it returned anything else. Which of the two happened is the
     * single most important thing for the author to see, and it is visible from the shape.
     */
    private JsonNode previewBatch(GraalJsTransform transform,
                                  List<com.dmp.connector.api.DataRecord> records) {
        var outcome = transform.applyBatch(records);
        if (outcome.replacesRecords()) {
            var array = com.dmp.common.json.Json.mapper().createArrayNode();
            outcome.replacements().forEach(array::add);
            return array;
        }
        return outcome.envelope();
    }

    /**
     * Runs a per-record script once for the console's preview.
     *
     * <p>Shows the first record produced. A script that fans out is common enough that returning
     * only the first is a lie worth avoiding, so a dropped record is reported as such and a
     * multi-record result is returned as the array the script produced.
     */
    private JsonNode firstOrNull(GraalJsTransform transform, JsonNode sample) {
        var produced = transform.applyRecord(com.dmp.connector.api.DataRecord.of(sample, 1));
        if (produced.isEmpty()) {
            return null;
        }
        if (produced.size() == 1) {
            return produced.get(0).payload();
        }
        var array = com.dmp.common.json.Json.mapper().createArrayNode();
        produced.forEach(record -> array.add(record.payload()));
        return array;
    }

    private Context newContext() {
        return Context.newBuilder("js")
                .engine(engine)
                .allowAllAccess(false)
                .allowHostAccess(HostAccess.NONE)
                .allowHostClassLookup(className -> false)
                .allowIO(IOAccess.NONE)
                .allowNativeAccess(false)
                .allowCreateThread(false)
                .allowCreateProcess(false)
                .allowEnvironmentAccess(EnvironmentAccess.NONE)
                .allowPolyglotAccess(PolyglotAccess.NONE)
                .option("js.ecmascript-version", "2023")
                .build();
    }

    @PreDestroy
    public void shutdown() {
        watchdog.close();
        engine.close();
    }
}
