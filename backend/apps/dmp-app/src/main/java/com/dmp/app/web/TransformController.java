package com.dmp.app.web;

import com.dmp.transform.api.TransformFactory;
import com.dmp.transform.api.TransformSpec;
import com.dmp.transform.api.TransformStage;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;

/**
 * Runs a transformation script once, against one record, and reports what it did.
 *
 * <p>Exists so the console can tell someone their script is broken while they are looking at it.
 * The alternative is finding out when the nightly run fails at 3am, having written half a table
 * with the wrong shape.
 *
 * <p>Safe to expose because the evaluator is the same locked-down sandbox the engine uses
 * (ADR-0008): no filesystem, no network, no host classes, and a wall-clock limit. A hostile script
 * posted here can do no more than one posted through a pipeline.
 */
@RestController
@RequestMapping("/api/v1/transforms")
@Tag(name = "Transforms", description = "Try a transformation script before running a migration")
public class TransformController {

    /** Shorter than a running pipeline's. Someone waiting on a browser should not wait long. */
    private static final Duration PREVIEW_TIMEOUT = Duration.ofSeconds(2);

    private final TransformFactory transforms;

    public TransformController(TransformFactory transforms) {
        this.transforms = transforms;
    }

    @PostMapping("/test")
    @Operation(summary = "Run a script against one sample record",
            description = "Returns what the script produced, or the error it raised. Never throws "
                    + "for a broken script — a failing script is the expected case here, so the "
                    + "failure is in the body rather than in the status code.")
    public TestResponse test(@RequestBody TestRequest request) {
        TransformFactory.TestResult result = transforms.test(
                new TransformSpec("preview", "This script", request.stage(), request.script(),
                        PREVIEW_TIMEOUT),
                request.sample());

        return new TestResponse(result.ok(), result.output(), result.message(),
                result.elapsedMillis(), describe(result, request.stage()));
    }

    /**
     * Says what happened in the terms the user is thinking in.
     *
     * <p>A script returning nothing is the single most confusing outcome — it looks broken and is
     * usually a working filter — so it gets said out loud rather than shown as an empty result box.
     */
    private String describe(TransformFactory.TestResult result, TransformStage stage) {
        if (!result.ok()) {
            return null;
        }
        if (result.output() == null) {
            return stage == TransformStage.BATCH
                    ? "The script returned nothing. A batch script must return the payload to send."
                    : "This record would be dropped — the script returned nothing for it.";
        }
        if (stage == TransformStage.SPLIT) {
            int groups = result.output().size();
            return groups == 1
                    ? "Every record lands in one group, so the destination is called once — the "
                            + "same as no split at all."
                    : "The destination would be called " + groups + " times, in the order shown.";
        }
        if (result.output().isArray()) {
            return stage == TransformStage.RECORD
                    ? "This one record would become " + result.output().size() + " records."
                    : null;
        }
        return null;
    }

    /**
     * @param script the user's JavaScript
     * @param stage  RECORD for {@code transform(record)}, BATCH for {@code transformBatch(records)},
     *               SPLIT for {@code split(records)}
     * @param sample one record, or an array of them. Batch- and split-scoped scripts need several
     *               to show anything useful: a running total over one record is that record, and a
     *               grouping of one record is one group.
     */
    public record TestRequest(
            @NotBlank String script,
            TransformStage stage,
            JsonNode sample) {

        public TestRequest {
            stage = stage == null ? TransformStage.RECORD : stage;
        }
    }

    /**
     * @param ok            whether the script compiled and ran
     * @param output        what it produced; null when it dropped the record or failed
     * @param error         the failure, phrased for whoever wrote the script
     * @param elapsedMillis how long one invocation took, so a slow script is visible
     * @param note          a plain-language remark about an outcome that reads as a bug but is not
     */
    public record TestResponse(
            boolean ok,
            JsonNode output,
            String error,
            long elapsedMillis,
            String note) {
    }
}
