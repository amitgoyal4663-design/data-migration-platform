package com.dmp.engine;

import com.dmp.common.json.Json;
import com.dmp.domain.run.RunId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Marks a chunk that reads rejected records from a previous run instead of from the pipeline's
 * source.
 *
 * <p>The parameters live in the chunk spec for the same reason {@link OpenEnded}'s marker does: the
 * spec is connector-defined and opaque, so the engine may put its own keys there, and a connector
 * looks only for the keys it wrote itself. Carrying them here rather than on the run means the rest
 * of the engine needs no notion of a replay at all — planning, leasing, checkpointing, transforms,
 * the sink write path, rejection thresholds and retry all behave exactly as they do for a chunk
 * reading from a real source, because as far as they can tell it is one.
 *
 * <p>The window is an offset and a length over a stable ordering rather than a set of record ids.
 * Ids would be exact but would put the whole replay list in the chunk document, which for a run
 * that rejected twenty thousand records is the DLQ written a second time into a place that has no
 * expiry.
 */
final class Replay {

    private static final String SOURCE_RUN = "_dmpReplayOf";
    private static final String SKIP = "_dmpReplaySkip";
    private static final String LIMIT = "_dmpReplayLimit";
    private static final String TRANSFORM = "_dmpReplayTransform";

    private Replay() {
    }

    static JsonNode spec(RunId originalRunId, int skip, int limit, boolean applyTransforms) {
        ObjectNode spec = Json.newObject();
        spec.put(SOURCE_RUN, originalRunId.toString());
        spec.put(SKIP, skip);
        spec.put(LIMIT, limit);
        spec.put(TRANSFORM, applyTransforms);
        return spec;
    }

    /**
     * Whether this replay runs its records through the pipeline's transforms again.
     *
     * <p>Normally it must not. What the dead-letter queue holds is the record <em>as the sink saw
     * it</em> — the batch is transformed and then written, and it is the write that fails, so the
     * stored payload is already the transform's output. Sending it back through the same transform
     * applies it twice, which is invisible when the script is idempotent and silently wrong when it
     * is not: a prefix gets prepended twice, a counter increments twice, a timestamp is restamped.
     *
     * <p>It is true only when the user is replaying through a <em>different</em> version because the
     * fix was in the pipeline itself, where applying the new logic is the entire point.
     *
     * <p>Absent means false: a spec written before this key existed came from the path that should
     * never have been transforming in the first place.
     */
    static boolean appliesTransforms(JsonNode spec) {
        return spec != null && spec.path(TRANSFORM).asBoolean(false);
    }

    static boolean isReplay(JsonNode spec) {
        return spec != null && spec.hasNonNull(SOURCE_RUN);
    }

    static RunId originalRunId(JsonNode spec) {
        return RunId.of(java.util.UUID.fromString(spec.path(SOURCE_RUN).asText()));
    }

    static int skip(JsonNode spec) {
        return spec.path(SKIP).asInt(0);
    }

    static int limit(JsonNode spec) {
        return spec.path(LIMIT).asInt(Integer.MAX_VALUE);
    }
}
