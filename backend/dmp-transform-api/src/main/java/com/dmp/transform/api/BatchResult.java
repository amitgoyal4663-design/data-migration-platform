package com.dmp.transform.api;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * What a batch script did.
 *
 * <p>Two outcomes, chosen by what the script returned, because a batch script is asked to do two
 * genuinely different jobs and they cannot both be expressed as one return shape:
 *
 * <ul>
 *   <li>An <b>array</b> replaces the records — one payload per record, in order. This is how a
 *       value that only exists at batch scope, such as a batch id or a load timestamp, gets onto
 *       every record in it. A per-record script cannot do this: records are transformed before they
 *       are grouped, so at that point there is no batch to take a value from.</li>
 *   <li>Anything else becomes the <b>envelope</b> — the single payload a sink sends as one request,
 *       for an API wanting {@code {"items": [...]}}. Only sinks that post a batch in one request
 *       can use it; a database writing rows individually has nothing to apply it to.</li>
 * </ul>
 *
 * <p>The array case must preserve the record count. The engine's central guarantee is that a record
 * entering the sink stage is either written or rejected, and a script quietly returning fewer would
 * make records vanish while the counters still balanced — the exact failure this platform exists to
 * make impossible.
 */
public record BatchResult(List<JsonNode> replacements, JsonNode envelope) {

    public BatchResult {
        replacements = replacements == null ? null : List.copyOf(replacements);
    }

    /** New payloads for the batch's records, positionally matched to them. */
    public static BatchResult replacing(List<JsonNode> payloads) {
        return new BatchResult(payloads, null);
    }

    /** The single payload the sink should send. */
    public static BatchResult enveloping(JsonNode payload) {
        return new BatchResult(null, payload);
    }

    /** No batch script exists; the sink assembles the batch as it normally would. */
    public static BatchResult none() {
        return new BatchResult(null, null);
    }

    public boolean replacesRecords() {
        return replacements != null;
    }

    public boolean hasEnvelope() {
        return envelope != null;
    }
}
