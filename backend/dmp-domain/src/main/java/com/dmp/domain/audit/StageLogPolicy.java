package com.dmp.domain.audit;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.util.Map;

/**
 * Which stages of the work the platform writes a log for.
 *
 * <p>Every other audit setting describes a <em>record</em>. None of them describe the <em>work</em>
 * — and the work happens in batches, so the two do not line up. Five hundred records refused in one
 * request is five hundred rejections and one status code; forty records read and thirty-one written
 * is one filter dropping nine, and no per-record entry says which node did it.
 *
 * <p>Three stages, matching the three things a chunk actually does:
 *
 * <ul>
 *   <li><b>{@code reads}</b> — one entry per window of reading, carrying the query the connector
 *       actually built and the cursor either side of it. The answer to "why did this move
 *       nothing?", which was previously assembled inside a connector and discarded.</li>
 *   <li><b>{@code transforms}</b> — one entry per transform stage per batch, carrying records in,
 *       records out and how long the scripts took. The answer to "where did my records go?"; a
 *       filter and a fan-out are both invisible in the counts until something names the node.</li>
 *   <li><b>{@code writes}</b> — one entry per call handed to the destination, carrying whatever
 *       the destination said back.</li>
 * </ul>
 *
 * <p>{@code bodies} is separate from all three because it costs a different order of magnitude.
 * The stage entries are one per batch and hold no customer data; a body is the batch itself, in a
 * store built to be searched, and can be megabytes per call.
 *
 * <p>All four default to off. A platform that silently began writing a new index on upgrade would
 * be spending a user's storage on a decision they never made.
 */
public record StageLogPolicy(boolean reads, boolean transforms, boolean writes, boolean bodies) {

    /** Nothing logged. What a pipeline that has never heard of this setting resolves to. */
    public static final StageLogPolicy OFF = new StageLogPolicy(false, false, false, false);

    public StageLogPolicy {
        if (bodies && !reads && !transforms && !writes) {
            // Refused rather than ignored, because the two readings of it are opposite: somebody
            // ticking "capture bodies" with no stage enabled either believes they have turned
            // logging on, or has left a setting behind after turning it off. Both are worth an
            // error at publish time rather than a surprise later.
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Capturing request and response bodies needs at least one stage to be logged. "
                            + "On its own there is nothing for it to attach the bodies to.",
                    Map.of("bodies", true));
        }
    }

    /** Whether anything at all is written, so a caller can skip building an entry. */
    public boolean logsAnything() {
        return reads || transforms || writes;
    }

    /**
     * Whether bodies are stored for the stages that are logged.
     *
     * <p>Guarded by {@link #logsAnything()} for the same reason {@link AuditPolicy#indexesPayloads()}
     * is guarded by its level: a switch that only makes sense underneath another one must answer
     * for the combination, not for itself.
     */
    public boolean capturesBodies() {
        return bodies && logsAnything();
    }
}
