package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.util.Map;

/**
 * How a batch is divided into calls on the sink.
 *
 * <p>Separate from {@link ChunkingPolicy} because it answers a different question. The batch size
 * decides how much is buffered and how much is redone after a crash; this decides how many times
 * the sink is called with what was buffered. Conflating them is what made
 * {@code writeBatchSize = 1} the only way to reach an API that wants one record per request — and
 * that also collapsed the checkpoint interval to one write per record, so the bookkeeping cost as
 * much as the work.
 *
 * <p>With the two separated, a pipeline can checkpoint every thousand records and still call the
 * destination one record at a time.
 *
 * <p>Four shapes, and the first three are the same idea with a number:
 *
 * <pre>
 *   groupSize 0   the whole batch in one call        the default
 *   groupSize 1   one call per record
 *   groupSize N   calls of N records each
 *   splitScript   one call per group, groups decided by a script
 * </pre>
 *
 * <p><b>A group never crosses a batch.</b> The engine holds a batch, never a chunk, so two records
 * sharing a key in different batches are two different calls. Grouping shapes each batch as it
 * passes; it does not collect records from across the run. Anything needing all of a key together
 * has to be sorted at the source.
 *
 * @param groupSize   records per call: {@code 0} whole batch, {@code 1} per record, {@code N} fixed
 * @param splitScript JavaScript returning one group label per record; null or blank for none
 */
public record DeliveryPolicy(int groupSize, String splitScript) {

    /** One call carrying everything the batch buffered. */
    public static final int WHOLE_BATCH = 0;

    /** One call per record — slower, but a failure names the record rather than the batch. */
    public static final int PER_RECORD = 1;

    private static final int MAX_GROUP_SIZE = 1_000_000;
    private static final int MAX_SCRIPT_LENGTH = 100_000;

    /** What every pipeline gets unless it says otherwise: hand the sink what it was given. */
    public static final DeliveryPolicy DEFAULT = new DeliveryPolicy(WHOLE_BATCH, null);

    public DeliveryPolicy {
        splitScript = splitScript == null || splitScript.isBlank() ? null : splitScript.strip();

        if (groupSize < 0 || groupSize > MAX_GROUP_SIZE) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Records per call must be between 0 (the whole batch) and " + MAX_GROUP_SIZE,
                    Map.of("field", "groupSize", "value", String.valueOf(groupSize)));
        }
        if (splitScript != null && splitScript.length() > MAX_SCRIPT_LENGTH) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "The split script is longer than " + MAX_SCRIPT_LENGTH + " characters",
                    Map.of("field", "splitScript"));
        }

        // Refused rather than resolved by precedence. Both are ways of saying how the batch is
        // divided, and a pipeline carrying both would divide it one way while its author read the
        // other from the screen.
        if (splitScript != null && groupSize != WHOLE_BATCH) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A split script and a fixed group size both decide how the batch is divided, "
                            + "so only one of them can be set. Remove the script to use groups of "
                            + groupSize + ", or clear the group size to let the script decide.",
                    Map.of("field", "splitScript"));
        }
    }

    public boolean splitsByScript() {
        return splitScript != null;
    }

    /** Whether the sink is handed exactly what the batch buffered, with no division at all. */
    public boolean isWholeBatch() {
        return splitScript == null && groupSize == WHOLE_BATCH;
    }

    /**
     * Whether every call carries a single record.
     *
     * <p>Asked by validation rather than by the executor: a sink whose write is not per-record in
     * any meaningful sense — one Salesforce bulk job is one chunk, however it is fed — should say
     * so at publish time instead of accepting a setting that changes nothing.
     */
    public boolean isPerRecord() {
        return splitScript == null && groupSize == PER_RECORD;
    }

    public DeliveryPolicy withGroupSize(int size) {
        return new DeliveryPolicy(size, null);
    }

    public DeliveryPolicy withSplitScript(String script) {
        return new DeliveryPolicy(WHOLE_BATCH, script);
    }
}
