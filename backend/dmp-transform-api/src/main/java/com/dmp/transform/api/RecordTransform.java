package com.dmp.transform.api;

import com.dmp.connector.api.DataRecord;

import java.util.List;

/**
 * A pipeline's transform nodes, compiled and ready to run.
 *
 * <p><b>Single-threaded.</b> One instance belongs to one chunk being executed by one worker
 * thread, and is closed when that chunk finishes. Script runtimes hold mutable state that is not
 * safe to share, and the alternative — locking around every record — would serialise the fleet.
 *
 * <p><b>Deterministic and side-effect free.</b> A chunk that fails resumes from its checkpoint and
 * re-transforms every record since. A script that called an API or appended to a file would do so
 * twice. The sandbox denies network and filesystem access, so this is enforced rather than merely
 * documented (ADR-0008).
 */
public interface RecordTransform extends AutoCloseable {

    /**
     * A transform that does nothing, for pipelines with no transform nodes.
     *
     * <p>Returning a singleton rather than null means the executor has no special case: it always
     * has a transform, and {@link #isIdentity()} lets it skip the per-record call entirely.
     */
    RecordTransform IDENTITY = new RecordTransform() {

        @Override
        public List<DataRecord> applyRecord(DataRecord record) {
            return List.of(record);
        }

        @Override
        public BatchResult applyBatch(List<DataRecord> records) {
            return BatchResult.none();
        }

        @Override
        public List<String> split(List<DataRecord> records) {
            return List.of();
        }

        @Override
        public boolean isIdentity() {
            return true;
        }

        @Override
        public boolean hasBatchStage() {
            return false;
        }

        @Override
        public boolean hasSplitStage() {
            return false;
        }

        @Override
        public void close() {
            // Nothing acquired, nothing to release.
        }
    };

    /**
     * Runs every per-record node in graph order.
     *
     * @return the records to carry forward: empty if the record was dropped, one if it was
     *         replaced, several if it was fanned out. Never null.
     * @throws TransformException if a script threw, timed out, or returned something unusable
     */
    List<DataRecord> applyRecord(DataRecord record);

    /**
     * Runs the per-batch node, if the pipeline has one.
     *
     * @param records the batch about to be written, after per-record transformation
     * @return replacement payloads, or the payload the sink should send; see {@link BatchResult}
     * @throws TransformException if the script threw, timed out, or returned an array of the wrong
     *         length
     */
    BatchResult applyBatch(List<DataRecord> records);

    /**
     * Divides a batch into the groups the sink is called with, one label per record.
     *
     * <p>Labels rather than groups of records, and the difference matters. Records cross into the
     * sandbox as payloads; the engine keeps their sequence number and key on its side and pairs the
     * results back by position. A script returning nested lists would have rearranged them, so
     * position would no longer identify anything — and the sequence number is the checkpoint's
     * resume coordinate while the key drives idempotent writes and the audit index. Returning
     * labels lets a script see the whole batch and decide anything it likes about it, while making
     * it impossible to lose a record's identity, drop one, or duplicate one.
     *
     * @param records the batch about to be written, after per-record transformation
     * @return one label per record, in the same order; empty when there is no split node
     * @throws TransformException if the script threw, timed out, or returned the wrong number
     */
    List<String> split(List<DataRecord> records);

    /** Whether this does nothing per record, so the executor can skip the call. */
    boolean isIdentity();

    /** Whether a batch node exists. Lets the executor avoid building a list it will not use. */
    boolean hasBatchStage();

    /** Whether a split node exists. */
    boolean hasSplitStage();

    @Override
    void close();
}
