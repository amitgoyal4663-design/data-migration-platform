package com.dmp.engine;

/**
 * Thrown when so much of a chunk was rejected that finishing it would be dishonest.
 *
 * <p>Its own type rather than a {@link com.dmp.connector.api.ConnectorException} because no
 * connector failed. Every individual write did exactly what it was asked and reported the outcome
 * correctly; what went wrong is the aggregate, and that is the engine's judgement to make, not the
 * connector's.
 *
 * <p>Never retried. The failures that reach this threshold are systematic by definition — a schema
 * that changed, a required field nobody populated, a key that already exists everywhere — and none
 * of them are altered by sending the same records again. Retrying would re-upload the whole chunk
 * to be rejected identically, three more times, burning whatever quota the target charges for the
 * privilege.
 */
public class RejectionThresholdExceededException extends RuntimeException {

    private final int chunkIndex;
    private final long produced;
    private final long failed;

    public RejectionThresholdExceededException(int chunkIndex, long produced, long failed,
                                               String reason) {
        super("Chunk " + chunkIndex + " was stopped because " + reason
                + ". Rejections on this scale are a property of the pipeline or the target rather "
                + "than of individual records, so the chunk is failed instead of completed and is "
                + "not retried.");
        this.chunkIndex = chunkIndex;
        this.produced = produced;
        this.failed = failed;
    }

    public int chunkIndex() {
        return chunkIndex;
    }

    public long produced() {
        return produced;
    }

    public long failed() {
        return failed;
    }
}
