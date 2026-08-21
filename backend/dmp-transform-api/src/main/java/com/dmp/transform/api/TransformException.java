package com.dmp.transform.api;

/**
 * A user's script failed.
 *
 * <p>Always names the node, because a pipeline with four transforms gives "transform failed" no
 * actionable content. Carries the offending record's sequence number where one exists, so the
 * record-error entry the engine writes can be tied back to a specific row.
 */
public class TransformException extends RuntimeException {

    private final String nodeId;
    private final String nodeName;
    private final long seq;

    public TransformException(String nodeId, String nodeName, long seq, String message,
                              Throwable cause) {
        super(message, cause);
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.seq = seq;
    }

    public TransformException(String nodeId, String nodeName, String message, Throwable cause) {
        this(nodeId, nodeName, -1, message, cause);
    }

    public String nodeId() {
        return nodeId;
    }

    public String nodeName() {
        return nodeName;
    }

    /** Sequence number of the record being processed, or -1 for a compile or batch failure. */
    public long seq() {
        return seq;
    }
}
