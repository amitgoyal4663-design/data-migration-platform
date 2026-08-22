package com.dmp.engine;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.util.Map;

/**
 * Thrown when a chunk that knew its own size did not read it.
 *
 * <p>Only chunks planned by a connector that <em>counted</em> can raise this — a Databricks
 * manifest that states the rows in each result chunk, not a key range guessed from minimum and
 * maximum. Where the number exists it is exact, so a shortfall is a fact rather than an estimate
 * gone slightly wrong.
 *
 * <p><b>Why this is worth failing a chunk over.</b> The failure it catches leaves no other trace.
 * A source that answers an empty result — because it was restarted, because the query behind the
 * chunk expired, because a result set was reaped — produces a chunk that reads nothing, transforms
 * nothing, writes nothing and completes. The run reports success. The chunk reports COMPLETED. The
 * counts all agree with each other, because zero is consistent with zero. Tens of thousands of
 * records are simply absent, and every screen says the migration worked.
 *
 * <p>That is not hypothetical: it is how thirty-six consecutive chunks of a hundred-thousand-row
 * run came to write nothing at all while the run looked healthy. The row count that makes this
 * detectable was added for an unrelated reason — sizing the batch to the chunk — and the check
 * costs one comparison.
 *
 * <p><b>Retryable, unlike a rejection threshold.</b> Rejections at scale are systematic and sending
 * the same records again changes nothing. A shortfall is usually transient: the source was
 * restarted, the statement was reaped, the warehouse was mid-failover. Re-reading is exactly the
 * right response, and if the shortfall is permanent the chunk exhausts its attempts and stops with
 * both numbers in the message.
 */
public class ChunkShortfallException extends DmpException {

    private final int chunkIndex;
    private final long planned;
    private final long read;

    public ChunkShortfallException(int chunkIndex, long planned, long read) {
        super(ErrorCode.UPSTREAM_UNAVAILABLE,
                "Chunk " + chunkIndex + " was planned as " + planned + " row(s) but read " + read
                        + ". The source counted these rows when the run was planned, so the "
                        + "shortfall is the source's answer changing rather than an estimate being "
                        + "wrong — the query behind this chunk may have expired, or the system "
                        + "holding the result may have been restarted. The chunk is failed rather "
                        + "than completed, because a chunk that reads nothing writes nothing and "
                        + "would otherwise report success.",
                Map.of("chunkIndex", chunkIndex, "plannedRows", planned, "recordsRead", read));
        this.chunkIndex = chunkIndex;
        this.planned = planned;
        this.read = read;
    }

    public int chunkIndex() {
        return chunkIndex;
    }

    public long planned() {
        return planned;
    }

    public long read() {
        return read;
    }
}
