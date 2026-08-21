package com.dmp.application.port.out;

import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;

import java.util.List;
import java.util.Optional;

/**
 * Persistence port for checkpoints. Implemented by the MongoDB adapter (ADR-0005).
 *
 * <p>Exactly one checkpoint exists per split, overwritten in place. This is the hottest write path
 * in the platform — a 10,000-split run committing batches every few seconds — which is the concrete
 * reason execution data does not live in PostgreSQL, where each of those writes would leave a dead
 * tuple for vacuum to collect.
 */
public interface CheckpointRepository {

    /**
     * Writes the checkpoint for a split.
     *
     * <p>Called after the sink has durably accepted a batch — which for an asynchronous sink means
     * after {@code checkCommit()} reports COMPLETE, not after {@code write()} returns
     * (ADR-0009, ADR-0012). Persisting earlier would advance the cursor past records the sink
     * subsequently rejected.
     */
    Checkpoint save(Checkpoint checkpoint);

    Optional<Checkpoint> findBySplit(TenantId tenantId, SplitId splitId);

    List<Checkpoint> findByRun(TenantId tenantId, RunId runId);

    /**
     * Returns the stored checkpoint, or a fresh one if the split has never committed a batch.
     *
     * <p>The call a worker makes when starting or resuming a split, so that resuming and starting
     * from scratch are the same code path rather than a branch the engine has to get right.
     */
    Checkpoint findOrCreate(TenantId tenantId, RunId runId, SplitId splitId);

    void deleteByRun(TenantId tenantId, RunId runId);
}
