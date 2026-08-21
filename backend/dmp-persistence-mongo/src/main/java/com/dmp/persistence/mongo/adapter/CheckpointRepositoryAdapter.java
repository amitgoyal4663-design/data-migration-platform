package com.dmp.persistence.mongo.adapter;

import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.mongo.document.CheckpointDocument;
import com.dmp.persistence.mongo.mapper.CheckpointMapper;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

/** MongoDB adapter for {@link CheckpointRepository}. */
@Repository
public class CheckpointRepositoryAdapter implements CheckpointRepository {

    private final MongoTemplate mongo;
    private final Clock clock;

    public CheckpointRepositoryAdapter(MongoTemplate mongo, Clock clock) {
        this.mongo = mongo;
        this.clock = clock;
    }

    /**
     * {@inheritDoc}
     *
     * <p>An upsert keyed by split id — one checkpoint per split, replaced in place rather than
     * appended. Retaining checkpoint history would multiply the platform's busiest write by the
     * number of batches in a run, for data nothing reads: only the latest position is ever used.
     */
    @Override
    public Checkpoint save(Checkpoint checkpoint) {
        return CheckpointMapper.toDomain(mongo.save(CheckpointMapper.toDocument(checkpoint)));
    }

    @Override
    public Optional<Checkpoint> findBySplit(TenantId tenantId, SplitId splitId) {
        return Optional.ofNullable(mongo.findOne(
                        scoped(tenantId).addCriteria(Criteria.where("_id").is(splitId.value())),
                        CheckpointDocument.class))
                .map(CheckpointMapper::toDomain);
    }

    @Override
    public List<Checkpoint> findByRun(TenantId tenantId, RunId runId) {
        Query query = scoped(tenantId).addCriteria(Criteria.where("runId").is(runId.value()));
        return mongo.find(query, CheckpointDocument.class).stream()
                .map(CheckpointMapper::toDomain)
                .toList();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Not persisted when absent. The initial checkpoint is a value, not a claim on the split —
     * writing one here would put a document behind every planned split before any work began,
     * inflating the collection by the full split count for a run that may never start.
     */
    @Override
    public Checkpoint findOrCreate(TenantId tenantId, RunId runId, SplitId splitId) {
        return findBySplit(tenantId, splitId)
                .orElseGet(() -> Checkpoint.initial(splitId, runId, tenantId, clock.instant()));
    }

    @Override
    public void deleteByRun(TenantId tenantId, RunId runId) {
        mongo.remove(
                scoped(tenantId).addCriteria(Criteria.where("runId").is(runId.value())),
                CheckpointDocument.class);
    }

    private static Query scoped(TenantId tenantId) {
        return new Query(Criteria.where("tenantId").is(tenantId.value()));
    }
}
