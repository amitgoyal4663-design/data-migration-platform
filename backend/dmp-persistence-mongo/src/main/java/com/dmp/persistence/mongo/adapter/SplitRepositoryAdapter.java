package com.dmp.persistence.mongo.adapter;

import com.dmp.application.port.out.SplitRepository;
import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.Split;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.run.SplitState;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.mongo.document.SplitDocument;
import com.dmp.persistence.mongo.mapper.SplitMapper;
import com.dmp.persistence.mongo.support.JsonDocuments;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** MongoDB adapter for {@link SplitRepository}. */
@Repository
public class SplitRepositoryAdapter implements SplitRepository {

    private static final Logger log = LoggerFactory.getLogger(SplitRepositoryAdapter.class);

    private final MongoTemplate mongo;

    public SplitRepositoryAdapter(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    /**
     * {@inheritDoc}
     *
     * <p>A single bulk insert. Planning a large migration produces tens of thousands of splits at
     * once; a round trip each would make planning take longer than reading the data it plans for.
     */
    @Override
    public void saveAll(List<Split> splits) {
        if (splits == null || splits.isEmpty()) {
            return;
        }
        mongo.insert(splits.stream().map(SplitMapper::toDocument).toList(), SplitDocument.class);
    }

    @Override
    public Optional<Split> findById(TenantId tenantId, SplitId id) {
        return Optional.ofNullable(mongo.findOne(
                        scoped(tenantId).addCriteria(Criteria.where("_id").is(id.value())),
                        SplitDocument.class))
                .map(SplitMapper::toDomain);
    }

    @Override
    public List<Split> findByRun(TenantId tenantId, RunId runId) {
        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("runId").is(runId.value()))
                .with(Sort.by(Sort.Direction.ASC, "index"));
        return mongo.find(query, SplitDocument.class).stream().map(SplitMapper::toDomain).toList();
    }

    @Override
    public Page<Split> findByRun(TenantId tenantId, RunId runId, PageQuery pageQuery) {
        Criteria criteria = Criteria.where("runId").is(runId.value());

        // Counted separately rather than derived from the page, because a short page means "this
        // is the end" and nothing about how many there were.
        long total = mongo.count(scoped(tenantId).addCriteria(criteria), SplitDocument.class);

        Query query = scoped(tenantId)
                .addCriteria(criteria)
                .with(Sort.by(Sort.Direction.ASC, "index"))
                .skip((long) pageQuery.page() * pageQuery.size())
                .limit(pageQuery.size());

        List<Split> splits = mongo.find(query, SplitDocument.class).stream()
                .map(SplitMapper::toDomain)
                .toList();
        return Page.of(splits, pageQuery, total);
    }

    @Override
    public java.util.Map<SplitState, Long> countByState(TenantId tenantId, RunId runId) {
        org.springframework.data.mongodb.core.aggregation.Aggregation aggregation =
                org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation(
                        org.springframework.data.mongodb.core.aggregation.Aggregation.match(
                                Criteria.where("tenantId").is(tenantId.value())
                                        .and("runId").is(runId.value())),
                        org.springframework.data.mongodb.core.aggregation.Aggregation
                                .group("state").count().as("count"));

        java.util.Map<SplitState, Long> counts = new java.util.EnumMap<>(SplitState.class);
        for (org.bson.Document row : mongo.aggregate(aggregation, SplitDocument.class, org.bson.Document.class)) {
            String state = row.getString("_id");
            if (state != null) {
                counts.merge(SplitState.valueOf(state), row.get("count", Number.class).longValue(),
                        Long::sum);
            }
        }
        return counts;
    }

    @Override
    public List<Split> findByRunAndState(TenantId tenantId, RunId runId, SplitState state) {
        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("runId").is(runId.value()))
                .addCriteria(Criteria.where("state").is(state.name()))
                .with(Sort.by(Sort.Direction.ASC, "index"));
        return mongo.find(query, SplitDocument.class).stream().map(SplitMapper::toDomain).toList();
    }

    /**
     * {@inheritDoc}
     *
     * <p>The whole claim is one {@code findAndModify}: match a PENDING split, stamp it RUNNING with
     * the worker's id, return it. Two workers issuing this concurrently cannot both receive the
     * same split — the second matches nothing and gets the next one, or empty.
     *
     * <p>Splits are claimed in index order so that a run's progress is legible when watched, rather
     * than completing in whatever order the storage engine happens to return.
     */
    @Override
    public Optional<Split> claimNextPending(TenantId tenantId, RunId runId, String workerId,
                                            Instant now, Duration lease) {
        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("runId").is(runId.value()))
                .addCriteria(Criteria.where("state").is(SplitState.PENDING.name()))
                // Held back until its time, if something asked for that — a chunk deferred because
                // the destination's agreed rate was spent. Absent on every chunk that was never
                // deferred, and absent must mean claimable: the field did not exist before
                // deferral did, and every chunk stored until then has no value for it.
                .addCriteria(new Criteria().orOperator(
                        Criteria.where("dueAt").exists(false),
                        Criteria.where("dueAt").is(null),
                        Criteria.where("dueAt").lte(now)))
                .with(Sort.by(Sort.Direction.ASC, "index"));

        Update update = new Update()
                .set("state", SplitState.RUNNING.name())
                .set("assignedTo", workerId)
                .set("leaseExpiresAt", now.plus(lease))
                .set("startedAt", now)
                .set("updatedAt", now)
                // The hold has been served. Left behind, it would be a time in the past on a
                // running chunk — harmless today and exactly the sort of stale field that later
                // gets read as though it meant something.
                .unset("dueAt");

        SplitDocument claimed = mongo.findAndModify(query, update,
                FindAndModifyOptions.options().returnNew(true), SplitDocument.class);

        return Optional.ofNullable(claimed).map(SplitMapper::toDomain);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The worker id is part of the filter, not just the update. A worker whose lease lapsed and
     * whose split was reclaimed by another pod must fail to extend it — otherwise both pods would
     * believe they hold the claim and process the same chunk, writing every one of its records
     * twice.
     */
    @Override
    public Optional<Split> heartbeat(TenantId tenantId, SplitId id, String workerId,
                                     Instant now, Duration lease) {
        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("_id").is(id.value()))
                .addCriteria(Criteria.where("state").is(SplitState.RUNNING.name()))
                .addCriteria(Criteria.where("assignedTo").is(workerId));

        Update update = new Update()
                .set("leaseExpiresAt", now.plus(lease))
                .set("updatedAt", now);

        SplitDocument extended = mongo.findAndModify(query, update,
                FindAndModifyOptions.options().returnNew(true), SplitDocument.class);

        if (extended == null) {
            // The worker has lost this split. It must stop processing immediately rather than
            // finish and write results another pod is also producing.
            log.warn("Worker {} no longer holds split {}; its lease was reclaimed", workerId, id);
        }
        return Optional.ofNullable(extended).map(SplitMapper::toDomain);
    }

    @Override
    public List<Split> findExpiredLeases(Instant now, int limit) {
        Query query = new Query()
                .addCriteria(Criteria.where("state").is(SplitState.RUNNING.name()))
                .addCriteria(Criteria.where("leaseExpiresAt").lt(now))
                .with(Sort.by(Sort.Direction.ASC, "leaseExpiresAt"))
                .limit(limit);
        return mongo.find(query, SplitDocument.class).stream().map(SplitMapper::toDomain).toList();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Ordered by how overdue they are, so the chunk that has been ignored longest is the first
     * one a constrained sweep gets to.
     */
    @Override
    public List<Split> findDueExternalWaits(Instant now, int limit) {
        Query query = new Query()
                .addCriteria(Criteria.where("state").is(SplitState.WAITING_EXTERNAL.name()))
                .addCriteria(Criteria.where("dueAt").lte(now))
                .with(Sort.by(Sort.Direction.ASC, "dueAt"))
                .limit(limit);
        return mongo.find(query, SplitDocument.class).stream().map(SplitMapper::toDomain).toList();
    }

    @Override
    public long countRunning(TenantId tenantId, RunId runId) {
        return countByState(tenantId, runId, SplitState.RUNNING);
    }

    @Override
    public Optional<Split> transitionState(TenantId tenantId, SplitId id, SplitState expectedState,
                                           Split updated) {
        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("_id").is(id.value()))
                .addCriteria(Criteria.where("state").is(expectedState.name()));

        Update update = new Update()
                .set("state", updated.state().name())
                .set("assignedTo", updated.assignedTo())
                .set("leaseExpiresAt", updated.leaseExpiresAt())
                .set("attempt", updated.attempt())
                .set("errorCode", updated.errorCode())
                .set("errorMessage", updated.errorMessage())
                .set("startedAt", updated.startedAt())
                .set("endedAt", updated.endedAt())
                .set("updatedAt", updated.updatedAt())
                // Written on every transition, not only when parking. A chunk that parks stores its
                // remote job handle here and a chunk that completes clears it, and omitting either
                // leaves a stale handle behind — which the executor would read as "settle this job"
                // on a later attempt that has nothing to do with it.
                .set("externalJob", JsonDocuments.toMap(updated.externalJob()))
                .set("dueAt", updated.dueAt());

        SplitDocument result = mongo.findAndModify(query, update,
                FindAndModifyOptions.options().returnNew(true), SplitDocument.class);

        return Optional.ofNullable(result).map(SplitMapper::toDomain);
    }

    @Override
    public long countByState(TenantId tenantId, RunId runId, SplitState state) {
        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("runId").is(runId.value()))
                .addCriteria(Criteria.where("state").is(state.name()));
        return mongo.count(query, SplitDocument.class);
    }

    @Override
    public void deleteByRun(TenantId tenantId, RunId runId) {
        mongo.remove(
                scoped(tenantId).addCriteria(Criteria.where("runId").is(runId.value())),
                SplitDocument.class);
    }

    private static Query scoped(TenantId tenantId) {
        return new Query(Criteria.where("tenantId").is(tenantId.value()));
    }
}
