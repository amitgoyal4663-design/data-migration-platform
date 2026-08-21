package com.dmp.persistence.mongo.adapter;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.RunRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.pipeline.ExecutionPolicy;
import com.dmp.domain.run.Run;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.RunMetrics;
import com.dmp.domain.run.RunState;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.mongo.document.RunDocument;
import com.dmp.persistence.mongo.mapper.RunMapper;
import com.dmp.persistence.mongo.support.JsonDocuments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * MongoDB adapter for {@link RunRepository}.
 *
 * <p>Uses {@code MongoTemplate} rather than a Spring Data repository interface because the
 * operations that matter here are conditional updates. {@code findAndModify} with the expected
 * state in the query is the concurrency primitive this whole design rests on, and it is not
 * expressible as a derived query method.
 */
@Repository
public class RunRepositoryAdapter implements RunRepository {

    private static final Logger log = LoggerFactory.getLogger(RunRepositoryAdapter.class);

    private static final String DEFAULT_SORT = "createdAt";

    private static final Map<String, String> SORTABLE = Map.of(
            "createdAt", "createdAt",
            "startedAt", "startedAt",
            "endedAt", "endedAt",
            "state", "state");

    private final MongoTemplate mongo;

    public RunRepositoryAdapter(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    @Override
    public Run create(Run run) {
        try {
            return RunMapper.toDomain(mongo.insert(RunMapper.toDocument(run)));
        } catch (DuplicateKeyException e) {
            // The partial unique index on idempotencyKey fired. This is the guard against a
            // delay-queue redelivery starting the same migration twice — an expected outcome under
            // the at-least-once semantics ADR-0002 accepts, not an error condition.
            throw new DmpException(ErrorCode.DUPLICATE,
                    "A run with this idempotency key already exists for the tenant",
                    Map.of("idempotencyKey", String.valueOf(run.idempotencyKey())), e);
        }
    }

    @Override
    public Optional<Run> findById(TenantId tenantId, RunId id) {
        return Optional.ofNullable(mongo.findOne(
                        scoped(tenantId).addCriteria(Criteria.where("_id").is(id.value())),
                        RunDocument.class))
                .map(RunMapper::toDomain);
    }

    @Override
    public Optional<Run> findByIdempotencyKey(TenantId tenantId, String idempotencyKey) {
        if (idempotencyKey == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mongo.findOne(
                        scoped(tenantId).addCriteria(Criteria.where("idempotencyKey").is(idempotencyKey)),
                        RunDocument.class))
                .map(RunMapper::toDomain);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The expected state is part of the query, so the update applies only if no one else has
     * moved the run in the meantime. Two workers racing to pause the same run cannot both succeed,
     * and the loser learns so from an empty result rather than from silently overwriting.
     */
    @Override
    public Optional<Run> transitionState(TenantId tenantId, RunId id, RunState expectedState, Run updated) {
        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("_id").is(id.value()))
                .addCriteria(Criteria.where("state").is(expectedState.name()));

        Update update = new Update()
                .set("state", updated.state().name())
                .set("updatedAt", updated.updatedAt())
                // The planned chunk count rides along with the transition into RUNNING. Without
                // it, only the completed counter is ever written, and progress reads as "2 of 0".
                .set("metrics.splitsTotal", updated.metrics().splitsTotal())
                .set("preparationState", JsonDocuments.toMap(updated.preparationState()))
                .set("errorCode", updated.errorCode())
                .set("errorMessage", updated.errorMessage())
                .set("startedAt", updated.startedAt())
                .set("endedAt", updated.endedAt());

        RunDocument result = mongo.findAndModify(query, update,
                FindAndModifyOptions.options().returnNew(true), RunDocument.class);

        if (result == null) {
            log.debug("Run {} was no longer in state {}; transition to {} did not apply",
                    id, expectedState, updated.state());
            return Optional.empty();
        }
        return Optional.of(RunMapper.toDomain(result));
    }

    @Override
    public Run save(Run run) {
        return RunMapper.toDomain(mongo.save(RunMapper.toDocument(run)));
    }

    /**
     * {@inheritDoc}
     *
     * <p>The limit is part of the query, not a preceding read. MongoDB applies the filter and the
     * increment as one operation, so between "is there room" and "take a slot" there is no window
     * for another pod to slip through. With a limit of one — strictly sequential execution —
     * exactly one worker in the entire fleet holds the slot at any instant, and no lock, lease
     * table or coordinator is involved.
     */
    @Override
    public boolean tryReserveSlot(TenantId tenantId, RunId id, int maxConcurrentChunks) {
        if (maxConcurrentChunks <= ExecutionPolicy.UNLIMITED) {
            // Unlimited runs skip reservation entirely, so the common case costs no round trip.
            return true;
        }

        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("_id").is(id.value()))
                .addCriteria(Criteria.where("activeSlots").lt(maxConcurrentChunks));

        RunDocument reserved = mongo.findAndModify(query, new Update().inc("activeSlots", 1),
                FindAndModifyOptions.options().returnNew(true), RunDocument.class);

        if (reserved == null) {
            log.trace("Run {} is at its concurrency limit of {}", id, maxConcurrentChunks);
            return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Guarded so the counter cannot go negative. A double release — a worker releasing on both
     * the failure path and a shared finally block — would otherwise let the run exceed its limit
     * permanently, which on a sequential pipeline means two chunks running at once and the
     * lock-contention failures the setting existed to prevent.
     */
    @Override
    public void releaseSlot(TenantId tenantId, RunId id) {
        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("_id").is(id.value()))
                .addCriteria(Criteria.where("activeSlots").gt(0));

        mongo.updateFirst(query, new Update().inc("activeSlots", -1), RunDocument.class);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Sets the counter to the observed truth rather than adjusting it. Drift accumulates in one
     * direction — workers that die holding a slot never decrement — and the terminal state of that
     * drift on a sequential run is a permanent deadlock: the counter reads 1, nothing is actually
     * running, and no worker can ever reserve again. Recomputing periodically removes the
     * possibility rather than reducing its likelihood.
     */
    @Override
    public void reconcileSlots(TenantId tenantId, RunId id, int actualRunningChunks) {
        Query query = scoped(tenantId).addCriteria(Criteria.where("_id").is(id.value()));
        Update update = new Update().set("activeSlots", Math.max(0, actualRunningChunks));

        mongo.updateFirst(query, update, RunDocument.class);
        log.debug("Reconciled run {} slot counter to {}", id, actualRunningChunks);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Every counter is applied with {@code $inc}, so no read precedes the write. Counters only
     * increase, which means concurrent workers need no ordering between them and no update can be
     * lost — on the hottest write path a run has.
     */
    @Override
    public void incrementMetrics(TenantId tenantId, RunId id, RunMetrics delta) {
        Update update = new Update().set("updatedAt", Instant.now());
        applyIfNonZero(update, "metrics.recordsRead", delta.recordsRead());
        applyIfNonZero(update, "metrics.recordsProduced", delta.recordsProduced());
        applyIfNonZero(update, "metrics.recordsWritten", delta.recordsWritten());
        applyIfNonZero(update, "metrics.recordsFailed", delta.recordsFailed());
        applyIfNonZero(update, "metrics.recordsFiltered", delta.recordsFiltered());
        applyIfNonZero(update, "metrics.bytesRead", delta.bytesRead());
        // Incremented, not only set at planning time: a lazily chunked run does not know its total
        // in advance and grows it by one as each chunk is generated. Callers that are not extending
        // the plan pass zero, which the non-zero guard drops.
        applyIfNonZero(update, "metrics.splitsTotal", delta.splitsTotal());
        applyIfNonZero(update, "metrics.splitsCompleted", delta.splitsCompleted());
        applyIfNonZero(update, "metrics.splitsFailed", delta.splitsFailed());

        mongo.updateFirst(
                scoped(tenantId).addCriteria(Criteria.where("_id").is(id.value())),
                update, RunDocument.class);
    }

    @Override
    public Page<Run> search(TenantId tenantId, RunSearch criteria, PageQuery pageQuery) {
        Query query = scoped(tenantId);

        if (criteria.pipelineId() != null) {
            query.addCriteria(Criteria.where("pipelineId").is(criteria.pipelineId().value()));
        }
        if (!criteria.states().isEmpty()) {
            query.addCriteria(Criteria.where("state")
                    .in(criteria.states().stream().map(Enum::name).toList()));
        }
        if (criteria.triggeredBy() != null) {
            query.addCriteria(Criteria.where("triggeredBy").is(criteria.triggeredBy()));
        }
        if (criteria.startedAfter() != null || criteria.startedBefore() != null) {
            Criteria started = Criteria.where("startedAt");
            if (criteria.startedAfter() != null) {
                started = started.gte(criteria.startedAfter());
            }
            if (criteria.startedBefore() != null) {
                started = started.lte(criteria.startedBefore());
            }
            query.addCriteria(started);
        }

        // Counted before paging is applied, because Query is mutable and skip/limit would
        // otherwise cap the count at one page.
        long total = mongo.count(query, RunDocument.class);

        // Null-checked before the lookup: Map.of() is an immutable map, and its getOrDefault
        // throws on a null key rather than returning the default. An unsorted request — which is
        // what the console sends by default — would otherwise fail with a NullPointerException.
        String requestedSort = pageQuery.sortBy();
        String sortField = requestedSort == null
                ? DEFAULT_SORT
                : SORTABLE.getOrDefault(requestedSort, DEFAULT_SORT);
        query.with(Sort.by(pageQuery.ascending() ? Sort.Direction.ASC : Sort.Direction.DESC, sortField))
                .skip(pageQuery.offset())
                .limit(pageQuery.size());

        List<Run> content = mongo.find(query, RunDocument.class).stream()
                .map(RunMapper::toDomain)
                .toList();
        return new Page<>(content, pageQuery.page(), pageQuery.size(), total);
    }

    @Override
    public List<Run> findActive(TenantId tenantId) {
        List<String> activeStates = new ArrayList<>();
        for (RunState state : RunState.values()) {
            if (state.isActive()) {
                activeStates.add(state.name());
            }
        }
        Query query = scoped(tenantId)
                .addCriteria(Criteria.where("state").in(activeStates))
                .with(Sort.by(Sort.Direction.DESC, "startedAt"));

        return mongo.find(query, RunDocument.class).stream().map(RunMapper::toDomain).toList();
    }

    @Override
    public List<Run> findByStates(java.util.Set<RunState> states, int limit) {
        if (states == null || states.isEmpty()) {
            return List.of();
        }
        Query query = new Query()
                .addCriteria(Criteria.where("state")
                        .in(states.stream().map(Enum::name).toList()))
                .with(Sort.by(Sort.Direction.ASC, "createdAt"))
                .limit(limit);

        return mongo.find(query, RunDocument.class).stream().map(RunMapper::toDomain).toList();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Not scoped by tenant. The reaper runs as a platform process across every tenant, and a
     * leaked Salesforce bulk job consumes an org-wide quota regardless of which tenant caused it.
     */
    @Override
    public List<Run> findWithUnreleasedResources(Instant olderThan, int limit) {
        List<String> terminalStates = new ArrayList<>();
        for (RunState state : RunState.values()) {
            if (state.isTerminal() && state.mayHoldExternalResources()) {
                terminalStates.add(state.name());
            }
        }
        Query query = new Query()
                .addCriteria(Criteria.where("state").in(terminalStates))
                .addCriteria(Criteria.where("updatedAt").lt(olderThan))
                .addCriteria(Criteria.where("preparationState").ne(Map.of()))
                .limit(limit);

        return mongo.find(query, RunDocument.class).stream().map(RunMapper::toDomain).toList();
    }

    @Override
    public List<Run> findPreparingDueForCheck(Instant dueBefore, int limit) {
        Query query = new Query()
                .addCriteria(Criteria.where("state").is(RunState.PREPARING.name()))
                .addCriteria(Criteria.where("updatedAt").lt(dueBefore))
                .with(Sort.by(Sort.Direction.ASC, "updatedAt"))
                .limit(limit);

        return mongo.find(query, RunDocument.class).stream().map(RunMapper::toDomain).toList();
    }

    private static Query scoped(TenantId tenantId) {
        return new Query(Criteria.where("tenantId").is(tenantId.value()));
    }

    /** Skips no-op increments so the update document stays small on the hot path. */
    private static void applyIfNonZero(Update update, String field, long delta) {
        if (delta != 0) {
            update.inc(field, delta);
        }
    }
}
