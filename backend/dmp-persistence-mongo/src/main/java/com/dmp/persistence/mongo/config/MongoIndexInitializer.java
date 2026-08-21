package com.dmp.persistence.mongo.config;

import com.dmp.persistence.mongo.document.CheckpointDocument;
import com.dmp.persistence.mongo.document.RunDocument;
import com.dmp.persistence.mongo.document.SplitDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.index.PartialIndexFilter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Component;

/**
 * Creates MongoDB indexes explicitly at startup.
 *
 * <p>Spring Boot disables automatic index creation by default, and that default is right. Indexes
 * inferred from annotations are invisible in review, and an index created implicitly on a
 * hundred-million-document collection during a rolling deploy is an outage. Declaring them here
 * makes each one a reviewable decision with a stated reason.
 *
 * <p>Index creation is idempotent, so this is safe to run on every boot of every replica.
 */
@Component
public class MongoIndexInitializer {

    private static final Logger log = LoggerFactory.getLogger(MongoIndexInitializer.class);

    private final MongoTemplate mongo;

    public MongoIndexInitializer(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void createIndexes() {
        createRunIndexes();
        createSplitIndexes();
        createCheckpointIndexes();
        createRecordErrorIndexes();
        log.info("MongoDB indexes verified");
    }

    private void createRunIndexes() {
        IndexOperations runs = mongo.indexOps(RunDocument.class);

        // The run list: one tenant, newest first.
        runs.createIndex(new Index()
                .on("tenantId", Sort.Direction.ASC)
                .on("createdAt", Sort.Direction.DESC)
                .named("idx_run_tenant_created"));

        // Run history for a single pipeline — the pipeline detail page.
        runs.createIndex(new Index()
                .on("tenantId", Sort.Direction.ASC)
                .on("pipelineId", Sort.Direction.ASC)
                .on("createdAt", Sort.Direction.DESC)
                .named("idx_run_tenant_pipeline_created"));

        // Active-run queries and concurrency limits.
        runs.createIndex(new Index()
                .on("tenantId", Sort.Direction.ASC)
                .on("state", Sort.Direction.ASC)
                .named("idx_run_tenant_state"));

        // Idempotency. Partial, so the many runs without a key do not all collide on null —
        // a plain unique index would permit exactly one keyless run per tenant, ever.
        runs.createIndex(new Index()
                .on("tenantId", Sort.Direction.ASC)
                .on("idempotencyKey", Sort.Direction.ASC)
                .unique()
                .partial(PartialIndexFilter.of(
                        new Document("idempotencyKey", new Document("$type", "string"))))
                .named("uq_run_idempotency"));

        // The PREPARING poll sweep and the external-resource reaper (ADR-0012). Not tenant-scoped:
        // both run platform-wide, because a leaked external job consumes an org quota irrespective
        // of which tenant created it.
        runs.createIndex(new Index()
                .on("state", Sort.Direction.ASC)
                .on("updatedAt", Sort.Direction.ASC)
                .named("idx_run_state_updated"));
    }

    private void createSplitIndexes() {
        IndexOperations splits = mongo.indexOps(SplitDocument.class);

        // Serves both the split list for a run and the claim query, which sorts by index within
        // a run and state.
        splits.createIndex(new Index()
                .on("tenantId", Sort.Direction.ASC)
                .on("runId", Sort.Direction.ASC)
                .on("state", Sort.Direction.ASC)
                .on("index", Sort.Direction.ASC)
                .named("idx_split_tenant_run_state_index"));

        // No two splits may share an index within a run — the guard against a double-planned run
        // silently processing a range twice.
        splits.createIndex(new Index()
                .on("runId", Sort.Direction.ASC)
                .on("index", Sort.Direction.ASC)
                .unique()
                .named("uq_split_run_index"));

        // The lease-reclaim sweep. Platform-wide rather than tenant-scoped: a worker that dies
        // holding a chunk stalls its run regardless of whose tenant it belongs to, and on a
        // sequential run it holds the single concurrency slot until reclaimed.
        splits.createIndex(new Index()
                .on("state", Sort.Direction.ASC)
                .on("leaseExpiresAt", Sort.Direction.ASC)
                .named("idx_split_state_lease"));

        // The fallback sweep for chunks parked on an external job whose status check never fired.
        // Platform-wide for the same reason as the lease sweep, and it should normally match
        // nothing at all — Quartz's clustered store is meant to make sure of that. It is indexed
        // because a query that runs every thirty seconds and usually returns nothing is exactly
        // the kind that quietly becomes a collection scan as the split collection grows.
        splits.createIndex(new Index()
                .on("state", Sort.Direction.ASC)
                .on("dueAt", Sort.Direction.ASC)
                .named("idx_split_state_due"));
    }

    private void createRecordErrorIndexes() {
        IndexOperations errors = mongo.indexOps(
                com.dmp.persistence.mongo.document.RecordErrorDocument.class);

        // The drill-down from a failed run to the records that caused it.
        errors.createIndex(new Index()
                .on("tenantId", Sort.Direction.ASC)
                .on("runId", Sort.Direction.ASC)
                .on("seq", Sort.Direction.ASC)
                .named("idx_record_error_run"));

        // TTL driven by the pipeline's own audit retention rather than a global setting, so a
        // payments pipeline can keep failures for a year while an analytics one keeps a week.
        // expireAfterSeconds is zero because the stored date is the expiry moment itself.
        errors.createIndex(new Index()
                .on("expiresAt", Sort.Direction.ASC)
                .expire(0)
                .named("ttl_record_error"));

        createRecordErrorSignatureIndexes();
    }

    private void createRecordErrorSignatureIndexes() {
        var signatures = mongo.indexOps(
                com.dmp.persistence.mongo.document.RecordErrorSignatureDocument.class);

        // The grouped view: distinct faults in a run, costliest first. Sorted in the index so the
        // console reads the top faults of a run that produced millions without a collection scan.
        signatures.createIndex(new Index()
                .on("tenantId", Sort.Direction.ASC)
                .on("runId", Sort.Direction.ASC)
                .on("count", Sort.Direction.DESC)
                .named("idx_record_error_signature_run"));

        // Same retention as the payloads it summarises, so a run's failures expire as one thing
        // rather than leaving counts pointing at samples that are already gone.
        signatures.createIndex(new Index()
                .on("expiresAt", Sort.Direction.ASC)
                .expire(0)
                .named("ttl_record_error_signature"));
    }

    private void createCheckpointIndexes() {
        // Keyed by split id, so _id already serves point lookups. This covers the run-wide sweep
        // used when resuming or when deleting a run's execution data.
        mongo.indexOps(CheckpointDocument.class).createIndex(new Index()
                .on("tenantId", Sort.Direction.ASC)
                .on("runId", Sort.Direction.ASC)
                .named("idx_checkpoint_tenant_run"));
    }
}
