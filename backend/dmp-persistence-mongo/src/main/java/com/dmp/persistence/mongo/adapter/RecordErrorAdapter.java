package com.dmp.persistence.mongo.adapter;

import com.dmp.application.port.out.RecordErrorPort;
import com.dmp.common.id.Ids;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.mongo.document.RecordErrorDocument;
import com.dmp.persistence.mongo.document.RecordErrorSignatureDocument;
import com.dmp.persistence.mongo.support.JsonDocuments;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/** MongoDB adapter for the dead-letter queue. */
@Repository
public class RecordErrorAdapter implements RecordErrorPort {

    private final MongoTemplate mongo;

    public RecordErrorAdapter(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Bulk-inserted. A batch of a thousand records can fail wholesale on a constraint the source
     * did not enforce, and a round trip per rejection would make the failure path slower than the
     * success path — exactly when the system is already under stress.
     */
    @Override
    public void recordAll(List<RecordErrorEntry> errors) {
        if (errors == null || errors.isEmpty()) {
            return;
        }
        mongo.insert(errors.stream().map(RecordErrorAdapter::toDocument).toList(),
                RecordErrorDocument.class);
    }

    @Override
    public List<RecordErrorEntry> findByRun(TenantId tenantId, RunId runId, int limit) {
        Query query = new Query()
                .addCriteria(Criteria.where("tenantId").is(tenantId.value()))
                .addCriteria(Criteria.where("runId").is(runId.value()))
                .with(Sort.by(Sort.Direction.ASC, "seq"))
                .limit(limit);

        return mongo.find(query, RecordErrorDocument.class).stream()
                .map(RecordErrorAdapter::toDomain)
                .toList();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Sorted on chunk then position then id. {@code seq} counts within a chunk and so repeats
     * across a run — paging on it alone would let two replay chunks fetch the same record and a
     * third fetch none. The id breaks any remaining tie, which matters because an unstable sort
     * under paging silently drops and duplicates rows rather than failing.
     */
    @Override
    public List<RecordErrorEntry> findForReplay(TenantId tenantId, RunId runId, int skip, int limit) {
        Query query = new Query()
                .addCriteria(Criteria.where("tenantId").is(tenantId.value()))
                .addCriteria(Criteria.where("runId").is(runId.value()))
                .with(Sort.by(Sort.Direction.ASC, "splitId", "seq", "_id"))
                .skip(skip)
                .limit(limit);

        return mongo.find(query, RecordErrorDocument.class).stream()
                .map(RecordErrorAdapter::toDomain)
                .toList();
    }

    @Override
    public long countByRun(TenantId tenantId, RunId runId) {
        return mongo.count(new Query()
                .addCriteria(Criteria.where("tenantId").is(tenantId.value()))
                .addCriteria(Criteria.where("runId").is(runId.value())), RecordErrorDocument.class);
    }

    /**
     * {@inheritDoc}
     *
     * <p>One round trip per distinct fault per batch, not per record: a batch of a thousand
     * rejections sharing one cause costs a single upsert. The count is incremented and the new
     * total returned in the same operation, so no read precedes the write and concurrent pods
     * cannot lose increments to each other.
     */
    @Override
    public int reserveSamples(SignatureKey key, long occurrences, int wanted, int cap,
                              Instant now, Instant expiresAt) {
        if (occurrences <= 0) {
            return 0;
        }

        String id = RecordErrorSignatureDocument.idFor(
                key.tenantId().value(), key.runId().value(), key.signature());

        Update update = new Update()
                .inc("count", occurrences)
                .setOnInsert("tenantId", key.tenantId().value())
                .setOnInsert("runId", key.runId().value())
                .setOnInsert("nodeId", key.nodeId())
                .setOnInsert("signature", key.signature())
                .setOnInsert("code", key.code())
                .setOnInsert("message", key.message())
                .setOnInsert("firstSeenAt", now)
                .setOnInsert("samplesStored", 0L)
                .set("lastSeenAt", now)
                // Refreshed on every occurrence so a fault still recurring does not expire out from
                // under a run that is still producing it.
                .set("expiresAt", expiresAt);

        RecordErrorSignatureDocument counted = mongo.findAndModify(
                new Query(Criteria.where("_id").is(id)),
                update,
                FindAndModifyOptions.options().upsert(true).returnNew(true),
                RecordErrorSignatureDocument.class);

        if (wanted <= 0) {
            return 0;
        }
        if (cap <= 0) {
            // No ceiling configured: every rejected record is kept whole, which is what a pipeline
            // written before sampling existed asked for and must keep getting.
            mongo.updateFirst(new Query(Criteria.where("_id").is(id)),
                    new Update().inc("samplesStored", wanted),
                    RecordErrorSignatureDocument.class);
            return wanted;
        }
        if (counted == null) {
            // Only reachable if the upsert raced another and neither returned a document. Storing
            // the payloads is the safe answer: too much evidence beats none.
            return wanted;
        }

        long alreadyStored = counted.getSamplesStored();
        int allowed = (int) Math.max(0, Math.min(wanted, cap - alreadyStored));
        if (allowed <= 0) {
            return 0;
        }

        // Claimed before the payloads are written, so a crash between the two loses samples rather
        // than uncapping the fault.
        mongo.updateFirst(new Query(Criteria.where("_id").is(id)),
                new Update().inc("samplesStored", allowed),
                RecordErrorSignatureDocument.class);
        return allowed;
    }

    @Override
    public List<SignatureSummary> summariseByRun(TenantId tenantId, RunId runId, int limit) {
        Query query = new Query()
                .addCriteria(Criteria.where("tenantId").is(tenantId.value()))
                .addCriteria(Criteria.where("runId").is(runId.value()))
                // Costliest fault first. The one that rejected nineteen thousand records is the one
                // to fix, and it is not necessarily the one that happened earliest.
                .with(Sort.by(Sort.Direction.DESC, "count"))
                .limit(limit);

        return mongo.find(query, RecordErrorSignatureDocument.class).stream()
                .map(doc -> new SignatureSummary(
                        doc.getSignature(), doc.getCode(), doc.getMessage(), doc.getNodeId(),
                        doc.getCount(), doc.getSamplesStored(),
                        doc.getFirstSeenAt(), doc.getLastSeenAt()))
                .toList();
    }

    private static RecordErrorDocument toDocument(RecordErrorEntry entry) {
        RecordErrorDocument doc = new RecordErrorDocument();
        doc.setId(Ids.newId());
        doc.setTenantId(entry.tenantId().value());
        doc.setRunId(entry.runId().value());
        doc.setSplitId(entry.splitId().value());
        doc.setNodeId(entry.nodeId());
        doc.setSeq(entry.seq());
        doc.setRecordKey(entry.key());
        doc.setCode(entry.code());
        doc.setMessage(entry.message());
        doc.setPayload(JsonDocuments.toMap(entry.payload()));
        doc.setOccurredAt(entry.occurredAt());
        doc.setExpiresAt(entry.expiresAt());
        return doc;
    }

    private static RecordErrorEntry toDomain(RecordErrorDocument doc) {
        return new RecordErrorEntry(
                TenantId.of(doc.getTenantId()),
                RunId.of(doc.getRunId()),
                SplitId.of(doc.getSplitId()),
                doc.getNodeId(),
                doc.getSeq(),
                doc.getRecordKey(),
                doc.getCode(),
                doc.getMessage(),
                JsonDocuments.toJson(doc.getPayload()),
                doc.getOccurredAt(),
                doc.getExpiresAt());
    }
}
