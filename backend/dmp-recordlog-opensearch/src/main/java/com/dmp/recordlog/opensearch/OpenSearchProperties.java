package com.dmp.recordlog.opensearch;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.Optional;

/**
 * Configuration for the searchable record log.
 *
 * <p>Off unless {@code dmp.recordlog.opensearch.enabled=true}. A search cluster is a real
 * operational commitment, so the platform does not assume one exists.
 *
 * @param url            base URL of the cluster
 * @param indexPrefix    daily indices are named {@code <prefix>-yyyy.MM.dd}
 * @param username       optional basic-auth user
 * @param password       optional basic-auth password
 * @param bulkSize       events per bulk request
 * @param queueCapacity  how many events may await indexing before further ones are dropped
 * @param flushInterval  longest an event waits before being sent, when volume is low
 * @param shards         primary shards per daily index
 * @param replicas       replicas per daily index
 * @param stageIndexName  the stage log's index. One index rather than one per day: a call log holds
 *                       one entry per batch, so at a thousand records to a batch it is roughly a
 *                       thousandth the size of the record index, and daily indices for that volume
 *                       would be more shards than documents
 */
@ConfigurationProperties(prefix = "dmp.recordlog.opensearch")
public record OpenSearchProperties(
        @DefaultValue("http://localhost:9200") String url,
        @DefaultValue("dmp-records") String indexPrefix,
        String username,
        String password,
        @DefaultValue("500") int bulkSize,

        /*
         * Bounded, and deliberately so. An unbounded queue in front of a struggling search cluster
         * does not prevent a problem — it converts a logging outage into an out-of-memory kill of
         * the worker, taking the migration with it. Dropping log events is the correct failure.
         */
        @DefaultValue("50000") int queueCapacity,

        @DefaultValue("2s") Duration flushInterval,
        @DefaultValue("1") int shards,
        @DefaultValue("1") int replicas,
        @DefaultValue("dmp-stage-log") String stageIndexName) {

    /** Basic-auth credentials, when both parts are configured. */
    public Optional<String> credentials() {
        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(username + ":" + password);
    }
}
