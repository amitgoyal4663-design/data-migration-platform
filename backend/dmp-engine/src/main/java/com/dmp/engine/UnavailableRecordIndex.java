package com.dmp.engine;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.RecordIndexPort;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.run.RunId;
import com.dmp.domain.tenant.TenantId;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

/**
 * Stands in for the record index when no search backend is configured.
 *
 * <p>It refuses rather than silently doing nothing, and that is the whole point of its existing. A
 * pipeline set to index its records, running against a deployment with nowhere to put them, must
 * not complete and report success — because the next thing that happens is somebody searching for a
 * record, finding nothing, and concluding it was never migrated. An empty index is
 * indistinguishable from a failed migration, and the difference matters enormously.
 *
 * <p>The failure surfaces on the chunk's first batch, which fails the chunk, which fails the run
 * with the message below. Loud and early beats quiet and wrong.
 *
 * <p>Reads answer empty rather than throwing: a console asking "any records for this key" on a
 * deployment without an index should get "none here", not an error page.
 */
@Configuration
public class UnavailableRecordIndex {

    @Bean
    @ConditionalOnMissingBean(RecordIndexPort.class)
    public RecordIndexPort recordIndexUnavailable() {
        return new RecordIndexPort() {

            @Override
            public void indexAll(List<RecordIndexEntry> entries) {
                if (entries == null || entries.isEmpty()) {
                    return;
                }
                throw new DmpException(ErrorCode.VALIDATION_FAILED,
                        "This pipeline's audit level is INDEXED, which records what happened to "
                                + "every record so it can be found later — but no search backend is "
                                + "configured in this deployment, so there is nowhere to put them. "
                                // The exact property names, checked against the code rather than
                                // remembered. This said dmp.recordindex.opensearch.enabled, which
                                // does not exist: the switch is dmp.recordindex.enabled and the
                                // cluster is dmp.recordlog.opensearch.url, two different prefixes.
                                // Following the old advice to the letter produced this same error
                                // again, with nothing to suggest the instruction was the problem.
                                + "Either set dmp.recordindex.enabled=true (the cluster defaults "
                                + "to http://localhost:9200 and is set with "
                                + "dmp.recordlog.opensearch.url), or lower the "
                                + "pipeline's audit level. The run is stopped "
                                + "rather than completed, because an empty index and a failed "
                                + "migration look identical to whoever searches it later.",
                        Map.of("entries", entries.size()));
            }

            @Override
            public Page<RecordIndexEntry> findByKey(TenantId tenantId,
                                                    com.dmp.domain.pipeline.PipelineId pipelineId,
                                                    String recordKey, PageQuery pageQuery) {
                return Page.of(List.of(), pageQuery, 0);
            }

            @Override
            public Page<RecordIndexEntry> findByRun(TenantId tenantId, RunId runId, Outcome outcome,
                                                    PageQuery pageQuery) {
                return Page.of(List.of(), pageQuery, 0);
            }

            @Override
            public Page<RecordIndexEntry> search(TenantId tenantId, Query query, PageQuery pageQuery) {
                return Page.of(List.of(), pageQuery, 0);
            }

            @Override
            public long countByRun(TenantId tenantId, RunId runId) {
                return 0;
            }

            @Override
            public boolean supportsContentSearch() {
                return false;
            }
        };
    }
}
