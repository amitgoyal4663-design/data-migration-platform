package com.dmp.application.port.out;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.domain.pipeline.Pipeline;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineStatus;
import com.dmp.domain.tenant.TenantId;

import java.util.Optional;
import java.util.Set;

/**
 * Persistence port for pipelines. Implemented by the PostgreSQL adapter (ADR-0005).
 *
 * <p>Every method takes a {@link TenantId}. See {@link com.dmp.application.common.TenantContext}
 * for why this is a parameter rather than ambient state.
 */
public interface PipelineRepository {

    /**
     * Inserts or updates.
     *
     * @throws com.dmp.common.error.DmpException {@code CONCURRENT_MODIFICATION} if the stored
     *         {@code rowVersion} has moved on, {@code DUPLICATE} if the name collides within the tenant
     */
    Pipeline save(Pipeline pipeline);

    Optional<Pipeline> findById(TenantId tenantId, PipelineId id);

    Optional<Pipeline> findByName(TenantId tenantId, String name);

    Page<Pipeline> search(TenantId tenantId, PipelineSearch criteria, PageQuery pageQuery);

    boolean existsByName(TenantId tenantId, String name);

    /**
     * Removes a pipeline and cascades to its versions.
     *
     * <p>Callers must confirm no runs reference it. Because runs live in MongoDB (ADR-0005) the
     * database cannot enforce that, so it is an application-layer obligation — and the reason
     * archiving is the recommended path rather than deletion.
     */
    void delete(TenantId tenantId, PipelineId id);

    /**
     * Filter criteria. A null or empty field means "no constraint on this dimension".
     *
     * @param tags matched conjunctively — a pipeline must carry every tag listed
     */
    record PipelineSearch(String nameContains, String folder, Set<String> tags, PipelineStatus status) {

        public PipelineSearch {
            tags = Set.copyOf(tags == null ? Set.of() : tags);
            if (nameContains != null && nameContains.isBlank()) {
                nameContains = null;
            }
            if (folder != null && folder.isBlank()) {
                folder = null;
            }
        }

        public static PipelineSearch none() {
            return new PipelineSearch(null, null, Set.of(), null);
        }
    }
}
