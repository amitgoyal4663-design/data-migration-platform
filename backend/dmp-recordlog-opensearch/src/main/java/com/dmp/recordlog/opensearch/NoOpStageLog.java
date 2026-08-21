package com.dmp.recordlog.opensearch;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.StageLogPort;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * What runs when no search cluster is configured.
 *
 * <p>Exists so the engine can call {@code stageLog.log(...)} unconditionally rather than
 * null-checking at every call site. {@link #isEnabled()} returns false, which lets a caller skip
 * building the entry at all — the useful saving, since timing a stage and redacting its body costs
 * more than throwing the result away.
 */
@Component
@ConditionalOnMissingBean(OpenSearchStageLog.class)
public class NoOpStageLog implements StageLogPort {

    @Override
    public void log(List<StageEntry> entries) {
        // Nowhere to send them.
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public Page<StageEntry> find(TenantId tenantId, RunId runId, SplitId splitId, Stage stage,
                                 PageQuery pageQuery) {
        return Page.of(List.of(), pageQuery, 0);
    }
}
