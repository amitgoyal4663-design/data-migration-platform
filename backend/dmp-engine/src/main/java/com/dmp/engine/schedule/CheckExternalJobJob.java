package com.dmp.engine.schedule;

import com.dmp.domain.run.SplitId;
import com.dmp.domain.tenant.TenantId;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The Quartz entry point for one status check on a parked chunk.
 *
 * <p>Three lines of logic, for the same reason {@link StartRunJob} is: Quartz constructs this
 * reflectively through a no-arg constructor and then autowires it, so everything worth testing
 * lives in {@link ExternalJobPoller}, which is an ordinary bean.
 *
 * <p>{@code @DisallowConcurrentExecution} because two firings for the same chunk would both ask the
 * destination the same question and both try to act on the answer. The transitions are conditional
 * and would sort that out, but paying an org's API quota twice to reach the same conclusion is not
 * a thing to leave to a race.
 */
@DisallowConcurrentExecution
public class CheckExternalJobJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(CheckExternalJobJob.class);

    /**
     * Field injection, as in {@link StartRunJob}, because Quartz owns this object's construction.
     * Its one collaborator is the bean holding the actual behaviour.
     */
    @Autowired
    private ExternalJobPoller poller;

    @Override
    public void execute(JobExecutionContext context) {
        String tenantId = context.getMergedJobDataMap().getString(ExternalJobScheduler.TENANT_ID);
        String splitId = context.getMergedJobDataMap().getString(ExternalJobScheduler.SPLIT_ID);

        if (tenantId == null || splitId == null) {
            log.error("A chunk status check fired with no chunk attached; ignoring it");
            return;
        }

        // Nothing is allowed to escape. An exception here marks the trigger as errored and Quartz
        // stops firing it, which would strand the chunk — the poller re-arms its own next firing,
        // so losing this one silently is exactly what must not happen.
        try {
            poller.poll(TenantId.parse(tenantId), SplitId.parse(splitId));
        } catch (Exception e) {
            log.error("Status check for chunk {} failed; the reaper will retry it", splitId, e);
        }
    }
}
