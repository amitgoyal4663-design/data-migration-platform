package com.dmp.application.port.out;

import com.dmp.domain.run.RunId;
import com.dmp.domain.tenant.TenantId;

import java.time.Instant;
import java.util.Map;

/**
 * Announces what the platform is doing, so other systems can react.
 *
 * <p><b>Events, never data.</b> One message when a run starts, one per chunk completing, one when
 * it finishes — a few hundred messages for a migration of fifty million records. Records themselves
 * never pass through here: a pod reads and writes its own chunk, and routing the data through a
 * broker would double the network cost and make a shared cluster the ceiling on every migration.
 *
 * <p>Subscribers use this to trigger downstream work when a load completes, to alert on failures,
 * or to feed a dashboard the platform does not own.
 *
 * <p>Like the record log, publishing must never block a run and never fail one. An event bus being
 * unavailable is not a reason for a migration to stop.
 */
public interface RunEventPublisher {

    void publish(RunEvent event);

    /** Whether a bus is configured, so callers can skip building events that go nowhere. */
    boolean isEnabled();

    /**
     * One thing that happened.
     *
     * @param runId keys the message, so a single run's events stay in order on one partition
     * @param details type-specific fields — counts, worker id, error text
     */
    record RunEvent(
            Type type,
            TenantId tenantId,
            RunId runId,
            String pipelineId,
            String pipelineName,
            int versionNumber,
            Instant occurredAt,
            Map<String, Object> details) {

        public RunEvent {
            details = Map.copyOf(details == null ? Map.of() : details);
        }
    }

    /**
     * The lifecycle moments worth announcing.
     *
     * <p>Deliberately coarse. An event per record would reintroduce exactly the volume this design
     * avoids; these are the points at which another system might reasonably want to act.
     */
    enum Type {
        RUN_CREATED,
        RUN_STARTED,
        /** Emitted once per chunk, carrying its counts — the finest granularity published. */
        CHUNK_COMPLETED,
        CHUNK_FAILED,
        RUN_PAUSED,
        RUN_RESUMED,
        /**
         * The moment a stop was asked for, not the moment it took effect.
         *
         * <p>Separate from {@link #RUN_STOPPED} because chunks in flight drain to their next
         * checkpoint first. A subscriber that reacted to the request as though the run had ended
         * would act while it was still writing.
         */
        RUN_STOP_REQUESTED,
        RUN_COMPLETED,
        RUN_FAILED,
        RUN_STOPPED
    }
}
