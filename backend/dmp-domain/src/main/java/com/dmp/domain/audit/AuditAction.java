package com.dmp.domain.audit;

/**
 * What happened, in the control-plane audit trail (ADR-0011).
 *
 * <p>A closed enumeration rather than free text so the trail is queryable and so a new action
 * cannot be introduced without someone deciding what it is called.
 */
public enum AuditAction {

    CREATE,
    UPDATE,
    DELETE,
    ARCHIVE,
    RESTORE,

    /** A pipeline version was frozen and made runnable. */
    PUBLISH,

    /** An earlier version was republished — the rollback path. */
    ROLLBACK,

    /** A connector instance's connectivity was tested. */
    TEST_CONNECTION,

    ENABLE,
    DISABLE,

    RUN_START,
    RUN_PAUSE,
    RUN_RESUME,
    RUN_STOP,

    /**
     * A schedule was created, altered or removed.
     *
     * <p>One action for all of them, with the detail in the summary. The distinction that matters
     * during an incident is "did anyone touch the schedules", and separate constants for create,
     * update, enable, disable and delete would fragment the query that answers it without telling
     * anyone anything the summary does not.
     */
    SCHEDULE_CHANGE
}
