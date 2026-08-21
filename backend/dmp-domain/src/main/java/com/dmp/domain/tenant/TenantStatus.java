package com.dmp.domain.tenant;

public enum TenantStatus {

    /** Normal operation. */
    ACTIVE,

    /** Readable but no new runs may start. Used for billing holds and maintenance. */
    SUSPENDED
}
