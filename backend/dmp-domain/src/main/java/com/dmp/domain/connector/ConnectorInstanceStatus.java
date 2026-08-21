package com.dmp.domain.connector;

/** Operational state of a configured connector instance. */
public enum ConnectorInstanceStatus {

    /** Configured but never successfully connected to. */
    UNTESTED,

    /** A connectivity check succeeded. */
    ACTIVE,

    /** A connectivity check failed. Pipelines referencing it will fail validation at run start. */
    FAILED,

    /** Deliberately taken out of service without being deleted. */
    DISABLED;

    public boolean isUsable() {
        return this == ACTIVE || this == UNTESTED;
    }
}
