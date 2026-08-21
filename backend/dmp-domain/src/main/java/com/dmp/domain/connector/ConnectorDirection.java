package com.dmp.domain.connector;

import com.dmp.domain.pipeline.NodeType;

/**
 * Which pipeline roles a connector instance may fill.
 *
 * <p>Most connectors are symmetric — a PostgreSQL connection is equally usable as a source or a
 * sink — which is why {@link #BOTH} is the common case rather than the exception. Some are not:
 * Salesforce Bulk API v2 query and ingest are different enough to be modelled separately.
 */
public enum ConnectorDirection {

    SOURCE,
    SINK,
    BOTH;

    public boolean supports(NodeType nodeType) {
        return switch (nodeType) {
            case SOURCE -> this == SOURCE || this == BOTH;
            case SINK -> this == SINK || this == BOTH;
            default -> false;
        };
    }
}
