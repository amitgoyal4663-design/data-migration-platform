package com.dmp.application.port.out;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Questions about a connector that only its own code can answer.
 *
 * <p>Narrow on purpose. This layer must not see {@code ConnectorSpec} or the connector SPI at all —
 * the module boundary is what stops connector concepts leaking into use cases — so the port asks
 * questions and receives answers rather than handing over the connector's metadata to be inspected
 * here. Every method is a decision the application needs, phrased in the application's own terms.
 */
public interface ConnectorCapabilityPort {

    /**
     * Whether a source configured this way can put an identity on the records it reads.
     *
     * <p>Answered from the field the connector marks as its record key, not from a list of field
     * names kept here — a platform that guessed from names would be wrong the first time a
     * connector chose a different one.
     *
     * <p>True when the connector declares no such field at all. A queue whose messages carry their
     * own key has nothing to configure, and warning about it would be noise.
     */
    boolean identifiesRecords(String connectorType, JsonNode config);

    /**
     * Whether calling this sink once per record means anything, or produces the same result as
     * calling it once per batch.
     *
     * <p>Answered without opening a session, because publish-time validation must not connect to
     * anybody's system — a pipeline has to be publishable while its destination is down.
     *
     * <p>True for an unknown connector. Its absence is already reported as a validation error, and
     * a second complaint about a connector nobody can load helps no one.
     */
    boolean supportsPerRecordDelivery(String connectorType);
}
