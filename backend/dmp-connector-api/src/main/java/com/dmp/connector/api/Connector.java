package com.dmp.connector.api;

/**
 * The root of the connector SPI.
 *
 * <p>Implementations are discovered with {@link java.util.ServiceLoader}, so a connector jar
 * declares itself in {@code META-INF/services/com.dmp.connector.api.Connector} and needs no
 * registration anywhere in the platform. Adding a connector must never require editing existing
 * code — including the frontend, which is why {@link ConnectorSpec} carries a JSON Schema the
 * console renders a form from.
 */
public interface Connector {

    /** Identity, direction and configuration schema. Must be cheap and side-effect free. */
    ConnectorSpec spec();

    /**
     * Verifies that the configuration can actually reach the remote system.
     *
     * <p>Called from the console's "Test connection" button. Should attempt a real but minimal
     * operation — open a connection, list one table, request one page — rather than only checking
     * that fields are present. A test that validates syntax and reports success is worse than no
     * test, because it produces confidence that a run will then contradict.
     *
     * @throws ConnectorException with a {@code Kind} that distinguishes bad credentials from an
     *         unreachable host, since the user's next action differs entirely
     */
    default void testConnection(ConnectorContext context) {
        throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                "Connector '" + spec().type() + "' does not support connection testing");
    }
}
