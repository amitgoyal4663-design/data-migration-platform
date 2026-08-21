package com.dmp.engine;

import com.dmp.application.port.out.ConnectorCapabilityPort;
import com.dmp.connector.api.ConfigFields;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Optional;

/**
 * Answers the application's questions about connectors by reading their declared schemas.
 *
 * <p>Lives here rather than in the application layer because it needs the connector SPI, and the
 * application module deliberately cannot see it. Keeping the boundary means a connector concept
 * cannot drift into a use case by accident.
 */
@Component
public class ConnectorCapabilities implements ConnectorCapabilityPort {

    private final ConnectorRegistry registry;

    public ConnectorCapabilities(ConnectorRegistry registry) {
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Read from the connector object rather than a session, so nothing is connected to. Asked
     * while a pipeline is being published, which must succeed whether or not the destination
     * happens to be reachable.
     *
     * <p>An unloadable or non-sink connector answers true: both are already reported as validation
     * errors of their own, and a second complaint about the same broken reference is noise.
     */
    @Override
    public boolean supportsPerRecordDelivery(String connectorType) {
        try {
            return registry.sink(connectorType).supportsPerRecordDelivery();
        } catch (RuntimeException e) {
            return true;
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>A connector marks one config field with {@link ConfigFields#RECORD_KEY}. The source can
     * identify records when that field has a value, or when the connector declares a default it
     * falls back to — MongoDB's {@code _id} being the obvious case, which is why the overwhelmingly
     * common pipeline needs no configuration at all to be searchable.
     *
     * <p>An uninstalled connector answers true. Its absence is already reported as a validation
     * error, and adding a second complaint about a connector nobody can load helps no one.
     */
    @Override
    public boolean identifiesRecords(String connectorType, JsonNode config) {
        Optional<ConnectorSpec> spec = registry.spec(connectorType);
        if (spec.isEmpty()) {
            return true;
        }

        JsonNode properties = spec.get().configSchema().path("properties");
        boolean declaresOne = false;

        Iterator<String> names = properties.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            JsonNode field = properties.path(name);

            if (!field.path(ConfigFields.RECORD_KEY).asBoolean(false)) {
                continue;
            }
            declaresOne = true;

            boolean configured = config != null && config.hasNonNull(name)
                    && !config.get(name).asText("").isBlank();

            if (configured || field.hasNonNull("default")) {
                return true;
            }
        }

        // No such field means the connector takes its key from the record itself — a queue message,
        // an API response — and has nothing for a user to get wrong.
        return !declaresOne;
    }
}
