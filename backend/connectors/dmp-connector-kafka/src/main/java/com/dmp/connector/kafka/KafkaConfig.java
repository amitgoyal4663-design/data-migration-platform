package com.dmp.connector.kafka;

import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * What a Kafka connection needs, read from the instance configuration.
 *
 * <p>Deliberately has no setting for creating a topic. Not an omission — see
 * {@link KafkaConnector} for why the capability is absent rather than defaulted off.
 */
record KafkaConfig(
        String bootstrapServers,
        String topic,
        StartFrom startFrom,
        String keyField,
        String securityProtocol,
        String saslMechanism,
        String username,
        String password) {

    /** Where a read begins when there is no checkpoint to resume from. */
    enum StartFrom {
        /** Everything the topic still retains. The right default for a migration. */
        EARLIEST,
        /** Only what arrives after the run starts. */
        LATEST
    }

    static KafkaConfig from(ConnectorContext context) {
        JsonNode config = context.config();

        String bootstrapServers = required(config, "bootstrapServers");
        String topic = required(config, "topic");

        return new KafkaConfig(
                bootstrapServers,
                topic,
                text(config, "startFrom", "EARLIEST").equalsIgnoreCase("LATEST")
                        ? StartFrom.LATEST : StartFrom.EARLIEST,
                text(config, "keyField", ""),
                text(config, "securityProtocol", "PLAINTEXT"),
                text(config, "saslMechanism", "PLAIN"),
                context.secret("username").orElse(null),
                context.secret("password").orElse(null));
    }

    /**
     * Client properties shared by the consumer and the producer.
     *
     * <p>Security is assembled here rather than at each call site so a cluster requiring SASL is
     * configured once and both directions inherit it.
     */
    Map<String, Object> commonProperties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("security.protocol", securityProtocol);

        if (securityProtocol.startsWith("SASL") && username != null) {
            properties.put("sasl.mechanism", saslMechanism);
            properties.put("sasl.jaas.config", jaasConfig());
        }
        return properties;
    }

    private String jaasConfig() {
        String loginModule = saslMechanism.startsWith("SCRAM")
                ? "org.apache.kafka.common.security.scram.ScramLoginModule"
                : "org.apache.kafka.common.security.plain.PlainLoginModule";

        // Escaped, because a password containing a quote would otherwise produce a JAAS string
        // that fails to parse with an error naming neither the password nor this line.
        return loginModule + " required username=\"" + escape(username)
                + "\" password=\"" + escape(password) + "\";";
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    Optional<String> keyFieldName() {
        return keyField.isBlank() ? Optional.empty() : Optional.of(keyField);
    }

    String describe() {
        return "topic '" + topic + "' on " + bootstrapServers;
    }

    private static String required(JsonNode config, String field) {
        String value = text(config, field, "");
        if (value.isBlank()) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "'" + field + "' is required for a Kafka connection");
        }
        return value;
    }

    private static String text(JsonNode config, String field, String fallback) {
        JsonNode node = config.get(field);
        return node == null || node.isNull() ? fallback : node.asText(fallback);
    }
}
