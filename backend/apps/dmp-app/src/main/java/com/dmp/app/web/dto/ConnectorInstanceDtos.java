package com.dmp.app.web.dto;

import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.RateLimitPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Duration;
import java.time.Instant;

/** Web contract for connector instances. */
public final class ConnectorInstanceDtos {

    private ConnectorInstanceDtos() {
    }

    /**
     * What the far end has agreed to accept, in the words the client used.
     *
     * <p>Windows travel as ISO-8601 durations — {@code PT5M}, {@code PT1H}, {@code P1D} — rather
     * than as a number plus a unit, because a number plus a unit is two fields that can disagree
     * and this is one fact. Both units are optional and absent means unlimited; a client who gave
     * exactly one number is the common case, not the exception.
     */
    @Schema(name = "RateLimit",
            description = "Records and/or calls the destination allows per period. Omit either "
                    + "for no limit on that unit. The budget belongs to this connector instance, "
                    + "so every pipeline using it draws on the same one.")
    public record RateLimit(
            @Schema(example = "10000") Long records,
            @Schema(description = "ISO-8601 period", example = "PT5M") String recordsWindow,
            @Schema(example = "100") Long calls,
            @Schema(description = "ISO-8601 period", example = "PT1M") String callsWindow,

            @Schema(description = "BURST spends a whole window at once then waits, which is what a "
                    + "client whose counter resets on the clock expects. EVEN never exceeds the "
                    + "limit in any window, sliding or not, and costs throughput in proportion to "
                    + "how much of the window one call takes up. Defaults to BURST.",
                    example = "BURST")
            RateLimitPolicy.Pacing pacing) {

        public static RateLimit from(RateLimitPolicy policy) {
            if (policy == null || policy.isUnlimited()) {
                return null;
            }
            return new RateLimit(
                    policy.limitsRecords() ? policy.records() : null,
                    policy.limitsRecords() ? policy.recordsWindow().toString() : null,
                    policy.limitsCalls() ? policy.calls() : null,
                    policy.limitsCalls() ? policy.callsWindow().toString() : null,
                    policy.pacing());
        }

        public RateLimitPolicy toPolicy() {
            return new RateLimitPolicy(
                    records == null ? 0 : records, period(recordsWindow),
                    calls == null ? 0 : calls, period(callsWindow),
                    pacing == null ? RateLimitPolicy.Pacing.BURST : pacing);
        }

        /**
         * A malformed period is reported as a validation failure rather than as a server error,
         * because it is one — somebody typed "5m" where "PT5M" was wanted.
         */
        private static Duration period(String text) {
            if (text == null || text.isBlank()) {
                return null;
            }
            try {
                return Duration.parse(text.strip());
            } catch (java.time.format.DateTimeParseException e) {
                throw new com.dmp.common.error.DmpException(
                        com.dmp.common.error.ErrorCode.VALIDATION_FAILED,
                        "'" + text + "' is not a period. Write it as ISO-8601: PT30S, PT5M, PT1H, P1D.",
                        java.util.Map.of("value", text));
            }
        }

        /** Null when nothing was sent, so "no limit" and "leave it alone" are the same request. */
        public static RateLimitPolicy policyOf(RateLimit sent) {
            return sent == null ? RateLimitPolicy.NONE : sent.toPolicy();
        }
    }

    @Schema(name = "ConnectorInstanceResponse")
    public record Response(
            String id,
            String name,
            @Schema(description = "Plugin identifier", example = "jdbc-postgres")
            String connectorType,
            String direction,
            @Schema(description = "Connector-specific configuration, validated against the plugin's JSON Schema")
            JsonNode config,
            @Schema(description = "References to secrets. Never values — safe to display and log.")
            JsonNode secretRefs,
            String status,
            String description,
            Instant lastTestedAt,
            String lastTestError,
            Instant createdAt,
            Instant updatedAt,
            @Schema(description = "What the far end allows. Null when there is no agreed limit.")
            RateLimit rateLimit) {

        public static Response from(ConnectorInstance instance) {
            return new Response(
                    instance.id().toString(),
                    instance.name(),
                    instance.connectorType(),
                    instance.direction().name(),
                    instance.config(),
                    instance.secretRefs(),
                    instance.status().name(),
                    instance.description(),
                    instance.lastTestedAt(),
                    instance.lastTestError(),
                    instance.createdAt(),
                    instance.updatedAt(),
                    RateLimit.from(instance.rateLimit()));
        }
    }

    @Schema(name = "CreateConnectorInstanceRequest")
    public record CreateRequest(
            @NotBlank(message = "Name is required")
            @Size(max = 255)
            String name,

            @NotBlank(message = "Connector type is required")
            @Pattern(regexp = "^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$",
                    message = "Connector type must be a lowercase plugin identifier such as 'jdbc-postgres'")
            String connectorType,

            @NotNull(message = "Direction is required")
            ConnectorDirection direction,

            @Schema(description = "Free-form until the plugin runtime can validate it against the connector's schema")
            JsonNode config,

            @Schema(description = "Secret references only. Sending a secret value here would store it in plain text.")
            JsonNode secretRefs,

            String description,

            RateLimit rateLimit) {
    }

    @Schema(name = "UpdateConnectorInstanceRequest",
            description = "Any change resets the instance to UNTESTED — a previous successful test "
                    + "says nothing about a configuration that has since been edited")
    public record UpdateRequest(
            @NotBlank(message = "Name is required")
            @Size(max = 255)
            String name,

            JsonNode config,

            JsonNode secretRefs,

            String description,

            @Schema(description = "Which pipeline roles this instance may fill. Omit to leave it "
                    + "unchanged. The connector type itself is not editable — a different type has "
                    + "a different configuration shape, so that is a new connection rather than an "
                    + "edit to this one.")
            ConnectorDirection direction,

            @Schema(description = "What the far end allows. Send null to remove the limit.")
            RateLimit rateLimit) {
    }

    /**
     * A few records as the source produced them.
     *
     * <p>Payloads are unaltered. This is the shape somebody is here to see, and normalising it
     * would show them the platform's idea of their data rather than their data — which is exactly
     * the mistake a preview exists to prevent.
     */
    @Schema(name = "SourcePreviewResponse")
    public record PreviewResponse(
            @Schema(description = "The records, exactly as the source produced them")
            java.util.List<com.fasterxml.jackson.databind.JsonNode> records,
            @Schema(description = "Union of every field name seen, in the order first encountered. "
                    + "Computed across all the records because a source is entitled to omit a null "
                    + "field on one row and include it on the next, and a table drawn from the "
                    + "first record alone would silently lose the column.")
            java.util.List<String> fields,
            @Schema(description = "What was actually asked, in the source's own language, so a "
                    + "preview that returns nothing is diagnosable rather than disappointing")
            String query,
            long durationMillis,
            @Schema(description = "Whether the source had more to give. Without this, a preview "
                    + "that hit the limit and one that reached the end of the data look identical.")
            boolean more) {

        public static PreviewResponse from(com.dmp.engine.SourcePreview.Result result) {
            java.util.Set<String> fields = new java.util.LinkedHashSet<>();
            result.records().forEach(record -> record.fieldNames().forEachRemaining(fields::add));

            return new PreviewResponse(result.records(), java.util.List.copyOf(fields),
                    result.query(), result.durationMillis(), result.more());
        }
    }
}
