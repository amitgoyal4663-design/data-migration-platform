package com.dmp.domain.audit;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.common.id.Ids;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * One immutable record of a control-plane action (ADR-0011).
 *
 * <p>Written to PostgreSQL in the same transaction as the change it describes. An audit log that
 * can disagree with the data it records is worse than none, because it is trusted.
 *
 * <p>{@code before} and {@code after} hold the affected aggregate's JSON state, so "what exactly
 * did that edit change" is answerable directly rather than by reconstructing state from
 * neighbouring entries.
 *
 * <p>There are no mutators. The type is a value, and the database role the application uses is
 * granted INSERT and SELECT only — so rewriting history is not expressible through this
 * application at all, by construction rather than by convention.
 */
public record AuditEntry(
        UUID id,
        TenantId tenantId,
        Instant occurredAt,
        String actor,
        AuditAction action,
        String resourceType,
        String resourceId,
        String summary,
        JsonNode before,
        JsonNode after,
        String requestId,
        String sourceIp) {

    private static final int MAX_SUMMARY_LENGTH = 1_000;

    public AuditEntry {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(action, "action");

        if (actor == null || actor.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Every audit entry must name an actor. Use a system identity such as "
                            + "'system:scheduler' for platform-initiated actions.");
        }
        if (resourceType == null || resourceType.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "Audit entry requires a resource type");
        }
        if (summary != null && summary.length() > MAX_SUMMARY_LENGTH) {
            summary = summary.substring(0, MAX_SUMMARY_LENGTH);
        }
    }

    public static AuditEntry of(TenantId tenantId, String actor, AuditAction action,
                                String resourceType, String resourceId, String summary,
                                JsonNode before, JsonNode after, Instant now) {
        return new AuditEntry(Ids.newId(), tenantId, now, actor, action, resourceType, resourceId,
                summary, before, after, null, null);
    }

    /** Attaches request correlation, populated by the web layer. */
    public AuditEntry withRequestContext(String newRequestId, String newSourceIp) {
        return new AuditEntry(id, tenantId, occurredAt, actor, action, resourceType, resourceId,
                summary, before, after, newRequestId, newSourceIp);
    }

    public Optional<JsonNode> beforeState() {
        return Optional.ofNullable(before);
    }

    public Optional<JsonNode> afterState() {
        return Optional.ofNullable(after);
    }
}
