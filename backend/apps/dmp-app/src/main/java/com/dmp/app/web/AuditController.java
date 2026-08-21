package com.dmp.app.web;

import com.dmp.app.web.dto.AuditDtos;
import com.dmp.app.web.dto.PageResponse;
import com.dmp.application.common.PageQuery;
import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.AuditLogPort;
import com.dmp.domain.audit.AuditAction;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads the control-plane audit trail (ADR-0011).
 *
 * <p>Read only, and there is no write endpoint anywhere: entries are recorded by the services that
 * make the changes, inside the same transaction, so an entry cannot exist without its change or a
 * change without its entry. The table refuses UPDATE and DELETE at the database, so this is the
 * whole of the audit trail's API surface.
 *
 * <p>Its existence is the point. The trail has been written since the platform's first commit and
 * had no reader, which made it evidence nobody could examine — worth no more than not collecting it,
 * and worse, because it looked as though the question was covered.
 */
@RestController
@RequestMapping("/api/v1/audit")
@Tag(name = "Audit", description = "Who changed what, and when")
public class AuditController {

    private final AuditLogPort auditLog;
    private final TenantContext tenantContext;

    public AuditController(AuditLogPort auditLog, TenantContext tenantContext) {
        this.auditLog = auditLog;
        this.tenantContext = tenantContext;
    }

    @GetMapping
    @Operation(summary = "Search the audit trail",
            description = """
                    Every filter is optional and they combine. With none, this is the whole trail,
                    newest first.

                    Results are always ordered by time and cannot be sorted otherwise — an audit
                    trail in any other order is not an audit trail.

                    Pass resourceType and resourceId together for one resource's history: that is
                    the "what happened to this pipeline" view.
                    """)
    public PageResponse<AuditDtos.Response> search(
            @RequestParam(required = false) String resourceType,
            @RequestParam(required = false) String resourceId,
            @RequestParam(required = false) String actor,
            @RequestParam(required = false) List<String> actions,
            @RequestParam(required = false) Instant occurredAfter,
            @RequestParam(required = false) Instant occurredBefore,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {

        AuditLogPort.AuditSearch criteria = new AuditLogPort.AuditSearch(
                blankToNull(resourceType),
                blankToNull(resourceId),
                blankToNull(actor),
                parseActions(actions),
                occurredAfter,
                occurredBefore);

        return PageResponse.from(
                auditLog.search(tenantContext.currentTenant(), criteria, new PageQuery(page, size, null, false)),
                AuditDtos.Response::from);
    }

    /**
     * An unrecognised action name is ignored rather than rejected.
     *
     * <p>A console built against a newer build may send an action this one has never heard of, and
     * failing the whole search over one unknown filter value would hide the entries the user can
     * legitimately see. Every name being unknown falls back to no action filter, which is the same
     * as not having asked.
     */
    private static Set<AuditAction> parseActions(List<String> names) {
        if (names == null || names.isEmpty()) {
            return Set.of();
        }
        return names.stream()
                .map(AuditController::parseAction)
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static AuditAction parseAction(String name) {
        try {
            return AuditAction.valueOf(name.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** An empty query parameter is a filter the user cleared, not a search for the empty string. */
    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
