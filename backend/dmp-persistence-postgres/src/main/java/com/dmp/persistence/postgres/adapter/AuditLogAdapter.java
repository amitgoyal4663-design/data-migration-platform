package com.dmp.persistence.postgres.adapter;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.AuditLogPort;
import com.dmp.domain.audit.AuditAction;
import com.dmp.domain.audit.AuditEntry;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.mapper.AuditLogMapper;
import com.dmp.persistence.postgres.repository.AuditLogJpaRepository;
import com.dmp.persistence.postgres.support.PersistenceSupport;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

/**
 * PostgreSQL adapter for {@link AuditLogPort}.
 *
 * <p>Deliberately offers no update and no delete. The port does not declare them, this class does
 * not implement them, the entity is {@code @Immutable}, and a database trigger rejects both. Four
 * layers agreeing is not redundancy here — it is the difference between an audit trail and a table
 * that happens to contain history.
 */
@Repository
public class AuditLogAdapter implements AuditLogPort {

    private final AuditLogJpaRepository jpa;

    public AuditLogAdapter(AuditLogJpaRepository jpa) {
        this.jpa = jpa;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Runs in the caller's transaction, so a failure here rolls back the change being recorded.
     * That is the intended behaviour: ADR-0011 treats an unaudited change as unacceptable rather
     * than as a degraded mode worth continuing in.
     */
    @Override
    public void record(AuditEntry entry) {
        PersistenceSupport.translatingExceptions("Audit entry",
                () -> jpa.save(AuditLogMapper.toEntity(entry)));
    }

    @Override
    public Page<AuditEntry> search(TenantId tenantId, AuditSearch criteria, PageQuery pageQuery) {
        // The query orders by occurred_at internally, so only page and size are taken from the
        // request. An audit trail sorted by anything other than time is not an audit trail.
        boolean noActionFilter = criteria.actions().isEmpty();
        Set<AuditAction> actions = criteria.actions();
        List<String> actionNames = noActionFilter
                ? List.of(AuditAction.CREATE.name())   // unused when noActionFilter is true, but
                : actions.stream().map(AuditAction::name).toList();  // IN () is invalid SQL

        return PersistenceSupport.toPage(
                jpa.search(tenantId.value(),
                        criteria.resourceType(),
                        criteria.resourceId(),
                        criteria.actor(),
                        noActionFilter,
                        actionNames,
                        criteria.occurredAfter(),
                        criteria.occurredBefore(),
                        PageRequest.of(pageQuery.page(), pageQuery.size())),
                pageQuery, AuditLogMapper::toDomain);
    }
}
