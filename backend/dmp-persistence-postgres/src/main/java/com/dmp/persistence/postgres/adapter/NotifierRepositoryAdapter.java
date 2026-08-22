package com.dmp.persistence.postgres.adapter;

import com.dmp.application.port.out.NotifierRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.notify.Notifier;
import com.dmp.domain.notify.NotifierId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.NotifierEntity;
import com.dmp.persistence.postgres.mapper.NotifierMapper;
import com.dmp.persistence.postgres.repository.NotifierJpaRepository;
import com.dmp.persistence.postgres.support.PersistenceSupport;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** PostgreSQL adapter for {@link NotifierRepository}. */
@Repository
public class NotifierRepositoryAdapter implements NotifierRepository {

    private final NotifierJpaRepository jpa;

    public NotifierRepositoryAdapter(NotifierJpaRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public Notifier create(Notifier notifier) {
        return PersistenceSupport.translatingExceptions("Notifier '" + notifier.name() + "'",
                () -> NotifierMapper.toDomain(jpa.save(NotifierMapper.toEntity(notifier))));
    }

    @Override
    public Notifier update(Notifier notifier) {
        String description = "Notifier '" + notifier.name() + "'";
        return PersistenceSupport.translatingExceptions(description, () -> {
            NotifierEntity entity = require(notifier.tenantId(), notifier.id());
            PersistenceSupport.requireCurrentVersion(
                    notifier.rowVersion(), entity.getRowVersion(), description);
            NotifierMapper.applyTo(entity, notifier);
            return NotifierMapper.toDomain(jpa.save(entity));
        });
    }

    @Override
    public Optional<Notifier> findById(TenantId tenantId, NotifierId id) {
        return jpa.findByIdAndTenantId(id.value(), tenantId.value()).map(NotifierMapper::toDomain);
    }

    @Override
    public List<Notifier> findAll(TenantId tenantId) {
        return jpa.findByTenantIdOrderByNameAsc(tenantId.value()).stream()
                .map(NotifierMapper::toDomain).toList();
    }

    @Override
    public List<Notifier> findEnabled(TenantId tenantId) {
        return jpa.findByTenantIdAndEnabledTrue(tenantId.value()).stream()
                .map(NotifierMapper::toDomain).toList();
    }

    @Override
    public void delete(TenantId tenantId, NotifierId id) {
        jpa.findByIdAndTenantId(id.value(), tenantId.value()).ifPresent(jpa::delete);
    }

    /**
     * Written without an optimistic check, and absent rather than failing.
     *
     * <p>This runs on a background sender after the run it describes has already ended. A notifier
     * deleted between the send and this write is not an error worth surfacing — the delivery either
     * happened or it did not, and there is no longer anywhere to record it.
     */
    @Override
    @Transactional
    public void recordAttempt(TenantId tenantId, NotifierId id, Instant at, boolean succeeded,
                              String error) {
        jpa.findByIdAndTenantId(id.value(), tenantId.value()).ifPresent(entity -> {
            entity.setLastAttemptAt(at);
            entity.setLastAttemptSucceeded(succeeded);
            entity.setLastAttemptError(error);
            jpa.save(entity);
        });
    }

    private NotifierEntity require(TenantId tenantId, NotifierId id) {
        return jpa.findByIdAndTenantId(id.value(), tenantId.value())
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Notifier not found", Map.of("notifierId", id.toString())));
    }
}
