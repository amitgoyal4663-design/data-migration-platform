package com.dmp.application.port.out;

import com.dmp.domain.notify.Notifier;
import com.dmp.domain.notify.NotifierId;
import com.dmp.domain.tenant.TenantId;

import java.util.List;
import java.util.Optional;

/** Storage for where to send word when a run ends. */
public interface NotifierRepository {

    Notifier create(Notifier notifier);

    Notifier update(Notifier notifier);

    Optional<Notifier> findById(TenantId tenantId, NotifierId id);

    List<Notifier> findAll(TenantId tenantId);

    /**
     * Every notifier that is switched on, for a run that has just ended.
     *
     * <p>Filtering by pipeline is left to the caller rather than pushed into a query, because a
     * notifier watching every pipeline and one watching a named pipeline are both matches and
     * expressing that as SQL means an OR over a nullable column for a list that is a handful of
     * rows. The filter that matters — enabled — is the indexed one.
     */
    List<Notifier> findEnabled(TenantId tenantId);

    void delete(TenantId tenantId, NotifierId id);

    /**
     * Records what happened on the last delivery, without touching anything else.
     *
     * <p>Separate from {@link #update} and deliberately not optimistic: this is written by a
     * background sender, and a delivery outcome losing a race with somebody editing the URL in the
     * console should not fail either of them. It is diagnostic, not a fact anybody edits.
     */
    void recordAttempt(TenantId tenantId, NotifierId id, java.time.Instant at, boolean succeeded,
                       String error);
}
