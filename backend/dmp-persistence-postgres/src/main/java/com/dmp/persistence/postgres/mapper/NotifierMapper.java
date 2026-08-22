package com.dmp.persistence.postgres.mapper;

import com.dmp.domain.notify.Notifier;
import com.dmp.domain.notify.NotifierId;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.NotifierEntity;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public final class NotifierMapper {

    private NotifierMapper() {
    }

    public static Notifier toDomain(NotifierEntity entity) {
        return new Notifier(
                NotifierId.of(entity.getId()),
                TenantId.of(entity.getTenantId()),
                entity.getName(),
                entity.getUrl(),
                entity.getPipelineId() == null ? null : PipelineId.of(entity.getPipelineId()),
                events(entity.getEvents()),
                entity.getSecretHeader(),
                entity.getSecretRef(),
                entity.isEnabled(),
                entity.getDescription(),
                entity.getLastAttemptAt(),
                entity.isLastAttemptSucceeded(),
                entity.getLastAttemptError(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getRowVersion());
    }

    public static NotifierEntity toEntity(Notifier notifier) {
        NotifierEntity entity = new NotifierEntity();
        entity.setId(notifier.id().value());
        entity.setTenantId(notifier.tenantId().value());
        entity.setCreatedAt(notifier.createdAt());
        applyTo(entity, notifier);
        return entity;
    }

    public static void applyTo(NotifierEntity entity, Notifier notifier) {
        entity.setPipelineId(notifier.pipelineId() == null ? null : notifier.pipelineId().value());
        entity.setName(notifier.name());
        entity.setUrl(notifier.url());
        entity.setEvents(notifier.events().stream().map(Enum::name)
                .sorted().collect(Collectors.joining(",")));
        entity.setSecretHeader(notifier.secretHeader());
        entity.setSecretRef(notifier.secretRef());
        entity.setEnabled(notifier.enabled());
        entity.setDescription(notifier.description());
        entity.setLastAttemptAt(notifier.lastAttemptAt());
        entity.setLastAttemptSucceeded(notifier.lastAttemptSucceeded());
        entity.setLastAttemptError(notifier.lastAttemptError());
        entity.setUpdatedAt(notifier.updatedAt());
    }

    /**
     * Unknown names are dropped rather than thrown on.
     *
     * <p>A row written by a newer version of the platform, then read by an older one during a
     * rolling deploy, must not make the whole notifier unloadable — which would take out the
     * notifiers that still work, at exactly the time somebody is deploying.
     */
    private static Set<Notifier.Event> events(String stored) {
        if (stored == null || stored.isBlank()) {
            return Set.of();
        }
        Set<Notifier.Event> events = new LinkedHashSet<>();
        for (String name : Arrays.stream(stored.split(",")).map(String::trim).toList()) {
            try {
                events.add(Notifier.Event.valueOf(name));
            } catch (IllegalArgumentException ignored) {
                // An event this version does not have. Nothing to subscribe to.
            }
        }
        return events;
    }
}
