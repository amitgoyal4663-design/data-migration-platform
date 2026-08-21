package com.dmp.persistence.postgres.mapper;

import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.connector.ConnectorInstanceStatus;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.ConnectorInstanceEntity;

/** Translates between {@link ConnectorInstance} and its JPA entity. */
public final class ConnectorInstanceMapper {

    private ConnectorInstanceMapper() {
    }

    public static ConnectorInstance toDomain(ConnectorInstanceEntity entity) {
        return new ConnectorInstance(
                ConnectorInstanceId.of(entity.getId()),
                TenantId.of(entity.getTenantId()),
                entity.getName(),
                entity.getConnectorType(),
                ConnectorDirection.valueOf(entity.getDirection()),
                entity.getConfig(),
                entity.getSecretRefs(),
                ConnectorInstanceStatus.valueOf(entity.getStatus()),
                entity.getDescription(),
                entity.getLastTestedAt(),
                entity.getLastTestError(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getRowVersion());
    }

    public static ConnectorInstanceEntity toEntity(ConnectorInstance domain) {
        return new ConnectorInstanceEntity(
                domain.id().value(),
                domain.tenantId().value(),
                domain.name(),
                domain.connectorType(),
                domain.direction().name(),
                domain.config(),
                domain.secretRefs(),
                domain.status().name(),
                domain.description(),
                domain.lastTestedAt(),
                domain.lastTestError(),
                domain.createdAt(),
                domain.updatedAt(),
                domain.rowVersion());
    }

    public static void applyTo(ConnectorInstanceEntity entity, ConnectorInstance domain) {
        entity.apply(
                domain.name(),
                domain.config(),
                domain.secretRefs(),
                domain.status().name(),
                domain.description(),
                domain.lastTestedAt(),
                domain.lastTestError(),
                domain.updatedAt());
    }
}
