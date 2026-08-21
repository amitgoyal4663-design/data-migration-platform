package com.dmp.persistence.postgres.adapter;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.ConnectorInstanceRepository;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.postgres.entity.ConnectorInstanceEntity;
import com.dmp.persistence.postgres.mapper.ConnectorInstanceMapper;
import com.dmp.persistence.postgres.repository.ConnectorInstanceJpaRepository;
import com.dmp.persistence.postgres.support.PersistenceSupport;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** PostgreSQL adapter for {@link ConnectorInstanceRepository}. */
@Repository
public class ConnectorInstanceRepositoryAdapter implements ConnectorInstanceRepository {

    private static final Map<String, String> SORTABLE = Map.of(
            "name", "name",
            "connectorType", "connector_type",
            "createdAt", "created_at",
            "updatedAt", "updated_at",
            "status", "status");

    private final ConnectorInstanceJpaRepository jpa;

    public ConnectorInstanceRepositoryAdapter(ConnectorInstanceJpaRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    public ConnectorInstance save(ConnectorInstance instance) {
        String description = "Connector instance '" + instance.name() + "'";
        return PersistenceSupport.translatingExceptions(description, () -> {
            ConnectorInstanceEntity entity = jpa
                    .findByTenantIdAndId(instance.tenantId().value(), instance.id().value())
                    .orElse(null);

            if (entity == null) {
                entity = ConnectorInstanceMapper.toEntity(instance);
            } else {
                PersistenceSupport.requireCurrentVersion(
                        instance.rowVersion(), entity.getRowVersion(), description);
                ConnectorInstanceMapper.applyTo(entity, instance);
            }
            return ConnectorInstanceMapper.toDomain(jpa.save(entity));
        });
    }

    @Override
    public Optional<ConnectorInstance> findById(TenantId tenantId, ConnectorInstanceId id) {
        return jpa.findByTenantIdAndId(tenantId.value(), id.value())
                .map(ConnectorInstanceMapper::toDomain);
    }

    @Override
    public List<ConnectorInstance> findAllById(TenantId tenantId, Collection<ConnectorInstanceId> ids) {
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }
        List<UUID> raw = ids.stream().map(ConnectorInstanceId::value).toList();
        return jpa.findByTenantIdAndIdIn(tenantId.value(), raw).stream()
                .map(ConnectorInstanceMapper::toDomain)
                .toList();
    }

    @Override
    public Page<ConnectorInstance> search(TenantId tenantId, ConnectorSearch criteria, PageQuery pageQuery) {
        return PersistenceSupport.toPage(
                jpa.search(tenantId.value(),
                        criteria.nameContains(),
                        criteria.connectorType(),
                        criteria.direction() == null ? null : criteria.direction().name(),
                        criteria.status() == null ? null : criteria.status().name(),
                        PersistenceSupport.toPageable(pageQuery, SORTABLE, "name")),
                pageQuery, ConnectorInstanceMapper::toDomain);
    }

    @Override
    public boolean existsByName(TenantId tenantId, String name) {
        return jpa.existsByTenantIdAndName(tenantId.value(), name);
    }

    @Override
    public void delete(TenantId tenantId, ConnectorInstanceId id) {
        jpa.deleteByTenantIdAndId(tenantId.value(), id.value());
    }
}
