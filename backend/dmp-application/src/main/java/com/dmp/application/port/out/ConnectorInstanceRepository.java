package com.dmp.application.port.out;

import com.dmp.application.common.Page;
import com.dmp.application.common.PageQuery;
import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.connector.ConnectorInstanceStatus;
import com.dmp.domain.tenant.TenantId;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** Persistence port for connector instances. Implemented by the PostgreSQL adapter. */
public interface ConnectorInstanceRepository {

    ConnectorInstance save(ConnectorInstance instance);

    Optional<ConnectorInstance> findById(TenantId tenantId, ConnectorInstanceId id);

    /**
     * Bulk lookup, used when validating a pipeline definition.
     *
     * <p>A DAG may reference a dozen connector instances. Fetching them individually would make
     * publish latency scale with node count for no reason.
     */
    List<ConnectorInstance> findAllById(TenantId tenantId, Collection<ConnectorInstanceId> ids);

    Page<ConnectorInstance> search(TenantId tenantId, ConnectorSearch criteria, PageQuery pageQuery);

    boolean existsByName(TenantId tenantId, String name);

    void delete(TenantId tenantId, ConnectorInstanceId id);

    record ConnectorSearch(String nameContains, String connectorType,
                           ConnectorDirection direction, ConnectorInstanceStatus status) {

        public ConnectorSearch {
            if (nameContains != null && nameContains.isBlank()) {
                nameContains = null;
            }
            if (connectorType != null && connectorType.isBlank()) {
                connectorType = null;
            }
        }

        public static ConnectorSearch none() {
            return new ConnectorSearch(null, null, null, null);
        }
    }
}
