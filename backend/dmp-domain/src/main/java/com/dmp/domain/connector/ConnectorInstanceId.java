package com.dmp.domain.connector;

import com.dmp.common.id.EntityId;
import com.dmp.common.id.Ids;

import java.util.Objects;
import java.util.UUID;

public record ConnectorInstanceId(UUID value) implements EntityId {

    public ConnectorInstanceId {
        Objects.requireNonNull(value, "connector instance id value");
    }

    public static ConnectorInstanceId newId() {
        return new ConnectorInstanceId(Ids.newId());
    }

    public static ConnectorInstanceId of(UUID value) {
        return new ConnectorInstanceId(value);
    }

    public static ConnectorInstanceId parse(String value) {
        return new ConnectorInstanceId(UUID.fromString(value));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
