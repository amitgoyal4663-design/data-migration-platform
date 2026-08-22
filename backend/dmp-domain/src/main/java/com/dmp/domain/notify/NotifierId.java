package com.dmp.domain.notify;

import com.dmp.common.id.EntityId;
import com.dmp.common.id.Ids;

import java.util.Objects;
import java.util.UUID;

public record NotifierId(UUID value) implements EntityId {

    public NotifierId {
        Objects.requireNonNull(value, "notifier id value");
    }

    public static NotifierId newId() {
        return new NotifierId(Ids.newId());
    }

    public static NotifierId of(UUID value) {
        return new NotifierId(value);
    }

    public static NotifierId parse(String value) {
        return new NotifierId(UUID.fromString(value));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
