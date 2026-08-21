package com.dmp.domain.schedule;

import com.dmp.common.id.EntityId;
import com.dmp.common.id.Ids;

import java.util.Objects;
import java.util.UUID;

public record ScheduleId(UUID value) implements EntityId {

    public ScheduleId {
        Objects.requireNonNull(value, "schedule id value");
    }

    public static ScheduleId newId() {
        return new ScheduleId(Ids.newId());
    }

    public static ScheduleId of(UUID value) {
        return new ScheduleId(value);
    }

    public static ScheduleId parse(String value) {
        return new ScheduleId(UUID.fromString(value));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
