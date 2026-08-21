package com.dmp.domain.run;

import com.dmp.common.id.EntityId;
import com.dmp.common.id.Ids;

import java.util.Objects;
import java.util.UUID;

public record SplitId(UUID value) implements EntityId {

    public SplitId {
        Objects.requireNonNull(value, "split id value");
    }

    public static SplitId newId() {
        return new SplitId(Ids.newId());
    }

    public static SplitId of(UUID value) {
        return new SplitId(value);
    }

    public static SplitId parse(String value) {
        return new SplitId(UUID.fromString(value));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
