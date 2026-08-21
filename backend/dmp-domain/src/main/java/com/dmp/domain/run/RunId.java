package com.dmp.domain.run;

import com.dmp.common.id.EntityId;
import com.dmp.common.id.Ids;

import java.util.Objects;
import java.util.UUID;

/**
 * Identifies a run.
 *
 * <p>The {@code run} table is the fastest-growing in the platform and is queried almost
 * exclusively by recency, which is why UUIDv7's time ordering matters here more than anywhere
 * else — see {@link Ids}.
 */
public record RunId(UUID value) implements EntityId {

    public RunId {
        Objects.requireNonNull(value, "run id value");
    }

    public static RunId newId() {
        return new RunId(Ids.newId());
    }

    public static RunId of(UUID value) {
        return new RunId(value);
    }

    public static RunId parse(String value) {
        return new RunId(UUID.fromString(value));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
