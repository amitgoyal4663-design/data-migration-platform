package com.dmp.common.id;

import java.util.UUID;

/**
 * Marker for the platform's typed identifiers.
 *
 * <p>Every aggregate has its own identifier type rather than passing bare {@link UUID}s.
 * Transposing a {@code PipelineId} and a {@code RunId} in an argument list is a real and
 * otherwise invisible bug class; this makes it a compile error.
 *
 * <p>The interface exists so persistence adapters can unwrap identifiers generically without
 * knowing every concrete type.
 */
public interface EntityId {

    UUID value();

    /** Creation time, derived from the UUIDv7 timestamp. */
    default long createdAtMillis() {
        return Ids.timestampOf(value());
    }
}
