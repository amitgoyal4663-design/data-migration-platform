package com.dmp.domain.pipeline;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.util.Map;
import java.util.Objects;

/**
 * A directed edge between two nodes.
 *
 * <p>{@code condition} supports conditional routing — the branch is taken only when the
 * expression evaluates truthy. A null condition means unconditional. This is what lets a
 * VALIDATION node send passing records one way and failures to an ERROR_HANDLER, without
 * needing a distinct edge type.
 *
 * <p>From ADR-0001, an edge is also the unit at which transport is chosen: in streaming mode
 * each edge becomes a Kafka topic, in batch mode a bounded in-process buffer.
 */
public record EdgeDefinition(String id, String from, String to, String condition) {

    public EdgeDefinition {
        Objects.requireNonNull(id, "edge id");

        if (from == null || from.isBlank() || to == null || to.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Edge endpoints must not be blank",
                    Map.of("edgeId", id, "from", String.valueOf(from), "to", String.valueOf(to)));
        }
        if (condition != null && condition.isBlank()) {
            condition = null;
        }
    }

    public static EdgeDefinition of(String id, String from, String to) {
        return new EdgeDefinition(id, from, to, null);
    }

    public boolean isConditional() {
        return condition != null;
    }
}
