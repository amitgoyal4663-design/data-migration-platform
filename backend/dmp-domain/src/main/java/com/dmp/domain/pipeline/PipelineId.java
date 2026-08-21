package com.dmp.domain.pipeline;

import com.dmp.common.id.EntityId;
import com.dmp.common.id.Ids;

import java.util.Objects;
import java.util.UUID;

public record PipelineId(UUID value) implements EntityId {

    public PipelineId {
        Objects.requireNonNull(value, "pipeline id value");
    }

    public static PipelineId newId() {
        return new PipelineId(Ids.newId());
    }

    public static PipelineId of(UUID value) {
        return new PipelineId(value);
    }

    public static PipelineId parse(String value) {
        return new PipelineId(UUID.fromString(value));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
