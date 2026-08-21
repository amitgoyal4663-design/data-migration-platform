package com.dmp.domain.pipeline;

import com.dmp.common.id.EntityId;
import com.dmp.common.id.Ids;

import java.util.Objects;
import java.util.UUID;

public record PipelineVersionId(UUID value) implements EntityId {

    public PipelineVersionId {
        Objects.requireNonNull(value, "pipeline version id value");
    }

    public static PipelineVersionId newId() {
        return new PipelineVersionId(Ids.newId());
    }

    public static PipelineVersionId of(UUID value) {
        return new PipelineVersionId(value);
    }

    public static PipelineVersionId parse(String value) {
        return new PipelineVersionId(UUID.fromString(value));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
