package com.dmp.domain.run;

import com.dmp.common.error.DmpException;
import com.dmp.common.json.Json;
import com.dmp.domain.tenant.TenantId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the invariants protecting the resume guarantee. */
class CheckpointTest {

    private static final Instant NOW = Instant.parse("2026-08-07T03:00:00Z");

    private Checkpoint initial() {
        return Checkpoint.initial(SplitId.newId(), RunId.newId(), TenantId.newId(), NOW);
    }

    @Test
    @DisplayName("starts with no progress, so resuming and starting fresh are the same path")
    void initialHasNoProgress() {
        assertThat(initial().hasProgress()).isFalse();
        assertThat(initial().batchesCommitted()).isZero();
    }

    @Test
    @DisplayName("accumulates counters across batches")
    void advanceAccumulates() {
        Checkpoint checkpoint = initial()
                .advance(Json.newObject().put("lastId", 1000), 1000, 1000, 1000, 990, 5, 5, 4096, NOW)
                .advance(Json.newObject().put("lastId", 2000), 2000, 1000, 1000, 995, 3, 2, 4096, NOW);

        assertThat(checkpoint.recordsRead()).isEqualTo(2000);
        assertThat(checkpoint.recordsWritten()).isEqualTo(1985);
        assertThat(checkpoint.batchesCommitted()).isEqualTo(2);
        assertThat(checkpoint.sourceCursor().get("lastId").asInt()).isEqualTo(2000);
    }

    @Test
    @DisplayName("refuses to move the cursor backwards")
    void cursorMustAdvanceMonotonically() {
        // A regressing cursor would cause silent re-processing on resume. Failing loudly here is
        // the single invariant protecting the resume guarantee, so it is worth rejecting rather
        // than tolerating.
        Checkpoint checkpoint = initial()
                .advance(Json.newObject().put("lastId", 5000), 5000, 5000, 5000, 5000, 0, 0, 1024, NOW);

        assertThatThrownBy(() -> checkpoint
                .advance(Json.newObject().put("lastId", 3000), 3000, 100, 100, 100, 0, 0, 512, NOW))
                .isInstanceOf(DmpException.class)
                .hasMessageContaining("advance monotonically");
    }

    @Test
    @DisplayName("permits a repeated sequence, since redelivery is an accepted outcome")
    void equalSequenceIsAllowed() {
        // At-least-once delivery means the same batch may be committed twice after a worker
        // restart. That is a duplicate to be deduplicated downstream, not a corruption to reject.
        Checkpoint checkpoint = initial()
                .advance(Json.newObject().put("lastId", 5000), 5000, 5000, 5000, 5000, 0, 0, 1024, NOW);

        assertThat(checkpoint.advance(Json.newObject().put("lastId", 5000), 5000, 0, 0, 0, 0, 0, 0, NOW))
                .isNotNull();
    }

    @Test
    @DisplayName("surfaces records that were read but never accounted for")
    void unaccountedRecordsAreVisible() {
        Checkpoint checkpoint = initial()
                .advance(Json.newObject(), 100, 100, 100, 90, 5, 0, 1024, NOW);

        assertThat(checkpoint.unaccountedRecords()).isEqualTo(5);
    }

    @Test
    @DisplayName("rejects negative counters")
    void rejectsNegativeCounters() {
        assertThatThrownBy(() -> new Checkpoint(SplitId.newId(), RunId.newId(), TenantId.newId(),
                Json.emptyObject(), 0, -1, 0, 0, 0, 0, 0, 0, NOW, NOW))
                .isInstanceOf(DmpException.class);
    }
}
