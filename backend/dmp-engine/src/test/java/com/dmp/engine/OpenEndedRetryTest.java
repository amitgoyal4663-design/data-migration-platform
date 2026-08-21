package com.dmp.engine;

import com.dmp.domain.run.RunId;
import com.dmp.domain.run.Split;
import com.dmp.domain.tenant.TenantId;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A lazily generated chunk has no start of its own.
 *
 * <p>Its spec carries no boundaries — the position is entirely the checkpoint cursor — so
 * discarding that cursor rewinds it to the beginning of the <em>source</em>, not to the beginning
 * of the chunk. Retrying chunk five of a lazily chunked run that way re-read the collection from
 * record one and collided with everything the first four chunks had already written, and the
 * duplicate guard could not catch it: that guard asks how much the retried chunk itself wrote,
 * which was nothing.
 */
class OpenEndedRetryTest {

    @Test
    void aLazilyGeneratedChunkIsMarkedAsHavingNoRange() {
        Split lazy = Split.plan(RunId.newId(), TenantId.newId(), 5,
                OpenEnded.spec(), Instant.parse("2026-08-08T00:00:00Z"));

        assertThat(OpenEnded.isOpenEnded(lazy.spec()))
                .as("the marker is what tells retry it must not discard the cursor")
                .isTrue();
    }

    @Test
    void aPlannedChunkCarriesItsOwnRangeAndMayBeRestarted() {
        var spec = com.dmp.common.json.Json.newObject();
        spec.put("from", 5_000);
        spec.put("to", 6_000);

        Split planned = Split.plan(RunId.newId(), TenantId.newId(), 5, spec,
                Instant.parse("2026-08-08T00:00:00Z"));

        assertThat(OpenEnded.isOpenEnded(planned.spec()))
                .as("a chunk with boundaries can genuinely be run again from its start")
                .isFalse();
    }
}
