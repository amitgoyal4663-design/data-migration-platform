package com.dmp.engine;

import com.dmp.application.port.out.RecordErrorPort;
import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.Source;
import com.dmp.domain.run.RunId;
import com.dmp.domain.tenant.TenantId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Reads a previous run's rejected records so they can be sent through the pipeline again.
 *
 * <p>Deliberately a {@link Source} rather than a separate replay service. Expressed this way a
 * replay is an ordinary run whose source happens to be the dead-letter queue: it is chunked,
 * leased, checkpointed, transformed, batched, written and — when it fails again — captured, counted
 * and retryable by exactly the code that does all of that for a run reading a database. A parallel
 * implementation would have had to reproduce every one of those behaviours, and would have drifted
 * from them at the first change to either.
 *
 * <p>Not registered in the connector catalogue and not loaded by the {@code ServiceLoader} the
 * registry uses. It is internal machinery rather than something a user points a pipeline at, and
 * listing it would offer a connector nobody can meaningfully configure.
 *
 * <p><b>What is replayed is what was stored.</b> Payloads reach the dead-letter queue already
 * redacted, so a pipeline that masks or hashes a field holds the masked value and not the original
 * — the real one was never written down. {@code RunOrchestrator} refuses to start such a replay
 * rather than quietly loading the target with placeholders.
 */
@Component
public class ReplaySource implements Source {

    /**
     * How many rejections are fetched per round trip.
     *
     * <p>A chunk is normally a thousand records; holding that many payloads at once is fine,
     * holding a whole run's worth is not, so the window is walked rather than loaded.
     */
    private static final int PAGE = 500;

    private final RecordErrorPort recordErrors;

    public ReplaySource(RecordErrorPort recordErrors) {
        this.recordErrors = recordErrors;
    }

    /**
     * Present only because {@link com.dmp.connector.api.Connector} requires it. Nothing reads it:
     * this source is never registered, never listed, and never configured.
     */
    @Override
    public ConnectorSpec spec() {
        return new ConnectorSpec("dlq-replay", "Rejected records",
                "Internal source that re-reads a previous run's rejected records.",
                ConnectorSpec.Direction.SOURCE, Json.emptyObject(), java.util.Set.of(), "1.0.0");
    }

    @Override
    public void testConnection(ConnectorContext context) {
        // Nothing to reach: the records are already in this platform's own store.
    }

    /**
     * Unreachable in practice — the engine opens a replay with {@link #openSource(TenantId)},
     * because a session has to be scoped to a tenant and {@link ConnectorContext} carries no
     * tenant. It deliberately carries none: a connector must never be able to widen its own scope.
     */
    @Override
    public SourceSession openSource(ConnectorContext context) {
        throw new UnsupportedOperationException(
                "The replay source must be opened with the tenant of the chunk being replayed");
    }

    SourceSession openSource(TenantId tenantId) {
        return new ReplaySession(tenantId);
    }

    private final class ReplaySession implements SourceSession {

        private final TenantId tenantId;

        ReplaySession(TenantId tenantId) {
            this.tenantId = tenantId;
        }

        /**
         * A replay's chunks are planned by the orchestrator, which already knows how many
         * rejections are waiting. Reaching here would mean a replay was planned the ordinary way.
         */
        @Override
        public List<SplitSpec> plan(Preparation preparation, PlanRequest request) {
            throw new UnsupportedOperationException(
                    "Replay chunks are planned from the dead-letter queue, not by this source");
        }

        @Override
        public RecordStream read(SplitSpec split, JsonNode fromCursor, int fetchSize) {
            JsonNode spec = split.spec();

            // Resuming continues from the offset already consumed, so a chunk that died part-way
            // does not re-send the part it had already written. The offset counts within this
            // chunk's window, not across the whole dead-letter queue.
            int consumed = fromCursor == null ? 0 : fromCursor.path("offset").asInt(0);

            return new ReplayStream(tenantId, Replay.originalRunId(spec),
                    Replay.skip(spec), Replay.limit(spec), consumed);
        }
    }

    private final class ReplayStream implements RecordStream {

        private final TenantId tenantId;
        private final RunId originalRunId;
        private final int windowStart;
        private final int windowSize;
        private final Deque<DataRecord> buffered = new ArrayDeque<>();

        private int consumed;
        private boolean exhausted;

        ReplayStream(TenantId tenantId, RunId originalRunId,
                     int windowStart, int windowSize, int consumed) {
            this.tenantId = tenantId;
            this.originalRunId = originalRunId;
            this.windowStart = windowStart;
            this.windowSize = windowSize;
            this.consumed = consumed;
        }

        @Override
        public DataRecord next() {
            if (buffered.isEmpty() && !exhausted) {
                fetchPage();
            }
            if (buffered.isEmpty()) {
                return null;
            }
            consumed++;
            return buffered.poll();
        }

        private void fetchPage() {
            int remaining = windowSize - consumed;
            if (remaining <= 0) {
                exhausted = true;
                return;
            }

            List<RecordErrorPort.RecordErrorEntry> page = recordErrors.findForReplay(
                    tenantId, originalRunId, windowStart + consumed, Math.min(PAGE, remaining));

            if (page.isEmpty()) {
                // Fewer rejections than the window expected. Entries expire on the audit retention,
                // possibly between planning this replay and running it, and covering what remains
                // is better than failing over what the retention has already removed.
                exhausted = true;
                return;
            }

            long seq = consumed;
            for (RecordErrorPort.RecordErrorEntry entry : page) {
                // Headed with the run it came from, so a target that keeps headers records that
                // this arrival was a replay rather than a first delivery.
                buffered.add(new DataRecord(entry.payload(), entry.key(),
                        Map.of("dmp-replay-of", originalRunId.toString()),
                        seq++, 0, DataRecord.estimateBytes(entry.payload())));
            }
        }

        /**
         * How far into this chunk's window the reader has got.
         *
         * <p>Read by the engine only after the sink has durably accepted a batch, like every other
         * source's cursor — which is what makes a resumed replay continue rather than restart.
         */
        @Override
        public JsonNode cursor() {
            ObjectNode cursor = Json.newObject();
            cursor.put("offset", consumed);
            return cursor;
        }

        @Override
        public void close() {
            buffered.clear();
        }
    }
}
