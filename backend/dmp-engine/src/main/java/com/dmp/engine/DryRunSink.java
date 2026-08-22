package com.dmp.engine;

import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.api.RecordBatch;
import com.dmp.connector.api.Sink;
import com.dmp.common.json.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The destination, for a run that is only rehearsing.
 *
 * <p>Stands in for the real sink so that everything before it happens exactly as it would: the
 * source is read in full, every chunk is planned and leased and checkpointed, every script runs
 * against every record, and every transform failure is captured. Only the last step is withheld.
 *
 * <p><b>The real connector is never opened.</b> Not opened-and-not-written to — not opened. Opening
 * a Salesforce sink submits a bulk job and spends one of an org-wide daily quota of ten thousand;
 * opening a file sink creates the file; opening a Kafka sink is the one case where nothing happens,
 * and building a rule around the harmless case is how the other two get discovered in production.
 * A rehearsal that creates things is not a rehearsal.
 *
 * <p>The cost of that decision, stated plainly because somebody will ask: a dry run cannot tell you
 * the destination would have <em>accepted</em> the records. It tells you what would be sent, how
 * many, and which records never got that far. Whether the destination likes them is a question only
 * the destination can answer, and asking it means writing.
 *
 * <p>Reports every record as written, because {@code written} here means "handed over" and that is
 * what the accounting downstream is built on. What keeps this from becoming a lie is the run
 * itself: a dry run is flagged, its records are never indexed as transferred, and its reconciliation
 * says which kind of run produced the numbers.
 */
final class DryRunSink implements Sink {

    /**
     * The real connector's specification, so the run is described in terms of the destination it
     * rehearses rather than in terms of this class. A stage log reading "wrote to dry-run" would
     * name the mechanism instead of the thing being tested.
     */
    private final ConnectorSpec spec;

    DryRunSink(ConnectorSpec spec) {
        this.spec = spec;
    }

    @Override
    public ConnectorSpec spec() {
        return spec;
    }

    /**
     * No, whatever the real sink says.
     *
     * <p>Splitting a batch into smaller calls exists to narrow down which record a destination
     * refused. Nothing is refused here, so dividing the batch would produce more entries saying the
     * same thing and make the rehearsal's timeline differ from the real run's for no reason.
     */
    @Override
    public boolean supportsPerRecordDelivery() {
        return false;
    }

    @Override
    public SinkSession openSink(ConnectorContext context) {
        return new SinkSession() {

            @Override
            public Capabilities capabilities() {
                // Idempotent and synchronous, both trivially true of doing nothing. Declaring an
                // asynchronous commit would park every chunk waiting for a verdict never coming.
                //
                // The batch sizes are the engine's own defaults rather than the real sink's. A
                // rehearsal that borrowed the destination's preferred size would report batch
                // counts the real run reproduces, which reads as more confirmation than a dry run
                // is entitled to give — nothing here was negotiated with a destination.
                return new Capabilities(true, null, false, false, false, false, 0, 500);
            }

            @Override
            public WriteResult write(RecordBatch batch) {
                ObjectNode details = Json.newObject();
                details.put("dryRun", true);
                details.put("wouldHaveWritten", batch.size());
                details.put("destination", spec.type());
                return new WriteResult(batch.size(), 0, batch.totalBytes(), java.util.List.of(), details);
            }
        };
    }
}
