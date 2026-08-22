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
    /**
     * The destination this stands in for.
     *
     * <p>Held whole rather than reduced to its spec, because every question the engine asks a sink
     * without opening it must be answered the way the real one would answer it. Inventing those
     * answers is not a smaller version of the destination, it is a different destination — and a
     * rehearsal against a different destination proves nothing.
     *
     * <p>That is not hypothetical: this class first declared that it could not take a batch as one
     * payload, while Kafka and REST both can. A dry run then failed a pipeline whose batch
     * transform returns one envelope — reporting a configuration error, naming the real connector,
     * for a rule the real connector does not have.
     */
    private final Sink real;

    DryRunSink(Sink real) {
        this.real = real;
    }

    @Override
    public ConnectorSpec spec() {
        return real.spec();
    }

    @Override
    public boolean sendsBatchAsSinglePayload() {
        return real.sendsBatchAsSinglePayload();
    }

    /**
     * Whatever the real sink says.
     *
     * <p>Answering "no" here would have been defensible on its own terms — nothing is refused, so
     * splitting the batch narrows nothing down — but it would change the shape of the rehearsal's
     * delivery groups, and the timeline of a dry run is read as a prediction of the real one.
     */
    @Override
    public boolean supportsPerRecordDelivery() {
        return real.supportsPerRecordDelivery();
    }

    @Override
    public SinkSession openSink(ConnectorContext context) {
        return new SinkSession() {

            @Override
            public Capabilities capabilities() {
                // Idempotent and synchronous, both trivially true of doing nothing. Declaring an
                // asynchronous commit would park every chunk waiting for a verdict never coming —
                // so these two are the stand-in's own answers, and they are the only two that are.
                //
                // Everything else is the real destination's, asked without opening it. Whether a
                // batch can be sent as one payload decides whether a batch transform's envelope is
                // accepted or rejected as a configuration error, and a rehearsal that answers that
                // differently from the real run is worse than no rehearsal.
                return new Capabilities(true, null, false, false, false,
                        real.sendsBatchAsSinglePayload(), 0, 500);
            }

            @Override
            public WriteResult write(RecordBatch batch) {
                ObjectNode details = Json.newObject();
                details.put("dryRun", true);
                details.put("wouldHaveWritten", batch.size());
                details.put("destination", real.spec().type());
                return new WriteResult(batch.size(), 0, batch.totalBytes(), java.util.List.of(), details);
            }
        };
    }
}
