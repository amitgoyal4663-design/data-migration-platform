package com.dmp.engine;

import com.dmp.application.port.out.StageLogPort;
import com.dmp.application.port.out.CheckpointRepository;
import com.dmp.application.port.out.RecordErrorPort;
import com.dmp.application.port.out.RecordIndexPort;
import com.dmp.application.port.out.SplitRepository;
import com.dmp.connector.api.ConnectorContext;
import com.dmp.connector.api.DataRecord;
import com.dmp.connector.api.Preparation;
import com.dmp.connector.api.RecordBatch;
import com.dmp.connector.api.Sink;
import com.dmp.connector.api.Source;
import com.dmp.connector.runtime.ConnectorContexts;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.audit.StageLogPolicy;
import com.dmp.domain.audit.ErrorSignature;
import com.dmp.domain.pipeline.ChunkingPolicy;
import com.dmp.domain.pipeline.DeliveryPolicy;
import com.dmp.domain.run.Checkpoint;
import com.dmp.domain.run.Split;
import com.dmp.transform.api.BatchResult;
import com.dmp.transform.api.RecordTransform;
import com.dmp.transform.api.TransformException;
import com.dmp.transform.api.TransformFactory;
import com.dmp.transform.api.TransformSpec;
import com.dmp.transform.api.TransformStage;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Executes one chunk end to end: read, batch, write, checkpoint, repeat.
 *
 * <p>This is where the platform's central guarantee lives. Records stream from source to sink
 * through a bounded buffer, so memory is a function of the batch size and never of the dataset:
 * a chunk of ten rows and a chunk of ten million occupy the same footprint. And the checkpoint
 * advances only after the sink has <em>durably accepted</em> a batch, which is what makes
 * "kill a worker mid-run and another resumes without loss or duplication" true rather than hoped for.
 *
 * <p>The ordering below is not an implementation detail and must not be rearranged:
 *
 * <pre>
 *   1. read records from the source
 *   2. accumulate until the batch is full by count or by bytes
 *   3. sink.write(batch)                 — may fail; nothing has been committed yet
 *   4. checkpoint.advance(cursor)        — only now is the position durable
 * </pre>
 *
 * <p>Swapping 3 and 4 turns an at-least-once pipeline into a lossy one: a crash between them would
 * resume past records the sink never accepted, and nothing would ever report it.
 */
@Component
public class ChunkExecutor {

    private static final Logger log = LoggerFactory.getLogger(ChunkExecutor.class);

    /**
     * How many times one call to a destination is attempted before its group is given up on.
     *
     * <p>Small, and about the call rather than the chunk. Anything longer belongs to the chunk's
     * own attempt budget, which re-reads the source — the expensive thing this exists to avoid.
     */
    private static final int SINK_CALL_ATTEMPTS = 3;
    private static final Duration SINK_CALL_BACKOFF = Duration.ofSeconds(2);

    /**
     * Floor on how often a destination is asked whether it has finished.
     *
     * <p>A connector returning zero — or nothing — must not turn the poll into a hot loop against
     * somebody's org. Two seconds is short enough that a fast job is not noticeably delayed and
     * long enough that a misbehaving connector cannot spend an API quota on its own.
     */
    private static final Duration MIN_POLL_INTERVAL = Duration.ofSeconds(2);

    private final ConnectorRegistry connectors;
    private final ConnectorContexts contexts;
    private final CheckpointRepository checkpoints;
    private final SplitRepository splits;
    private final RecordErrorPort recordErrors;
    private final RecordIndexPort recordIndex;
    private final StageLogPort stageLog;
    private final TransformFactory transforms;
    private final ReplaySource replaySource;
    private final Clock clock;
    private final EngineMetrics metrics;

    public ChunkExecutor(ConnectorRegistry connectors,
                         ConnectorContexts contexts,
                         CheckpointRepository checkpoints,
                         SplitRepository splits,
                         RecordErrorPort recordErrors,
                         RecordIndexPort recordIndex,
                         StageLogPort stageLog,
                         TransformFactory transforms,
                         ReplaySource replaySource,
                         Clock clock) {
        this(connectors, contexts, checkpoints, splits, recordErrors, recordIndex, stageLog,
                transforms, replaySource, clock, EngineMetrics.NONE);
    }

    /**
     * The instrumented form, which is the one Spring wires.
     *
     * <p>An overload rather than an extra argument on the only constructor, because five call sites
     * build this directly and instrumentation must never be the reason a component becomes harder
     * to construct — the next person would simply skip it.
     */
    // Named explicitly, because a second public constructor makes the choice ambiguous and Spring
    // then looks for a no-arg one and fails at startup rather than at compile time. The overload
    // above exists for the five call sites that build this directly; this is the one to wire.
    @org.springframework.beans.factory.annotation.Autowired
    public ChunkExecutor(ConnectorRegistry connectors,
                         ConnectorContexts contexts,
                         CheckpointRepository checkpoints,
                         SplitRepository splits,
                         RecordErrorPort recordErrors,
                         RecordIndexPort recordIndex,
                         StageLogPort stageLog,
                         TransformFactory transforms,
                         ReplaySource replaySource,
                         Clock clock,
                         EngineMetrics metrics) {
        this.metrics = metrics == null ? EngineMetrics.NONE : metrics;
        this.connectors = connectors;
        this.contexts = contexts;
        this.checkpoints = checkpoints;
        this.splits = splits;
        this.recordErrors = recordErrors;
        this.recordIndex = recordIndex;
        this.stageLog = stageLog;
        this.transforms = transforms;
        this.replaySource = replaySource;
        this.clock = clock;
    }

    /**
     * Runs one chunk to completion.
     *
     * <p><b>A stop does not interrupt this.</b> Stopping a run stops it claiming further chunks; a
     * chunk already executing finishes. The alternative was tried and abandoned: breaking at the
     * nearest batch boundary left a chunk half done, and a half-done chunk is the one state nothing
     * else in the platform recognises. Its counts describe part of a range, its destination results
     * file covers records it did not finish sending, and a Salesforce job could be left mid-upload
     * with nobody watching it. Every other consumer — retry, resume, the results download, the
     * run's totals — is built on a chunk being a whole thing.
     *
     * <p>The cost is that Stop is not instant. Under a rate limit a chunk can take minutes, and the
     * run stays in STOPPING until the chunk it was running finishes. That is the trade, taken
     * deliberately: a stop that waits is easier to explain than a chunk that half happened.
     */
    public ChunkResult execute(ResolvedPipeline pipeline, Split split, String workerId) {
        // Wrapped rather than measured inline, because a chunk has half a dozen ways out — three
        // exception paths, a park, a shortfall — and instrumenting each one is how a meter ends up
        // counting five of the six.
        Instant chunkStarted = clock.instant();
        try (AutoCloseable inFlight = metrics.inFlight()) {
            ChunkResult result = run(pipeline, split, workerId);
            // "completed" covers a chunk that finished, however many records it lost on the way —
            // the losses are counted separately, and conflating them would make the outcome tag
            // mean two different things.
            metrics.chunk("completed", Duration.between(chunkStarted, clock.instant()));
            String source = pipeline.sourceInstance().connectorType();
            metrics.records("read", source, result.recordsRead());
            metrics.records("filtered", source, result.recordsFiltered());
            return result;

        } catch (LeaseLostException e) {
            // Its own meter, because it is silent everywhere else and it means the lease is too
            // short for the work — which, left alone, produces duplicate writes rather than errors.
            metrics.leaseLost();
            metrics.chunk("lease_lost", Duration.between(chunkStarted, clock.instant()));
            throw e;
        } catch (ChunkParkedException e) {
            metrics.chunk("parked", Duration.between(chunkStarted, clock.instant()));
            throw e;
        } catch (RuntimeException e) {
            metrics.chunk("failed", Duration.between(chunkStarted, clock.instant()));
            throw e;
        } catch (Exception e) {
            // Only the in-flight gauge's close() declares this, and it does not throw.
            throw new IllegalStateException(e);
        }
    }

    private ChunkResult run(ResolvedPipeline pipeline, Split split, String workerId) {

        // A chunk carrying a remote job handle has already read everything it is going to read and
        // has already handed it over. It is here to be settled, not re-executed.
        //
        // This branch is the single most dangerous line in the file. Falling through it would open
        // the source, read from the checkpoint and submit a second bulk job for records the first
        // one is still processing — which under an insert operation is how a migration silently
        // doubles its data.
        if (split.hasExternalJob()) {
            return settleParkedChunk(pipeline, split, workerId);
        }

        ChunkingPolicy chunking = pipeline.chunking();
        StageRecorder stages = new StageRecorder(pipeline, split);
        Checkpoint checkpoint = checkpoints.findOrCreate(
                split.tenantId(), split.runId(), split.id());

        if (checkpoint.hasProgress()) {
            log.info("Resuming chunk {} of run {} from sequence {} ({} batches already committed)",
                    split.index(), split.runId(), checkpoint.lastSeq(), checkpoint.batchesCommitted());
        }

        // The chunk index reaches the connectors: a sink that names its own target must scope that
        // name per chunk, or all of a run's chunks write over one another.
        ConnectorContext sourceContext = contexts.forChunk(
                pipeline.sourceInstance(), split.runId().toString(), workerId,
                split.index(), checkpoint.hasProgress(), pipeline.runParameters());
        ConnectorContext sinkContext = contexts.forChunk(
                pipeline.sinkInstance(), split.runId().toString(), workerId,
                split.index(), checkpoint.hasProgress());

        // A replay reads the previous run's rejections instead of the pipeline's source. Only the
        // reading end changes: the transforms, the sink and everything between are the pipeline's
        // own, which is the entire point — records go back through the path that rejected them.
        boolean replaying = Replay.isReplay(split.spec());
        Sink realSink = connectors.sink(pipeline.sinkInstance().connectorType());
        // Substituted here, at the one place the sink is resolved, so nothing downstream needs to
        // know a rehearsal is different from a delivery — the batching, the delivery groups, the
        // retries and the stage log are the real ones, which is what makes the rehearsal worth
        // anything.
        Sink sink = pipeline.dryRun() ? new DryRunSink(realSink) : realSink;

        long read = 0;
        long produced = 0;
        long filtered = 0;
        long written = 0;
        long failed = 0;
        long bytes = 0;
        int batches = 0;
        // Transform failures since the last saved checkpoint, as distinct from {@code failed},
        // which runs for the whole chunk and is never reset.
        long transformFailedSinceFlush = 0;

        // Cumulative for the whole chunk, seeded from the checkpoint so a resumed chunk judges
        // itself on everything it has ever done rather than on this attempt alone. The counters
        // above are reset at each flush; these deliberately are not.
        long producedTotal = checkpoint.recordsProduced();
        long failedTotal = checkpoint.recordsFailed();

        // An open-ended chunk has no upper bound of its own, so something has to decide where it
        // ends. A planned chunk must never be cut short this way: its range is its contract, and
        // stopping early would leave rows nobody comes back for.
        boolean openEnded = OpenEnded.isOpenEnded(split.spec());
        long rowBudget = openEnded
                ? pipeline.execution().effectiveRowsPerChunk(chunking.readFetchSizeOrDefault())
                : Long.MAX_VALUE;
        long readThisChunk = checkpoint.recordsRead();
        // Cleared with every batch, so it holds one batch's source payloads and no more.
        Map<Long, com.fasterxml.jackson.databind.JsonNode> sourceBySeq = new HashMap<>();
        boolean indexesPayloads = pipeline.audit().level().indexesEveryRecord()
                && pipeline.audit().indexesPayloads();
        boolean sourceExhausted = true;

        // Where this chunk had already reached. A source counts what the current read has emitted,
        // starting at one each time a stream is opened, so a resumed chunk's sequences have to be
        // shifted by what came before them to stay comparable with the checkpoint.
        long seqOffset = checkpoint.lastSeq();

        // Compiled first, and outside the connector sessions, so a script that does not compile
        // fails before anything opens a connection or reads a row.
        // A replay of stored rejections normally runs no transforms. The dead-letter queue holds
        // the record as the sink saw it — already transformed — so applying the chain again would
        // apply it twice. Only a replay deliberately routed through a different version transforms,
        // because there the new logic is the whole reason for the replay.
        List<com.dmp.transform.api.TransformSpec> chain =
                replaying && !Replay.appliesTransforms(split.spec())
                        ? List.of()
                        : pipeline.transforms();

        try (RecordTransform transform = transforms.compile(chain);
             Source.SourceSession sourceSession = replaying
                     ? replaySource.openSource(split.tenantId())
                     : connectors.source(pipeline.sourceInstance().connectorType())
                             .openSource(sourceContext);
             Sink.SinkSession sinkSession = sink.openSink(sinkContext)) {

            // Where the three sizes are finally settled: the pipeline's numbers, or the sink's own
            // preference where the pipeline left them automatic, capped by the sink's protocol
            // The batch is the chunk. Only two things lower it: the sink's own protocol limit —
            // a bulk API refusing more than 10,000 a request must not be handed 50,000 and find
            // out mid-migration — and the byte ceiling, which the batch builder applies as records
            // accumulate, because only the records know what they weigh.
            //
            // Two ways a chunk knows how big it is, and both belong here. An open-ended chunk is
            // as big as the budget that will stop it. A planned one is as big as the connector
            // counted at planning time — and passing 0 for those, as this did, threw that number
            // away: a Databricks chunk of a thousand rows became two batches of five hundred
            // because five hundred is what the REST sink happens to prefer, the delivery setting
            // that was meant to split it had nothing left to split, and the log showed the source
            // query twice for one call.
            ChunkingPolicy.EffectiveSizes effective = chunking.resolved(
                    openEnded ? rowBudget : split.plannedRows(),
                    sinkSession.capabilities().maxBatchSize(),
                    sinkSession.capabilities().preferredBatchSize());

            // A sink that can absorb a repeated write lets the resume position be saved less
            // often. The sink declares the capability; the user is not asked to reason about it.
            //
            // Idempotence only. Transactionality was once accepted here as well, and it is not the
            // same property: an atomic batch of inserts that committed, and is then re-sent because
            // the chunk resumed behind it, duplicates exactly as a non-atomic one would. Treating
            // the two as equivalent gave every transactional insert sink a fifty-batch resume
            // window — twenty-five thousand rows at the default read size — silently re-written
            // after any crash.
            // A sink that decides later has not decided yet. Every count and every index entry
            // this chunk writes before settling is provisional, and saying so is the difference
            // between a record search that answers correctly and one that answers confidently.
            boolean verdictIsPending = sinkSession.capabilities().commitIsAsynchronous();

            boolean sinkIsIdempotent = sinkSession.capabilities().writeIsIdempotent();
            int checkpointInterval = chunking.effectiveCheckpointInterval(sinkIsIdempotent);

            if (checkpointInterval > 1) {
                log.debug("Chunk {} will save its resume position every {} batches "
                                + "({} sink can absorb a repeated write)",
                        split.index(), checkpointInterval,
                        sinkSession.capabilities().deliveryGuarantee());
            }

            // The sink's own preparation, before a single record is handed to it. An
            // asynchronous sink creates the remote job here — a bulk job, a staging table — and a
            // synchronous one inherits a default that does nothing.
            Preparation sinkJob = sinkSession.prepare();

            Source.SplitSpec spec = new Source.SplitSpec(
                    split.index(), split.spec(), "chunk " + split.index());

            try (Source.RecordStream stream = sourceSession.read(
                    spec, checkpoint.sourceCursor(), effective.readFetchSize())) {

                RecordBatch.Builder builder =
                        new RecordBatch.Builder(effective.writeBatchSize(), effective.maxBatchBytes());

                Instant lastFlush = clock.instant();
                // Nanoseconds spent inside the source since the last flush, subtracted from the
                // linger so it measures how long records have waited rather than how long they
                // took to arrive.
                long blockedInSourceNanos = 0;
                Instant lastHeartbeat = clock.instant();

                // Where the current window of reading began, and where the stream was when it did.
                // A read window is bounded by the batch it fills, because that is the only boundary
                // the engine can see: pages happen inside the connector, and asking every connector
                // to report them would change the SPI for all of them to gain a finer timestamp.
                Instant readWindowStart = clock.instant();
                JsonNode readWindowCursor = checkpoint.sourceCursor();
                int rowsThisWindow = 0;
                long bytesThisWindow = 0;

                // The record-transform stage, summed over the cycle. Time is kept in nanoseconds
                // because a script that takes 0.4ms per record over a thousand records is most of
                // a second, and rounding each one to a millisecond would report either zero or
                // double.
                int transformIn = 0;
                int transformOut = 0;
                long transformNanos = 0;

                // Progress since the last saved checkpoint. Held here rather than written each
                // time, so a one-record-per-call pipeline does not pay a database write per record.
                PendingProgress pending = new PendingProgress();

                DataRecord record;
                while (true) {
                    if (readThisChunk >= rowBudget) {
                        // Budget spent with the source still producing. The run continues in a new
                        // chunk starting from this cursor, which is what keeps each chunk exactly
                        // one budget long instead of however many rows a guessed key range held.
                        sourceExhausted = false;
                        break;
                    }
                    Instant askedSource = clock.instant();
                    try {
                        record = stream.next();
                    } catch (RuntimeException e) {
                        // The read that failed is worth an entry of its own. Without one, a source
                        // that dies mid-chunk leaves only the chunk's own error, which says the
                        // migration stopped but not which query it stopped on.
                        //
                        // The calls first, and they are what actually names the fault: a connector
                        // that retried twice and gave up reports three fetches, and the chunk's
                        // single exception says none of that.
                        stages.fetches(stream.drainFetches());
                        stages.failed(StageLogPort.Stage.READ, readWindowStart, rowsThisWindow,
                                bytesThisWindow, e);
                        throw e;
                    }
                    // Time the source spent producing nothing. The linger below is meant to answer
                    // "records are waiting, send them rather than hold them", and measuring it from
                    // the last flush counted the source's own latency as waiting: a connector that
                    // blocked six seconds polling a warehouse returned its first record into an
                    // interval that had already expired, and the engine wrote a batch of one
                    // record — then another of nine hundred and ninety-nine. On a slow source that
                    // is a stream of tiny writes to a destination that may charge per call.
                    blockedInSourceNanos += Duration.between(askedSource, clock.instant()).toNanos();

                    if (record == null) {
                        break;
                    }
                    read++;
                    readThisChunk++;
                    bytes += record.bytes();
                    rowsThisWindow++;
                    bytesThisWindow += record.bytes();

                    // Transformation happens before batching, not after, because a filter changes
                    // how many records a batch should hold and a splitter changes it again. A
                    // batch assembled from untransformed records would be the wrong size by the
                    // time it was written.
                    Instant recordStageStarted = clock.instant();
                    TransformOutcome transformed = apply(transform, pipeline, split, record);

                    // What the source produced, kept only until this batch is written and only
                    // when payloads are indexed at all. Without it the index can say what was sent
                    // and never what was read, which is precisely the comparison somebody makes
                    // when they suspect a transform of being the problem.
                    if (indexesPayloads && !transform.isIdentity() && !transformed.outputs().isEmpty()) {
                        sourceBySeq.put(record.seq(), record.payload());
                    }
                    // Accumulated across the cycle rather than logged per record: an entry per
                    // record would be the record index again, at the record index's cost, for a
                    // stage whose interesting number is how the count changed over a batch.
                    transformIn++;
                    transformOut += transformed.outputs().size();
                    transformNanos += Duration.between(recordStageStarted, clock.instant()).toNanos();

                    if (transformed.rejected()) {
                        // Already written to the dead-letter queue. Counted as produced-and-failed
                        // so the sink-side invariant still balances.
                        produced++;
                        failed++;
                        producedTotal++;
                        failedTotal++;
                        // Kept separately because the checkpoint is written per flush and this
                        // counter is not reset there. Without it these never reached the saved
                        // progress: a chunk that lost a hundred records to a broken script and a
                        // hundred and thirty-three to the destination reported a hundred and
                        // thirty-three failures against a produced count that included all two
                        // hundred and thirty-three — a percentage computed from two different
                        // populations, which is how the console came to show 19%.
                        transformFailedSinceFlush++;
                        indexUnsent(pipeline, split, stages, record, seqOffset,
                                RecordIndexPort.Outcome.TRANSFORM_FAILED, "TRANSFORM_FAILED",
                                transformed.failure());
                    } else if (transformed.outputs().isEmpty()) {
                        filtered++;
                        indexUnsent(pipeline, split, stages, record, seqOffset,
                                RecordIndexPort.Outcome.FILTERED, null, null);
                    } else {
                        produced += transformed.outputs().size();
                        producedTotal += transformed.outputs().size();
                        transformed.outputs().forEach(builder::add);
                    }

                    boolean lingerElapsed = Duration.between(lastFlush, clock.instant())
                            .minusNanos(blockedInSourceNanos)
                            .compareTo(effective.flushInterval()) >= 0;

                    if (builder.isFull() || lingerElapsed) {
                        // What the source was actually asked, before the window that collected it.
                        // Drained here rather than per record because it is the batch boundary that
                        // makes the two grains comparable: however many calls appear against one
                        // read window is exactly the fact the window itself cannot state.
                        stages.fetches(stream.drainFetches());
                        // The read window closes before the write opens, so the three stages sit
                        // in the log in the order they happened and their durations do not overlap.
                        stages.read(readWindowStart, readWindowCursor, stream.cursor(),
                                rowsThisWindow, bytesThisWindow, stream.describe());
                        stages.transformed(transformIn, transformOut, transformNanos,
                                bytesThisWindow, !transform.isIdentity());

                        BatchOutcome outcome = writeBatch(pipeline, split, sinkSession, builder,
                                transform, verdictIsPending, seqOffset, stages, sourceBySeq);
                        sourceBySeq.clear();
                        Sink.WriteResult result = outcome.result();
                        pending.accumulate(read, produced, result.written(),
                                result.failed() + transformFailedSinceFlush,
                                filtered, bytes, stream.cursor(),
                                Math.max(outcome.lastSeq(), pending.lastSeq()));
                        written += result.written();
                        failed += result.failed();
                        failedTotal += result.failed();
                        batches++;
                        lastFlush = clock.instant();
                        blockedInSourceNanos = 0;
                        read = 0;
                        produced = 0;
                        filtered = 0;
                        bytes = 0;
                        transformFailedSinceFlush = 0;

                        // A new cycle: the next read, its transforms and its write share a fresh
                        // trace id, which is what makes the log group into one story per batch.
                        stages.endCycle();
                        readWindowStart = clock.instant();
                        readWindowCursor = stream.cursor();
                        rowsThisWindow = 0;
                        bytesThisWindow = 0;
                        transformIn = 0;
                        transformOut = 0;
                        transformNanos = 0;

                        // Checked here rather than only at the end, which is the entire point of
                        // having a threshold: discovering after twenty million rows that none of
                        // them landed is not meaningfully better than discovering it never.
                        Optional<String> breach = pipeline.execution()
                                .rejectionBreach(producedTotal, failedTotal, false);

                        // Saved when due, and always before giving up, so the counts that justify
                        // the failure are durable and visible on the run rather than lost with the
                        // exception.
                        if (pending.batches() >= checkpointInterval || breach.isPresent()) {
                            checkpoint = persist(checkpoint, pending, seqOffset);
                        }
                        if (breach.isPresent()) {
                            throw new RejectionThresholdExceededException(
                                    split.index(), producedTotal, failedTotal, breach.get());
                        }

                        lastHeartbeat = heartbeatIfDue(pipeline, split, workerId, lastHeartbeat);
                    }
                }

                // The tail of the chunk. Without this, every chunk would lose up to one batch.
                // Also runs when the batch is empty but records were read, which happens when a
                // filter dropped every record since the last flush — those drops still have to be
                // counted and the cursor still has to advance past them.
                // Drained unconditionally, unlike the read window below. A call that returned rows
                // the chunk then skipped — a resumed chunk landing mid-result — still happened, was
                // still paid for, and is exactly the call somebody wonders about when a resume
                // looks slower than it should.
                stages.fetches(stream.drainFetches());

                if (rowsThisWindow > 0) {
                    // The tail of the reading, whether or not it produced a batch to write. A
                    // filter that dropped everything since the last flush still cost a read, and
                    // the log that omitted it would make the source look faster than it was.
                    stages.read(readWindowStart, readWindowCursor, stream.cursor(), rowsThisWindow,
                            bytesThisWindow, stream.describe());
                    stages.transformed(transformIn, transformOut, transformNanos, bytesThisWindow,
                            !transform.isIdentity());
                }

                if (!builder.isEmpty()) {
                    BatchOutcome outcome = writeBatch(pipeline, split, sinkSession, builder,
                            transform, verdictIsPending, seqOffset, stages, sourceBySeq);
                    sourceBySeq.clear();
                    Sink.WriteResult result = outcome.result();
                    pending.accumulate(read, produced, result.written(),
                            result.failed() + transformFailedSinceFlush, filtered,
                            bytes, stream.cursor(), Math.max(outcome.lastSeq(), pending.lastSeq()));
                    written += result.written();
                    failed += result.failed();
                    failedTotal += result.failed();
                    batches++;
                } else if (read > 0 || transformFailedSinceFlush > 0) {
                    // Records that never formed a batch because a script threw on every one of
                    // them. They still failed, and a chunk that saved no progress for them would
                    // re-read and re-fail them on resume.
                    pending.accumulate(read, produced, 0, transformFailedSinceFlush, filtered,
                            bytes, stream.cursor(), pending.lastSeq());
                    batches++;
                }

                // Always saved at the end, whatever the interval. Without this a chunk could
                // complete with its resume position dozens of batches behind what it actually did.
                if (pending.batches() > 0) {
                    checkpoint = persist(checkpoint, pending, seqOffset);
                }

                // Evaluated once more now the chunk is complete, this time without the sample
                // floor. A chunk of forty records that rejected every one of them never processed
                // enough for a percentage to be trusted mid-flight, and would otherwise slip past
                // the limit purely for being small.
                Optional<String> finalBreach = pipeline.execution()
                        .rejectionBreach(producedTotal, failedTotal, true);
                if (finalBreach.isPresent()) {
                    throw new RejectionThresholdExceededException(
                            split.index(), producedTotal, failedTotal, finalBreach.get());
                }
            }

            // Everything read and handed over; now the sink is told there is no more and, if it
            // works asynchronously, waited for. Inside the try-with-resources so the sessions are
            // still open, and after the loop so a sink that batches a whole chunk into one remote
            // job has the whole chunk.
            long lateFailures;
            try {
                lateFailures = settleSink(pipeline, split, sinkSession, sinkJob, sourceExhausted);
            } catch (ChunkParkedException parked) {
                // Not a failure. The records are with the destination and the chunk is being put
                // down so the worker can do something else; the counts stay exactly as they are.
                throw parked;
            } catch (RuntimeException e) {
                // The destination refused the whole batch after taking it. Those records were
                // counted as written when they were handed over, and they were not written — so
                // the count is corrected before the failure propagates, or the run reports having
                // written records that the destination never accepted.
                checkpoints.save(checkpoint.recordingLateFailures(checkpoint.recordsWritten()));
                throw e;
            }
            if (lateFailures > 0) {
                failed += lateFailures;
                failedTotal += lateFailures;
                checkpoint = checkpoints.save(checkpoint.recordingLateFailures(lateFailures));

                Optional<String> breach = pipeline.execution()
                        .rejectionBreach(producedTotal, failedTotal, true);
                if (breach.isPresent()) {
                    throw new RejectionThresholdExceededException(
                            split.index(), producedTotal, failedTotal, breach.get());
                }
            }
        }

        // A chunk that knows its own size must deliver it. Only a connector that counted at
        // planning time sets this — a Databricks manifest, not a guessed key range — so a
        // shortfall is not a soft signal: the source said a thousand rows and produced eight
        // hundred, or none.
        //
        // Worth failing over rather than logging, because the failure it catches is invisible.
        // A source that answers an empty result for a chunk that has rows reads nothing,
        // transforms nothing, writes nothing, and completes: the run reports success, the chunk
        // reports COMPLETED, and thirty-six thousand records are simply absent. That happened,
        // and nothing anywhere said so. Retrying is the right response — the commonest cause is
        // a source that was restarted, moved on, or briefly lost the query behind the chunk.
        if (split.plannedRows() > 0 && checkpoint.recordsRead() != split.plannedRows()) {
            throw new ChunkShortfallException(split.index(), split.plannedRows(),
                    checkpoint.recordsRead());
        }

        ChunkResult result = new ChunkResult(
                checkpoint.recordsRead(), checkpoint.recordsProduced(), checkpoint.recordsWritten(),
                checkpoint.recordsFailed(), checkpoint.recordsFiltered(), checkpoint.bytesRead(),
                stages.sinkCalls, batches, sourceExhausted);

        if (result.unaccounted() != 0) {
            // Records that entered the sink stage and were neither written nor rejected have gone
            // missing. Logged at error because a migration that loses rows silently is the worst
            // failure this system has.
            log.error("Chunk {} of run {} finished with {} unaccounted record(s): "
                            + "produced={} written={} failed={}",
                    split.index(), split.runId(), result.unaccounted(),
                    result.recordsProduced(), result.recordsWritten(), result.recordsFailed());
        }

        log.info("Chunk {} of run {} finished: read={} produced={} written={} failed={} "
                        + "filtered={} batches={}",
                split.index(), split.runId(), result.recordsRead(), result.recordsProduced(),
                result.recordsWritten(), result.recordsFailed(), result.recordsFiltered(), batches);

        return result;
    }

    /**
     * Writes one batch to the sink.
     *
     * <p>Separated from saving the resume position so the two can happen at different rates. The
     * ordering between them is unchanged and not negotiable: the sink accepts the batch first, and
     * only then may the position advance. Reversing that would resume past records the sink never
     * took.
     */
    private BatchOutcome writeBatch(ResolvedPipeline pipeline, Split split,
                                    Sink.SinkSession sinkSession, RecordBatch.Builder builder,
                                    RecordTransform transform, boolean verdictIsPending,
                                    long seqOffset, StageRecorder stages,
                                     Map<Long, com.fasterxml.jackson.databind.JsonNode> sourceBySeq) {
        RecordBatch drained = builder.drain();

        // Taken from the batch as read, before any division. Groups reorder records, so the last
        // record of the last group is not the last record of the batch — and this number is the
        // checkpoint's resume coordinate.
        long lastSeq = drained.lastSeq();

        List<List<DataRecord>> groups = divide(pipeline, transform, drained);

        int written = 0;
        int failed = 0;
        long bytesWritten = 0;
        List<Sink.RecordError> errors = new ArrayList<>();

        for (List<DataRecord> group : groups) {
            // Each group is written, dead-lettered and indexed before the next one starts. If a
            // later group throws, the chunk is retried from its checkpoint and the earlier groups
            // are written again — the at-least-once behaviour a partly-written batch has always
            // had, rather than a new failure mode introduced by dividing it.
            Sink.WriteResult result = writeGroup(pipeline, split, sinkSession, transform,
                    RecordBatch.of(group), verdictIsPending, seqOffset, stages, sourceBySeq);

            written += result.written();
            failed += result.failed();
            bytesWritten += result.bytesWritten();
            errors.addAll(result.errors());
        }

        // Merged so the caller sees one answer for the batch it handed over, whether that was one
        // call on the sink or fifty. Every count the run reports is derived from this.
        return new BatchOutcome(
                new Sink.WriteResult(written, failed, bytesWritten, List.copyOf(errors)), lastSeq);
    }


    /**
     * Calls the destination, retrying the call itself while it is worth retrying.
     *
     * <p><b>The call, not the chunk.</b> The batch is already in memory, so a destination that
     * hiccups costs a short wait and nothing else — no second query against the source, no second
     * pass of the transforms. That distinction is the whole point: a two-second outage used to
     * dead-letter five hundred records or, worse, re-read the chunk three times to reach the same
     * request again.
     *
     * <p>Only while the connector says the failure is worth retrying. A 503 or a timeout is the
     * destination being briefly unwell; a 400 is this payload being unacceptable, and sending it
     * again unchanged is a waste of the destination's time and the run's.
     */
    /** The connector's own classification where it has one, the exception's type otherwise. */
    private static String errorCodeOf(Throwable failure) {
        return failure instanceof com.dmp.connector.api.ConnectorException connector
                ? connector.kind().name()
                : failure.getClass().getSimpleName();
    }

    private Sink.WriteResult callSink(Sink.SinkSession sinkSession, RecordBatch batch,
                                      StageRecorder stages, Split split) {
        RuntimeException last = null;
        String destination = stages.pipeline.sinkInstance().connectorType();

        for (int attempt = 1; attempt <= SINK_CALL_ATTEMPTS; attempt++) {
            stages.sinkCalls++;
            // Measured per attempt, not per batch. A destination that fails twice and succeeds on
            // the third is three calls, and averaging them into one would hide both the failures
            // and how long they took to fail — which is the shape of an outage.
            Instant callStarted = clock.instant();
            try {
                Sink.WriteResult result = sinkSession.write(batch);
                metrics.sinkCall(destination, Duration.between(callStarted, clock.instant()), true);
                metrics.records("written", destination, result.written());
                metrics.records("refused", destination, result.failed());
                return result;
            } catch (RuntimeException e) {
                metrics.sinkCall(destination, Duration.between(callStarted, clock.instant()), false);
                last = e;
                boolean worthRetrying = e instanceof com.dmp.connector.api.ConnectorException connector
                        && connector.isRetryable();
                if (!worthRetrying || attempt == SINK_CALL_ATTEMPTS) {
                    throw e;
                }
                Duration backoff = SINK_CALL_BACKOFF.multipliedBy(attempt);
                log.warn("Chunk {} could not write {} record(s) to the destination "
                                + "(attempt {} of {}): {}. Retrying the call in {}s.",
                        split.index(), batch.size(), attempt, SINK_CALL_ATTEMPTS,
                        e.getMessage(), backoff.toSeconds());
                try {
                    Thread.sleep(backoff.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        throw last;
    }

    /**
     * Records a delivery that failed as a whole, and reports it as this group's result.
     *
     * <p>Delivery is the batch script and the call together — the script runs per group,
     * immediately before it — so a failure in either loses the same records for the same group and
     * is recorded the same way. What differs is the reason, and that is carried through: a script
     * that threw and a destination that refused are different problems and a support desk must be
     * able to tell them apart.
     *
     * <p>Written to the dead-letter queue as well as the index, so these records can be replayed
     * once whatever caused it is fixed. Returned rather than thrown, because the next group may be
     * perfectly deliverable and the run's own rejection threshold is what decides whether this
     * chunk has failed.
     */
    private Sink.WriteResult deliveryFailed(ResolvedPipeline pipeline, Split split,
                                            RecordBatch batch, long seqOffset,
                                            StageRecorder stages,
                                            Map<Long, com.fasterxml.jackson.databind.JsonNode> sourceBySeq,
                                            RecordIndexPort.Outcome outcome, String code,
                                            String message) {
        List<Sink.RecordError> errors = new ArrayList<>(batch.size());
        for (DataRecord record : batch.records()) {
            errors.add(new Sink.RecordError(record.seq(), record.key(), code, message,
                    record.payload()));
        }
        persistRecordErrors(pipeline, split, errors);
        indexDeliveryFailure(pipeline, split, batch, seqOffset, stages, sourceBySeq, outcome, code,
                message);

        return new Sink.WriteResult(0, batch.size(), 0, errors);
    }

    /**
     * Divides a batch into the groups the sink will be called with.
     *
     * <p>Three shapes, one of which is "no division at all" and is overwhelmingly the common case.
     * Returning a single-element list for it keeps the caller free of a special case, at the cost
     * of one wrapper object per batch.
     *
     * <p><b>A group never crosses a batch.</b> The engine holds a batch, never a chunk, so two
     * records sharing a label in different batches are two separate calls on the sink. Grouping
     * shapes each batch as it passes rather than collecting records from across the run, which
     * would mean buffering a whole chunk — the one thing the streaming design exists to prevent.
     */
    private List<List<DataRecord>> divide(ResolvedPipeline pipeline, RecordTransform transform,
                                          RecordBatch batch) {
        List<DataRecord> records = batch.records();

        if (transform.hasSplitStage()) {
            List<String> labels = transform.split(records);

            // Insertion-ordered, so groups are written in the order their first record appeared.
            // A sink receiving them in hash order would make the same pipeline behave differently
            // between runs, which is indistinguishable from a bug when something goes wrong.
            Map<String, List<DataRecord>> byLabel = new LinkedHashMap<>();
            for (int i = 0; i < records.size(); i++) {
                byLabel.computeIfAbsent(labels.get(i), key -> new ArrayList<>()).add(records.get(i));
            }
            return List.copyOf(byLabel.values());
        }

        int groupSize = pipeline.delivery().groupSize();
        if (groupSize <= DeliveryPolicy.WHOLE_BATCH || groupSize >= records.size()) {
            return List.of(records);
        }

        List<List<DataRecord>> groups = new ArrayList<>((records.size() / groupSize) + 1);
        for (int from = 0; from < records.size(); from += groupSize) {
            groups.add(records.subList(from, Math.min(from + groupSize, records.size())));
        }
        return groups;
    }

    /**
     * Applies the batch script to one group and writes it.
     *
     * <p>The script runs per group rather than per batch, because a group is what the sink is
     * actually handed: a script building a request envelope has to describe the records in that
     * request, and describing the whole batch would put a header on each call claiming records the
     * other calls carried.
     */
    private Sink.WriteResult writeGroup(ResolvedPipeline pipeline, Split split,
                                        Sink.SinkSession sinkSession, RecordTransform transform,
                                        RecordBatch group, boolean verdictIsPending,
                                        long seqOffset, StageRecorder stages,
                                        Map<Long, com.fasterxml.jackson.databind.JsonNode> sourceBySeq) {
        RecordBatch batch = group;

        // A batch script either rewrites the records or produces the payload the sink sends. It
        // may never change how many records there are — the count is what the run has already
        // reported as read, and the transform API rejects a mismatched array before this point.
        if (transform.hasBatchStage()) {
            Instant batchStageStarted = clock.instant();
            BatchResult outcome;
            try {
                outcome = transform.applyBatch(batch.records());
            } catch (RuntimeException e) {
                // The group fails, not the chunk. A batch script is part of delivering this group
                // — it runs per group, immediately before the call — so a script that throws on
                // one group's records says nothing about the next group's, which may be entirely
                // different data. Failing the chunk here would re-query the source to re-run a
                // deterministic script over the same records and get the same exception.
                //
                // Unlike a record script this cannot be blamed on one record: it ran over the
                // whole group, so the whole group is what failed.
                stages.failed(StageLogPort.Stage.TRANSFORM, batchStageStarted, batch.size(),
                        batch.totalBytes(), e);
                return deliveryFailed(pipeline, split, batch, seqOffset, stages, sourceBySeq,
                        RecordIndexPort.Outcome.TRANSFORM_FAILED, "BATCH_TRANSFORM_FAILED",
                        e.getMessage());
            }
            stages.transform(batchStageStarted, TransformStage.BATCH, batch.size(), batch.size(),
                    batch.totalBytes());

            if (outcome.replacesRecords()) {
                List<DataRecord> original = batch.records();
                List<DataRecord> rewritten = new ArrayList<>(original.size());
                for (int i = 0; i < original.size(); i++) {
                    // withPayload, not a fresh record: the sequence number is the checkpoint's
                    // resume coordinate and the key drives idempotent writes. A script rewrites
                    // the document, never the engine's coordinates for it.
                    rewritten.add(original.get(i).withPayload(outcome.replacements().get(i)));
                }
                batch = RecordBatch.of(rewritten);

            } else if (outcome.hasEnvelope()) {
                // Refused rather than ignored. An envelope is the single payload a sink posts in
                // one request; a sink writing records individually has nothing to apply it to, so
                // the run would finish having changed nothing — indistinguishable from a broken
                // script, and only discoverable by inspecting the destination afterwards.
                //
                // Checked here rather than before the chunk starts, because whether a script
                // produces an envelope or rewrites the records is only knowable by running it.
                if (!sinkSession.capabilities().sendsBatchAsSinglePayload()) {
                    throw new com.dmp.connector.api.ConnectorException(
                            com.dmp.connector.api.ConnectorException.Kind.CONFIGURATION,
                            "The Batch transform returned an object, which is the payload for a "
                                    + "sink that posts a whole batch in one request. The sink '"
                                    + pipeline.sinkNode().name() + "' ("
                                    + pipeline.sinkInstance().connectorType() + ") writes records "
                                    + "individually and cannot use it, so the script would have no "
                                    + "effect. To change the records themselves, return an array "
                                    + "of them instead — one entry per record, same order.");
                }
                batch = batch.withEnvelope(outcome.envelope());
            }
        }

        Instant callStarted = clock.instant();
        Sink.WriteResult result;
        try {
            result = callSink(sinkSession, batch, stages, split);
        } catch (RuntimeException e) {
            // The destination refused this call and went on refusing it. The group fails; the
            // chunk does not. Retrying the chunk would re-query the source and re-run every
            // transform to reach the same call again — which is what it used to do, three times,
            // for a single failed request.
            stages.failed(StageLogPort.Stage.WRITE, callStarted, batch.size(),
                    batch.totalBytes(), e);
            return deliveryFailed(pipeline, split, batch, seqOffset, stages, sourceBySeq,
                    RecordIndexPort.Outcome.CALL_FAILED, errorCodeOf(e), e.getMessage());
        }
        stages.write(callStarted, batch, result);

        if (result.hasFailures()) {
            persistRecordErrors(pipeline, split, result.errors());
        }

        // Indexed after the write and before the caller advances the checkpoint. That order is the
        // same guarantee the checkpoint itself has: if this throws, the chunk is retried and the
        // records are indexed on the next attempt, whereas indexing first would claim a record was
        // written whenever the process died between the two.
        indexRecords(pipeline, split, batch, result, verdictIsPending, seqOffset, stages, sourceBySeq);

        return result;
    }

    /**
     * Records what became of each record in this batch, so it can be found by key later.
     *
     * <p>Opt-in per pipeline, because it costs roughly a hundred bytes per record forever — nothing
     * for a migration of thousands, gigabytes for one of crores, and worth it only where somebody
     * will actually be asked "did customer 88291 come across?".
     *
     * <p>Records with no key are indexed too, with a null one. They cannot be found by key, but
     * they are in the run's own list — and leaving them out made the index report fewer records
     * than the run moved, which is the one thing an audit trail may not do.
     *
     * @param seqOffset what this chunk had already read before the current stream was opened. A
     *                  source numbers from one each time it is opened, so a resumed chunk repeats
     *                  numbers it has used; the entry's identity has to be the position within the
     *                  chunk, or a resume files its re-indexed records as new ones.
     */
    private void indexRecords(ResolvedPipeline pipeline, Split split, RecordBatch batch,
                              Sink.WriteResult result, boolean verdictIsPending, long seqOffset,
                              StageRecorder stages,
                              Map<Long, com.fasterxml.jackson.databind.JsonNode> sourceBySeq) {

        if (!pipeline.audit().level().indexesEveryRecord() || batch.isEmpty()) {
            return;
        }

        Map<Long, Sink.RecordError> rejected = new HashMap<>();
        for (Sink.RecordError error : result.errors()) {
            rejected.put(error.seq(), error);
        }

        AuditPolicy audit = pipeline.audit();
        Instant now = clock.instant();
        Instant expiresAt = now.plus(audit.indexRetention());
        boolean withPayloads = audit.indexesPayloads();

        List<RecordIndexPort.RecordIndexEntry> entries = new ArrayList<>(batch.size());
        for (DataRecord record : batch.records()) {
            Sink.RecordError failure = rejected.get(record.seq());
            entries.add(new RecordIndexPort.RecordIndexEntry(
                    split.tenantId(),
                    pipeline.version().pipelineId(),
                    split.runId(),
                    split.id(),
                    stages.traceId(),
                    seqOffset + record.seq(),
                    record.ordinal(),
                    record.key() == null || record.key().isBlank() ? null : record.key(),
                    // An asynchronous sink has not looked at this record yet. Recording WRITTEN here
                    // because the batch was accepted is how the index came to answer "yes, it was
                    // transferred" for records the destination went on to refuse.
                    failure != null
                            ? RecordIndexPort.Outcome.REJECTED
                            : verdictIsPending
                                    ? RecordIndexPort.Outcome.SENT
                                    : RecordIndexPort.Outcome.WRITTEN,
                    failure == null ? null : failure.code(),
                    // Redacted and size-capped exactly as a dead-lettered payload is. An index is
                    // not an exemption from the policy the rest of the platform obeys, and a search
                    // index is the worse place to leak into — it exists to be queried.
                    withPayloads
                            ? Redaction.apply(
                                    Payloads.truncate(record.payload(), audit.maxPayloadBytes()),
                                    audit)
                            : null,
                    withPayloads && sourceBySeq.get(record.seq()) != null
                            ? Redaction.apply(
                                    Payloads.truncate(sourceBySeq.get(record.seq()),
                                            audit.maxPayloadBytes()),
                                    audit)
                            : null,
                    failure == null ? null : failure.message(),
                    now,
                    expiresAt));
        }

        recordIndex.indexAll(entries);
    }

    /**
     * Marks every record of a delivery that failed as a whole.
     *
     * <p>Two ways to get here and one consequence: a batch script that threw over the group, or a
     * destination that refused the call. Either way none of these records arrived, and before this
     * existed none of them was recorded anywhere — the exception unwound past indexing, so a
     * support search for any of them returned nothing, which reads as "we never received it".
     *
     * <p>The reason is carried through rather than flattened. A script that broke and a
     * destination that refused are different problems with different fixes, and the code and the
     * message are what tell them apart.
     *
     * <p>Best-effort: the delivery has already failed and is about to be reported. If the index is
     * unreachable too, the delivery's own failure is the one worth keeping.
     */
    private void indexDeliveryFailure(ResolvedPipeline pipeline, Split split, RecordBatch batch,
                                      long seqOffset, StageRecorder stages,
                                      Map<Long, com.fasterxml.jackson.databind.JsonNode> sourceBySeq,
                                      RecordIndexPort.Outcome outcome, String code,
                                      String message) {
        if (!pipeline.audit().level().indexesEveryRecord() || batch.isEmpty()) {
            return;
        }

        AuditPolicy audit = pipeline.audit();
        Instant now = clock.instant();
        Instant expiresAt = now.plus(audit.indexRetention());
        boolean withPayloads = audit.indexesPayloads();

        List<RecordIndexPort.RecordIndexEntry> entries = new ArrayList<>(batch.size());
        for (DataRecord record : batch.records()) {
            entries.add(new RecordIndexPort.RecordIndexEntry(
                    split.tenantId(), pipeline.version().pipelineId(), split.runId(), split.id(),
                    stages.traceId(), seqOffset + record.seq(), record.ordinal(),
                    record.key() == null || record.key().isBlank() ? null : record.key(),
                    outcome, code,
                    withPayloads
                            ? Redaction.apply(
                                    Payloads.truncate(record.payload(), audit.maxPayloadBytes()),
                                    audit)
                            : null,
                    withPayloads && sourceBySeq.get(record.seq()) != null
                            ? Redaction.apply(
                                    Payloads.truncate(sourceBySeq.get(record.seq()),
                                            audit.maxPayloadBytes()),
                                    audit)
                            : null,
                    message, now, expiresAt));
        }

        try {
            recordIndex.indexAll(entries);
        } catch (RuntimeException indexFailure) {
            log.warn("Chunk {} of run {} could not record the {} record(s) in a delivery that "
                            + "failed. They will have no index entry for this attempt.",
                    split.index(), split.runId(), batch.size(), indexFailure);
        }
    }

    /**
     * Indexes a record that left the pipeline before the destination ever saw it.
     *
     * <p>Until this existed the index held only records that reached a sink, so a record dropped by
     * a filter and a record that never existed were the same empty search result. That is the worst
     * answer a support screen can give: "we have no evidence of this order" reads as "we never
     * received it", when the truth may be "your transform excluded it" or "your script threw on
     * it, and the reason is in the dead-letter queue".
     *
     * <p>Written one at a time rather than batched with the sink's entries, because these records
     * never join a batch — that is precisely what makes them invisible. The volume is the volume of
     * filtering, which is a property of the pipeline the user configured.
     *
     * <p>The payload obeys the same audit policy as everything else. A record you chose not to send
     * is still customer data, and a search index is the worse place to leak into: it exists to be
     * queried.
     */
    private void indexUnsent(ResolvedPipeline pipeline, Split split, StageRecorder stages,
                             DataRecord record, long seqOffset,
                             RecordIndexPort.Outcome outcome, String errorCode,
                             String errorMessage) {

        AuditPolicy audit = pipeline.audit();
        if (!audit.level().indexesEveryRecord()) {
            return;
        }

        Instant now = clock.instant();
        recordIndex.indexAll(List.of(new RecordIndexPort.RecordIndexEntry(
                split.tenantId(),
                pipeline.version().pipelineId(),
                split.runId(),
                split.id(),
                stages.traceId(),
                seqOffset + record.seq(),
                record.ordinal(),
                record.key() == null || record.key().isBlank() ? null : record.key(),
                outcome,
                errorCode,
                audit.indexesPayloads()
                        ? Redaction.apply(
                                Payloads.truncate(record.payload(), audit.maxPayloadBytes()), audit)
                        : null,
                // The record never reached a transform's output, so there is no "before" distinct
                // from what is already stored above.
                null,
                errorMessage,
                now,
                now.plus(audit.indexRetention()))));
    }

    /**
     * Runs the pipeline's per-record transforms.
     *
     * <p>A script failing on one record rejects that record; it does not fail the chunk. One
     * malformed row in a million must not stop a migration — the same rule the sink's own
     * rejections follow — and the record is captured with its error so it can be fixed and
     * replayed.
     */
    private TransformOutcome apply(RecordTransform transform, ResolvedPipeline pipeline,
                                   Split split, DataRecord record) {
        if (transform.isIdentity()) {
            return new TransformOutcome(List.of(record), false);
        }
        try {
            return new TransformOutcome(transform.applyRecord(record), false);
        } catch (TransformException e) {
            // The exception has always carried which node threw; this used to discard it.
            persistRecordErrors(pipeline, split, List.of(new Sink.RecordError(
                            record.seq(), record.key(), "TRANSFORM_FAILED", e.getMessage(),
                            record.payload())),
                    e.nodeId() == null ? pipeline.sinkNode().id() : e.nodeId());
            return new TransformOutcome(List.of(), true, e.getMessage());
        }
    }

    /**
     * What a transform did with one record.
     *
     * <p>A script choosing to drop a record and a script blowing up on it both produce no output,
     * and counting them the same would let a broken transform look like a working filter. The flag
     * keeps them apart: a drop is {@code filtered}, a failure is {@code failed}.
     */
    private record TransformOutcome(List<DataRecord> outputs, boolean rejected, String failure) {

        TransformOutcome(List<DataRecord> outputs, boolean rejected) {
            this(outputs, rejected, null);
        }
    }

    /**
     * Closes the sink's work for this chunk and waits for the destination to decide.
     *
     * <p>Without this the asynchronous half of the connector SPI was specified, documented in
     * ADR-0012 and implemented by connectors — and never driven. A Salesforce chunk staged its
     * records to disk, reported every one as written, and uploaded nothing: the run completed, the
     * counters agreed with each other, and the org was empty. Exactly the shape of failure this
     * platform exists to make impossible.
     *
     * <p>A synchronous sink inherits defaults that make all of this a no-op, so a database or a
     * file pays a few method calls and nothing else.
     *
     * @return how many records the destination rejected after having accepted them
     * @throws ChunkParkedException if the destination has not finished deciding
     */
    private long settleSink(ResolvedPipeline pipeline, Split split, Sink.SinkSession sinkSession,
                            Preparation sinkJob, boolean sourceExhausted) {

        Preparation committed = sinkSession.commit(sinkJob);

        if (sinkSession.capabilities().commitIsAsynchronous()) {
            // Asked once, here, for three reasons: a job that finished while we were still
            // uploading needs no park at all; a job the destination has already failed should say
            // so before anything is written down; and the connector's answer carries the interval
            // to wait, which nothing else knows.
            Preparation.Status status = sinkSession.checkCommit(committed);

            if (status.isFailed()) {
                throw destinationRejected(split, status);
            }
            if (!status.isReady()) {
                throw new ChunkParkedException(split.index(), committed, pollInterval(status),
                        sourceExhausted);
            }
        }
        return harvestAndRelease(pipeline, split, sinkSession, committed);
    }

    /**
     * Finishes a chunk that was parked on a remote job the destination has now completed.
     *
     * <p>No source is opened and no record is read. Every count this chunk will ever report is
     * already in its checkpoint, durably, from before it parked — all that is left is to ask the
     * destination which records it refused, put those in the dead-letter queue, and let the remote
     * job go.
     *
     * <p>The sink session is a fresh one, on whichever pod happened to claim the chunk. That works
     * because the SPI requires a connector to put everything it needs to resume into the handle and
     * to hold nothing in instance fields — the same contract that lets the poller ask about the job
     * from a third pod that never touched either.
     */
    private ChunkResult settleParkedChunk(ResolvedPipeline pipeline, Split split, String workerId) {
        JsonNode parked = split.externalJob();
        Preparation committed = ChunkParkedException.sinkJobOf(parked);
        boolean sourceExhausted = ChunkParkedException.sourceWasExhausted(parked);

        Checkpoint checkpoint = checkpoints.findOrCreate(
                split.tenantId(), split.runId(), split.id());

        ConnectorContext sinkContext = contexts.forChunk(pipeline.sinkInstance(),
                split.runId().toString(), workerId, split.index(), true);

        long lateFailures;
        try (Sink.SinkSession sinkSession =
                     connectors.sink(pipeline.sinkInstance().connectorType()).openSink(sinkContext)) {

            Preparation.Status status = sinkSession.checkCommit(committed);

            if (status.isFailed()) {
                // The handle is not cleared. The chunk is about to be abandoned — a destination
                // that failed a whole job will fail an identical one — and keeping the id on the
                // record is what lets somebody go and look at the job in the org afterwards.
                throw destinationRejected(split, status);
            }
            if (!status.isReady()) {
                // Claimed a shade early. Park again rather than waiting: the cost is one Mongo
                // write, and the alternative is holding a slot for a job that is not ready.
                throw new ChunkParkedException(split.index(), committed, pollInterval(status),
                        sourceExhausted);
            }

            lateFailures = harvestAndRelease(pipeline, split, sinkSession, committed);
        }

        if (lateFailures > 0) {
            checkpoint = checkpoints.save(checkpoint.recordingLateFailures(lateFailures));

            Optional<String> breach = pipeline.execution().rejectionBreach(
                    checkpoint.recordsProduced(), checkpoint.recordsFailed(), true);
            if (breach.isPresent()) {
                throw new RejectionThresholdExceededException(split.index(),
                        checkpoint.recordsProduced(), checkpoint.recordsFailed(), breach.get());
            }
        }

        log.info("Chunk {} of run {} settled after the destination finished: "
                        + "read={} written={} failed={} (of which {} refused after acceptance)",
                split.index(), split.runId(), checkpoint.recordsRead(),
                checkpoint.recordsWritten(), checkpoint.recordsFailed(), lateFailures);

        return new ChunkResult(
                checkpoint.recordsRead(), checkpoint.recordsProduced(), checkpoint.recordsWritten(),
                checkpoint.recordsFailed(), checkpoint.recordsFiltered(), checkpoint.bytesRead(),
                checkpoint.batchesCommitted(), sourceExhausted);
    }

    /**
     * Asks what the destination made of the records, and lets the remote job go.
     *
     * <p>The count and the detail are separate answers. A sink may report "five thousand were
     * refused" while naming none of them, because naming them costs a download of every rejected
     * row and not every pipeline is going to keep those payloads. The run's totals are taken from
     * the count either way, so the numbers stay right whether or not anything is stored.
     *
     * @return how many records the destination refused after having accepted them
     */
    private long harvestAndRelease(ResolvedPipeline pipeline, Split split,
                                   Sink.SinkSession sinkSession, Preparation committed) {
        Sink.Harvest harvest = sinkSession.harvest(committed);

        if (harvest.hasDetail()) {
            log.warn("Chunk {} of run {}: the destination refused {} record(s) after accepting "
                            + "them; they are in the dead-letter queue",
                    split.index(), split.runId(), harvest.errors().size());
            persistRecordErrors(pipeline, split, harvest.errors());

        } else if (harvest.failedCount() > 0) {
            // Counted but not collected. Said plainly, because the alternative is somebody finding
            // a run reporting thousands of failures with an empty dead-letter queue and concluding
            // the platform lost them.
            log.warn("Chunk {} of run {}: the destination refused {} record(s) after accepting "
                            + "them. The individual records were not collected, so there is nothing "
                            + "to replay for them — this sink is configured to report the count only.",
                    split.index(), split.runId(), harvest.failedCount());
        }

        // Idempotent by contract, and called here as well as by the reaper — a remote job left
        // behind consumes the destination's quota long after the run that made it has finished.
        sinkSession.release(committed);
        return harvest.failedCount();
    }

    /**
     * A whole job the destination failed, classified as non-retryable on purpose.
     *
     * <p>Salesforce fails a job for reasons that do not change on a second attempt: an object that
     * does not exist, a field the integration user cannot write, a line ending that does not match
     * what the job was told to expect. Re-uploading the same records finds the same answer, and
     * against a system that meters bulk jobs it finds it at a price.
     */
    private static com.dmp.connector.api.ConnectorException destinationRejected(
            Split split, Preparation.Status status) {
        return new com.dmp.connector.api.ConnectorException(
                com.dmp.connector.api.ConnectorException.Kind.CONFIGURATION,
                "The destination rejected chunk " + split.index() + ": " + status.message());
    }

    /** The connector's chosen interval, with a floor so a zero cannot turn into a hot loop. */
    public static Duration pollInterval(Preparation.Status status) {
        Duration asked = status.retryAfter();
        return asked == null || asked.compareTo(MIN_POLL_INTERVAL) < 0 ? MIN_POLL_INTERVAL : asked;
    }

    /**
     * Writes the accumulated progress as the new resume position, and clears it.
     *
     * <p>The sequence is offset by whatever the chunk had already reached. A source restarts its
     * record counter at one every time a stream is opened — it counts what <em>this</em> read has
     * emitted, not where the chunk is — so a chunk resumed at sequence 2,400 immediately proposes
     * 200 and the monotonic guard refuses it.
     *
     * <p>That refusal failed every mid-chunk resume there has ever been: the chunk exhausted its
     * attempts on the same error each time and was abandoned. It went unnoticed because a chunk
     * killed between batches resumes at sequence zero, where the offset is zero and nothing looks
     * wrong — only a process dying <em>inside</em> a chunk hits it, which is exactly the case the
     * checkpoint exists for.
     */
    private Checkpoint persist(Checkpoint checkpoint, PendingProgress pending, long seqOffset) {
        Checkpoint advanced = checkpoints.save(checkpoint.advance(
                pending.cursor(),
                seqOffset + pending.lastSeq(),
                pending.read(),
                pending.produced(),
                pending.written(),
                pending.failed(),
                pending.filtered(),
                pending.bytes(),
                clock.instant()));
        pending.clear();
        return advanced;
    }

    /**
     * A write's outcome and the sequence it reached.
     *
     * <p>Returned rather than stashed on the executor. The executor is a singleton shared by every
     * chunk this pod runs concurrently, so a field holding "the last sequence written" would be
     * overwritten by whichever chunk wrote most recently — and a chunk would then checkpoint at
     * another chunk's position, skipping its own records on resume.
     */
    private record BatchOutcome(Sink.WriteResult result, long lastSeq) {
    }

    /**
     * Records the calls one chunk makes, if its pipeline asked for that.
     *
     * <p>One instance per chunk execution, for the same reason {@link BatchOutcome} is returned
     * rather than stored: the executor is shared by every chunk on the pod, and a call counter on
     * it would number one chunk's calls with another chunk's total.
     *
     * <p>Every method here is best-effort. A failure to describe the work must never become a
     * failure of the work, so the port drops rather than throws and the timing is taken from a
     * clock the caller already holds.
     */
    private final class StageRecorder {

        private final ResolvedPipeline pipeline;
        private final Split split;
        private final StageLogPolicy policy;
        private final boolean enabled;
        private final Instant expiresAt;

        /**
         * Requests actually made of the destination during this execution.
         *
         * <p>Counted even when the stage log is switched off, because this is not logging: a rate
         * limit reserves a pessimistic number of calls before the chunk starts — one per record,
         * where a split script makes the real count unknowable in advance — and hands back what was
         * provably never used. Without a count there is nothing to hand back against.
         *
         * <p>A call that threw is still counted. We cannot tell a request that never left from one
         * whose response was lost, and assuming it arrived is the direction that cannot exceed
         * somebody's limit.
         */
        private long sinkCalls;

        /**
         * Which read → transform → write cycle the chunk is on.
         *
         * <p>Part of the trace id, and therefore derived rather than generated: a retried chunk
         * walks the same cycles in the same order, so it reuses the trace ids it had. That is what
         * lets a re-indexed record overwrite its predecessor instead of appearing twice under a
         * new trace.
         */
        private int cycle;

        /**
         * Every entry this chunk has written, in order, whatever stage it was.
         *
         * <p>The log's only reliable ordering. See {@code StageEntry#position}.
         */
        private int position;

        private int fetches;
        private int reads;
        private int transformsRun;
        private int writes;

        StageRecorder(ResolvedPipeline pipeline, Split split) {
            this.pipeline = pipeline;
            this.split = split;
            this.policy = pipeline.audit().stageLog();
            this.enabled = stageLog.isEnabled() && policy.logsAnything();
            // The same floor the record index uses. Stage entries hold no record content unless
            // bodies were asked for, so keeping them as long as the index they explain costs
            // little — and stops a run's entries outliving the explanation for them.
            this.expiresAt = clock.instant().plus(pipeline.audit().indexRetention());
        }

        /** The trace the current cycle's reads, records and writes all share. */
        String traceId() {
            return StageLogPort.Trace.of(split.id(), cycle);
        }

        /** Called once a batch has been written, so the next read begins a new cycle. */
        void endCycle() {
            cycle++;
        }

        boolean logsReads() {
            return enabled && policy.reads();
        }

        boolean logsTransforms() {
            return enabled && policy.transforms();
        }

        boolean logsWrites() {
            return enabled && policy.writes();
        }

        /**
         * The calls the source actually made, as the connector reported them.
         *
         * <p>Written before the read window that collected them, so the log reads in the order
         * things happened: the calls, then the window they filled, then the write.
         *
         * <p>Each entry keeps the connector's own timing rather than being measured here. A fetch
         * finished some time before it was drained, and dating it from the drain would attribute
         * the whole read window to the last call in it.
         */
        void fetches(List<Source.Fetch> fetches) {
            for (Source.Fetch fetch : fetches) {
                // Metered whatever the audit policy says. A stage-log entry is opt-in because it
                // holds bodies; a duration and a count are neither sensitive nor large, and the
                // question "is that source slower than last week" cannot be answered after the
                // fact from a store nobody switched on.
                metrics.sourceFetch(pipeline.sourceInstance().connectorType(),
                        java.time.Duration.ofMillis(fetch.durationMillis()), fetch.succeeded());
            }
            for (Source.Fetch fetch : fetches) {
                // Narrated whatever the audit policy says. The stage log is a store somebody
                // chose to switch on and pays for; this is the debug log, and a developer who has
                // turned DEBUG on has already said what they want. Parameterised, so a disabled
                // level costs the call and nothing else.
                log.debug("FETCH {} — {} in {}ms, {} row(s){}",
                        fetch.succeeded() ? "ok" : "FAILED",
                        fetch.reason(), fetch.durationMillis(), fetch.rows(),
                        fetch.succeeded() ? "" : ": " + fetch.errorCode() + " " + fetch.errorMessage());
                if (fetch.describe() != null) {
                    log.debug("      asked: {}", fetch.describe());
                }
                if (fetch.response() != null) {
                    log.debug("      answered: {}", Payloads.abbreviate(fetch.response(), 2_000));
                }
            }
            if (fetches.isEmpty() || !logsReads()) {
                return;
            }
            for (Source.Fetch fetch : fetches) {
                int rows = (int) Math.min(fetch.rows(), Integer.MAX_VALUE);
                // Why the call was made, alongside what was called. A URL answers the second
                // question and never the first, so two fetches against one chunk read as a
                // mystery — a retry? a second page? — until something says "column names" and
                // "result chunk 5".
                com.fasterxml.jackson.databind.node.ObjectNode details =
                        com.dmp.common.json.Json.newObject();
                if (fetch.reason() != null && !fetch.reason().isBlank()) {
                    details.put("reason", fetch.reason());
                }
                submit(StageLogPort.Stage.FETCH, pipeline.sourceNode().id(),
                        pipeline.sourceNode().name(), pipeline.sourceInstance().connectorType(),
                        this.fetches++, rows, rows, fetch.bytes(),
                        fetch.durationMillis(),
                        fetch.succeeded()
                                ? StageLogPort.Outcome.OK
                                : StageLogPort.Outcome.FAILED,
                        fetch.errorCode(), fetch.errorMessage(), fetch.describe(),
                        null, null, details.isEmpty() ? null : details,
                        policy.capturesBodies() ? fetch.request() : null,
                        policy.capturesBodies() ? fetch.response() : null);
            }
        }

        /**
         * One window of reading, bounded by the batch it filled.
         *
         * <p><b>Carries the query only when nothing else did.</b> A query describes a call, and a
         * connector that reports its own calls has already put it on the FETCH those rows came
         * from — repeating it here says a request was made when none was. That is not a
         * theoretical tidiness: two read windows filled from a single call, each showing the same
         * query, read as the query having run twice, and it was believed.
         *
         * <p>Kept for connectors that report no fetches, because for them this is the only place
         * the query can appear, and losing it would take away the answer to the commonest question
         * in any migration — <em>why did this move nothing?</em>
         */
        void read(Instant startedAt, JsonNode cursorIn, JsonNode cursorOut, int rows, long bytes,
                  String query) {
            log.debug("READ  {} row(s), {} byte(s), cursor {} -> {}",
                    rows, bytes, cursorIn, cursorOut);
            if (!logsReads()) {
                return;
            }
            submit(StageLogPort.Stage.READ, pipeline.sourceNode().id(),
                    pipeline.sourceNode().name(), pipeline.sourceInstance().connectorType(),
                    reads++, rows, rows, bytes, startedAt, StageLogPort.Outcome.OK, null, null,
                    fetches > 0 ? null : query, cursorIn, cursorOut, null, null, null);
        }

        /**
         * One pass of the scripts over a batch, and what it did to the count.
         *
         * <p>{@code recordsOut} is the whole point. A filter and a fan-out are both invisible in a
         * run's totals — forty read and thirty-one written could be nine dropped, nine rejected or
         * nine still in flight — and nothing else names the stage that changed the number.
         */
        void transform(Instant startedAt, TransformStage stage, int in, int out, long bytes) {
            log.debug("TRANSFORM ({}) {} -> {} record(s) through [{}]",
                    stage, in, out, nodeNamesFor(stage));
            if (!logsTransforms()) {
                return;
            }
            submit(StageLogPort.Stage.TRANSFORM, nodeIdsFor(stage), nodeNamesFor(stage), null,
                    transformsRun++, in, out, bytes, startedAt, StageLogPort.Outcome.OK,
                    null, null, null, null, null, whichStage(stage), null, null);
        }

        /**
         * Which transform stage an entry came from, for anyone reading the log.
         *
         * <p>Both stages are TRANSFORM entries and nothing distinguished them, so a console could
         * not tell the pass over every record from the pass over one delivery group — which is the
         * difference between "this ran once for the chunk" and "this ran once per call, and this
         * one is call two of three".
         */
        private com.fasterxml.jackson.databind.JsonNode whichStage(TransformStage stage) {
            com.fasterxml.jackson.databind.node.ObjectNode details =
                    com.dmp.common.json.Json.newObject();
            details.put("transformStage", stage.name());
            return details;
        }

        /**
         * The nodes that make up one transform stage, named as the user named them.
         *
         * <p>Joined rather than reported one at a time because a compiled transform runs its whole
         * chain as a unit and does not say which node spent the time. Naming the nodes it is made
         * of is honest; inventing a per-node breakdown from a single measurement would not be.
         */
        private String nodeIdsFor(TransformStage stage) {
            return pipeline.transforms().stream()
                    .filter(spec -> spec.stage() == stage)
                    .map(TransformSpec::nodeId)
                    .collect(java.util.stream.Collectors.joining(","));
        }

        private String nodeNamesFor(TransformStage stage) {
            return pipeline.transforms().stream()
                    .filter(spec -> spec.stage() == stage)
                    .map(TransformSpec::name)
                    .collect(java.util.stream.Collectors.joining(" → "));
        }

        /**
         * The record stage over one cycle, from totals the caller accumulated.
         *
         * <p>Takes elapsed nanoseconds rather than a start instant, because this stage did not run
         * in one contiguous span — it ran once per record, interleaved with reading. Passing a
         * start time would report the whole read-and-transform window as transform time and make
         * the source's cost look like the script's.
         *
         * <p>Silent when nothing runs at this stage. Judged from the compiled transform rather
         * than from the pipeline's declared nodes, because those are different things: a chain
         * that compiles to the identity does no work, and an entry saying ten records went in and
         * ten came out in zero milliseconds is noise in a log read as a sequence.
         */
        void transformed(int in, int out, long elapsedNanos, long bytes, boolean hasRecordStage) {
            if (hasRecordStage && in > 0) {
                log.debug("TRANSFORM (RECORD) {} -> {} record(s) in {}ms through [{}]",
                        in, out, elapsedNanos / 1_000_000, nodeNamesFor(TransformStage.RECORD));
            }
            if (!logsTransforms() || in == 0 || !hasRecordStage) {
                return;
            }
            Instant now = clock.instant();
            submit(StageLogPort.Stage.TRANSFORM, nodeIdsFor(TransformStage.RECORD),
                    nodeNamesFor(TransformStage.RECORD), null,
                    transformsRun++, in, out, bytes,
                    now.minusNanos(elapsedNanos), StageLogPort.Outcome.OK,
                    null, null, null, null, null, whichStage(TransformStage.RECORD), null, null);
        }

        /** One call handed to the destination, whatever it made of the records inside it. */
        void write(Instant startedAt, RecordBatch batch, Sink.WriteResult result) {
            log.debug("WRITE {} record(s) to {} — {} written, {} refused{}",
                    batch.size(), pipeline.sinkNode().name(), result.written(), result.failed(),
                    result.details() == null
                            ? ""
                            : "; destination said " + Payloads.abbreviate(result.details(), 2_000));
            if (!logsWrites()) {
                return;
            }
            // In is what was handed over, out is what the destination kept. They differ when it
            // refused some, and that difference is the whole reason to look at a write: a call
            // reporting "250 records" for a batch of which a hundred and ten were refused is true
            // about the request and wrong about the outcome, and the row gave no hint which it
            // meant. A sink that decides later reports everything written at this point, which is
            // also correct — nothing has been refused yet — and the SENT outcome on each record is
            // what says the verdict is still outstanding.
            submit(StageLogPort.Stage.WRITE, pipeline.sinkNode().id(), pipeline.sinkNode().name(),
                    pipeline.sinkInstance().connectorType(), writes++,
                    batch.size(), result.written(),
                    batch.totalBytes(), startedAt, StageLogPort.Outcome.OK, null, null, null,
                    // Deliberately no request body. It was the whole batch — five hundred records
                    // written a second time, into a second store, where they were already
                    // individually searchable in the record index and where nothing could find one
                    // of them anyway. It was routinely too large to keep and arrived truncated to
                    // a marker, so it answered nothing while costing the most of anything here.
                    // What a person needs off a call is what the call did, and that is
                    // result.details(): the status, the job id, the destination's own reply.
                    null, null, result.details(), null, null);
        }

        /**
         * A stage that did not come back.
         *
         * <p>The one entry that matters most, and the one the platform had no way to write. A
         * destination refusing an entire request produced a chunk-level failure with a percentage
         * and nothing else; this is where the status and the message live.
         */
        void failed(StageLogPort.Stage stage, Instant startedAt, int records, long bytes,
                    Throwable failure) {
            log.debug("{} FAILED after {} record(s): {}", stage, records, failure.toString());

            // A batch transform belongs to neither end of the pipeline, and omitting it here meant
            // a script that threw over a whole batch left the timeline showing a read and then
            // nothing at all.
            if (stage == StageLogPort.Stage.TRANSFORM) {
                if (!logsTransforms()) {
                    return;
                }
                submit(stage, nodeIdsFor(TransformStage.BATCH), nodeNamesFor(TransformStage.BATCH),
                        null, transformsRun++, records, 0, bytes, startedAt,
                        StageLogPort.Outcome.FAILED, codeOf(failure), failure.getMessage(),
                        null, null, null, whichStage(TransformStage.BATCH), null, null);
                return;
            }

            boolean read = stage == StageLogPort.Stage.READ;
            if (!(read ? logsReads() : logsWrites())) {
                return;
            }
            submit(stage,
                    read ? pipeline.sourceNode().id() : pipeline.sinkNode().id(),
                    read ? pipeline.sourceNode().name() : pipeline.sinkNode().name(),
                    read ? pipeline.sourceInstance().connectorType()
                            : pipeline.sinkInstance().connectorType(),
                    read ? reads++ : writes++,
                    records, 0, bytes, startedAt, StageLogPort.Outcome.FAILED,
                    codeOf(failure), failure.getMessage(), null, null, null, null, null, null);
        }

            private void submit(StageLogPort.Stage stage, String nodeId, String nodeName,
                            String connectorType, int sequence, int recordsIn, int recordsOut,
                            long bytes, Instant startedAt, StageLogPort.Outcome outcome,
                            String errorCode, String errorMessage, String query, JsonNode cursorIn,
                            JsonNode cursorOut, JsonNode details, JsonNode request,
                            JsonNode response) {
            submit(stage, nodeId, nodeName, connectorType, sequence, recordsIn, recordsOut, bytes,
                    Duration.between(startedAt, clock.instant()).toMillis(), outcome, errorCode,
                    errorMessage, query, cursorIn, cursorOut, details, request, response);
        }

        /**
         * The same, for a stage that was timed by somebody else.
         *
         * <p>A fetch is reported by the connector after the fact, so its duration is the
         * connector's measurement. Deriving it from a start instant here would measure the gap
         * until the engine got round to collecting it, which for the last call before a batch
         * closes is the whole read window.
         */
        private void submit(StageLogPort.Stage stage, String nodeId, String nodeName,
                            String connectorType, int sequence, int recordsIn, int recordsOut,
                            long bytes, long durationMillis, StageLogPort.Outcome outcome,
                            String errorCode, String errorMessage, String query, JsonNode cursorIn,
                            JsonNode cursorOut, JsonNode details, JsonNode request,
                            JsonNode response) {
            Instant now = clock.instant();
            try {
                stageLog.log(List.of(new StageLogPort.StageEntry(
                        split.tenantId(), pipeline.version().pipelineId(), split.runId(),
                        split.id(), traceId(), stage, nodeId, nodeName, connectorType, sequence,
                        position++,
                        split.attempt(), recordsIn, recordsOut, bytes, durationMillis,
                        outcome, errorCode, errorMessage, query, cursorIn, cursorOut, details,
                        request, response, now, expiresAt)));
            } catch (RuntimeException e) {
                // Describing the work must not break the work. The port is contracted not to
                // throw; this is the belt for a third-party implementation that ignores that.
                log.warn("Could not record a {} stage for chunk {} of run {}",
                        stage, split.index(), split.runId(), e);
            }
        }

        private String codeOf(Throwable failure) {
            return failure instanceof com.dmp.connector.api.ConnectorException connector
                    ? connector.kind().name()
                    : failure.getClass().getSimpleName();
        }
    }

    /**
     * Sends rejected records to the dead-letter queue.
     *
     * <p>Rejections do not fail the chunk. One malformed row out of a million must not stop a
     * migration; it must be captured with enough context to be fixed and replayed.
     *
     * <p>A pipeline that does not keep rejected payloads still records <em>why</em> records were
     * rejected. Those are two different things, and conflating them cost a run its entire
     * explanation: forty of forty records were refused, the chunk reported the percentage, and
     * because payload capture was off nothing wrote the code or the message either. The run said
     * "40 of 40 rejected" and there was nowhere left to look. What payload capture buys is the
     * ability to <em>replay</em>; the reason is the cheapest and most valuable thing here, it is
     * one row per distinct fault however many records hit it, and it is never optional.
     */
    /**
     * Files rejections against the step that produced them.
     *
     * <p>The node was hardcoded to the sink, which is right for a destination's refusal and wrong
     * for everything else. A transform failure filed under the sink says the destination rejected
     * a record it never received — and since the group is keyed on the node, every fault in the
     * pipeline shared one bucket. The console then read "Kafka orders.v1" beside a message thrown
     * by a validation step three nodes upstream.
     */
    private void persistRecordErrors(ResolvedPipeline pipeline, Split split,
                                     List<Sink.RecordError> errors) {
        persistRecordErrors(pipeline, split, errors, pipeline.sinkNode().id());
    }

    private void persistRecordErrors(ResolvedPipeline pipeline, Split split,
                                     List<Sink.RecordError> errors, String nodeId) {
        AuditPolicy audit = pipeline.audit();
        boolean keepPayloads = audit.capturesRejectedPayloads();

        if (!keepPayloads) {
            log.warn("{} record(s) rejected in chunk {} of run {}; the reasons are recorded but "
                            + "their payloads are not, because this pipeline does not keep "
                            + "rejected records, so there is nothing to replay for them",
                    errors.size(), split.index(), split.runId());
        }

        Instant now = clock.instant();
        Instant expiresAt = now.plus(audit.retention());

        // Grouped before anything is written. Twenty thousand records failing one rule are one
        // fault with a count beside it, not twenty thousand documents saying the same sentence —
        // and it is the group, not the record, that the sampling cap applies to.
        Map<String, List<Sink.RecordError>> bySignature = new LinkedHashMap<>();
        for (Sink.RecordError error : errors) {
            bySignature.computeIfAbsent(
                    ErrorSignature.of(error.code(), error.message()),
                    key -> new ArrayList<>()).add(error);
        }

        List<RecordErrorPort.RecordErrorEntry> entries = new ArrayList<>();
        for (Map.Entry<String, List<Sink.RecordError>> group : bySignature.entrySet()) {
            List<Sink.RecordError> occurrences = group.getValue();
            Sink.RecordError representative = occurrences.get(0);

            // Counted first, and always in full. However few payloads survive the cap — none at
            // all, for a pipeline that keeps no rejected records — the run still reports exactly
            // how many records were rejected and why. The count and the reason live on the
            // signature, which is written here; `wanted` governs only the payloads.
            int allowed = recordErrors.reserveSamples(
                    new RecordErrorPort.SignatureKey(
                            split.tenantId(), split.runId(), nodeId, group.getKey(),
                            representative.code(),
                            ErrorSignature.normalise(representative.message())),
                    occurrences.size(),
                    keepPayloads ? occurrences.size() : 0,
                    audit.samplesPerSignature(),
                    now, expiresAt);

            for (int i = 0; i < allowed && i < occurrences.size(); i++) {
                Sink.RecordError error = occurrences.get(i);
                entries.add(new RecordErrorPort.RecordErrorEntry(
                        split.tenantId(), split.runId(), split.id(), nodeId,
                        error.seq(), error.key(), error.code(), error.message(),
                        Redaction.apply(
                                Payloads.truncate(error.payload(), audit.maxPayloadBytes()), audit),
                        now, expiresAt));
            }
        }

        if (!entries.isEmpty()) {
            recordErrors.recordAll(entries);
        }
        if (audit.samplesRejections() && entries.size() < errors.size()) {
            log.debug("Chunk {} of run {}: {} rejection(s) counted, {} payload(s) kept "
                            + "({} distinct fault(s), {} sample(s) each)",
                    split.index(), split.runId(), errors.size(), entries.size(),
                    bySignature.size(), audit.samplesPerSignature());
        }
    }

    /**
     * Extends the worker's lease on this chunk if enough time has passed.
     *
     * <p>Done between batches rather than on a timer thread, so a wedged connector stops
     * heartbeating naturally and the chunk becomes reclaimable. A background heartbeat would keep
     * asserting liveness for a worker that has stopped making progress, which is precisely the
     * situation the lease exists to detect.
     */
    private Instant heartbeatIfDue(ResolvedPipeline pipeline, Split split, String workerId,
                                   Instant lastHeartbeat) {
        Duration interval = pipeline.execution().heartbeatInterval();
        Instant now = clock.instant();

        if (Duration.between(lastHeartbeat, now).compareTo(interval) < 0) {
            return lastHeartbeat;
        }

        boolean stillOurs = splits
                .heartbeat(split.tenantId(), split.id(), workerId, now, pipeline.execution().chunkLease())
                .isPresent();

        if (!stillOurs) {
            // Another worker has taken this chunk. Continuing would write every remaining record
            // twice, so this stops immediately rather than finishing politely.
            throw new LeaseLostException(
                    "Worker " + workerId + " lost its lease on chunk " + split.index()
                            + " of run " + split.runId() + "; another worker has taken it");
        }
        return now;
    }

    /**
     * Work done since the resume position was last saved.
     *
     * <p>Exists so the checkpoint write can lag behind the sink writes without losing count. The
     * cursor it holds is always the one reported after the most recent accepted batch, never ahead
     * of it — a cursor that led the writes would skip records on resume.
     */
    private static final class PendingProgress {

        private JsonNode cursor;
        private long lastSeq;
        private long read;
        private long produced;
        private long written;
        private long failed;
        private long filtered;
        private long bytes;
        private int batches;

        void accumulate(long readDelta, long producedDelta, long writtenDelta, long failedDelta,
                        long filteredDelta, long bytesDelta, JsonNode latestCursor, long latestSeq) {
            this.read += readDelta;
            this.produced += producedDelta;
            this.written += writtenDelta;
            this.failed += failedDelta;
            this.filtered += filteredDelta;
            this.bytes += bytesDelta;
            this.cursor = latestCursor;
            this.lastSeq = Math.max(this.lastSeq, latestSeq);
            this.batches++;
        }

        void clear() {
            read = 0;
            produced = 0;
            written = 0;
            failed = 0;
            filtered = 0;
            bytes = 0;
            batches = 0;
        }

        JsonNode cursor() {
            return cursor;
        }

        long lastSeq() {
            return lastSeq;
        }

        long read() {
            return read;
        }

        long produced() {
            return produced;
        }

        long filtered() {
            return filtered;
        }

        long written() {
            return written;
        }

        long failed() {
            return failed;
        }

        long bytes() {
            return bytes;
        }

        int batches() {
            return batches;
        }
    }

    /** Thrown when a worker discovers its chunk was reclaimed while it was still processing. */
    public static class LeaseLostException extends RuntimeException {
        public LeaseLostException(String message) {
            super(message);
        }
    }
}
