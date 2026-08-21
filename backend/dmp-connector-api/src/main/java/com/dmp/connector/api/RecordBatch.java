package com.dmp.connector.api;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A group of records handed to a sink in one write.
 *
 * <p>Batches are assembled by the engine according to the pipeline's chunking policy, which bounds
 * them by <em>both</em> record count and byte size. The byte ceiling is what makes the memory
 * footprint predictable: a thousand records may be a megabyte or a gigabyte depending on the table,
 * and record count alone provides no bound at all.
 */
public final class RecordBatch {

    private final List<DataRecord> records;
    private final long totalBytes;
    private final JsonNode envelope;

    private RecordBatch(List<DataRecord> records, long totalBytes, JsonNode envelope) {
        this.records = records;
        this.totalBytes = totalBytes;
        this.envelope = envelope;
    }

    public static RecordBatch of(List<DataRecord> records) {
        long bytes = records.stream().mapToLong(DataRecord::bytes).sum();
        return new RecordBatch(List.copyOf(records), bytes, null);
    }

    public static RecordBatch empty() {
        return new RecordBatch(List.of(), 0, null);
    }

    /**
     * The same batch, carrying the payload a user's batch transform produced.
     *
     * <p>The records remain the batch's truth — every count the engine reports is derived from
     * them, and the envelope is only how they are presented to a sink that sends a batch as one
     * request. That separation is what stops a shaping script from appearing to change how many
     * records were migrated.
     */
    public RecordBatch withEnvelope(JsonNode payload) {
        return new RecordBatch(records, totalBytes, payload);
    }

    /**
     * The payload a batch transform produced, if the pipeline has one.
     *
     * <p>Sinks that send a batch as a single request should use this in place of assembling the
     * records themselves. Sinks that write records individually — a database, a file — have
     * nothing to apply it to and correctly ignore it.
     */
    public Optional<JsonNode> envelope() {
        return Optional.ofNullable(envelope);
    }

    public List<DataRecord> records() {
        return records;
    }

    public int size() {
        return records.size();
    }

    public long totalBytes() {
        return totalBytes;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    /** Sequence number of the last record, which is what the checkpoint advances to. */
    public long lastSeq() {
        return records.isEmpty() ? 0 : records.get(records.size() - 1).seq();
    }

    /**
     * Accumulates records until a size or byte limit is reached.
     *
     * <p>Not thread-safe; one builder belongs to one chunk being executed by one worker.
     */
    public static final class Builder {

        private final List<DataRecord> buffered = new ArrayList<>();
        private final int maxRecords;
        private final long maxBytes;
        private long bytes;

        public Builder(int maxRecords, long maxBytes) {
            this.maxRecords = maxRecords;
            this.maxBytes = maxBytes;
        }

        public void add(DataRecord record) {
            buffered.add(record);
            bytes += record.bytes();
        }

        /**
         * Whether the batch should be flushed now.
         *
         * <p>Either limit triggers it. The byte limit is checked after adding rather than before,
         * so a single record larger than the ceiling still goes through as a batch of one instead
         * of stalling the chunk forever.
         */
        public boolean isFull() {
            return buffered.size() >= maxRecords || bytes >= maxBytes;
        }

        public boolean isEmpty() {
            return buffered.isEmpty();
        }

        public int size() {
            return buffered.size();
        }

        /** Returns the accumulated batch and resets the builder. */
        public RecordBatch drain() {
            RecordBatch batch = new RecordBatch(List.copyOf(buffered), bytes, null);
            buffered.clear();
            bytes = 0;
            return batch;
        }

        public List<DataRecord> peek() {
            return Collections.unmodifiableList(buffered);
        }
    }
}
