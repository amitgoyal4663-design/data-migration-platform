package com.dmp.domain.audit;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Per-pipeline record-level audit configuration, per ADR-0011.
 *
 * <p>Redaction lives here rather than in the audit writer's own configuration because it must
 * travel with the pipeline definition. A pipeline moving payment data carries its redaction rules
 * with it into every environment it is deployed to; a globally configured writer would not.
 *
 * @param level          how much per-record detail to capture
 * @param redactedFields JSON pointer paths to sensitive fields, for example {@code /customer/email}
 * @param redactionMode  how those fields are handled before serialisation
 * @param retention      how long captured records are kept
 * @param samplesPerSignature how many payloads to keep for each distinct fault. The count of every
 *                       rejection is always exact; this bounds only how many are stored whole.
 *                       {@code 0} keeps every one of them
 * @param maxPayloadBytes ceiling on one stored payload, beyond which it is truncated; {@code 0}
 *                       stores it however large it is
 * @param captureRejectedPayloads whether a rejected record's content is kept, which is what makes
 *                       it replayable. Separate from {@code level} because they answer different
 *                       questions at different prices: the level decides whether every record's
 *                       <em>identity</em> is searchable, this decides whether a <em>failure's</em>
 *                       content is stored. A pipeline can reasonably want an identity index and no
 *                       stored payloads, or payloads and no index, and folding both into one enum
 *                       forced a choice nobody actually wanted to make
 * @param stageLog       which stages of the work are logged — see {@link StageLogPolicy}. Every
 *                       other setting on this record describes a record; this one describes the
 *                       work that carried it. Null resolves to {@link StageLogPolicy#OFF}
 */
public record AuditPolicy(
        RecordAuditLevel level,
        Set<String> redactedFields,
        RedactionMode redactionMode,
        Duration retention,
        int samplesPerSignature,
        int maxPayloadBytes,
        boolean indexPayloads,
        Boolean captureRejectedPayloads,
        StageLogPolicy stageLog) {

    private static final int MAX_REDACTED_FIELDS = 256;
    private static final Duration MAX_RETENTION = Duration.ofDays(3_650);

    /** Floor for index entries. They hold no record content, so a year of them costs little. */
    private static final Duration MIN_INDEX_RETENTION = Duration.ofDays(365);

    /**
     * Keep every rejected record whole, however many share a cause.
     *
     * <p>What a pipeline stored before this setting existed deserialises to, since a JSON document
     * without the field yields zero. Upgrading the platform must not quietly start discarding
     * evidence somebody was relying on.
     */
    public static final int KEEP_EVERY_SAMPLE = 0;

    /** No ceiling on payload size. */
    public static final int PAYLOAD_UNTRUNCATED = 0;

    /**
     * Payloads kept per distinct fault for a newly created pipeline.
     *
     * <p>Ten, because the tenth example of a duplicate-key violation has never told anyone anything
     * the first did not, while twenty million of them is a second copy of the source data sitting
     * in the execution store. Enough to see whether the failures really are one fault, few enough
     * that a total failure costs kilobytes.
     */
    public static final int DEFAULT_SAMPLES_PER_SIGNATURE = 10;

    /** Generous for a record, small enough that a file smuggled into a field cannot repeat. */
    public static final int DEFAULT_MAX_PAYLOAD_BYTES = 32 * 1024;

    /** Errors only, retained 30 days. The right default: cheap, and captures what gets debugged. */
    public static final AuditPolicy DEFAULT = new AuditPolicy(
            RecordAuditLevel.ERRORS, Set.of(), RedactionMode.HASH, Duration.ofDays(30),
            DEFAULT_SAMPLES_PER_SIGNATURE, DEFAULT_MAX_PAYLOAD_BYTES, false, true,
            StageLogPolicy.OFF);

    /**
     * The same policy with the dead-letter queue switched off.
     *
     * <p>Applied to a pipeline whose destination cannot name the records it refused. Storing a
     * queue that can only ever be empty is worse than storing none: it invites a support engineer
     * to look for records there, find nothing, and conclude the platform lost them.
     */
    public AuditPolicy withoutRejectedPayloads() {
        return new AuditPolicy(level, redactedFields, redactionMode, retention,
                samplesPerSignature, maxPayloadBytes, indexPayloads, false, stageLog);
    }

    /** This policy with stage logging configured. */
    public AuditPolicy logging(StageLogPolicy newStageLog) {
        return new AuditPolicy(level, redactedFields, redactionMode, retention,
                samplesPerSignature, maxPayloadBytes, indexPayloads, captureRejectedPayloads,
                newStageLog);
    }

    /**
     * Whether a rejected record's content is stored, and therefore whether it can be replayed.
     *
     * <p>{@code COUNTERS} overrides the switch. That level is the statement that nothing beyond
     * aggregate counters is kept, so honouring a dead-letter queue underneath it would contradict
     * the thing the user selected.
     */
    public boolean capturesRejectedPayloads() {
        return captureRejectedPayloads && level.capturesFailures();
    }

    /**
     * Whether the record index carries each record's content as well as its identity.
     *
     * <p>Two settings rather than one because they buy different things at wildly different prices.
     * The identity index answers "was 88291 transferred", costs about 140 bytes a record, holds no
     * personal data and can be kept for years. Adding payloads answers "find every record whose
     * email ends @acme.com" — a genuinely different question — at thousands of bytes a record, and
     * it puts customer data into a second store that then needs its own redaction, retention and
     * erasure story.
     *
     * <p>Redaction applies here exactly as it does to the dead-letter queue: a field the pipeline
     * masks is masked in the index too. An index is not an exemption from a policy the rest of the
     * platform obeys.
     */
    public boolean indexesPayloads() {
        return level.indexesEveryRecord() && indexPayloads;
    }

    public AuditPolicy {
        Objects.requireNonNull(level, "level");
        Objects.requireNonNull(redactionMode, "redactionMode");
        Objects.requireNonNull(retention, "retention");

        // Boxed, and absent means on. Every pipeline stored before this setting existed has no such
        // key, and a primitive boolean would deserialise those to false — silently switching off
        // the dead-letter queue for every existing pipeline on the deploy that introduced the
        // switch. Upgrading the platform must not quietly stop capturing evidence somebody relies
        // on, which is the same rule samplesPerSignature follows just above.
        if (captureRejectedPayloads == null) {
            captureRejectedPayloads = Boolean.TRUE;
        }

        // Absent means off, which is the opposite default to the line above and correct for the
        // same reason: this switch turns logging *on*, so a missing key must not start writing an
        // index for every pipeline that predates it.
        if (stageLog == null) {
            stageLog = StageLogPolicy.OFF;
        }

        redactedFields = normalisePaths(redactedFields);

        if (retention.isNegative() || retention.isZero() || retention.compareTo(MAX_RETENTION) > 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Audit retention must be positive and at most 10 years",
                    Map.of("retention", retention.toString()));
        }
        if (samplesPerSignature < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "samplesPerSignature must not be negative; use 0 to keep every rejected record",
                    Map.of("samplesPerSignature", samplesPerSignature));
        }
        if (maxPayloadBytes < 0) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "maxPayloadBytes must not be negative; use 0 for no ceiling",
                    Map.of("maxPayloadBytes", maxPayloadBytes));
        }
    }

    /** Whether payloads are sampled per fault rather than all kept. */
    public boolean samplesRejections() {
        return samplesPerSignature != KEEP_EVERY_SAMPLE;
    }

    /**
     * How long a record's index entry is kept — deliberately far longer than its payload.
     *
     * <p>They are governed by opposite pressures. A stored payload is customer data: it is a
     * liability the moment the failure it documents has been fixed, and thirty days is generous.
     * An index entry is a key, an outcome and a timestamp — no content, no erasure obligation, a
     * hundred bytes — and the question it answers is asked <em>late</em>: a business queries one
     * customer's migration months after cutover, long after the payloads have rightly gone.
     *
     * <p>Tying the two together would force a choice between keeping personal data for a year and
     * being unable to answer "was this record migrated" after a month. Neither is acceptable, so
     * the index gets its own floor of a year and follows the payload retention only where that is
     * longer still.
     */
    public Duration indexRetention() {
        return retention.compareTo(MIN_INDEX_RETENTION) > 0 ? retention : MIN_INDEX_RETENTION;
    }

    /**
     * How many more payloads may be stored for a fault that already has this many.
     *
     * <p>Counting is separate from storing throughout: a run always reports exactly how many
     * records were rejected, and this governs only how many of them are kept whole.
     */
    public int sampleAllowance(long alreadyStored) {
        if (!samplesRejections()) {
            return Integer.MAX_VALUE;
        }
        return (int) Math.max(0, samplesPerSignature - alreadyStored);
    }

    /** This policy with rejection sampling configured. */
    public AuditPolicy samplingPerSignature(int samples, int payloadBytes) {
        return new AuditPolicy(level, redactedFields, redactionMode, retention,
                samples, payloadBytes, indexPayloads, captureRejectedPayloads, stageLog);
    }

    /** This policy with record indexing configured. */
    public AuditPolicy indexing(RecordAuditLevel newLevel, boolean payloads) {
        return new AuditPolicy(newLevel, redactedFields, redactionMode, retention,
                samplesPerSignature, maxPayloadBytes, payloads, captureRejectedPayloads, stageLog);
    }

    /**
     * Rejects a policy the platform cannot honour yet.
     *
     * <p>{@code FULL} needs the Parquet archival path delivered in Phase 11. Failing validation is
     * the only honest response — silently downgrading would leave a user
     * believing they have complete lineage when they do not, which is worse than an error.
     */
    public void requireSupported(boolean archivalPipelineAvailable) {
        if (level == RecordAuditLevel.FULL && !archivalPipelineAvailable) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "FULL record audit requires the object-storage archival pipeline, "
                            + "which is not available in this deployment",
                    Map.of("level", level.name()));
        }
    }

    /** Whether a payload-capturing level is configured without any field marked sensitive. */
    public boolean capturesPayloadsWithoutRedaction() {
        return level.capturesPayloads() && redactedFields.isEmpty();
    }

    private static Set<String> normalisePaths(Set<String> paths) {
        if (paths == null || paths.isEmpty()) {
            return Set.of();
        }
        if (paths.size() > MAX_REDACTED_FIELDS) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "At most " + MAX_REDACTED_FIELDS + " redacted fields may be declared",
                    Map.of("count", paths.size()));
        }
        Set<String> normalised = new LinkedHashSet<>();
        for (String path : paths) {
            if (path == null || path.isBlank()) {
                continue;
            }
            String trimmed = path.strip();
            if (!trimmed.startsWith("/")) {
                trimmed = "/" + trimmed;
            }
            normalised.add(trimmed);
        }
        return Set.copyOf(normalised);
    }
}
