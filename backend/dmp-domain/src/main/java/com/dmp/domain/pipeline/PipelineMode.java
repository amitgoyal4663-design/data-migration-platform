package com.dmp.domain.pipeline;

/**
 * How a pipeline moves data, and by implication which transport it uses.
 *
 * <p>The mapping from mode to {@link ChannelType} is the point at which ADR-0001 stops being a
 * document and starts being behaviour. A user picks a mode that describes their intent; the
 * platform derives the transport.
 */
public enum PipelineMode {

    /** Reads everything, every time. The default for an initial migration. */
    FULL_LOAD(ChannelType.IN_PROCESS, false),

    /** Reads only what changed since the last checkpoint, using a watermark column or cursor. */
    INCREMENTAL(ChannelType.IN_PROCESS, false),

    /** Continuously consumes an unbounded source such as a Kafka topic. Does not self-terminate. */
    STREAMING(ChannelType.KAFKA, true),

    /** Change data capture from a database log. Phase 12. */
    CDC(ChannelType.KAFKA, true);

    private final ChannelType channelType;
    private final boolean continuous;

    PipelineMode(ChannelType channelType, boolean continuous) {
        this.channelType = channelType;
        this.continuous = continuous;
    }

    public ChannelType channelType() {
        return channelType;
    }

    /**
     * Whether a run in this mode terminates on its own.
     *
     * <p>Continuous runs never reach COMPLETED without an explicit stop, which changes how the
     * lifecycle, alerting and the UI treat a long-lived run.
     */
    public boolean isContinuous() {
        return continuous;
    }
}
