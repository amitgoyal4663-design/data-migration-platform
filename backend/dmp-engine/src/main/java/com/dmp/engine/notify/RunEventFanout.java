package com.dmp.engine.notify;

import com.dmp.application.port.out.RunEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Sends every run event to every publisher there is.
 *
 * <p>There are two now — the Kafka stream that other systems consume, and the webhooks a person
 * subscribes to — and they are unrelated: one is integration, the other is somebody's phone at two
 * in the morning. Without this the second could only be added by making it the first's problem, or
 * by threading a second call through the orchestrator beside the first, which is the same thing
 * written twice.
 *
 * <p>A failure in one publisher never reaches another, and never reaches the run. Publishing is
 * telling somebody what happened; it is not part of what happened, and a webhook nobody is reading
 * must not be able to fail a migration.
 */
@Component
@Primary
public class RunEventFanout implements RunEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(RunEventFanout.class);

    private final List<RunEventPublisher> publishers;

    public RunEventFanout(List<RunEventPublisher> publishers) {
        // Itself excluded, or the first publish recurses until the stack runs out.
        this.publishers = publishers.stream().filter(p -> p != this).toList();
    }

    @Override
    public void publish(RunEvent event) {
        for (RunEventPublisher publisher : publishers) {
            try {
                publisher.publish(event);
            } catch (Exception e) {
                log.warn("{} could not publish {} for run {}: {}",
                        publisher.getClass().getSimpleName(), event.type(), event.runId(),
                        e.getMessage());
            }
        }
    }

    @Override
    public boolean isEnabled() {
        return publishers.stream().anyMatch(RunEventPublisher::isEnabled);
    }
}
