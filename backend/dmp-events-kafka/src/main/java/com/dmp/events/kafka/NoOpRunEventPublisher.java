package com.dmp.events.kafka;

import com.dmp.application.port.out.RunEventPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

/**
 * What runs when no event bus is configured.
 *
 * <p>Lets the engine call {@code events.publish(...)} unconditionally instead of null-checking at
 * every call site. {@link #isEnabled()} returns false so a caller can skip building an event that
 * would go nowhere — the only part worth optimising, since constructing the event costs more than
 * discarding it.
 */
@Component
@ConditionalOnMissingBean(KafkaRunEventPublisher.class)
public class NoOpRunEventPublisher implements RunEventPublisher {

    @Override
    public void publish(RunEvent event) {
        // Nowhere to send it.
    }

    @Override
    public boolean isEnabled() {
        return false;
    }
}
