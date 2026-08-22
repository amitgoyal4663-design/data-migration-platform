package com.dmp.events.kafka;

import com.dmp.application.port.out.WorkNudge;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * The behaviour a worker had before nudges existed: sleep the interval.
 *
 * <p>Present so the worker loop has one code path rather than two. Without it the loop would need
 * a null check around every wait, and the deployment without Kafka — which is a supported
 * deployment — would be the one exercising the untested branch.
 *
 * <p>Correct, merely slower: a run spends its poll interval between chunks.
 */
@Component
@ConditionalOnMissingBean(KafkaWorkNudge.class)
public class SleepingWorkNudge implements WorkNudge {

    @Override
    public void publish() {
        // Nobody is listening. The worker will find the work on its next poll.
    }

    @Override
    public boolean await(Duration timeout) {
        try {
            Thread.sleep(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
