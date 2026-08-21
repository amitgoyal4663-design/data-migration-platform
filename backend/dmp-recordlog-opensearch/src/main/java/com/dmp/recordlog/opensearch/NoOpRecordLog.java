package com.dmp.recordlog.opensearch;

import com.dmp.application.port.out.RecordLogPort;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * What runs when no search cluster is configured.
 *
 * <p>Exists so the engine can call {@code recordLog.log(...)} unconditionally rather than
 * null-checking at every call site. {@link #isEnabled()} returns false, which lets a caller skip
 * the work of building events at all — the useful optimisation, since constructing and redacting a
 * payload costs more than discarding it.
 */
@Component
@ConditionalOnMissingBean(OpenSearchRecordLog.class)
public class NoOpRecordLog implements RecordLogPort {

    @Override
    public void log(List<RecordEvent> events) {
        // Nowhere to send them.
    }

    @Override
    public boolean isEnabled() {
        return false;
    }
}
