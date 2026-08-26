package com.dmp.engine;

import com.dmp.application.service.ConnectorInstanceService;
import com.dmp.connector.api.Connector;
import com.dmp.connector.api.ConnectorException;
import com.dmp.connector.runtime.ConnectorContexts;
import com.dmp.connector.runtime.ConnectorRegistry;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.connector.ConnectorInstance;
import com.dmp.domain.connector.ConnectorInstanceStatus;
import com.dmp.domain.connector.ConnectorInstanceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Runs a connector's own connectivity check and records what happened.
 *
 * <p>The alternative is finding out at the start of a run, which is far more expensive than it
 * sounds: a wrong password, a missing table or an absent topic is discovered after a pipeline has
 * been designed, published and queued, and the failure arrives as a run that has to be read rather
 * than a red field next to the thing that is wrong.
 *
 * <p>Each connector decides what a meaningful test is, and every one of them does real work rather
 * than opening a socket — a query against the configured table, a lookup of the configured topic. A
 * test that passes while the table is missing produces confidence the first run immediately
 * contradicts, which is worse than no test at all.
 */
@Service
public class ConnectorTester {

    private static final Logger log = LoggerFactory.getLogger(ConnectorTester.class);

    private final ConnectorInstanceService instances;
    private final ConnectorRegistry connectors;
    private final ConnectorContexts contexts;

    public ConnectorTester(ConnectorInstanceService instances,
                           ConnectorRegistry connectors,
                           ConnectorContexts contexts) {
        this.instances = instances;
        this.connectors = connectors;
        this.contexts = contexts;
    }

    /**
     * Tests one instance, returning it with the outcome recorded.
     *
     * <p>A failure is a result, not an error: the endpoint answers "is this configuration usable",
     * and "no, because the topic does not exist" is a successful answer to that question. Throwing
     * would leave the caller unable to distinguish an unusable connector from an unreachable API,
     * and would leave the recorded status untouched.
     */
    public ConnectorInstance test(ConnectorInstanceId id) {
        ConnectorInstance instance = instances.get(id);

        // Disabled means out of service, and this is the one place that could contradict it. A
        // connection is disabled precisely when somebody has decided the platform should stop
        // talking to that system — a decommissioned org, a vendor whose contract ended, a host
        // somebody was told to stop reaching. Testing it opened a real connection to exactly that
        // system, so the button that was supposed to be inert was the only one left that could
        // still make the call.
        //
        // Refused rather than faked. A test that reported success without connecting would be a
        // worse lie than the one this replaces.
        if (instance.status() == ConnectorInstanceStatus.DISABLED) {
            throw new DmpException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "'" + instance.name() + "' is disabled, so it is not contacted. Enable it "
                            + "first if the connection is meant to be used again.",
                    java.util.Map.of("connectorInstanceId", id.toString(),
                            "name", instance.name()));
        }

        Connector connector = connectors.require(instance.connectorType());

        try {
            connector.testConnection(contexts.forInstance(instance, "connection-test", "console"));
            log.info("Connection test succeeded for '{}'", instance.name());
            return instances.recordTestResult(id, true, null);

        } catch (ConnectorException e) {
            // The connector's own words. It knows why a topic being absent matters and what the
            // user should do about it; the platform would only be able to say "it failed".
            log.info("Connection test failed for '{}': {}", instance.name(), e.getMessage());
            return instances.recordTestResult(id, false, e.getMessage());

        } catch (Exception e) {
            log.warn("Connection test for '{}' failed unexpectedly", instance.name(), e);
            return instances.recordTestResult(id, false,
                    e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
}
