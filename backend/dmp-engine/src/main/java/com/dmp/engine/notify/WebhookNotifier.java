package com.dmp.engine.notify;

import com.dmp.application.port.out.NotifierRepository;
import com.dmp.application.port.out.RunEventPublisher;
import com.dmp.common.json.Json;
import com.dmp.connector.runtime.SecretsProvider;
import com.dmp.domain.notify.Notifier;
import com.dmp.domain.pipeline.PipelineId;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Posts a run's outcome to whatever somebody asked to be told.
 *
 * <p>A webhook rather than email or a chat integration, because a webhook is all three: every chat
 * tool accepts one, every alerting system accepts one, and an organisation wanting email already
 * owns something that turns a webhook into email.
 *
 * <p><b>Never on the run's thread, and never able to fail it.</b> The run has finished by the time
 * this is called and the outcome is already durable; a slow endpoint would otherwise hold a worker
 * that could be executing the next chunk, and an unreachable one would turn "the migration
 * succeeded" into "the migration failed while telling you it succeeded". Delivery is best effort,
 * and the outcome of the attempt is recorded where somebody configuring it can see it.
 */
@Component
public class WebhookNotifier implements RunEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(WebhookNotifier.class);

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Only these reach a person, whatever else the platform publishes.
     *
     * <p>The event stream carries every transition including chunk-level ones, and a notifier that
     * fired on all of them would be muted within a day — after which the one that mattered arrives
     * in the same silence as the rest.
     */
    private static final Map<RunEventPublisher.Type, Notifier.Event> NOTIFIABLE = Map.of(
            RunEventPublisher.Type.RUN_FAILED, Notifier.Event.RUN_FAILED,
            RunEventPublisher.Type.RUN_COMPLETED, Notifier.Event.RUN_COMPLETED,
            RunEventPublisher.Type.RUN_STOPPED, Notifier.Event.RUN_STOPPED);

    private final NotifierRepository notifiers;
    private final Map<String, SecretsProvider> secretsByScheme;
    private final HttpClient http;
    private final ExecutorService sender;
    private final Clock clock;

    public WebhookNotifier(NotifierRepository notifiers, List<SecretsProvider> secrets,
                           Clock clock) {
        this.notifiers = notifiers;
        this.secretsByScheme = secrets.stream()
                .collect(Collectors.toMap(SecretsProvider::scheme, Function.identity()));
        this.clock = clock;
        this.http = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
        // Virtual threads: these are almost entirely waiting on somebody else's HTTP endpoint, and
        // a fixed pool would let one slow endpoint delay every other notification behind it.
        this.sender = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void publish(RunEvent event) {
        Notifier.Event notifiable = notifiableFor(event);
        if (notifiable == null) {
            return;
        }

        List<Notifier> subscribed;
        try {
            PipelineId pipeline = event.pipelineId() == null
                    ? null : PipelineId.parse(event.pipelineId());
            subscribed = notifiers.findEnabled(event.tenantId()).stream()
                    .filter(notifier -> notifier.wants(notifiable, pipeline))
                    .toList();
        } catch (Exception e) {
            // Reading the notifier list is the platform's own database. Failing here would fail the
            // publish, which the fanout catches — but saying so is worth more than a silent return.
            log.warn("Could not read notifiers for run {}: {}", event.runId(), e.getMessage());
            return;
        }

        if (subscribed.isEmpty()) {
            return;
        }

        String body = payload(event, notifiable).toString();
        for (Notifier notifier : subscribed) {
            sender.submit(() -> send(notifier, body));
        }
    }

    /**
     * Maps a platform event to a notifiable one, splitting completion by whether anything failed.
     *
     * <p>A completed run with four thousand rejections is the outcome that most often goes
     * unnoticed: the run is green, the dashboard is green, and the records are not there. It is
     * therefore its own event, so a channel can subscribe to it without subscribing to every
     * successful nightly load.
     */
    private static Notifier.Event notifiableFor(RunEvent event) {
        Notifier.Event mapped = NOTIFIABLE.get(event.type());
        if (mapped != Notifier.Event.RUN_COMPLETED) {
            return mapped;
        }
        return failedRecords(event) > 0
                ? Notifier.Event.RUN_COMPLETED_WITH_FAILURES
                : Notifier.Event.RUN_COMPLETED;
    }

    private static long failedRecords(RunEvent event) {
        Object failed = event.details().get("recordsFailed");
        return failed instanceof Number number ? number.longValue() : 0;
    }

    /**
     * What is actually sent.
     *
     * <p>Flat, named, and carrying the console URL. Whoever reads this is reading it on a phone at
     * two in the morning, and the one thing they need is a link to the run — not a payload they
     * have to correlate against something else to identify.
     */
    private ObjectNode payload(RunEvent event, Notifier.Event notifiable) {
        ObjectNode body = Json.newObject();
        body.put("event", notifiable.name());
        body.put("runId", event.runId().toString());
        body.put("pipelineId", event.pipelineId());
        body.put("pipeline", event.pipelineName());
        body.put("version", event.versionNumber());
        body.put("occurredAt", event.occurredAt().toString());
        body.put("consoleUrl", consoleBaseUrl + "/runs/" + event.runId());

        ObjectNode details = body.putObject("details");
        event.details().forEach((key, value) -> details.set(key, Json.mapper().valueToTree(value)));
        return body;
    }

    /**
     * Where this platform's console is reachable, for the link in the message.
     *
     * <p>Configuration rather than a guess, because the platform cannot know the hostname somebody
     * reaches it on — a link to localhost in an alert is worse than no link, since it looks like one
     * that should work.
     */
    @Value("${dmp.console.base-url:http://localhost:5173}")
    private String consoleBaseUrl = "http://localhost:5173";

    private void send(Notifier notifier, String body) {
        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(notifier.url()))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "dmp-notifier")
                    .POST(HttpRequest.BodyPublishers.ofString(body));

            credential(notifier).ifPresent(value -> request.header(notifier.secretHeader(), value));

            HttpResponse<String> response =
                    http.send(request.build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 300) {
                // The endpoint's own status, kept short. A webhook that answers 404 because its URL
                // was rotated is the commonest failure and the one this has to name precisely.
                record(notifier, false, "HTTP " + response.statusCode() + " — "
                        + truncate(response.body()));
                return;
            }
            record(notifier, true, null);

        } catch (Exception e) {
            record(notifier, false, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private java.util.Optional<String> credential(Notifier notifier) {
        String reference = notifier.secretRef();
        if (reference == null || reference.isBlank()
                || notifier.secretHeader() == null || notifier.secretHeader().isBlank()) {
            return java.util.Optional.empty();
        }
        int separator = reference.indexOf(':');
        if (separator <= 0) {
            return java.util.Optional.empty();
        }
        SecretsProvider provider = secretsByScheme.get(reference.substring(0, separator));
        return provider == null
                ? java.util.Optional.empty()
                : provider.resolve(notifier.tenantId(), reference);
    }

    /**
     * Records the outcome, and swallows a failure to record it.
     *
     * <p>This runs after the run has ended and after the message has been sent or not. Losing the
     * diagnostic is a smaller problem than a background thread throwing where nothing is watching.
     */
    private void record(Notifier notifier, boolean succeeded, String error) {
        if (!succeeded) {
            log.warn("Notifier '{}' could not deliver: {}", notifier.name(), error);
        }
        try {
            notifiers.recordAttempt(notifier.tenantId(), notifier.id(), clock.instant(), succeeded,
                    error);
        } catch (Exception e) {
            log.debug("Could not record the delivery outcome for '{}'", notifier.name(), e);
        }
    }

    private static String truncate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 300 ? body : body.substring(0, 300) + "…";
    }

    /** Sends one message now, for the console's "Send a test" button. Returns the failure, or null. */
    public String test(Notifier notifier) {
        ObjectNode body = Json.newObject();
        body.put("event", "TEST");
        body.put("pipeline", notifier.pipelineId() == null
                ? "every pipeline" : notifier.pipelineId().toString());
        body.put("occurredAt", clock.instant().toString());
        body.put("message", "A test from the data migration platform. Nothing has failed.");

        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(notifier.url()))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "dmp-notifier")
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
            credential(notifier).ifPresent(value -> request.header(notifier.secretHeader(), value));

            HttpResponse<String> response =
                    http.send(request.build(), HttpResponse.BodyHandlers.ofString());

            String error = response.statusCode() >= 300
                    ? "HTTP " + response.statusCode() + " — " + truncate(response.body())
                    : null;
            record(notifier, error == null, error);
            return error;

        } catch (Exception e) {
            String error = e.getClass().getSimpleName() + ": " + e.getMessage();
            record(notifier, false, error);
            return error;
        }
    }
}
