package com.dmp.domain.notify;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.tenant.TenantId;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Somewhere to send word when a run ends badly.
 *
 * <p>Until this existed, a pipeline that failed at two in the morning was discovered by somebody
 * opening the console at eleven. For a scheduled pipeline that is not a gap in observability, it is
 * the difference between the platform being usable unattended and not.
 *
 * <p>A webhook rather than email or a chat integration, because a webhook is all three: every chat
 * tool accepts one, every alerting system accepts one, and an organisation that wants email already
 * has something that turns a webhook into email. Building the narrow ones first would have meant
 * building them all.
 *
 * @param pipelineId the one pipeline this watches, or null for every pipeline in the tenant. Both
 *                   are wanted and neither substitutes for the other: an operations channel wants
 *                   everything, and the team that owns one migration wants only theirs.
 * @param events     which outcomes are worth a message. Chosen rather than fixed, because a
 *                   notifier that reports every completion trains people to ignore it — and the one
 *                   that mattered arrives in the same silence as the rest.
 * @param secretRef  a reference to a credential sent as a header, never the credential. Same rule
 *                   as connector instances: a secret must not reach the definition store, an audit
 *                   entry or a log line.
 */
public record Notifier(
        NotifierId id,
        TenantId tenantId,
        String name,
        String url,
        PipelineId pipelineId,
        Set<Event> events,
        /** Header name for the credential, e.g. {@code Authorization}. Null when none is needed. */
        String secretHeader,
        String secretRef,
        boolean enabled,
        String description,
        /**
         * What happened the last time this fired.
         *
         * <p>Recorded because the commonest way an alerting system fails is silently: a webhook
         * whose URL was rotated returns 404 forever and nobody notices, precisely because the thing
         * that would have told them is the thing that is broken. This puts the last outcome on the
         * screen where somebody configures it.
         */
        Instant lastAttemptAt,
        boolean lastAttemptSucceeded,
        String lastAttemptError,
        Instant createdAt,
        Instant updatedAt,
        long rowVersion) {

    private static final int MAX_NAME_LENGTH = 255;
    private static final int MAX_ERROR_LENGTH = 2_000;

    public Notifier {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(createdAt, "createdAt");

        if (name == null || name.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "A notifier needs a name");
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Notifier name must be at most " + MAX_NAME_LENGTH + " characters");
        }
        requireHttpUrl(url);

        events = Set.copyOf(events == null ? Set.of() : events);
        if (events.isEmpty()) {
            // A notifier subscribed to nothing is indistinguishable on the screen from one that
            // works, and will be trusted for exactly as long as it takes for something to go wrong.
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "Choose at least one event for '" + name + "', or disable it. A notifier "
                            + "subscribed to nothing looks like one that works.");
        }
        if (secretRef != null && !secretRef.isBlank()
                && (secretHeader == null || secretHeader.isBlank())) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A credential was given for '" + name + "' but no header to send it in");
        }
        if (lastAttemptError != null && lastAttemptError.length() > MAX_ERROR_LENGTH) {
            lastAttemptError = lastAttemptError.substring(0, MAX_ERROR_LENGTH);
        }
        updatedAt = updatedAt == null ? createdAt : updatedAt;
    }

    /**
     * Rejected at creation rather than at delivery.
     *
     * <p>A URL naming a scheme this cannot speak fails on the first real incident, which is the
     * worst possible moment to learn that the alert never worked.
     */
    private static void requireHttpUrl(String url) {
        if (url == null || url.isBlank()) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED, "A notifier needs a URL");
        }
        URI parsed;
        try {
            parsed = URI.create(url.trim());
        } catch (IllegalArgumentException e) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "'" + url + "' is not a URL", Map.of("url", url));
        }
        String scheme = parsed.getScheme();
        if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))
                || parsed.getHost() == null) {
            throw new DmpException(ErrorCode.VALIDATION_FAILED,
                    "A notifier URL must be http or https with a host, but was '" + url + "'",
                    Map.of("url", url));
        }
    }

    public static Notifier create(TenantId tenantId, String name, String url,
                                  PipelineId pipelineId, Set<Event> events, String secretHeader,
                                  String secretRef, String description, Instant now) {
        return new Notifier(NotifierId.newId(), tenantId, name, url, pipelineId, events,
                secretHeader, secretRef, true, description, null, false, null, now, now, 0L);
    }

    /** Whether this notifier wants to hear about {@code event} on {@code pipeline}. */
    public boolean wants(Event event, PipelineId pipeline) {
        if (!enabled || !events.contains(event)) {
            return false;
        }
        return pipelineId == null || pipelineId.equals(pipeline);
    }

    public Notifier delivered(Instant now) {
        return new Notifier(id, tenantId, name, url, pipelineId, events, secretHeader, secretRef,
                enabled, description, now, true, null, createdAt, updatedAt, rowVersion);
    }

    public Notifier failed(Instant now, String error) {
        return new Notifier(id, tenantId, name, url, pipelineId, events, secretHeader, secretRef,
                enabled, description, now, false, error, createdAt, updatedAt, rowVersion);
    }

    /**
     * The outcomes worth telling somebody about.
     *
     * <p>Deliberately fewer than the run lifecycle has states. Every one of these is a thing a
     * person might act on; RUN_STARTED and RUN_PAUSED are not, and offering them would let somebody
     * build a notifier that fires constantly and is therefore ignored.
     */
    public enum Event {

        /** The run stopped with an error. The one everybody subscribes to. */
        RUN_FAILED,

        /**
         * The run finished, but the destination refused records or a script threw on them.
         *
         * <p>Separate from success because it is the outcome that most often goes unnoticed: the
         * run is green, the dashboard is green, and four thousand records are not there.
         */
        RUN_COMPLETED_WITH_FAILURES,

        /** The run finished cleanly. For a channel that wants to see the nightly load land. */
        RUN_COMPLETED,

        /** Somebody stopped it, or a chunk failure stopped it. */
        RUN_STOPPED
    }
}
