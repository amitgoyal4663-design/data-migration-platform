package com.dmp.app.web.dto;

import com.dmp.domain.notify.Notifier;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/** Web contract for notifiers. */
public final class NotifierDtos {

    private NotifierDtos() {
    }

    @Schema(name = "NotifierRequest")
    public record Request(
            String name,
            @Schema(description = "http or https, with a host. Rejected here rather than at "
                    + "delivery: a bad URL otherwise fails on the first real incident, which is "
                    + "the worst moment to learn the alert never worked.")
            String url,
            @Schema(description = "One pipeline, or null for every pipeline in the tenant")
            String pipelineId,
            @Schema(description = "RUN_FAILED, RUN_COMPLETED_WITH_FAILURES, RUN_COMPLETED, "
                    + "RUN_STOPPED. At least one — a notifier subscribed to nothing looks "
                    + "exactly like one that works.")
            Set<Notifier.Event> events,
            @Schema(description = "Header to send the credential in, e.g. Authorization")
            String secretHeader,
            @Schema(description = "A reference such as env:SLACK_WEBHOOK_TOKEN, never the value. "
                    + "The platform resolves it when sending; it is never stored or returned.")
            String secretRef,
            Boolean enabled,
            String description) {
    }

    @Schema(name = "NotifierResponse")
    public record Response(
            String id,
            String name,
            String url,
            String pipelineId,
            Set<Notifier.Event> events,
            String secretHeader,
            @Schema(description = "The reference, never the value")
            String secretRef,
            boolean enabled,
            String description,
            @Schema(description = "When this last tried to deliver. The commonest way alerting "
                    + "fails is silently — a rotated URL answers 404 forever and nobody notices, "
                    + "because the thing that would say so is the thing that is broken.")
            Instant lastAttemptAt,
            boolean lastAttemptSucceeded,
            String lastAttemptError,
            Instant createdAt,
            Instant updatedAt,
            long rowVersion) {

        public static Response from(Notifier notifier) {
            return new Response(
                    notifier.id().toString(),
                    notifier.name(),
                    notifier.url(),
                    notifier.pipelineId() == null ? null : notifier.pipelineId().toString(),
                    notifier.events(),
                    notifier.secretHeader(),
                    notifier.secretRef(),
                    notifier.enabled(),
                    notifier.description(),
                    notifier.lastAttemptAt(),
                    notifier.lastAttemptSucceeded(),
                    notifier.lastAttemptError(),
                    notifier.createdAt(),
                    notifier.updatedAt(),
                    notifier.rowVersion());
        }

        public static List<Response> from(List<Notifier> notifiers) {
            return notifiers.stream().map(Response::from).toList();
        }
    }

    @Schema(name = "NotifierTestResponse")
    public record TestResponse(
            boolean delivered,
            @Schema(description = "The endpoint's own words when it refused. Null on success.")
            String error) {
    }
}
