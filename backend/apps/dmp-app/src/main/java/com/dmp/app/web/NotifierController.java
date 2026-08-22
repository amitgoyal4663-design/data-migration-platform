package com.dmp.app.web;

import com.dmp.app.web.dto.NotifierDtos;
import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.NotifierRepository;
import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.domain.notify.Notifier;
import com.dmp.domain.notify.NotifierId;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.engine.notify.WebhookNotifier;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.List;
import java.util.Map;

/**
 * Where to send word when a run ends.
 *
 * <p>Tenant-wide rather than nested under a pipeline, because the common case is one channel
 * watching everything and a pipeline-scoped API would make that N notifiers to maintain.
 */
@RestController
@RequestMapping("/api/v1/notifiers")
@Tag(name = "Notifiers", description = "Webhooks told when a run fails or finishes")
public class NotifierController {

    private final NotifierRepository notifiers;
    private final WebhookNotifier webhooks;
    private final TenantContext tenantContext;
    private final Clock clock;

    public NotifierController(NotifierRepository notifiers, WebhookNotifier webhooks,
                              TenantContext tenantContext, Clock clock) {
        this.notifiers = notifiers;
        this.webhooks = webhooks;
        this.tenantContext = tenantContext;
        this.clock = clock;
    }

    @GetMapping
    @Operation(summary = "List notifiers")
    public List<NotifierDtos.Response> list() {
        return NotifierDtos.Response.from(notifiers.findAll(tenantContext.currentTenant()));
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Add somewhere to send word",
            description = "The credential is a reference such as env:SLACK_TOKEN, never a value. "
                    + "It is resolved when sending and is never written to the definition store, "
                    + "an audit entry or a log line.")
    public NotifierDtos.Response create(@RequestBody NotifierDtos.Request request) {
        Notifier notifier = Notifier.create(
                tenantContext.currentTenant(), request.name(), request.url(),
                request.pipelineId() == null ? null : PipelineId.parse(request.pipelineId()),
                request.events(), request.secretHeader(), request.secretRef(),
                request.description(), clock.instant());

        return NotifierDtos.Response.from(notifiers.create(notifier));
    }

    @PutMapping("/{id}")
    @Operation(summary = "Change a notifier")
    public NotifierDtos.Response update(@PathVariable String id,
                                        @RequestBody NotifierDtos.Request request) {
        Notifier existing = require(id);

        // The stored reference is kept when the request omits one, so editing a URL does not
        // silently strip the credential — the same mistake that emptied connector secretRefs.
        String secretRef = request.secretRef() == null ? existing.secretRef() : request.secretRef();

        Notifier updated = new Notifier(existing.id(), existing.tenantId(), request.name(),
                request.url(),
                request.pipelineId() == null ? null : PipelineId.parse(request.pipelineId()),
                request.events(), request.secretHeader(), secretRef,
                request.enabled() == null || request.enabled(), request.description(),
                existing.lastAttemptAt(), existing.lastAttemptSucceeded(),
                existing.lastAttemptError(), existing.createdAt(), clock.instant(),
                existing.rowVersion());

        return NotifierDtos.Response.from(notifiers.update(updated));
    }

    @PostMapping("/{id}/test")
    @Operation(summary = "Send one message now",
            description = "Delivers a message saying nothing has failed, so a channel can be "
                    + "proved before it is relied on. A refusal is a 200 with the endpoint's own "
                    + "words — \"no, that URL is gone\" is a successful answer to \"does this work\".")
    public NotifierDtos.TestResponse test(@PathVariable String id) {
        String error = webhooks.test(require(id));
        return new NotifierDtos.TestResponse(error == null, error);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "Delete a notifier")
    public void delete(@PathVariable String id) {
        notifiers.delete(tenantContext.currentTenant(), NotifierId.parse(id));
    }

    private Notifier require(String id) {
        return notifiers.findById(tenantContext.currentTenant(), NotifierId.parse(id))
                .orElseThrow(() -> new DmpException(ErrorCode.NOT_FOUND,
                        "Notifier not found", Map.of("notifierId", id)));
    }
}
