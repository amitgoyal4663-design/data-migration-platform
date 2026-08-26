package com.dmp.app.web;

import com.dmp.app.web.dto.ConnectorInstanceDtos;
import com.dmp.app.web.dto.RunDtos;
import com.dmp.app.web.dto.PageResponse;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.ConnectorInstanceRepository;
import com.dmp.application.service.ConnectorInstanceService;
import com.dmp.domain.connector.ConnectorDirection;
import com.dmp.domain.connector.ConnectorInstanceId;
import com.dmp.domain.connector.ConnectorInstanceStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Connector instance management.
 *
 * <p>A connector instance is a configured connection — "finance-postgres-replica" — not a connector
 * type. Configuration is stored opaquely until the plugin runtime lands in Phase 2, at which point
 * it will be validated against the JSON Schema each connector declares, and the console will render
 * its form from that same schema with no frontend change.
 */
@RestController
@RequestMapping("/api/v1/connector-instances")
@Tag(name = "Connector instances", description = "Configured connections to source and sink systems")
public class ConnectorInstanceController {

    private final ConnectorInstanceService connectors;

    private final com.dmp.engine.ConnectorTester tester;

    private final com.dmp.engine.SourcePreview preview;

    public ConnectorInstanceController(ConnectorInstanceService connectors,
                                       com.dmp.engine.ConnectorTester tester,
                                       com.dmp.engine.SourcePreview preview) {
        this.tester = tester;
        this.connectors = connectors;
        this.preview = preview;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Create a connector instance",
            description = "Starts in UNTESTED. Send secret references, never secret values.")
    public ResponseEntity<ConnectorInstanceDtos.Response> create(
            @Valid @RequestBody ConnectorInstanceDtos.CreateRequest request) {

        var instance = connectors.create(new ConnectorInstanceService.CreateConnectorInstance(
                request.name(), request.connectorType(), request.direction(),
                request.config(), request.secretRefs(), request.description(),
                ConnectorInstanceDtos.RateLimit.policyOf(request.rateLimit())));

        return ResponseEntity
                .created(UriComponentsBuilder.fromPath("/api/v1/connector-instances/{id}")
                        .buildAndExpand(instance.id().toString()).toUri())
                .body(ConnectorInstanceDtos.Response.from(instance));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Fetch a connector instance")
    public ConnectorInstanceDtos.Response get(@PathVariable String id) {
        return ConnectorInstanceDtos.Response.from(connectors.get(ConnectorInstanceId.parse(id)));
    }

    @GetMapping
    @Operation(summary = "Search connector instances",
            description = "Filtering by direction also returns instances configured as BOTH, "
                    + "since those can fill either role.")
    public PageResponse<ConnectorInstanceDtos.Response> search(
            @RequestParam(required = false) String name,

            @Parameter(description = "Plugin identifier", example = "jdbc-postgres")
            @RequestParam(required = false) String connectorType,

            @RequestParam(required = false) ConnectorDirection direction,
            @RequestParam(required = false) ConnectorInstanceStatus status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size,

            @Parameter(description = "One of: name, connectorType, createdAt, updatedAt, status")
            @RequestParam(required = false) String sortBy,

            @RequestParam(defaultValue = "true") boolean ascending) {

        var result = connectors.search(
                new ConnectorInstanceRepository.ConnectorSearch(name, connectorType, direction, status),
                new PageQuery(page, size, sortBy, ascending));

        return PageResponse.from(result, ConnectorInstanceDtos.Response::from);
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update a connector instance",
            description = "Resets status to UNTESTED. A previous successful connection test says "
                    + "nothing about a configuration that has since changed.")
    public ConnectorInstanceDtos.Response update(
            @PathVariable String id,
            @Valid @RequestBody ConnectorInstanceDtos.UpdateRequest request) {

        var instance = connectors.update(ConnectorInstanceId.parse(id),
                new ConnectorInstanceService.UpdateConnectorInstance(
                        request.name(), request.config(), request.secretRefs(), request.description(),
                        request.direction(),
                        ConnectorInstanceDtos.RateLimit.policyOf(request.rateLimit())));

        return ConnectorInstanceDtos.Response.from(instance);
    }

    @PostMapping("/{id}/test")
    @Operation(summary = "Test that this connector can actually reach its target",
            description = """
                    Runs the connector's own connectivity check, which does real work rather than
                    opening a socket — a query against the configured table, a lookup of the
                    configured topic. A test that passes while the table is missing produces
                    confidence the first run immediately contradicts.

                    A failed test is a 200 with status FAILED and the connector's own message, not
                    an error response. "No, because the topic does not exist" is a successful answer
                    to "is this configuration usable".
                    """)
    public ConnectorInstanceDtos.Response test(@PathVariable String id) {
        return ConnectorInstanceDtos.Response.from(tester.test(ConnectorInstanceId.parse(id)));
    }

    @GetMapping("/{id}/queries")
    @Operation(summary = "The named queries this connection offers, in the order they were written",
            description = "Empty for a connection that declares none, which is every one written "
                    + "before they existed — a preview of those shows no picker, exactly as it did.")
    public RunDtos.ParameterNames queries(@PathVariable String id) {
        // The same shape the pipeline's own /queries returns, so the console has one reader.
        return new RunDtos.ParameterNames(preview.queryNames(ConnectorInstanceId.parse(id)));
    }

    @GetMapping("/{id}/parameters")
    @Operation(summary = "The placeholders one of this source's queries expects",
            description = "So a preview can ask for exactly the right values instead of failing "
                    + "with the connector's refusal. Per query, because that is the point of "
                    + "them: 'by date range' wants a from and a to, 'by policy number' wants a "
                    + "list, and a form asking for all three asks for values two of which cannot "
                    + "be used. Costs no connection.")
    public RunDtos.ParameterNames parameters(
            @PathVariable String id,
            @Parameter(description = "Which named query. Omit for the first one declared, which "
                    + "is what a run gets when it names none.")
            @RequestParam(required = false) String query) {
        ConnectorInstanceId instanceId = ConnectorInstanceId.parse(id);
        return new RunDtos.ParameterNames(
                List.copyOf(preview.parameterNames(instanceId, query)),
                List.copyOf(preview.listParameterNames(instanceId, query)));
    }

    @PostMapping("/{id}/preview")
    @Operation(summary = "Read a few records, to see what they actually look like",
            description = """
                    Answers the question every mapping starts with: what does a record from this
                    system look like? Reads from the real source with the real credentials and
                    returns the payloads unaltered.

                    Not a run. Nothing is planned, no chunk exists, no checkpoint is written and
                    nothing appears in the run history. The session is opened, a few rows are read,
                    and it is closed — including cancelling any job the source had to submit to
                    answer, so a form clicked twice does not leave two statements running.

                    Capped at 100 rows. A preview is read to learn the shape of a record, which is
                    evident from a handful; the cap is there because "preview" invites somebody to
                    ask for ten thousand and use it as an export.
                    """)
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Records, possibly none"),
            @ApiResponse(responseCode = "422", description = "This connector is a destination"),
            @ApiResponse(responseCode = "503", description =
                    "The source refused, or is still preparing. Carries the connector's own words.")
    })
    public ConnectorInstanceDtos.PreviewResponse preview(
            @PathVariable String id,
            @Parameter(description = "Rows to read. Capped at 100.")
            @RequestParam(defaultValue = "10") int limit,
            @Parameter(description = "Which named query to preview. Omit for the first one "
                    + "declared — the same one a run gets when it names none, so a preview shows "
                    + "the records a run would actually read.")
            @RequestParam(required = false) String query,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(description =
                    "Values for a parameterised query, exactly as a run would supply them. A "
                            + "source reading WHERE ts > :from cannot be previewed without them.")
            @RequestBody(required = false) JsonNode parameters) {

        return ConnectorInstanceDtos.PreviewResponse.from(
                preview.read(ConnectorInstanceId.parse(id), limit,
                        com.dmp.common.json.Json.orEmpty(parameters), query));
    }

    @PostMapping("/{id}/disable")
    @Operation(summary = "Take an instance out of service without deleting it")
    public ConnectorInstanceDtos.Response disable(@PathVariable String id) {
        return ConnectorInstanceDtos.Response.from(connectors.disable(ConnectorInstanceId.parse(id)));
    }

    @PostMapping("/{id}/enable")
    @Operation(summary = "Return an instance to service",
            description = "Returns it to UNTESTED rather than ACTIVE — re-enabling is not evidence "
                    + "that the connection works.")
    public ConnectorInstanceDtos.Response enable(@PathVariable String id) {
        return ConnectorInstanceDtos.Response.from(connectors.enable(ConnectorInstanceId.parse(id)));
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "Delete a connector instance",
            description = "Published pipeline versions referencing it are immutable and would "
                    + "become unrunnable. Phase 2 adds a reference check; until then prefer "
                    + "disabling.")
    public void delete(@PathVariable String id) {
        connectors.delete(ConnectorInstanceId.parse(id));
    }
}
