package com.dmp.app.web;

import com.dmp.app.web.dto.PipelineDtos;
import com.dmp.app.web.dto.PipelineVersionDtos;
import com.dmp.application.service.PipelineVersionService;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineVersionId;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
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

import java.util.List;

/**
 * Pipeline version lifecycle.
 *
 * <p>The editing flow is: create a draft, edit its definition, validate, publish. Publishing
 * freezes the version permanently, so editing something published means creating a new version
 * rather than modifying the old one. Rollback republishes an earlier version rather than copying
 * its content forward — which keeps the version list a record of what happened rather than a
 * sequence of copies.
 */
@RestController
@RequestMapping("/api/v1/pipelines/{pipelineId}/versions")
@Tag(name = "Pipeline versions", description = "Draft, validate, publish and roll back pipeline definitions")
public class PipelineVersionController {

    private final PipelineVersionService versions;

    public PipelineVersionController(PipelineVersionService versions) {
        this.versions = versions;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Create a draft version",
            description = "Allocates the next version number. The draft is editable and may be "
                    + "structurally invalid while work is in progress.")
    public PipelineVersionDtos.Response createDraft(
            @PathVariable String pipelineId,
            @Valid @RequestBody PipelineVersionDtos.CreateRequest request) {

        var version = versions.createDraft(PipelineId.parse(pipelineId),
                new PipelineVersionService.CreateVersion(
                        request.definition(), request.chunkingPolicy(), request.executionPolicy(),
                        request.auditPolicy(), request.deliveryPolicy(), request.mode(),
                        request.changeNote()));

        return PipelineVersionDtos.Response.from(version);
    }

    @GetMapping
    @Operation(summary = "List versions, newest first",
            description = "Returns summaries without the full DAG. Fetch a single version for that.")
    public List<PipelineVersionDtos.Summary> list(@PathVariable String pipelineId) {
        return versions.listVersions(PipelineId.parse(pipelineId)).stream()
                .map(PipelineVersionDtos.Summary::from)
                .toList();
    }

    @GetMapping("/{versionId}")
    @Operation(summary = "Fetch a version including its full definition")
    public PipelineVersionDtos.Response get(@PathVariable String pipelineId,
                                            @PathVariable String versionId) {
        return PipelineVersionDtos.Response.from(versions.get(PipelineVersionId.parse(versionId)));
    }

    @PutMapping("/{versionId}/definition")
    @Operation(summary = "Replace the DAG",
            description = "Rejected on a published version — create a new version instead.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Updated"),
            @ApiResponse(responseCode = "409", description = "Version is published and immutable")
    })
    public PipelineVersionDtos.Response updateDefinition(
            @PathVariable String pipelineId,
            @PathVariable String versionId,
            @Valid @RequestBody PipelineVersionDtos.UpdateDefinitionRequest request) {

        return PipelineVersionDtos.Response.from(
                versions.updateDefinition(PipelineVersionId.parse(versionId), request.definition()));
    }

    @PutMapping("/{versionId}/policies")
    @Operation(summary = "Update a draft version's chunking, execution and audit policies",
            description = """
                    Read size and write size are configured separately because they are constrained
                    by different things — see the chunking policy schema.

                    The audit policy decides what survives a run: samplesPerSignature caps how many
                    payloads are stored per distinct fault, and only a stored payload can be
                    replayed. Set it to 0 to keep every rejected record, which is what makes a whole
                    run's rejections recoverable rather than a sample of them.

                    Omitted fields are left unchanged. Refused on a published version.
                    """)
    public PipelineVersionDtos.Response updatePolicies(
            @PathVariable String pipelineId,
            @PathVariable String versionId,
            @Valid @RequestBody PipelineVersionDtos.UpdatePoliciesRequest request) {

        return PipelineVersionDtos.Response.from(versions.updatePolicies(
                PipelineVersionId.parse(versionId),
                new PipelineVersionService.UpdatePolicies(
                        request.chunkingPolicy(), request.executionPolicy(),
                        request.auditPolicy(), request.deliveryPolicy(), request.mode())));
    }

    @PostMapping("/{versionId}/validate")
    @Operation(summary = "Validate a version",
            description = "Runs structural checks on the graph and verifies that every referenced "
                    + "connector instance exists and can fill its role. Returns all problems at "
                    + "once rather than stopping at the first, and never fails the request — read "
                    + "the response body.")
    public PipelineVersionDtos.ValidationResponse validate(@PathVariable String pipelineId,
                                                           @PathVariable String versionId) {
        return PipelineVersionDtos.ValidationResponse.from(
                versions.validate(PipelineVersionId.parse(versionId)));
    }

    @PostMapping("/{versionNumber}/publish")
    @Operation(summary = "Publish a version",
            description = "Freezes the version and makes it the pipeline's runnable one. "
                    + "Irreversible: a published version can never be edited. Validation runs "
                    + "again here even if it passed a moment ago, because the definition may have "
                    + "changed in between.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Published"),
            @ApiResponse(responseCode = "400", description = "Validation failed; the response lists every error"),
            @ApiResponse(responseCode = "409", description = "Already published")
    })
    public PipelineVersionDtos.Response publish(@PathVariable String pipelineId,
                                                @PathVariable int versionNumber) {
        return PipelineVersionDtos.Response.from(
                versions.publish(PipelineId.parse(pipelineId), versionNumber));
    }

    @PostMapping("/{versionNumber}/rollback")
    @Operation(summary = "Roll back to an earlier version",
            description = "Republishes a version that was previously published. Does not copy its "
                    + "content into a new version — runs before and after the rollback reference "
                    + "the same immutable definition, which is what keeps history honest.")
    public PipelineDtos.Response rollback(@PathVariable String pipelineId,
                                          @PathVariable int versionNumber) {
        return PipelineDtos.Response.from(
                versions.rollback(PipelineId.parse(pipelineId), versionNumber));
    }

    @DeleteMapping("/{versionId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "Delete a draft version",
            description = "Only drafts. A published version is referenced by runs and cannot be removed.")
    public void deleteDraft(@PathVariable String pipelineId, @PathVariable String versionId) {
        versions.deleteDraft(PipelineVersionId.parse(versionId));
    }
}
