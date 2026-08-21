package com.dmp.app.web;

import com.dmp.app.web.dto.PageResponse;
import com.dmp.app.web.dto.PipelineDtos;
import com.dmp.application.common.PageQuery;
import com.dmp.application.port.out.PipelineRepository;
import com.dmp.application.service.PipelineService;
import com.dmp.domain.pipeline.PipelineId;
import com.dmp.domain.pipeline.PipelineStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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

import java.util.Set;

/** Pipeline management. */
@RestController
@RequestMapping("/api/v1/pipelines")
@Tag(name = "Pipelines", description = "Create, search and manage pipelines")
public class PipelineController {

    private final PipelineService pipelines;

    public PipelineController(PipelineService pipelines) {
        this.pipelines = pipelines;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Create a pipeline",
            description = "Creates the container only. The pipeline starts in DRAFT with no "
                    + "versions and is not runnable until a version is created and published.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Created"),
            @ApiResponse(responseCode = "409", description = "A pipeline with this name already exists")
    })
    public ResponseEntity<PipelineDtos.Response> create(
            @Valid @RequestBody PipelineDtos.CreateRequest request) {

        var pipeline = pipelines.create(new PipelineService.CreatePipeline(
                request.name(), request.description(), request.folder(),
                request.tags() == null ? Set.of() : request.tags()));

        return ResponseEntity
                .created(UriComponentsBuilder.fromPath("/api/v1/pipelines/{id}")
                        .buildAndExpand(pipeline.id().toString()).toUri())
                .body(PipelineDtos.Response.from(pipeline));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Fetch a pipeline")
    public PipelineDtos.Response get(@PathVariable String id) {
        return PipelineDtos.Response.from(pipelines.get(PipelineId.parse(id)));
    }

    @GetMapping
    @Operation(summary = "Search pipelines",
            description = "All filters are optional and combine conjunctively. Tag filtering "
                    + "requires a pipeline to carry every tag listed, not any of them.")
    public PageResponse<PipelineDtos.Response> search(
            @Parameter(description = "Case-insensitive substring of the name")
            @RequestParam(required = false) String name,

            @Parameter(description = "Exact folder path", example = "/finance/daily")
            @RequestParam(required = false) String folder,

            @Parameter(description = "Every listed tag must be present")
            @RequestParam(required = false) Set<String> tags,

            @RequestParam(required = false) PipelineStatus status,

            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size,

            @Parameter(description = "One of: name, createdAt, updatedAt, status")
            @RequestParam(required = false) String sortBy,

            @RequestParam(defaultValue = "false") boolean ascending) {

        var result = pipelines.search(
                new PipelineRepository.PipelineSearch(name, folder, tags, status),
                new PageQuery(page, size, sortBy, ascending));

        return PageResponse.from(result, PipelineDtos.Response::from);
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update pipeline metadata",
            description = "Changes name, description, folder and tags. The DAG lives on versions "
                    + "and is not touched here.")
    public PipelineDtos.Response update(@PathVariable String id,
                                        @Valid @RequestBody PipelineDtos.UpdateRequest request) {

        var pipeline = pipelines.update(PipelineId.parse(id),
                new PipelineService.UpdatePipeline(request.name(), request.description(),
                        request.folder(), request.tags() == null ? Set.of() : request.tags()));

        return PipelineDtos.Response.from(pipeline);
    }

    @PostMapping("/{id}/archive")
    @Operation(summary = "Archive a pipeline",
            description = "Removes it from active views and prevents new runs. History is retained "
                    + "and the pipeline can be restored.")
    public PipelineDtos.Response archive(@PathVariable String id) {
        return PipelineDtos.Response.from(pipelines.archive(PipelineId.parse(id)));
    }

    @PostMapping("/{id}/restore")
    @Operation(summary = "Restore an archived pipeline",
            description = "Returns to ACTIVE if a version was published, otherwise to DRAFT — "
                    + "restoring must not make an unpublished pipeline runnable.")
    public PipelineDtos.Response restore(@PathVariable String id) {
        return PipelineDtos.Response.from(pipelines.restore(PipelineId.parse(id)));
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "Delete a pipeline permanently",
            description = "Refused once any version has been published, because runs reference "
                    + "versions and deleting them would make their history uninterpretable. "
                    + "Archive instead.")
    @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Deleted"),
            @ApiResponse(responseCode = "409", description = "Has published versions; archive it instead")
    })
    public void delete(@PathVariable String id) {
        pipelines.delete(PipelineId.parse(id));
    }
}
