package com.dmp.app.web;

import com.dmp.app.web.dto.PageResponse;
import com.dmp.app.web.dto.RecordSearchDtos;
import com.dmp.application.common.PageQuery;
import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.RecordIndexPort;
import com.dmp.domain.run.RunId;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import java.time.Instant;
import org.springframework.web.bind.annotation.RestController;

import java.util.Locale;

/**
 * "Was this record transferred, and what happened to it?"
 *
 * <p>The question a support engineer is actually asked, months after a migration, about one
 * identifier out of crores. Neither the run counters nor the destination can answer it: counters
 * say how many, and the destination says whether the record is there now — not whether this
 * platform put it there, in which run, or when.
 *
 * <p>Answers only for pipelines whose audit level is INDEXED. Anything else returns nothing, which
 * the console states as "this pipeline does not index records" rather than as "not found" — the two
 * mean very different things to whoever is asking.
 */
@RestController
@RequestMapping("/api/v1/records")
@Tag(name = "Record search", description = "Find what happened to an individual record")
public class RecordSearchController {

    private final RecordIndexPort recordIndex;
    private final TenantContext tenantContext;

    public RecordSearchController(RecordIndexPort recordIndex, TenantContext tenantContext) {
        this.recordIndex = recordIndex;
        this.tenantContext = tenantContext;
    }

    @GetMapping
    @Operation(summary = "Find a record by its key",
            description = """
                    Every run that handled this record, newest first: what happened to it, in which
                    chunk, and when.

                    Scoped to one pipeline, and the scope is required. A record key is only unique
                    within the source it came from — two pipelines moving different systems can both
                    hold a record numbered 88291 — and access will be granted per pipeline, so a
                    query with no pipeline in it could not be authorized.

                    The key is the source's own identifier — a MongoDB _id, a primary key, a Kafka
                    message key — exactly as the source reported it, so an ObjectId appears as
                    'oid:6a74f8d8...' rather than reformatted.

                    An empty result means no indexed run handled this key. It does not mean the
                    record was never migrated: a pipeline whose audit level is not INDEXED writes no
                    entries at all.
                    """)
    public PageResponse<RecordSearchDtos.Response> byKey(
            @RequestParam String pipelineId,
            @RequestParam String key,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size) {

        return PageResponse.from(
                recordIndex.findByKey(tenantContext.currentTenant(),
                        com.dmp.domain.pipeline.PipelineId.parse(pipelineId), key.trim(),
                        new PageQuery(page, size, null, false)),
                RecordSearchDtos.Response::from);
    }

    @GetMapping("/search")
    @Operation(summary = "Find records by anything — a value, a field, an outcome, a date range",
            description = """
                    What a support desk actually has is an order number and a complaint, not a
                    pipeline id and a MongoDB _id. This searches the record's own contents, so
                    typing ORD-100123 finds it whatever field it lives in and whichever pipeline
                    moved it.

                    q          any value, matched across every field of the payload
                    field      restrict q to one field, for example record.email
                    key        the source's own identifier, when they happen to have it
                    outcome    WRITTEN, REJECTED, SENT, FILTERED or TRANSFORM_FAILED
                    pipelineId optional; omit it to search everything
                    runId, after, before

                    An empty result is not proof a record was never migrated. It can also mean the
                    pipeline does not index records, or that the entry is older than the retention
                    window. Both are stated on the screen rather than left as an absence.
                    """)
    public PageResponse<RecordSearchDtos.Response> search(
            @RequestParam(required = false) String q,
            @RequestParam(required = false) String field,
            @RequestParam(required = false) String key,
            @RequestParam(required = false) String pipelineId,
            @RequestParam(required = false) String runId,
            @RequestParam(required = false) RecordIndexPort.Outcome outcome,
            @RequestParam(required = false) Instant after,
            @RequestParam(required = false) Instant before,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size) {

        var query = new RecordIndexPort.Query(
                pipelineId == null ? null : com.dmp.domain.pipeline.PipelineId.parse(pipelineId),
                key == null || key.isBlank() ? null : key.trim(),
                q == null || q.isBlank() ? null : q.trim(),
                field == null || field.isBlank() ? null : field.trim(),
                runId == null ? null : com.dmp.domain.run.RunId.parse(runId),
                outcome, after, before);

        return PageResponse.from(
                recordIndex.search(tenantContext.currentTenant(), query,
                        new PageQuery(page, size, null, false)),
                RecordSearchDtos.Response::from);
    }

    @GetMapping("/by-run")
    @Operation(summary = "List the records one run handled",
            description = """
                    For reconciling a single migration: every record the run indexed, optionally
                    filtered to WRITTEN, REJECTED or FILTERED.

                    The total here is the count of records the platform can still account for
                    individually. Compared against the run's own recordsWritten it is a check on
                    the run's arithmetic rather than a restatement of it.
                    """)
    public PageResponse<RecordSearchDtos.Response> byRun(
            @RequestParam String runId,
            @RequestParam(required = false) String outcome,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {

        return PageResponse.from(
                recordIndex.findByRun(tenantContext.currentTenant(), RunId.parse(runId),
                        parseOutcome(outcome), new PageQuery(page, size, null, false)),
                RecordSearchDtos.Response::from);
    }

    /** An unrecognised outcome filters nothing rather than failing the search. */
    private static RecordIndexPort.Outcome parseOutcome(String outcome) {
        if (outcome == null || outcome.isBlank()) {
            return null;
        }
        try {
            return RecordIndexPort.Outcome.valueOf(outcome.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
