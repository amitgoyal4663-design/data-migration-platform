package com.dmp.app.web;

import com.dmp.app.web.dto.PageResponse;
import com.dmp.app.web.dto.StageLogDtos;
import com.dmp.application.common.PageQuery;
import com.dmp.application.common.TenantContext;
import com.dmp.application.port.out.StageLogPort;
import com.dmp.domain.run.RunId;
import com.dmp.domain.run.SplitId;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Locale;

/**
 * "What did the platform do, in what order, and how long did each part take?"
 *
 * <p>The other half of {@link RecordSearchController}. That one answers questions about a record;
 * this one answers questions about the work — and the work happens in batches while the index is
 * per record, so the two are not interchangeable.
 *
 * <p>Empty unless the pipeline switched stage logging on. Off is the default: this is a diagnostic
 * that costs storage, not a record the platform is obliged to keep.
 */
@RestController
@RequestMapping("/api/v1/stages")
@Tag(name = "Stage log", description = "What the platform did, in order, with timings")
public class StageLogController {

    private final StageLogPort stageLog;
    private final TenantContext tenantContext;

    public StageLogController(StageLogPort stageLog, TenantContext tenantContext) {
        this.stageLog = stageLog;
        this.tenantContext = tenantContext;
    }

    @GetMapping("/by-run")
    @Operation(summary = "One run's stages, oldest first",
            description = """
                    Every read, transform and write the run performed, in the order they happened.

                    Oldest first, deliberately. This is read as a sequence — this read, then the
                    transform that dropped nine records, then the write the destination refused —
                    and newest-first turns a story into a list.

                    Entries sharing a traceId are one cycle: one window of reading, the transforms
                    over it, and the call or calls that carried it out. Every record in that cycle
                    carries the same traceId in the record index, so the two can be shown together.

                    Narrow with chunkId to follow a single chunk, or with stage=READ, TRANSFORM or
                    WRITE to see one kind of work across the whole run — which is how you compare
                    read time against write time and find out which side is slow.

                    An empty result means this pipeline does not log its stages. It does not mean
                    the run did no work.
                    """)
    public PageResponse<StageLogDtos.Response> byRun(
            @RequestParam String runId,
            @RequestParam(required = false) String chunkId,
            @RequestParam(required = false) String stage,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "100") int size) {

        return PageResponse.from(
                stageLog.find(tenantContext.currentTenant(), RunId.parse(runId),
                        parseChunk(chunkId), parseStage(stage),
                        new PageQuery(page, size, null, false)),
                StageLogDtos.Response::from);
    }

    /**
     * A chunk id, or a trace id that contains one.
     *
     * <p>Accepting both means somebody can paste whichever identifier they happen to be holding —
     * a trace id copied out of a support ticket narrows to its chunk rather than returning
     * nothing, which is what a bare parse would have done.
     */
    private static SplitId parseChunk(String chunkId) {
        if (chunkId == null || chunkId.isBlank()) {
            return null;
        }
        return SplitId.parse(StageLogPort.Trace.chunkOf(chunkId.trim()));
    }

    /** An unrecognised stage filters nothing rather than failing the search. */
    private static StageLogPort.Stage parseStage(String stage) {
        if (stage == null || stage.isBlank()) {
            return null;
        }
        try {
            return StageLogPort.Stage.valueOf(stage.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
