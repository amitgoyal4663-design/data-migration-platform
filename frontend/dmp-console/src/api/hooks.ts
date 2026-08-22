import {
  keepPreviousData,
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query'
import { api, query } from './client'
import type {
  ChunkSummary,
  AuditPolicy,
  Chunk,
  ChunkingPolicy,
  DeliveryPolicy,
  ConnectorInstance,
  ConnectorSpec,
  ErrorGroup,
  ExecutionPolicy,
  Page,
  Pipeline,
  PipelineDefinition,
  PipelineMode,
  PipelineVersion,
  PipelineVersionSummary,
  RateLimit,
  RecordError,
  Reconciliation,
  RecordSearchCriteria,
  RecordIndexEntry,
  StageLogEntry,
  AuditEntry,
  ReplayRequest,
  RetryRequest,
  Run,
  RunState,
  Schedule,
  CreateScheduleRequest,
  UpdateScheduleRequest,
  TransformTestRequest,
  TransformTestResponse,
  ValidationResponse,
  WindowPreview,
} from './types'

/**
 * Query keys, centralised.
 *
 * Invalidation is the whole reason: publishing a version has to refresh the pipeline, its version
 * list and that one version. Keys scattered across components drift, and the symptom is a screen
 * that quietly shows stale data after a successful action.
 */
export const keys = {
  schedules: (pipelineId?: string) => ['schedules', pipelineId ?? 'all'] as const,
  pipelines: (params?: unknown) => ['pipelines', params] as const,
  pipeline: (id: string) => ['pipeline', id] as const,
  versions: (pipelineId: string) => ['versions', pipelineId] as const,
  version: (pipelineId: string, versionId: string) => ['version', pipelineId, versionId] as const,
  connectors: (params?: unknown) => ['connector-instances', params] as const,
  connector: (id: string) => ['connector-instance', id] as const,
  catalogue: () => ['connector-catalogue'] as const,
  runs: (params?: unknown) => ['runs', params] as const,
  run: (id: string) => ['run', id] as const,
  chunks: (runId: string) => ['chunks', runId] as const,
  runErrors: (runId: string) => ['run-errors', runId] as const,
  errorGroups: (runId: string) => ['run-error-groups', runId] as const,
  reconciliation: (runId: string) => ['run-reconciliation', runId] as const,
}

/** How often a live view refetches. Fast enough to feel live, slow enough not to hammer the API. */
const LIVE_REFRESH_MS = 2000

// ------------------------------------------------------------------ pipelines

export function usePipelines(params: { name?: string; folder?: string; page?: number } = {}) {
  return useQuery({
    queryKey: keys.pipelines(params),
    queryFn: () =>
      api.get<Page<Pipeline>>(
        `/api/v1/pipelines${query({ name: params.name, folder: params.folder, page: params.page ?? 0, size: 50 })}`,
      ),
  })
}

export function usePipeline(id: string | undefined) {
  return useQuery({
    queryKey: keys.pipeline(id!),
    queryFn: () => api.get<Pipeline>(`/api/v1/pipelines/${id}`),
    enabled: Boolean(id),
  })
}

export function useCreatePipeline() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (body: { name: string; description?: string; folder?: string; tags?: string[] }) =>
      api.post<Pipeline>('/api/v1/pipelines', body),
    onSuccess: () => client.invalidateQueries({ queryKey: ['pipelines'] }),
  })
}

export function useArchivePipeline() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => api.post<Pipeline>(`/api/v1/pipelines/${id}/archive`),
    onSuccess: () => client.invalidateQueries({ queryKey: ['pipelines'] }),
  })
}

// ------------------------------------------------------------------- versions

export function useVersions(pipelineId: string | undefined) {
  return useQuery({
    queryKey: keys.versions(pipelineId!),
    queryFn: () => api.get<PipelineVersionSummary[]>(`/api/v1/pipelines/${pipelineId}/versions`),
    enabled: Boolean(pipelineId),
  })
}

export function useVersion(pipelineId: string | undefined, versionId: string | undefined) {
  return useQuery({
    queryKey: keys.version(pipelineId!, versionId!),
    queryFn: () =>
      api.get<PipelineVersion>(`/api/v1/pipelines/${pipelineId}/versions/${versionId}`),
    enabled: Boolean(pipelineId && versionId),
  })
}

/**
 * Starts a new draft from a specific existing version.
 *
 * A plain new version already inherits from the <em>latest</em> one, which is right when you are
 * iterating forwards. This is for the other case: going back to what is actually live, or to the
 * version before a change that turned out to be wrong, and continuing from there. Without it the
 * only route back was rolling production onto the old version first, which is a production change
 * made in order to start an edit.
 *
 * The whole version is copied — graph, chunking, execution and audit policies, mode — because a
 * copy that silently reset the policies would look identical on the canvas and behave differently
 * at 3am.
 */
export function useCopyVersion(pipelineId: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: async (source: PipelineVersionSummary) => {
      const full = await api.get<PipelineVersion>(
        `/api/v1/pipelines/${pipelineId}/versions/${source.id}`,
      )
      return api.post<PipelineVersion>(`/api/v1/pipelines/${pipelineId}/versions`, {
        definition: full.definition,
        chunkingPolicy: full.chunkingPolicy,
        executionPolicy: full.executionPolicy,
        auditPolicy: full.auditPolicy,
        mode: full.mode,
        changeNote: `Copied from v${full.versionNumber}`,
      })
    },
    onSuccess: () => {
      client.invalidateQueries({ queryKey: keys.versions(pipelineId) })
      client.invalidateQueries({ queryKey: keys.pipeline(pipelineId) })
    },
  })
}

export function useCreateVersion(pipelineId: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (body: { definition?: PipelineDefinition; changeNote?: string }) =>
      api.post<PipelineVersion>(`/api/v1/pipelines/${pipelineId}/versions`, body),
    onSuccess: () => {
      client.invalidateQueries({ queryKey: keys.versions(pipelineId) })
      client.invalidateQueries({ queryKey: keys.pipeline(pipelineId) })
    },
  })
}

/**
 * Removes a draft version.
 *
 * Drafts only — the server refuses a published one, because a run pins the version it executed and
 * deleting it would leave that run unable to say what it ran.
 */
export function useDeleteVersion(pipelineId: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (versionId: string) =>
      api.delete(`/api/v1/pipelines/${pipelineId}/versions/${versionId}`),
    onSuccess: () => {
      client.invalidateQueries({ queryKey: keys.versions(pipelineId) })
      client.invalidateQueries({ queryKey: keys.pipeline(pipelineId) })
    },
  })
}

/**
 * Runs a connector's connectivity check.
 *
 * A failed test resolves rather than rejects — the server answers "is this usable", and "no,
 * the topic does not exist" is a successful answer carrying the connector's own explanation.
 */
export function useUpdateConnectorInstance() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: ({ id, ...body }: {
      id: string
      name: string
      direction: string
      config: Record<string, unknown>
      secretRefs: Record<string, string>
      description?: string
      rateLimit?: RateLimit | null
    }) => api.put<ConnectorInstance>(`/api/v1/connector-instances/${id}`, body),
    onSuccess: () => client.invalidateQueries({ queryKey: ['connector-instances'] }),
  })
}

export function useTestConnection() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (id: string) =>
      api.post<ConnectorInstance>(`/api/v1/connector-instances/${id}/test`),
    onSuccess: () => client.invalidateQueries({ queryKey: ['connector-instances'] }),
  })
}

export function useSaveDefinition(pipelineId: string, versionId: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (definition: PipelineDefinition) =>
      api.put<PipelineVersion>(
        `/api/v1/pipelines/${pipelineId}/versions/${versionId}/definition`,
        { definition },
      ),
    onSuccess: () => client.invalidateQueries({ queryKey: keys.version(pipelineId, versionId) }),
  })
}

/** Updates sizing and parallelism on a draft version. Rejected on a published one. */
export function useUpdatePolicies(pipelineId: string, versionId: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (body: {
      chunkingPolicy: ChunkingPolicy
      executionPolicy: ExecutionPolicy
      auditPolicy: AuditPolicy
      deliveryPolicy: DeliveryPolicy
      mode: PipelineMode
    }) =>
      api.put<PipelineVersion>(
        `/api/v1/pipelines/${pipelineId}/versions/${versionId}/policies`,
        body,
      ),
    onSuccess: () => client.invalidateQueries({ queryKey: keys.version(pipelineId, versionId) }),
  })
}

export function useValidateVersion(pipelineId: string, versionId: string) {
  return useMutation({
    mutationFn: () =>
      api.post<ValidationResponse>(
        `/api/v1/pipelines/${pipelineId}/versions/${versionId}/validate`,
      ),
  })
}

/**
 * Runs a script against one record and reports what it did.
 *
 * A mutation rather than a query: it has no cache key worth keeping, and the user decides when it
 * happens. A failing script is the expected case here, so the failure arrives in the response body
 * rather than as a rejected promise — nothing about a broken script should look like an outage.
 */
export function useTestTransform() {
  return useMutation({
    mutationFn: (request: TransformTestRequest) =>
      api.post<TransformTestResponse>('/api/v1/transforms/test', request),
  })
}

export function usePublishVersion(pipelineId: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (versionNumber: number) =>
      api.post<PipelineVersion>(`/api/v1/pipelines/${pipelineId}/versions/${versionNumber}/publish`),
    onSuccess: () => {
      // Publishing changes the pipeline's runnability and every version's standing, so the whole
      // subtree is refreshed rather than one key.
      client.invalidateQueries({ queryKey: keys.versions(pipelineId) })
      client.invalidateQueries({ queryKey: keys.pipeline(pipelineId) })
      client.invalidateQueries({ queryKey: ['pipelines'] })
    },
  })
}

// ----------------------------------------------------------------- connectors

/**
 * The installed connector catalogue.
 *
 * Cached indefinitely: connectors change only when a worker restarts with a new plugin, and
 * refetching a static catalogue on every screen would be noise.
 */
export function useConnectorCatalogue() {
  return useQuery({
    queryKey: keys.catalogue(),
    queryFn: () => api.get<ConnectorSpec[]>('/api/v1/connectors'),
    staleTime: Infinity,
  })
}

export function useConnectorInstances(params: { name?: string; connectorType?: string } = {}) {
  return useQuery({
    queryKey: keys.connectors(params),
    queryFn: () =>
      api.get<Page<ConnectorInstance>>(
        `/api/v1/connector-instances${query({ ...params, size: 100 })}`,
      ),
  })
}

export function useCreateConnectorInstance() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (body: {
      name: string
      connectorType: string
      direction: string
      config: Record<string, unknown>
      secretRefs: Record<string, string>
      description?: string
      rateLimit?: RateLimit | null
    }) => api.post<ConnectorInstance>('/api/v1/connector-instances', body),
    onSuccess: () => client.invalidateQueries({ queryKey: ['connector-instances'] }),
  })
}

export function useDeleteConnectorInstance() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => api.delete(`/api/v1/connector-instances/${id}`),
    onSuccess: () => client.invalidateQueries({ queryKey: ['connector-instances'] }),
  })
}

// ----------------------------------------------------------------------- runs

/**
 * A page of runs.
 *
 * <p>Paged on the server rather than in the browser. Runs are unbounded — a nightly pipeline has
 * thousands within a year — so the alternative is fetching a set that grows for ever to show
 * twenty-five rows of it.
 *
 * <p>`placeholderData` keeps the previous page on screen while the next one loads. Without it the
 * table empties on every page change, and because this query also polls, it would flash on every
 * refresh too.
 */
export function useRuns(
  params: { pipelineId?: string; state?: RunState[]; page?: number; size?: number } = {},
) {
  const page = params.page ?? 0
  const size = params.size ?? 50

  return useQuery({
    queryKey: keys.runs({ ...params, page, size }),
    queryFn: () => {
      const search = new URLSearchParams({ page: String(page), size: String(size) })
      if (params.pipelineId) search.set('pipelineId', params.pipelineId)
      params.state?.forEach((state) => search.append('state', state))
      return api.get<Page<Run>>(`/api/v1/runs?${search}`)
    },
    placeholderData: keepPreviousData,
    refetchInterval: LIVE_REFRESH_MS,
  })
}

/**
 * One run, polled while it is still moving.
 *
 * Polling stops once the run reaches a terminal state — a completed run's page should not keep
 * issuing requests for data that can no longer change.
 */
export function useRun(id: string | undefined) {
  return useQuery({
    queryKey: keys.run(id!),
    queryFn: () => api.get<Run>(`/api/v1/runs/${id}`),
    enabled: Boolean(id),
    refetchInterval: (queryData) => (queryData.state.data?.terminal ? false : LIVE_REFRESH_MS),
  })
}

/**
 * A run's chunks, a page at a time.
 *
 * <p>It used to fetch every chunk of the run to draw the fifty rows that fit on the screen — a
 * hundred thousand documents and as many DTOs for a run that size, repeated on every live refresh.
 * The count still comes back with the first page, so the tab can say how many there are without
 * holding them.
 */
export function useChunks(runId: string | undefined, live: boolean) {
  return useInfiniteQuery({
    queryKey: keys.chunks(runId!),
    initialPageParam: 0,
    queryFn: ({ pageParam }) =>
      api.get<Page<Chunk>>(`/api/v1/runs/${runId}/chunks${query({ page: pageParam, size: 10 })}`),
    getNextPageParam: (last) => (last.hasNext ? last.page + 1 : undefined),
    enabled: Boolean(runId),
    // Only the pages already loaded are refreshed; scrolling further still asks for the rest.
    refetchInterval: live ? LIVE_REFRESH_MS : false,
  })
}

/** The few numbers the retry banner needs, without the chunks behind them. */
export function useChunkSummary(runId: string | undefined, live: boolean) {
  return useQuery({
    queryKey: ['chunk-summary', runId],
    queryFn: () => api.get<ChunkSummary>(`/api/v1/runs/${runId}/chunk-summary`),
    enabled: Boolean(runId),
    refetchInterval: live ? LIVE_REFRESH_MS : false,
  })
}

/**
 * A run's balance sheet.
 *
 * Kept polling while the run is live so the tab is not stale when it finishes, but the server
 * withholds a verdict until then — a mid-run figure is arithmetic about a moving target, and the
 * number somebody remembers is the first one they read.
 */
export function useReconciliation(runId: string | undefined, live: boolean) {
  return useQuery({
    queryKey: keys.reconciliation(runId!),
    queryFn: () => api.get<Reconciliation>(`/api/v1/runs/${runId}/reconciliation`),
    enabled: Boolean(runId),
    refetchInterval: live ? LIVE_REFRESH_MS : false,
  })
}

export function useRunErrors(runId: string | undefined) {
  return useQuery({
    queryKey: keys.runErrors(runId!),
    queryFn: () => api.get<RecordError[]>(`/api/v1/runs/${runId}/errors?limit=200`),
    enabled: Boolean(runId),
  })
}

/**
 * A run's rejections collapsed to the distinct faults behind them.
 *
 * Twenty thousand records failing one rule are a single row here. The flat list is still available
 * for the sample payloads, but this is what gets read first.
 */
export function useErrorGroups(runId: string | undefined) {
  return useQuery({
    queryKey: keys.errorGroups(runId!),
    queryFn: () => api.get<ErrorGroup[]>(`/api/v1/runs/${runId}/error-groups`),
    enabled: Boolean(runId),
  })
}

/**
 * Re-attempts a finished run's unsuccessful chunks, or one named chunk.
 *
 * Returns a new run rather than reviving this one, so the caller navigates to the result.
 */
export function useRetryRun(runId: string) {
  const client = useQueryClient()
  const invalidate = () => client.invalidateQueries({ queryKey: ['runs'] })

  return {
    run: useMutation({
      mutationFn: (request: RetryRequest) =>
        api.post<Run>(`/api/v1/runs/${runId}/retry`, request),
      onSuccess: invalidate,
    }),
    chunk: useMutation({
      mutationFn: ({ chunkId, ...request }: RetryRequest & { chunkId: string }) =>
        api.post<Run>(`/api/v1/runs/${runId}/chunks/${chunkId}/retry`, request),
      onSuccess: invalidate,
    }),
  }
}

/**
 * Re-delivers the records a run rejected, through the same transforms and sink.
 *
 * Returns a new run, like retry does, so the caller navigates to the result rather than watching
 * this one change underneath them.
 */
export function useReplayRun(runId: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (request: ReplayRequest) =>
      api.post<Run>(`/api/v1/runs/${runId}/replay`, request),
    onSuccess: () => client.invalidateQueries({ queryKey: ['runs'] }),
  })
}

/**
 * The placeholders this pipeline's source query expects, so the Run dialog asks for the right ones.
 *
 * Asked of the backend rather than parsed here: only the connector knows that a Databricks query
 * writes its placeholders as `:from`, and a second copy of that rule in TypeScript would drift from
 * the one that actually binds them.
 */
export function useRunParameterNames(pipelineId: string | undefined) {
  return useQuery({
    queryKey: ['run-parameters', pipelineId],
    queryFn: () =>
      api.get<{ names: string[] }>(`/api/v1/pipelines/${pipelineId}/run-parameters`),
    enabled: Boolean(pipelineId),
  })
}

export function useStartRun() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: ({
      pipelineId,
      parameters,
    }: {
      pipelineId: string
      parameters?: Record<string, string>
    }) =>
      api.post<Run>(
        `/api/v1/pipelines/${pipelineId}/runs`,
        parameters && Object.keys(parameters).length > 0 ? { parameters } : undefined,
        {
          // Makes a double-click harmless: the second request returns the run the first created
          // rather than starting a second migration.
          'Idempotency-Key': `console-${pipelineId}-${Date.now()}`,
        },
      ),
    onSuccess: () => client.invalidateQueries({ queryKey: ['runs'] }),
  })
}

export function useRunControl(runId: string) {
  const client = useQueryClient()
  const invalidate = () => {
    client.invalidateQueries({ queryKey: keys.run(runId) })
    client.invalidateQueries({ queryKey: ['runs'] })
  }

  return {
    pause: useMutation({
      mutationFn: () => api.post<Run>(`/api/v1/runs/${runId}/pause`),
      onSuccess: invalidate,
    }),
    resume: useMutation({
      mutationFn: () => api.post<Run>(`/api/v1/runs/${runId}/resume`),
      onSuccess: invalidate,
    }),
    stop: useMutation({
      mutationFn: () => api.post<Run>(`/api/v1/runs/${runId}/stop`),
      onSuccess: invalidate,
    }),
  }
}


// ---------------------------------------------------------------- schedules

/**
 * Recurring rules.
 *
 * Every mutation invalidates the whole list rather than one key, because the next fire time is
 * computed by the scheduler and changes on the server the moment a rule is saved — an optimistic
 * update would show a stale one, which is the single number people open this screen to read.
 */
export function useSchedules(pipelineId?: string) {
  return useQuery({
    queryKey: keys.schedules(pipelineId),
    queryFn: () =>
      api.get<Schedule[]>(
        pipelineId ? `/api/v1/schedules?pipelineId=${pipelineId}` : '/api/v1/schedules',
      ),
    // The next fire time is a moving target; a stale one is worse than a refetch.
    refetchInterval: 30_000,
  })
}

export function useCreateSchedule() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (request: CreateScheduleRequest) =>
      api.post<Schedule>('/api/v1/schedules', request),
    onSuccess: () => client.invalidateQueries({ queryKey: ['schedules'] }),
  })
}

export function useUpdateSchedule(scheduleId: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (request: UpdateScheduleRequest) =>
      api.put<Schedule>(`/api/v1/schedules/${scheduleId}`, request),
    onSuccess: () => client.invalidateQueries({ queryKey: ['schedules'] }),
  })
}

/**
 * Shows what a window script would produce for the next few firings.
 *
 * A mutation rather than a query: it saves nothing and the user decides when to run it, and a
 * script mid-edit is usually broken — refetching one on every keystroke would fill the panel with
 * syntax errors from a line half typed.
 */
export function usePreviewWindow() {
  return useMutation({
    mutationFn: (body: { cronExpression: string; timezone: string; windowScript: string }) =>
      api.post<WindowPreview>('/api/v1/schedules/preview-window', body),
  })
}

export function useSetScheduleEnabled() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: ({ id, enabled }: { id: string; enabled: boolean }) =>
      api.post<Schedule>(`/api/v1/schedules/${id}/${enabled ? 'enable' : 'disable'}`),
    onSuccess: () => client.invalidateQueries({ queryKey: ['schedules'] }),
  })
}

export function useDeleteSchedule() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => api.delete(`/api/v1/schedules/${id}`),
    onSuccess: () => client.invalidateQueries({ queryKey: ['schedules'] }),
  })
}

// ------------------------------------------------------- record search & audit

/**
 * Finds what happened to one record, across every run.
 *
 * Scoped to one pipeline, because a record key is only unique within the source it came from, and
 * because access will be granted per pipeline — an unscoped query could not be authorized.
 *
 * Disabled until both a pipeline and a key are chosen: an empty key would ask the server for
 * everything, and the answer to "show me all records" is not a page the support engineer wants.
 */
/**
 * Finds records by anything a support desk actually has.
 *
 * A customer quotes an order number, not a pipeline id and a MongoDB _id — so `q` searches every
 * field of the payload, and every filter is optional. Disabled until something is typed: an empty
 * query asks the server for every record ever indexed, which is nobody's question.
 */
export function useRecordLookup(criteria: RecordSearchCriteria) {
  const something = Boolean(criteria.q?.trim() || criteria.key?.trim())
  return useQuery({
    queryKey: ['record-lookup', criteria],
    queryFn: () =>
      api.get<Page<RecordIndexEntry>>(
        `/api/v1/records/search${query({ ...criteria, size: 50 })}`,
      ),
    enabled: something,
  })
}

export function useRecordSearch(pipelineId: string, key: string) {
  return useQuery({
    queryKey: ['record-search', pipelineId, key],
    queryFn: () =>
      api.get<Page<RecordIndexEntry>>(
        `/api/v1/records${query({ pipelineId, key, size: 50 })}`,
      ),
    enabled: Boolean(pipelineId) && key.trim().length > 0,
  })
}

/** The records one run handled, for reconciling a single migration. */
export function useRunRecords(runId: string, outcome?: string) {
  return useQuery({
    queryKey: ['run-records', runId, outcome],
    queryFn: () =>
      api.get<Page<RecordIndexEntry>>(
        `/api/v1/records/by-run${query({ runId, outcome, size: 50 })}`,
      ),
    enabled: Boolean(runId),
  })
}

/**
 * One run's stages, oldest first — the run read as a sequence rather than as a set of lists.
 *
 * Optionally narrowed to one chunk, or to one stage across the whole run (which is how read time
 * gets compared against write time). Empty when the pipeline does not log its stages, which the
 * page has to state as "not switched on" rather than as "nothing happened".
 */
/**
 * A run's stages, a page at a time.
 *
 * <p>Infinite rather than a single large fetch, because the number of entries is a property of the
 * data and not of the screen: a run delivering one record per call writes four entries per record,
 * so a modest migration produces hundreds of thousands. The previous version asked for two hundred
 * and told the reader the rest existed somewhere they could not reach.
 *
 * <p>Fifty entries a page, which is about ten cycles of an ordinary pipeline — so one scroll is one
 * request rather than one request feeding several scrolls. Only about: the server pages by entry,
 * because an entry is what it stores, and a cycle is four entries or four hundred depending on how
 * the batch was delivered. Paging by cycle would need the server to group before it pages.
 */
export function useRunStages(runId: string, chunkId?: string, stage?: string) {
  return useInfiniteQuery({
    queryKey: ['run-stages', runId, chunkId, stage],
    initialPageParam: 0,
    queryFn: ({ pageParam }) =>
      api.get<Page<StageLogEntry>>(
        `/api/v1/stages/by-run${query({ runId, chunkId, stage, page: pageParam, size: 50 })}`,
      ),
    // The server says whether more exists; deriving it from a short page would stop early on a
    // page that happens to be exactly full.
    getNextPageParam: (last) => (last.hasNext ? last.page + 1 : undefined),
    enabled: Boolean(runId),
  })
}

/** The control-plane trail: who changed what, newest first. Every filter optional. */
export function useAuditTrail(filters: {
  resourceType?: string
  resourceId?: string
  actor?: string
  page?: number
}) {
  return useQuery({
    queryKey: ['audit', filters],
    queryFn: () =>
      api.get<Page<AuditEntry>>(
        `/api/v1/audit${query({
          resourceType: filters.resourceType,
          resourceId: filters.resourceId,
          actor: filters.actor,
          page: filters.page ?? 0,
          size: 50,
        })}`,
      ),
  })
}
