/**
 * The API contract, mirrored in TypeScript.
 *
 * Hand-written for now. Phase 8 generates these from the backend's OpenAPI document, which is the
 * reason backend and console share one repository — across two, every API change would open a
 * window where the spec and its consumer disagree.
 */

export type PipelineStatus = 'DRAFT' | 'ACTIVE' | 'ARCHIVED'
export type VersionStatus = 'DRAFT' | 'VALIDATED' | 'PUBLISHED'
export type PipelineMode = 'FULL_LOAD' | 'INCREMENTAL' | 'STREAMING' | 'CDC'
export type ConnectorDirection = 'SOURCE' | 'SINK' | 'BOTH'
export type ConnectorStatus = 'UNTESTED' | 'ACTIVE' | 'FAILED' | 'DISABLED'

export type RunState =
  | 'CREATED'
  | 'VALIDATED'
  | 'PREPARING'
  | 'RUNNING'
  | 'PAUSED'
  | 'STOPPING'
  | 'FINALIZING'
  | 'COMPLETED'
  | 'FAILED'
  | 'STOPPED'
  | 'ARCHIVED'

export type ChunkState =
  | 'PENDING'
  | 'RUNNING'
  // Handed to a system that answers later - a Salesforce bulk job - and put down until it does.
  // The worker is free; a scheduled check asks the destination and returns the chunk to the pool.
  | 'WAITING_EXTERNAL'
  | 'COMPLETED'
  | 'FAILED'
  | 'ABANDONED'
  | 'CANCELLED'

export type NodeType =
  | 'SOURCE'
  | 'SINK'
  | 'TRANSFORM'
  | 'BATCH_TRANSFORM'
  | 'FILTER'
  | 'MAPPER'
  | 'SPLITTER'
  | 'MERGER'
  | 'VALIDATION'
  | 'ERROR_HANDLER'
  | 'DELAY'
  | 'RETRY'

export interface Page<T> {
  content: T[]
  page: number
  size: number
  /** -1 when the store cannot count cheaply. Never render it without checking. */
  totalElements: number
  totalPages: number
  hasNext: boolean
}

export interface Pipeline {
  id: string
  /** On the support team's daily operations dashboard. */
  monitored: boolean
  name: string
  description: string | null
  folder: string | null
  tags: string[]
  status: PipelineStatus
  publishedVersion: number | null
  latestVersion: number
  runnable: boolean
  createdAt: string
  updatedAt: string
}

export interface NodeDefinition {
  id: string
  type: NodeType
  name: string
  connectorInstanceId: string | null
  config: Record<string, unknown>
}

export interface EdgeDefinition {
  id: string
  from: string
  to: string
  condition: string | null
}

export interface PipelineDefinition {
  nodes: NodeDefinition[]
  edges: EdgeDefinition[]
}

/**
 * Flow control.
 *
 * There is no write size: the batch *is* the chunk. Two numbers for one nested thing could only
 * agree by accident — a chunk of 100 with a batch of 1,000 wrote batches of 100 and the setting
 * meant nothing. Where a destination needs smaller calls than a chunk, that is `DeliveryPolicy`.
 */
export interface ChunkingPolicy {
  /** 0 takes the platform default. Never larger than the chunk — the engine clamps it. */
  readFetchSize: number
  /** The real memory guarantee. Record count alone provides none. */
  maxBatchBytes: number
  flushInterval: string
  maxInFlightBatches: number
  /** 0 lets the engine decide from what the destination declares it can absorb. */
  checkpointEveryNBatches: number
}

/**
 * A run's chunks counted by state, without holding the chunks.
 *
 * Answers "can this be retried, and what would it re-send" in a few numbers. The console used to
 * derive these by fetching every chunk of the run.
 */
export interface ChunkSummary {
  byState: Record<string, number>
  /** Failed or abandoned. */
  failed: number
  /** Never finished: cancelled, pending, running, or parked on a destination that answers later. */
  unfinished: number
  /** Already written by chunks that did not finish, and so what a retry would deliver twice. */
  recordsAtRisk: number
}

/** How loudly a dashboard finding should be read. */
export type FindingSeverity = 'INFO' | 'WARNING' | 'CRITICAL'

export interface OperationsFinding {
  severity: FindingSeverity
  code: string
  message: string
  /** What the number was judged against, so the screen can show the comparison, not just a verdict. */
  detail: string | null
}

export interface PipelineHealth {
  pipelineId: string
  name: string
  /** The most recent real run. Null for a watched pipeline that has never run. */
  latest: Run | null
  /** Median records read over recent completed runs. Null until there are enough to say anything. */
  typicalRows: number | null
  typicalSeconds: number | null
  /** How many runs the comparison rests on — three and ten deserve different belief. */
  baselineRuns: number
  worst: FindingSeverity
  healthy: boolean
  findings: OperationsFinding[]
  /** Biggest first. "180 Policy_Number__c is required" says whose problem it is; "222" does not. */
  reasons: OperationsFailureReason[]
  /** The last seven runs, newest first. */
  trend: OperationsAttempt[]
  schedule: OperationsSchedule | null
}

export interface OperationsLiveRun {
  runId: string
  pipeline: string
  state: RunState
  /** 0 to 1; null before planning finishes. */
  progress: number | null
  recordsRead: number
  recordsWritten: number
  seconds: number
}

export interface OperationsTotals {
  completed: number
  failed: number
  recordsRead: number
  recordsWritten: number
  recordsFailed: number
  running: number
}

/** Why records failed on the last run — the reason, not just the count. */
export interface OperationsFailureReason {
  count: number
  code: string
  reason: string
}

/** One earlier run, for the trend beside today's number. */
export interface OperationsAttempt {
  runId: string
  state: RunState
  at: string
  read: number
  written: number
  failed: number
  seconds: number
}

export interface OperationsSchedule {
  name: string
  cron: string
  timezone: string
  lastFiredAt: string | null
  /** So "it has not run" reads as late, or as simply not due yet. */
  nextDueAt: string | null
}

/** One line of the summary strip: a whole sentence, already interpreted. */
export interface OperationsHeadline {
  severity: FindingSeverity
  headline: string
  detail: string
  pipelineId: string | null
  runId: string | null
}

export interface OperationsDashboard {
  /** Worst first: a screen read every morning is scanned from the top. */
  pipelines: PipelineHealth[]
  watched: number
  healthy: number
  /** In flight now. Excludes paused runs — they hold a slot but nothing is happening. */
  live: OperationsLiveRun[]
  /** Every run in the window, watched or not. */
  totals: OperationsTotals
  /** Biggest first. Read at a glance and often relayed to somebody else verbatim. */
  headlines: OperationsHeadline[]
  generatedAt: string
}

/** How a reconciliation row should be read. The server decides; the console only styles. */
export type ReconciliationKind =
  | 'TOTAL'
  | 'DEDUCTION'
  | 'SUBTOTAL'
  | 'RESULT'
  | 'PENDING'
  | 'BALANCE'

export interface ReconciliationLine {
  label: string
  count: number
  kind: ReconciliationKind
  note: string
}

/** One comparison between the run's own counters and the record index. */
export interface ReconciliationCheck {
  label: string
  /** What the run's counters say. */
  expected: number
  /** What the index says. */
  actual: number
  difference: number
  passed: boolean
  note: string
}

/**
 * A run's balance sheet — what a migration is signed off with.
 *
 * The sheet is rendered without the console knowing what any line means, so adding a line on the
 * server never needs a change here.
 */
export interface Reconciliation {
  verdict: 'BALANCED' | 'DISCREPANCY' | 'INCOMPLETE'
  sheet: ReconciliationLine[]
  /** Empty when the pipeline does not index records, so there is nothing to check against. */
  checks: ReconciliationCheck[]
  byOutcome: Record<string, number>
  indexedTotal: number
  indexed: boolean
  complete: boolean
  runId: string
  pipelineName: string
  /** These numbers describe a rehearsal. Carried here because the CSV is read away from the page. */
  dryRun: boolean
  generatedAt: string
}

/**
 * A few records as the source produced them.
 *
 * Payloads are unaltered — the shape somebody is here to see. Normalising them would show the
 * platform's idea of their data, which is exactly what a preview exists to prevent.
 */
export interface SourcePreview {
  records: Record<string, unknown>[]
  /**
   * Every field name seen, in the order first encountered.
   *
   * Across all records, not just the first: a source may omit a null field on one row and include
   * it on the next, and a table built from record one would silently lose the column.
   */
  fields: string[]
  /** What was actually asked, so an empty preview is diagnosable rather than disappointing. */
  query: string | null
  durationMillis: number
  /** The source had more. Without this, hitting the limit and reaching the end look identical. */
  more: boolean
}

/** The outcomes worth telling somebody about. Fewer than the run lifecycle has, deliberately. */
export type NotifierEvent =
  | 'RUN_FAILED'
  /** Finished, but records were refused or a script threw. The outcome that goes unnoticed. */
  | 'RUN_COMPLETED_WITH_FAILURES'
  | 'RUN_COMPLETED'
  | 'RUN_STOPPED'

export interface Notifier {
  id: string
  name: string
  url: string
  /** One pipeline, or null for every pipeline in the tenant. */
  pipelineId: string | null
  events: NotifierEvent[]
  secretHeader: string | null
  /** The reference, such as env:SLACK_TOKEN. Never the value. */
  secretRef: string | null
  enabled: boolean
  description: string | null
  /**
   * When this last tried to deliver.
   *
   * Shown because the commonest way alerting fails is silently: a rotated URL answers 404 forever
   * and nobody notices, because the thing that would say so is the thing that is broken.
   */
  lastAttemptAt: string | null
  lastAttemptSucceeded: boolean
  lastAttemptError: string | null
  createdAt: string
  updatedAt: string
  rowVersion: number
}

/** 0 = unlimited, 1 = strictly sequential, N = exactly N chunks in flight across the fleet. */
export interface ExecutionPolicy {
  maxConcurrentChunks: number
  maxChunksPerPod: number
  chunkLease: string
  maxAttemptsPerChunk: number
  /** Source rows one chunk covers. 0 derives it from the read size. */
  rowsPerChunk: number
  /**
   * Share of a chunk's records that may be rejected before the chunk counts as failed.
   * null means no limit; 0 means any rejection at all fails the chunk.
   */
  maxFailedPercent: number | null
  /** The same limit as an absolute count. null means no limit; 0 means any rejection fails. */
  maxFailedRecords: number | null
  /** Whether the first abandoned chunk ends the run, instead of the rest running on regardless. */
  stopRunOnChunkFailure: boolean
}

export interface AuditPolicy {
  level: 'COUNTERS' | 'ERRORS' | 'INDEXED' | 'FULL'
  /** INDEXED only: index each record's content too, so any field is searchable. */
  indexPayloads: boolean
  redactedFields: string[]
  redactionMode: 'MASK' | 'HASH' | 'DROP'
  /** Read as seconds (what the API sends); written as an ISO-8601 duration (what it accepts). */
  retention: string | number
  /** Payloads kept per distinct fault. 0 keeps every one — and only a stored payload can be replayed. */
  samplesPerSignature: number
  /** Ceiling on one stored payload in bytes. 0 stores it however large it is. */
  maxPayloadBytes: number
  /**
   * Whether a rejected record's content is kept, which is what makes it replayable.
   *
   * Separate from `level`: that decides whether every record's identity is searchable, this decides
   * whether a failure's content is stored. Forced off for a Salesforce sink, which reports how many
   * records a bulk job refused but not which ones.
   */
  captureRejectedPayloads: boolean
  /** Which stages of the work are logged, as distinct from the records they carried. */
  stageLog: StageLogPolicy
}

/**
 * Which stages of the work the platform writes a log for.
 *
 * Every other audit setting describes a *record*. None of them describe the *work* — and the work
 * happens in batches, so the two do not line up. Five hundred records refused in one request is
 * five hundred rejections and one status code; forty read and thirty-one written is one filter
 * dropping nine, and no per-record entry names the node that did it.
 *
 * All off by default. Counts and timings are tiny; bodies are customer data in a store built to be
 * searched, which is why they are a fourth switch rather than part of the other three.
 */
export interface StageLogPolicy {
  /** One entry per window of reading: the query, the cursor either side, rows and duration. */
  reads: boolean
  /** One entry per pass of the scripts: records in, records out, and how long they took. */
  transforms: boolean
  /** One entry per call handed over: records, bytes, duration, and what the destination said. */
  writes: boolean
  /** Whether those entries also carry the request and response content, redacted and capped. */
  bodies: boolean
}

/** One thing the platform did — a window of reading, a pass of the transforms, or a call out. */
export interface StageLogEntry {
  runId: string
  chunkId: string
  /** The one read → transform → write cycle this belongs to, as `<chunkId>#<cycle>`. */
  traceId: string
  /**
   * `FETCH` is the source's own unit — one call it actually made — and `READ` is the engine's:
   * however much reading it took to fill one batch. They are deliberately both here. One FETCH
   * against two READs says a single call was buffered into two batches, which is the distinction
   * a READ entry cannot draw and which everyone reading two identical queries drew wrongly.
   */
  stage: 'FETCH' | 'READ' | 'TRANSFORM' | 'WRITE'
  nodeId: string
  nodeName: string
  connectorType: string
  /** Counts within one stage — the third read, the third write. Not an ordering across stages. */
  sequence: number
  /** Where this falls among all of its chunk's entries. What the log is actually ordered by. */
  position: number
  attempt: number
  recordsIn: number
  /** Differs from `recordsIn` only at TRANSFORM — which is the whole reason that stage is logged. */
  recordsOut: number
  bytes: number
  durationMs: number
  outcome: 'OK' | 'FAILED'
  errorCode: string | null
  errorMessage: string | null
  query: string | null
  cursorIn: unknown
  cursorOut: unknown
  details: Record<string, unknown> | null
  request: unknown
  response: unknown
  occurredAt: string
}

/**
 * How a batch is divided into calls on the sink.
 *
 * Separate from the batch size, which decides how much is buffered and how much is redone after a
 * crash. Before they were separated, reaching an API that wants one record per request meant
 * setting the batch size to 1 — which also made the engine checkpoint after every single record.
 *
 * A group never crosses a batch: two records sharing a label in different batches are two calls.
 */
export interface DeliveryPolicy {
  /** 0 = the whole batch in one call, 1 = one call per record, N = calls of N records. */
  groupSize: number
  /** JavaScript returning one group label per record. Mutually exclusive with groupSize. */
  splitScript: string | null
}

export interface PipelineVersion {
  id: string
  pipelineId: string
  versionNumber: number
  status: VersionStatus
  definition: PipelineDefinition
  chunkingPolicy: ChunkingPolicy
  executionPolicy: ExecutionPolicy
  auditPolicy: AuditPolicy
  deliveryPolicy: DeliveryPolicy
  mode: PipelineMode
  channelType: 'IN_PROCESS' | 'KAFKA'
  changeNote: string | null
  createdBy: string | null
  createdAt: string
  publishedAt: string | null
}

export interface PipelineVersionSummary {
  id: string
  versionNumber: number
  status: VersionStatus
  mode: PipelineMode
  nodeCount: number
  changeNote: string | null
  createdBy: string | null
  createdAt: string
  publishedAt: string | null
}

export interface ValidationIssue {
  code: string
  message: string
  nodeId: string | null
  edgeId: string | null
}

export interface ValidationResponse {
  valid: boolean
  errors: ValidationIssue[]
  warnings: ValidationIssue[]
}

/**
 * What the far end has agreed to accept.
 *
 * Either unit may be null, and null means unlimited — a client who gave exactly one number is the
 * common case. Windows are ISO-8601 periods (`PT5M`, `PT1H`, `P1D`) so a period is one field
 * rather than a number and a unit that can disagree.
 */
export interface RateLimit {
  records: number | null
  recordsWindow: string | null
  calls: number | null
  callsWindow: string | null
  /**
   * BURST spends a whole window at once and then waits — what a client whose counter resets on the
   * clock expects. EVEN never exceeds the limit in any window, sliding or not, and costs throughput
   * in proportion to how much of the window one call takes up.
   */
  pacing: 'BURST' | 'EVEN'
}

export interface ConnectorInstance {
  id: string
  name: string
  connectorType: string
  direction: ConnectorDirection
  config: Record<string, unknown>
  secretRefs: Record<string, string>
  status: ConnectorStatus
  description: string | null
  lastTestedAt: string | null
  lastTestError: string | null
  createdAt: string
  updatedAt: string
  rateLimit: RateLimit | null
}

/** A connector's self-description. The configuration form is rendered from `configSchema`. */
export interface ConnectorSpec {
  type: string
  displayName: string
  description: string
  direction: ConnectorDirection
  configSchema: JsonSchema
  secretFields: string[]
  version: string
  /**
   * How one chunk counts against a rate limit. PER_CHUNK means the whole chunk is one unit of work
   * — a bulk job, created and polled to completion — however many requests that takes underneath.
   */
  callCost: 'PER_REQUEST' | 'PER_CHUNK'
}

/** A connector's config schema. `x-dmp-role` marks a field that applies to only one role. */
export interface JsonSchema {
  type?: string
  description?: string
  properties?: Record<string, JsonSchema>
  required?: string[]
  enum?: string[]
  default?: unknown
}

export interface RunMetrics {
  recordsRead: number
  /** What the transform stage handed to the sink. Differs from read when a script filters or splits. */
  recordsProduced: number
  recordsWritten: number
  /** A script threw on these, or the destination refused them. Neither arrived. */
  recordsFailed: number
  /** Records a transform deliberately dropped. Never added to `recordsFailed`. */
  recordsFiltered: number
  bytesRead: number
  chunksTotal: number
  chunksCompleted: number
  chunksFailed: number
  /** Non-zero on a completed run means records went missing. Surfaced prominently. */
  unaccountedRecords: number
  throughputPerSecond: number | null
}

export interface Run {
  id: string
  /**
   * This run rehearsed rather than delivered: read and transformed everything, wrote nothing.
   *
   * Its records are deliberately absent from the record index — "would have been transferred"
   * must never be searchable as "was transferred".
   */
  dryRun: boolean
  pipelineId: string
  pipelineVersionId: string
  versionNumber: number
  mode: PipelineMode
  trigger: string
  /** The run this one re-attempts. Separate runs on purpose, so the original stays truthful. */
  retryOf: string | null
  /**
   * Resumes and retries of this run, oldest first, flattened into a sequence.
   *
   * Sent by the server so a page counts migrations rather than rows: a run stopped and resumed
   * three times is one entry with three attempts, not four entries.
   */
  attempts: Run[]
  state: RunState
  active: boolean
  terminal: boolean
  waitingOnExternalSystem: boolean
  metrics: RunMetrics
  progress: number | null
  durationSeconds: number | null
  /** Values bound into the source's query — the range this run actually covered. */
  parameters: Record<string, unknown> | null
  errorCode: string | null
  errorMessage: string | null
  triggeredBy: string | null
  createdAt: string
  startedAt: string | null
  endedAt: string | null
}

export interface Chunk {
  id: string
  index: number
  state: ChunkState
  spec: Record<string, unknown>
  assignedTo: string | null
  leaseExpiresAt: string | null
  attempt: number
  errorCode: string | null
  errorMessage: string | null
  /** From the checkpoint. Also what a restart-from-the-beginning would re-send. */
  recordsWritten: number
  /** A script threw on these, or the destination refused them. Neither arrived. */
  recordsFailed: number
  /** Dropped by a transform on purpose. Beside `recordsFailed`, never added to it. */
  recordsFiltered: number
  /** 0-100, or null when the chunk produced nothing to measure. */
  rejectionPercent: number | null
  /** Whether it has a saved position, so resuming differs from starting over. */
  resumable: boolean
  /**
   * Whether the destination took this chunk as a job of its own and still holds a result file.
   *
   * True only for a sink that decides asynchronously — a Salesforce bulk job. Every other sink
   * answers while the batch is being written, so there is no file to fetch and no button to show.
   */
  hasDestinationResults: boolean
  /** The destination's own id for that job, for finding it in the target system. */
  destinationJobId: string | null
  startedAt: string | null
  endedAt: string | null
}

/** One distinct fault, standing in for however many records hit it. */
export interface ErrorGroup {
  signature: string
  code: string
  message: string
  nodeId: string
  /** The step's name from the canvas — what somebody called it, and so what they look for. */
  node: string
  /** Exact, regardless of how many payloads were kept. */
  count: number
  /** Payloads available to inspect. Capped by the pipeline's audit policy. */
  samplesStored: number
  firstSeenAt: string
  lastSeenAt: string
}

export type RetryFrom = 'CHECKPOINT' | 'CHUNK_START'
export type RetryScope = 'FAILED' | 'FAILED_AND_CANCELLED'

export interface RetryRequest {
  from?: RetryFrom
  scope?: RetryScope
  acknowledgeDuplicates?: boolean
}

/**
 * Re-delivery of the records a run rejected. Not a retry: retry re-reads failed chunks from the
 * source, replay sends stored records that were rejected inside chunks that succeeded.
 */
export interface ReplayRequest {
  /** Send them through the currently published version — for when the fix was in the pipeline. */
  throughLatestVersion?: boolean
  /** Required when the pipeline redacts fields, because the stored copies hold placeholders. */
  acknowledgeRedaction?: boolean
}

export interface RecordError {
  chunkId: string
  nodeId: string
  seq: number
  key: string | null
  code: string
  message: string
  payload: Record<string, unknown>
  occurredAt: string
}

/** RFC 7807 problem detail, as returned by every failing endpoint. */
export interface ProblemDetail {
  type: string
  title: string
  status: number
  detail: string
  code: string
  retryable: boolean
  details?: Record<string, unknown>
  fieldErrors?: Record<string, string>
}

/** Where a transformation script runs: on one record, or on the outgoing batch. */
export type TransformStage = 'RECORD' | 'BATCH' | 'SPLIT'

export interface TransformTestRequest {
  script: string
  stage: TransformStage
  sample: unknown
}

export interface TransformTestResponse {
  ok: boolean
  output: unknown
  error: string | null
  elapsedMillis: number
  /** A plain-language remark about an outcome that reads as a bug but is not, such as a drop. */
  note: string | null
}

export interface Schedule {
  id: string
  pipelineId: string
  name: string
  /** Quartz cron: six or seven fields, not the five-field Unix form. */
  cronExpression: string
  /** IANA zone. Required — "3am" is not a moment without one. */
  timezone: string
  /** JavaScript computing the range each firing covers. Null means the whole query. */
  windowScript: string | null
  enabled: boolean
  description: string | null
  lastFiredAt: string | null
  /** Computed by the scheduler. Null when disabled. */
  nextFireAt: string | null
  createdAt: string
  updatedAt: string
}

export interface CreateScheduleRequest {
  pipelineId: string
  name: string
  cronExpression: string
  timezone: string
  windowScript?: string | null
  description?: string | null
}

export interface UpdateScheduleRequest {
  name: string
  cronExpression: string
  timezone: string
  windowScript?: string | null
  description?: string | null
}

/** One firing and the values it would produce, as returned by the preview endpoint. */
export interface WindowPreview {
  firings: {
    firesAt: string
    parameters: Record<string, string>
    error: string | null
  }[]
}

/**
 * What one run did with one record.
 *
 * Written only by pipelines whose audit level is INDEXED, which is why an empty result means
 * "no indexed run handled this key" rather than "this record was never migrated".
 */
export interface RecordIndexEntry {
  /** Null where the source has no key of its own. The entry still exists and still counts. */
  recordKey: string | null
  pipelineId: string
  runId: string
  chunkId: string
  /** The cycle that carried this record. Joins to the stage log's `traceId`. */
  traceId: string | null
  /**
   * Position within the chunk, and which output of that position this is.
   *
   * Together with `chunkId` these identify the entry. `recordKey` does not: a source is free to
   * hold the same key twice, and two rows that share one are two records, not one.
   */
  seq: number
  ordinal: number
  /**
   * Every way a record can leave a pipeline.
   *
   * REJECTED is the destination refusing this record; CALL_FAILED is the destination refusing the
   * whole request it travelled in, having formed no opinion of the record at all. Almost every API
   * fails the second way, and a retry can still write those records.
   *
   * FILTERED is a transform dropping it on purpose — a success. TRANSFORM_FAILED is a script
   * throwing on it — not. SENT means a destination that decides later has it and has not said.
   */
  outcome: 'WRITTEN' | 'SENT' | 'REJECTED' | 'FILTERED' | 'TRANSFORM_FAILED' | 'CALL_FAILED'
  errorCode: string | null
  /** Null when the pipeline's audit policy does not index payloads. Not the same as no record. */
  payload: Record<string, unknown> | null
  /** The record as the source produced it, present only when a transform changed it. */
  sourcePayload: Record<string, unknown> | null
  /** What the destination or the failing script actually said. */
  errorMessage: string | null
  occurredAt: string
}

/** Everything a support desk can narrow a record search by. */
export interface RecordSearchCriteria {
  q?: string
  field?: string
  key?: string
  pipelineId?: string
  outcome?: string
  /** ISO-8601 instants. Both optional; either may be given alone. */
  after?: string
  before?: string
}

/** One recorded change to a definition, or one run-lifecycle command. */
export interface AuditEntry {
  id: string
  occurredAt: string
  actor: string
  action: string
  resourceType: string
  resourceId: string | null
  summary: string | null
  before: unknown | null
  after: unknown | null
  requestId: string | null
  sourceIp: string | null
}
