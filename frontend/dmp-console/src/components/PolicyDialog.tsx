import Accordion from '@mui/material/Accordion'
import AccordionDetails from '@mui/material/AccordionDetails'
import AccordionSummary from '@mui/material/AccordionSummary'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogTitle from '@mui/material/DialogTitle'
import Divider from '@mui/material/Divider'
import FormControl from '@mui/material/FormControl'
import FormControlLabel from '@mui/material/FormControlLabel'
import FormLabel from '@mui/material/FormLabel'
import Switch from '@mui/material/Switch'
import Radio from '@mui/material/Radio'
import RadioGroup from '@mui/material/RadioGroup'
import Grid from '@mui/material/Grid2'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import Editor from '@monaco-editor/react'
import { useState } from 'react'
import { ErrorPanel } from '@/components/Feedback'
import { useTestTransform, useUpdatePolicies } from '@/api/hooks'
import { muted } from '@/theme'
import { useThemeMode } from '@/store'
import type {
  AuditPolicy,
  StageLogPolicy,
  ChunkingPolicy,
  DeliveryPolicy,
  ExecutionPolicy,
  PipelineMode,
  PipelineVersion,
} from '@/api/types'

/**
 * Read size, write size and parallelism, with their consequences shown.
 *
 * <p>Every number here has a knock-on effect the user cannot be expected to compute — how many
 * chunks a table becomes, how much heap a worker needs, how many API calls a run will make. The
 * dialog derives and displays those live, because a form that accepts <code>writeBatchSize:
 * 50000</code> without mentioning it implies a 400 MB buffer is a form that produces incidents.
 */
/**
 * The four things a pipeline can keep about individual records.
 *
 * <p>One control rather than two, because the underlying model is a 2x2 — payloads or not, index or
 * not — and expressing it as a level plus a switch produced two labels a sentence apart ("Keep the
 * rejected records" beside "Keep rejected records for replay") that half overlapped. The corner
 * that motivated splitting them, an identity index with no stored payloads, is simply one of the
 * four choices here.
 *
 * <p>Each option maps onto both stored fields, so the domain keeps its separate settings and the
 * form asks one question.
 */
const RECORD_DETAIL: Record<
  string,
  { label: string; help: string; policy: Pick<AuditPolicy, 'level' | 'captureRejectedPayloads'> }
> = {
  // Each label names what the pipeline will be able to answer afterwards, not the mechanism.
  // "Identity", "index" and "dead-letter queue" are the platform's words for these things and
  // they are precise, but they require already knowing the design to choose between them —
  // which is the wrong order. Someone opening this dialog knows what question they will need
  // to answer in six months; they do not yet know what an index entry is.
  COUNTS: {
    label: 'No records — counts only',
    help: 'For pipelines carrying data that must not be written down anywhere else. You will know how many failed and why, but not which ones, and nothing can be replayed.',
    policy: { level: 'COUNTERS', captureRejectedPayloads: false },
  },
  IDENTITY: {
    label: 'Every record — searchable afterwards, but failures cannot be replayed',
    help: 'Answers "was record 88291 transferred, and in which run?" months later, at about 140 bytes a record. A rejected record is listed with its error, but not kept, so there is nothing to re-send once you have fixed the cause.',
    policy: { level: 'INDEXED', captureRejectedPayloads: false },
  },
  REJECTED: {
    label: 'Rejected records only — kept whole, so they can be replayed',
    help: 'Payload, error code and message for each failure, and nothing at all about the records that succeeded. This is the dead-letter queue: fix the cause, replay the failures, and no other record is touched.',
    policy: { level: 'ERRORS', captureRejectedPayloads: true },
  },
  // Deliberately silent about payloads. This option pairs with the "what goes in the index"
  // question below, and the answer there decides whether payloads are kept for the failures
  // alone or for every record. A label that named either one would be a correct description
  // of one sub-choice and a false one of the other, which is how it read before: "payloads
  // for the failures" was accurate until you asked for content in the index, and then it
  // quietly contradicted the setting immediately beneath it.
  BOTH: {
    label: 'Every record — searchable afterwards, and failures kept for replay',
    help: 'The fullest answer short of archiving every record: search any record by key, and replay any that failed.',
    policy: { level: 'INDEXED', captureRejectedPayloads: true },
  },
}

/** Which of the four a stored policy corresponds to. */
function recordDetailChoice(audit: AuditPolicy): string {
  const keeps = audit.level !== 'COUNTERS' && audit.captureRejectedPayloads !== false
  if (audit.level === 'COUNTERS') return 'COUNTS'
  if (audit.level === 'INDEXED') return keeps ? 'BOTH' : 'IDENTITY'
  return keeps ? 'REJECTED' : 'COUNTS'
}

export function PolicyDialog({
  version,
  pipelineId,
  sourceRowEstimate,
  sinkConnectorType,
  readOnly = false,
  onClose,
}: {
  version: PipelineVersion
  pipelineId: string
  sourceRowEstimate?: number
  /** Used to disable settings the destination cannot honour — see SINKS_WITHOUT_RECORD_REJECTIONS. */
  sinkConnectorType?: string
  /**
   * Readable but not editable, for a published version.
   *
   * <p>The same dialog rather than a different screen. A published version was previously shown as
   * a diagram and a summary panel, with no way to reach these settings at all — so the chunk size,
   * the delivery mode and the split script a production version actually runs with were invisible.
   * Editing is refused; looking never was the problem.
   */
  readOnly?: boolean
  onClose: () => void
}) {
  const update = useUpdatePolicies(pipelineId, version.id)

  const [chunking, setChunking] = useState<ChunkingPolicy>(version.chunkingPolicy)
  // Defaulted field by field rather than trusted wholesale. A version stored before a setting
  // existed comes back without it, and reading .toLocaleString() off the gap crashes the dialog
  // rather than the field — which is how a missing number takes down the whole screen.
  const [execution, setExecution] = useState<ExecutionPolicy>({
    ...version.executionPolicy,
    maxFailedPercent: version.executionPolicy.maxFailedPercent ?? null,
    maxFailedRecords: version.executionPolicy.maxFailedRecords ?? null,
    stopRunOnChunkFailure: version.executionPolicy.stopRunOnChunkFailure ?? false,
  })
  const [mode, setMode] = useState<PipelineMode>(version.mode)

  // Absent means the default, matching the domain: every version stored before delivery existed
  // sent the whole batch in one call, and that is what it should keep doing.
  const [delivery, setDelivery] = useState<DeliveryPolicy>({
    groupSize: version.deliveryPolicy?.groupSize ?? 0,
    splitScript: version.deliveryPolicy?.splitScript ?? null,
  })

  // Same field-by-field defaulting as the execution policy above, and for the same reason: a
  // version stored before one of these settings existed comes back without it.
  const [audit, setAudit] = useState<AuditPolicy>({
    ...version.auditPolicy,
    level: version.auditPolicy?.level ?? 'ERRORS',
    samplesPerSignature: version.auditPolicy?.samplesPerSignature ?? 10,
    maxPayloadBytes: version.auditPolicy?.maxPayloadBytes ?? 32768,
    redactedFields: version.auditPolicy?.redactedFields ?? [],
    redactionMode: version.auditPolicy?.redactionMode ?? 'HASH',
    retention: version.auditPolicy?.retention ?? 'PT720H',
    indexPayloads: version.auditPolicy?.indexPayloads ?? false,
    // Absent means on, matching the domain. A version stored before this setting existed was
    // capturing rejected records, and reading the gap as "off" would silently stop it.
    captureRejectedPayloads: version.auditPolicy?.captureRejectedPayloads ?? true,
    // Absent means off, the opposite of the line above and correct for the same reason: these
    // switches turn logging *on*, so a version that predates them must not start writing an index
    // nobody asked for.
    stageLog: {
      reads: version.auditPolicy?.stageLog?.reads ?? false,
      transforms: version.auditPolicy?.stageLog?.transforms ?? false,
      writes: version.auditPolicy?.stageLog?.writes ?? false,
      bodies: version.auditPolicy?.stageLog?.bodies ?? false,
    },
  })

  // The API sends seconds and takes an ISO-8601 duration; people think in days. daysOf() handles
  // the asymmetry so neither the field nor the request has to know about it.
  const retentionDays = daysOf(audit.retention)

  // Destinations that report how many records they refused but not which ones. A dead-letter queue
  // here could only ever be empty, and an empty queue invites a support engineer to look for
  // records, find none, and conclude the platform lost them. The engine enforces this too — the
  // value is recomputed on every run — so this only keeps the form honest about it.
  const dlqAvailable = sinkConnectorType !== 'salesforce'

  // Salesforce stages a whole chunk to a file and submits it once, so however the records reach
  // that file is not a decision. The engine refuses the setting at publish; the form should not
  // offer it in the first place.
  const perRecordAvailable = sinkConnectorType !== 'salesforce'

  // Read size may be 0, meaning "decide for me" — so every derived figure below works from the
  // resolved value rather than the stored one, or a pipeline left on the defaults would report a
  // chunk of zero rows and divide by it.
  const effectiveReadSize = chunking.readFetchSize > 0 ? chunking.readFetchSize : 1000

  const rowsPerChunk =
    execution.rowsPerChunk > 0 ? execution.rowsPerChunk : effectiveReadSize * 10

  const estimatedChunks = sourceRowEstimate
    ? Math.max(1, Math.ceil(sourceRowEstimate / rowsPerChunk))
    : null

  const peakHeapMb = Math.round(
    (chunking.maxInFlightBatches * chunking.maxBatchBytes) / (1024 * 1024),
  )

  // Clamped the same way the engine clamps them, so the figures shown describe the run that will
  // actually happen rather than the numbers as typed.
  const readsPerChunk = Math.max(1, Math.ceil(rowsPerChunk / Math.min(effectiveReadSize, rowsPerChunk)))

  // How many calls the destination gets is now delivery's answer, not a batch size's. One per
  // chunk unless the pipeline divides it — which is the whole reason delivery exists.
  const writesPerChunk =
    delivery.splitScript ? null
      : delivery.groupSize === 1 ? rowsPerChunk
      : delivery.groupSize > 1 ? Math.max(1, Math.ceil(rowsPerChunk / delivery.groupSize))
      : 1

  return (
    <Dialog open onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>Execution settings — v{version.versionNumber}</DialogTitle>

      <DialogContent dividers>
        {readOnly && (
          <Alert severity="info" sx={{ mb: 2, '& .MuiAlert-message': { fontSize: 12.5 } }}>
            This version is published and frozen, so these settings are shown but cannot be
            changed. Copy it to a new version to edit them — that is what keeps a run from months
            ago able to say exactly what it executed.
          </Alert>
        )}

        {/*
          A native fieldset, because it disables every form control inside it by specification —
          including ones added later. Disabling each field by hand works until somebody adds the
          next one and forgets, and the failure mode there is an editable control on a frozen
          version, which is worse than an ugly one.
        */}
        <Box
          component="fieldset"
          disabled={readOnly}
          sx={{ border: 0, m: 0, p: 0, minWidth: 0, opacity: readOnly ? 0.85 : 1 }}
        >
        <Stack spacing={3} sx={{ pt: 1 }}>
          <TextField
            select
            label="Mode"
            value={mode}
            onChange={(event) => setMode(event.target.value as PipelineMode)}
            size="small"
            fullWidth
            helperText="Determines how records travel. Batch modes stay in the worker; streaming uses Kafka."
          >
            <MenuItem value="FULL_LOAD">Full load — read everything, every time</MenuItem>
            <MenuItem value="INCREMENTAL">Incremental — only what changed since last time</MenuItem>
            <MenuItem value="STREAMING">Streaming — continuous, never finishes on its own</MenuItem>
            <MenuItem value="CDC">Change data capture — from the database log</MenuItem>
          </TextField>

          <Divider textAlign="left">
            <Typography variant="caption" sx={{ fontWeight: 700, letterSpacing: '0.06em' }}>
              SIZING
            </Typography>
          </Divider>

          {/*
            One number, prominently, and two that answer for themselves.

            All three were equal fields before, and the shape they describe is not: a chunk is the
            work one worker takes, a batch and a fetch are bites inside it. Presented as peers, a
            chunk of 100 alongside a batch of 1,000 looked configured and meant nothing — every
            batch was short, and the batch setting was decoration. The engine clamps that now, but
            the form should not have invited it.
          */}
          <NumberField
            label="Rows per chunk"
            value={execution.rowsPerChunk}
            onChange={(rowsPerChunkValue) =>
              setExecution({ ...execution, rowsPerChunk: rowsPerChunkValue })
            }
            help="How much work one worker takes at a time. This is the number worth thinking about — the read and write sizes below fit inside it. 0 derives it from the read size; ten thousand is a good starting point."
          />

          {rowsPerChunk < 1000 && (
            <Alert severity="warning" sx={{ '& .MuiAlert-message': { fontSize: 12.5 } }}>
              At {rowsPerChunk.toLocaleString()} rows a chunk, most of the run is bookkeeping rather
              than data — every chunk costs a claim, a checkpoint, a state update and an event.
            </Alert>
          )}

          <Accordion
            disableGutters
            elevation={0}
            sx={{ border: 1, borderColor: 'divider', '&:before': { display: 'none' } }}
          >
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Typography variant="body2">Read and write sizes</Typography>
              <Typography variant="caption" sx={{ color: muted, ml: 1, alignSelf: 'center' }}>
                the destination answers for itself unless you say otherwise
              </Typography>
            </AccordionSummary>
            <AccordionDetails>
              <Grid container spacing={2.5}>
                <Grid size={{ xs: 12, sm: 6 }}>
                  <NumberField
                    label="Read size"
                    value={chunking.readFetchSize}
                    onChange={(readFetchSize) => setChunking({ ...chunking, readFetchSize })}
                    help="Records fetched per round trip to the source. 0 uses the platform default. Never larger than the chunk."
                  />
                </Grid>
                <Grid size={{ xs: 12, sm: 6 }}>
                  <NumberField
                    label="Max batch size (MB)"
                    value={Math.round(chunking.maxBatchBytes / (1024 * 1024))}
                    onChange={(mb) => setChunking({ ...chunking, maxBatchBytes: mb * 1024 * 1024 })}
                    help="Byte ceiling per batch — the real memory guarantee. A thousand records may be 1 MB or 1 GB, so a record count alone bounds nothing."
                  />
                </Grid>
              </Grid>
            </AccordionDetails>
          </Accordion>

        </Stack>
        </Box>

        {/*
          Outside the fieldset above, and it owns its own. A disabled fieldset disables every
          descendant control by specification, which would take the split's Try button with it —
          and trying a frozen script against a sample changes nothing. DeliveryField therefore
          disables its own inputs and leaves the trial alone, so the trial can sit where it
          belongs: directly under the editor it tries.
        */}
        <Stack spacing={3} sx={{ pt: 3 }}>
          <Divider textAlign="left">
            <Typography variant="caption" sx={{ fontWeight: 700, letterSpacing: '0.06em' }}>
              DELIVERY — how the destination is called
            </Typography>
          </Divider>

          <DeliveryField
            value={delivery}
            onChange={setDelivery}
            perRecordAvailable={perRecordAvailable}
            sinkConnectorType={sinkConnectorType}
            readOnly={readOnly}
          />
        </Stack>

        <Box
          component="fieldset"
          disabled={readOnly}
          sx={{ border: 0, m: 0, p: 0, minWidth: 0, opacity: readOnly ? 0.85 : 1 }}
        >
        <Stack spacing={3} sx={{ pt: 3 }}>

          <Divider textAlign="left">
            <Typography variant="caption" sx={{ fontWeight: 700, letterSpacing: '0.06em' }}>
              EXECUTION — how chunks are scheduled
            </Typography>
          </Divider>

          <Grid container spacing={2.5}>
            {/*
              A named choice rather than a number where 0 means unlimited. That convention reads
              backwards — "0 parallelism" looks like "none" and actually means "no limit" — and it
              cost a user a wrong setting. Parallel is what the engine does by default; this is how
              you constrain it, so the control says that in words.
            */}
            <Grid size={{ xs: 12, sm: 4 }}>
              <TextField
                select
                label="How chunks run"
                size="small"
                fullWidth
                value={
                  execution.maxConcurrentChunks === 0
                    ? 'parallel'
                    : execution.maxConcurrentChunks === 1
                      ? 'sequential'
                      : 'limited'
                }
                onChange={(event) =>
                  setExecution({
                    ...execution,
                    maxConcurrentChunks:
                      event.target.value === 'parallel'
                        ? 0
                        : event.target.value === 'sequential'
                          ? 1
                          : Math.max(2, execution.maxConcurrentChunks || 4),
                  })
                }
                helperText="Chunks run in parallel unless you limit them."
              >
                <MenuItem value="parallel">In parallel — as many as the fleet allows</MenuItem>
                <MenuItem value="sequential">One at a time — strictly sequential</MenuItem>
                <MenuItem value="limited">Limited to a fixed number</MenuItem>
              </TextField>
            </Grid>

            {execution.maxConcurrentChunks > 1 && (
              <Grid size={{ xs: 12, sm: 4 }}>
                <NumberField
                  label="Chunks at once"
                  value={execution.maxConcurrentChunks}
                  onChange={(maxConcurrentChunks) =>
                    setExecution({ ...execution, maxConcurrentChunks })
                  }
                  help="Across the whole fleet, not per pod."
                />
              </Grid>
            )}
            {/* Meaningless once the fleet is capped at one, so it is simply not shown. */}
            {execution.maxConcurrentChunks !== 1 && (
              <Grid size={{ xs: 12, sm: 4 }}>
                <NumberField
                  label="Max chunks per pod"
                  value={execution.maxChunksPerPod}
                  onChange={(maxChunksPerPod) => setExecution({ ...execution, maxChunksPerPod })}
                  help="Stops one pod claiming a whole run and leaving others idle."
                />
              </Grid>
            )}
            <Grid size={{ xs: 12, sm: 4 }}>
              <NumberField
                label="Attempts per chunk"
                value={execution.maxAttemptsPerChunk}
                onChange={(maxAttemptsPerChunk) =>
                  setExecution({ ...execution, maxAttemptsPerChunk })
                }
                help={
                  execution.maxAttemptsPerChunk <= 1
                    ? 'One attempt — a failed chunk is abandoned immediately, no retry.'
                    : `Up to ${execution.maxAttemptsPerChunk} attempts, so ${execution.maxAttemptsPerChunk - 1} retr${execution.maxAttemptsPerChunk === 2 ? 'y' : 'ies'} before the chunk is abandoned.`
                }
              />
            </Grid>
          </Grid>

          <Divider />

          <Stack spacing={0.5}>
            <Typography variant="subtitle2">When is a chunk a failure?</Typography>
            <Typography variant="body2" sx={{ color: muted }}>
              Rejected records normally do not fail a chunk — one malformed row must not stop a
              migration of a million. But past some share it is no longer bad rows, it is a broken
              configuration, and a run that wrote nothing at all should not report success.
            </Typography>
          </Stack>

          <Grid container spacing={2}>
            <Grid size={{ xs: 12, sm: 6 }}>
              <OptionalNumberField
                label="Fail the chunk at % rejected"
                value={execution.maxFailedPercent}
                onChange={(maxFailedPercent) => setExecution({ ...execution, maxFailedPercent })}
                help={
                  execution.maxFailedPercent === null
                    ? 'No limit — rejections never fail a chunk, however many there are.'
                    : execution.maxFailedPercent === 0
                      ? 'Strictest: a single rejected record fails the chunk.'
                      : execution.maxFailedPercent === 100
                        ? 'Only a total failure trips it: the chunk fails when every record is rejected.'
                        : `The chunk fails once ${execution.maxFailedPercent}% of its records are rejected, and is not retried — rejections at that scale do not fix themselves.`
                }
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6 }}>
              <OptionalNumberField
                label="Fail the chunk at N rejected"
                value={execution.maxFailedRecords}
                onChange={(maxFailedRecords) => setExecution({ ...execution, maxFailedRecords })}
                help={
                  execution.maxFailedRecords === null
                    ? 'No limit — leave empty unless you want an absolute ceiling.'
                    : execution.maxFailedRecords === 0
                      ? 'Strictest: a single rejected record fails the chunk.'
                      : `The chunk fails at ${execution.maxFailedRecords.toLocaleString()} rejected records, whatever share of the chunk that is.`
                }
              />
            </Grid>
          </Grid>

          <FormControl>
            <FormLabel sx={{ mb: 1, fontSize: '0.875rem' }}>
              And when a chunk does fail?
            </FormLabel>
            <RadioGroup
              value={execution.stopRunOnChunkFailure ? 'stop' : 'continue'}
              onChange={(event) =>
                setExecution({
                  ...execution,
                  stopRunOnChunkFailure: event.target.value === 'stop',
                })
              }
            >
              <FormControlLabel
                value="continue"
                control={<Radio size="small" />}
                label={
                  <Stack sx={{ py: 0.5 }}>
                    <Typography variant="body2">Carry on with the other chunks</Typography>
                    <Typography variant="caption">
                      The run finishes everything it can and fails at the end. One bad range out of
                      four hundred still leaves you three hundred and ninety-nine migrated.
                    </Typography>
                  </Stack>
                }
              />
              <FormControlLabel
                value="stop"
                control={<Radio size="small" />}
                label={
                  <Stack sx={{ py: 0.5 }}>
                    <Typography variant="body2">Stop the run immediately</Typography>
                    <Typography variant="caption">
                      For when a failure says something about the target rather than the data — a
                      changed schema, an expired credential. Every remaining chunk is about to
                      discover the same thing at its own expense.
                    </Typography>
                  </Stack>
                }
              />
            </RadioGroup>
          </FormControl>

          {execution.maxFailedPercent === null && execution.maxFailedRecords === null && (
            <Alert
              severity="warning"
              action={
                <Button
                  size="small"
                  onClick={() => setExecution({ ...execution, maxFailedPercent: 100 })}
                >
                  Set to 100%
                </Button>
              }
            >
              {execution.stopRunOnChunkFailure ? (
                <>
                  <strong>Stopping the run has nothing to stop on.</strong> It triggers when a chunk
                  fails, and with both limits off a chunk never fails from rejections — a run whose
                  destination rejects <em>every</em> record still reports COMPLETED. Set a limit
                  and the two work together.
                </>
              ) : (
                <>
                  With both limits off, a run whose destination rejects <em>every</em> record still
                  reports <strong>COMPLETED</strong> — it read everything, wrote nothing, and nobody
                  investigates a green run.
                </>
              )}
            </Alert>
          )}

          {/*
            The consequences, computed live. Nobody should have to work out that a 50,000-record
            write batch at 8 MB apiece implies a 16 MB buffer and four chunks for their table.
          */}
          <Alert severity="info" icon={false}>
            <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
              What these settings mean
            </Typography>
            <Box component="ul" sx={{ m: 0, pl: 2.5 }}>
              <li>
                <Typography variant="body2">
                  Each chunk does <strong>{readsPerChunk}</strong> read
                  {readsPerChunk === 1 ? '' : 's'} and{' '}
                  {writesPerChunk === null ? (
                    <>as many writes as the split script produces groups</>
                  ) : (
                    <>
                      <strong>{writesPerChunk.toLocaleString()}</strong> write
                      {writesPerChunk === 1 ? '' : 's'}
                    </>
                  )}
                </Typography>
              </li>
              {estimatedChunks && (
                <li>
                  <Typography variant="body2">
                    About <strong>{estimatedChunks.toLocaleString()}</strong> chunks for{' '}
                    {sourceRowEstimate?.toLocaleString()} records
                  </Typography>
                </li>
              )}
              <li>
                <Typography variant="body2">
                  Worst-case memory per running chunk: <strong>{peakHeapMb} MB</strong>
                </Typography>
              </li>
              <li>
                <Typography variant="body2">
                  {execution.maxConcurrentChunks === 0
                    ? 'Unlimited parallelism — as many chunks as the fleet can hold'
                    : execution.maxConcurrentChunks === 1
                      ? 'Strictly sequential — one chunk at a time, anywhere in the fleet'
                      : `At most ${execution.maxConcurrentChunks} chunks at once across all pods`}
                </Typography>
              </li>
            </Box>
          </Alert>

          {peakHeapMb * execution.maxChunksPerPod > 1024 && (
            <Alert severity="warning">
              A pod running {execution.maxChunksPerPod} chunks at {peakHeapMb} MB each needs{' '}
              <strong>{Math.round((peakHeapMb * execution.maxChunksPerPod) / 1024)} GB</strong> of
              heap just for buffering. Lower the batch size, the byte ceiling, or the chunks per pod.
            </Alert>
          )}

          <Divider />

          <Stack spacing={0.5}>
            <Typography variant="subtitle2">What to keep about individual records</Typography>
            <Typography variant="body2" sx={{ color: muted }}>
              Counts are always exact — a run reports how many records were read, written and
              rejected whatever you choose here. These settings decide only what survives beyond
              those numbers: which records leave a trace you can look up later, and how much of
              each record that trace holds. Only a record kept whole can be replayed.
            </Typography>
          </Stack>

          <FormControl>
            <FormLabel sx={{ mb: 1, fontSize: '0.875rem' }}>Which records leave a trace</FormLabel>
            <RadioGroup
              value={recordDetailChoice(audit)}
              onChange={(event) => {
                const chosen = RECORD_DETAIL[event.target.value]
                if (chosen) setAudit({ ...audit, ...chosen.policy })
              }}
            >
              {Object.entries(RECORD_DETAIL).map(([key, option]) => {
                // Anything that stores a rejected record is unavailable where the destination
                // reports how many it refused but not which ones. Offering it there would be an
                // option that silently does nothing, which is worse than one visibly unavailable.
                const blocked = !dlqAvailable && option.policy.captureRejectedPayloads
                return (
                  <FormControlLabel
                    key={key}
                    value={key}
                    control={<Radio size="small" />}
                    disabled={blocked}
                    label={
                      <Stack sx={{ py: 0.5 }}>
                        <Typography variant="body2" sx={blocked ? { color: muted } : undefined}>
                          {option.label}
                        </Typography>
                        <Typography variant="caption">
                          {blocked
                            ? `Not available: a ${sinkConnectorType} destination reports how many records it refused but not which ones, so there would be nothing to keep. The count is still exact.`
                            : option.help}
                        </Typography>
                      </Stack>
                    }
                  />
                )
              })}
            </RadioGroup>
          </FormControl>

          {audit.level === 'INDEXED' && (
            <FormControl sx={{ pl: 2, borderLeft: 2, borderColor: 'divider' }}>
              {/*
                * A second question, not a refinement of the first. The heading has to say so:
                * titled "What goes in the index" it read as an elaboration of the option above
                * it, and its first choice repeated that option's label word for word, so the
                * two were impossible to tell apart without knowing the model underneath.
                */}
              <FormLabel sx={{ mb: 1, fontSize: '0.875rem' }}>
                And how much of each record that trace holds
              </FormLabel>
              <RadioGroup
                value={audit.indexPayloads ? 'payloads' : 'identity'}
                onChange={(event) =>
                  setAudit({ ...audit, indexPayloads: event.target.value === 'payloads' })
                }
              >
                <FormControlLabel
                  value="identity"
                  control={<Radio size="small" />}
                  label={
                    <Stack sx={{ py: 0.5 }}>
                      <Typography variant="body2">
                        Key, outcome, run and time — no record content
                      </Typography>
                      <Typography variant="caption">
                        About 140 bytes a record. Searchable by record key or by run. Holds no
                        customer data, so it needs no redaction and can be kept for years.
                      </Typography>
                    </Stack>
                  }
                />
                <FormControlLabel
                  value="payloads"
                  control={<Radio size="small" />}
                  label={
                    <Stack sx={{ py: 0.5 }}>
                      <Typography variant="body2">
                        The record&rsquo;s content as well — every record, written and rejected alike
                      </Typography>
                      <Typography variant="caption">
                        Makes every field searchable — find by email, by status, by amount, not just
                        by key. Costs roughly twice as much for small records and far more for large
                        ones, and it puts customer data in a second store, so the redaction settings
                        below apply to it too.
                      </Typography>
                    </Stack>
                  }
                />
              </RadioGroup>

              <Alert severity="info" sx={{ mt: 1 }}>
                Needs a search backend. Without one the run stops rather than completing with an
                empty index — an index nobody wrote to and a migration that never happened look
                identical to whoever searches later.
              </Alert>
            </FormControl>
          )}

          <StageLogField
            value={audit.stageLog}
            onChange={(stageLog) => setAudit({ ...audit, stageLog })}
          />

          <Grid container spacing={2}>
            <Grid size={{ xs: 12, sm: 4 }}>
              <NumberField
                label="Records kept per fault"
                value={audit.samplesPerSignature}
                onChange={(samplesPerSignature) => setAudit({ ...audit, samplesPerSignature })}
                disabled={audit.level === 'COUNTERS'}
                help={
                  audit.samplesPerSignature === 0
                    ? 'Every rejected record is kept, so a whole run of rejections can be replayed. Costs storage in proportion to the failure.'
                    : `Twenty thousand records failing one rule leave ${audit.samplesPerSignature} payloads behind — enough to diagnose, but only those ${audit.samplesPerSignature} can be replayed. Set 0 to keep them all.`
                }
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 4 }}>
              <NumberField
                label="Max payload size (KB)"
                value={Math.round(audit.maxPayloadBytes / 1024)}
                onChange={(kb) => setAudit({ ...audit, maxPayloadBytes: kb * 1024 })}
                disabled={audit.level === 'COUNTERS'}
                help={
                  audit.maxPayloadBytes === 0
                    ? 'No ceiling — a record is stored however large it is.'
                    : 'Larger records are truncated to a marker that keeps their key, so one oversized field cannot fill the store.'
                }
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 4 }}>
              <NumberField
                label="Keep for (days)"
                value={retentionDays}
                onChange={(days) => setAudit({ ...audit, retention: `PT${days * 24}H` })}
                disabled={audit.level === 'COUNTERS'}
                help={`Rejected records are deleted after ${retentionDays} day${retentionDays === 1 ? '' : 's'}. This is also the window in which they can be replayed.`}
              />
            </Grid>
          </Grid>

          <Stack spacing={1}>
            <TextField
              label="Fields to redact before storing"
              size="small"
              fullWidth
              disabled={audit.level === 'COUNTERS'}
              value={audit.redactedFields.join(', ')}
              onChange={(event) =>
                setAudit({
                  ...audit,
                  redactedFields: event.target.value
                    .split(',')
                    .map((field) => field.trim())
                    .filter(Boolean),
                })
              }
              placeholder="/email, /customer/ssn"
              helperText="JSON pointers, comma separated. Applied before anything is written down — it cannot be undone afterwards, and it cannot be applied to what is already stored."
            />

            {audit.redactedFields.length > 0 && (
              <>
                <FormControl>
                  <FormLabel sx={{ mb: 1, fontSize: '0.875rem' }}>How</FormLabel>
                  <RadioGroup
                    row
                    value={audit.redactionMode}
                    onChange={(event) =>
                      setAudit({
                        ...audit,
                        redactionMode: event.target.value as AuditPolicy['redactionMode'],
                      })
                    }
                  >
                    <FormControlLabel value="HASH" control={<Radio size="small" />} label="Hash" />
                    <FormControlLabel value="MASK" control={<Radio size="small" />} label="Mask" />
                    <FormControlLabel value="DROP" control={<Radio size="small" />} label="Drop" />
                  </RadioGroup>
                </FormControl>
                <Alert severity="warning">
                  {audit.redactionMode === 'HASH'
                    ? 'Hashing keeps the same value hashing the same way, so you can still tell whether two runs hit the same customer without storing who they are.'
                    : audit.redactionMode === 'MASK'
                      ? 'Masked fields are stored as ***.'
                      : 'Dropped fields are removed entirely.'}{' '}
                  These records can no longer be replayed faithfully — the stored copy holds the
                  placeholder, and the original value is not kept anywhere.
                </Alert>
              </>
            )}
          </Stack>

          <ErrorPanel error={update.error} />
        </Stack>
        </Box>

      </DialogContent>

      <DialogActions>
        <Button onClick={onClose}>{readOnly ? 'Close' : 'Cancel'}</Button>
        {!readOnly && (
        <Button
          variant="contained"
          disabled={update.isPending}
          onClick={() =>
            update.mutate(
              {
                chunkingPolicy: chunking,
                executionPolicy: execution,
                auditPolicy: audit,
                // Blank rather than empty-string, so the domain reads "no script" instead of
                // trying to compile nothing and failing at the first chunk.
                deliveryPolicy: {
                  ...delivery,
                  splitScript: delivery.splitScript?.trim() ? delivery.splitScript : null,
                },
                mode,
              },
              { onSuccess: onClose },
            )
          }
        >
          Save
        </Button>
        )}
      </DialogActions>
    </Dialog>
  )
}

/**
 * A number that may legitimately be absent.
 *
 * <p>Separate from {@link NumberField} because for a limit, "none" and "zero" are opposite
 * instructions: no limit at all, versus fail on the very first rejection. Collapsing them onto one
 * number — which is what a sentinel does — makes the strictest value a user can type mean no
 * protection whatsoever. An empty field says absent; a zero says zero.
 */
/**
 * How the batch just buffered is divided into calls on the destination.
 *
 * <p>A separate question from the write size, and separating them is the point. The write size
 * decides how much is buffered before a checkpoint; this decides how many times the destination is
 * called with it. While they were one number, reaching an API that wants one record per request
 * meant setting the batch to 1 — which also made the engine save a resume position after every
 * single record, so the bookkeeping cost as much as the work.
 *
 * <p>The first three options are the same idea with a number, so they share one field. The script
 * is the fourth, and earns its place on the case the others cannot express: one call per region,
 * each with its own envelope.
 */
/**
 * Whether the platform records the calls it makes.
 *
 * <p>Placed after the record settings and not inside them, because it answers a different
 * question. Everything above describes what happened to a *record*; this describes what happened
 * to a *request*. A destination is handed records in batches, so a batch refused outright is many
 * rejections and one reason — and the reason is the part nobody could see.
 */
function StageLogField({
  value,
  onChange,
}: {
  value: StageLogPolicy
  onChange: (policy: StageLogPolicy) => void
}) {
  const anyOn = value.reads || value.transforms || value.writes

  const stages: { key: keyof StageLogPolicy; label: string; help: string }[] = [
    {
      key: 'reads',
      label: 'Reads from the source',
      help:
        'The query the connector actually ran, where the cursor was either side of it, how many ' +
        'rows came back and how long it took. Without this the query is built inside the ' +
        'connector and never written down — so "why did this move nothing?" has no evidence.',
    },
    {
      key: 'transforms',
      label: 'Transforms',
      help:
        'Records in, records out, and how long the scripts took. The only place a filter or a ' +
        'splitter shows up: forty read and thirty-one written could be nine dropped, nine ' +
        'rejected or nine still in flight, and nothing else tells them apart.',
    },
    {
      key: 'writes',
      label: 'Writes to the destination',
      help:
        'How many records the call carried, how long it took, and whatever the destination ' +
        'reported back — a bulk job id, matched and modified counts, an error code. A batch ' +
        'refused whole is one status code explaining every rejection inside it.',
    },
  ]

  return (
    <FormControl>
      <FormLabel sx={{ mb: 1, fontSize: '0.875rem' }}>
        Whether to log the work itself
      </FormLabel>
      <Typography variant="caption" sx={{ mb: 1, color: muted }}>
        One entry per batch, not per record — so at a thousand records to a batch this costs about
        a thousandth of what the record trace above does. Entries from one batch share a trace id,
        which is what lets a run be read as <em>read → transform → write</em> rather than as three
        unrelated lists.
      </Typography>

      {stages.map((stage) => (
        <FormControlLabel
          key={stage.key}
          control={
            <Switch
              size="small"
              checked={Boolean(value[stage.key])}
              onChange={(event) => onChange({ ...value, [stage.key]: event.target.checked })}
            />
          }
          label={
            <Stack sx={{ py: 0.5 }}>
              <Typography variant="body2">{stage.label}</Typography>
              <Typography variant="caption">{stage.help}</Typography>
            </Stack>
          }
        />
      ))}

      {/*
        * Nested, and disabled until a stage is on. It is a refinement of the three above rather
        * than a fourth independent thing, and the domain refuses the combination outright — so
        * offering it on its own would let somebody build a version that cannot be published.
        */}
      <Box sx={{ pl: 4, mt: 0.5 }}>
        <FormControlLabel
          control={
            <Switch
              size="small"
              checked={value.bodies && anyOn}
              disabled={!anyOn}
              onChange={(event) => onChange({ ...value, bodies: event.target.checked })}
            />
          }
          label={
            <Stack sx={{ py: 0.5 }}>
              <Typography variant="body2">Store what was sent and what came back</Typography>
              <Typography variant="caption">
                {anyOn
                  ? 'Turn on while debugging something specific, then turn off. This is customer data in a store built to be searched, and a single batch body can be megabytes. Redaction and the size cap below apply to it.'
                  : 'Needs one of the stages above. There is nothing to attach a body to otherwise.'}
              </Typography>
            </Stack>
          }
        />
      </Box>

      {value.bodies && anyOn && (
        <Alert severity="warning" sx={{ mt: 1 }}>
          Request and response bodies are stored for every logged stage. On a run of any size this
          is the largest thing the platform writes.
        </Alert>
      )}
    </FormControl>
  )
}

function DeliveryField({
  value,
  onChange,
  perRecordAvailable,
  sinkConnectorType,
  readOnly = false,
}: {
  value: DeliveryPolicy
  onChange: (value: DeliveryPolicy) => void
  perRecordAvailable: boolean
  sinkConnectorType?: string
  readOnly?: boolean
}) {
  const themeMode = useThemeMode((state) => state.mode)

  const choice = value.splitScript
    ? 'SCRIPT'
    : value.groupSize === 0
      ? 'WHOLE'
      : value.groupSize === 1
        ? 'SINGLE'
        : 'FIXED'

  const select = (next: string) => {
    if (next === 'WHOLE') onChange({ groupSize: 0, splitScript: null })
    if (next === 'SINGLE') onChange({ groupSize: 1, splitScript: null })
    if (next === 'FIXED') onChange({ groupSize: 50, splitScript: null })
    if (next === 'SCRIPT') onChange({ groupSize: 0, splitScript: STARTER_SPLIT })
  }

  return (
    <FormControl>
      <Box
        component="fieldset"
        disabled={readOnly}
        sx={{ border: 0, m: 0, p: 0, minWidth: 0, opacity: readOnly ? 0.85 : 1 }}
      >
      <RadioGroup value={choice} onChange={(event) => select(event.target.value)}>
        <FormControlLabel
          value="WHOLE"
          control={<Radio size="small" />}
          label={
            <Stack sx={{ py: 0.5 }}>
              <Typography variant="body2">Everything at once</Typography>
              <Typography variant="caption">
                One call carrying the whole batch. Fastest, and what every pipeline did before this
                setting existed.
              </Typography>
            </Stack>
          }
        />

        <FormControlLabel
          value="SINGLE"
          control={<Radio size="small" />}
          disabled={!perRecordAvailable}
          label={
            <Stack sx={{ py: 0.5 }}>
              <Typography variant="body2" sx={perRecordAvailable ? undefined : { color: muted }}>
                One record at a time
              </Typography>
              <Typography variant="caption">
                {perRecordAvailable
                  ? 'Slower, but a failure names the record rather than the batch — so the other records still land and the bad one can be replayed.'
                  : `Not available: a ${sinkConnectorType} destination stages a whole chunk and hands it over once, so sending records singly would produce exactly the same result. Size the chunk instead.`}
              </Typography>
            </Stack>
          }
        />

        <FormControlLabel
          value="FIXED"
          control={<Radio size="small" />}
          label={
            <Stack sx={{ py: 0.5 }}>
              <Typography variant="body2">Fixed groups</Typography>
              <Typography variant="caption">
                For an API with a documented maximum per request, without changing how often the
                run checkpoints.
              </Typography>
            </Stack>
          }
        />

        {choice === 'FIXED' && (
          <Box sx={{ pl: 4, pb: 1 }}>
            <NumberField
              label="Records per call"
              value={value.groupSize}
              onChange={(groupSize) => onChange({ groupSize: Math.max(1, groupSize), splitScript: null })}
              help="A batch of 1,000 at 50 per call is 20 calls, and still one checkpoint."
            />
          </Box>
        )}

        <FormControlLabel
          value="SCRIPT"
          control={<Radio size="small" />}
          label={
            <Stack sx={{ py: 0.5 }}>
              <Typography variant="body2">Split each batch by a script</Typography>
              <Typography variant="caption">
                One call per group, where the groups are yours to decide — per region, per target
                table, or wherever a running total crosses a limit.
              </Typography>
            </Stack>
          }
        />
      </RadioGroup>
      </Box>

      {choice === 'SCRIPT' && (
        <Box sx={{ pl: 4, pt: 1 }}>
          <Typography variant="caption" sx={{ display: 'block', color: muted, mb: 1 }}>
            Return <strong>one label per record</strong>, in the same order. Records sharing a label
            are written together, and each distinct label is one call. Labels rather than groups of
            records so that a script cannot drop one, duplicate one, or lose the sequence number the
            run resumes from.
          </Typography>

          <Box sx={{ border: 1, borderColor: 'divider', borderRadius: 1, overflow: 'hidden' }}>
            <Editor
              height="140px"
              defaultLanguage="javascript"
              theme={themeMode === 'dark' ? 'vs-dark' : 'light'}
              value={value.splitScript ?? ''}
              onChange={(next) => onChange({ groupSize: 0, splitScript: next ?? '' })}
              options={{
                // Monaco is not a form control, so the fieldset above does not reach it.
                readOnly,
                minimap: { enabled: false },
                fontSize: 13,
                lineNumbers: 'off',
                scrollBeyondLastLine: false,
                tabSize: 2,
                automaticLayout: true,
                folding: false,
                glyphMargin: false,
                lineDecorationsWidth: 0,
                overviewRulerLanes: 0,
              }}
            />
          </Box>

          {/*
            Always shown, frozen version included. Trying a script against a sample changes
            nothing, and it is exactly what somebody looking at a production version wants to do:
            see what this split actually makes of some records.
          */}
          <SplitTrial script={value.splitScript ?? ''} />

          <Alert severity="info" sx={{ mt: 1, '& .MuiAlert-message': { fontSize: 12.5 } }}>
            A group never crosses a batch. Two records with the same label in different batches are
            two separate calls — the engine holds a batch, never a whole chunk. If everything with
            one label has to arrive together, sort by it at the source.
          </Alert>
        </Box>
      )}
    </FormControl>
  )
}

/**
 * Runs the split against sample records and shows the calls it would produce.
 *
 * <p>Several records rather than one, unlike the per-record editor's trial. A split's whole purpose
 * is deciding which records travel together, and one record can only ever be one group — so a
 * single-record trial would report success for a script that is completely wrong.
 *
 * <p>What comes back is the grouping rather than the labels. The labels are the mechanism; the
 * question the author has is "how many calls, carrying what, in what order".
 */
function SplitTrial({ script }: { script: string }) {
  const [sample, setSample] = useState(DEFAULT_SPLIT_SAMPLE)
  const [sampleError, setSampleError] = useState<string | null>(null)
  const trial = useTestTransform()

  const run = () => {
    let parsed: unknown
    try {
      parsed = JSON.parse(sample)
    } catch (error) {
      setSampleError(error instanceof Error ? error.message : 'Not valid JSON')
      return
    }
    setSampleError(null)
    trial.mutate({ script, stage: 'SPLIT', sample: parsed })
  }

  const groups = (trial.data?.ok ? trial.data.output : null) as
    | { label: string; records: number; payloads: unknown[] }[]
    | null

  return (
    <Box sx={{ mt: 1.5 }}>
      <TextField
        label="Sample records"
        value={sample}
        onChange={(event) => setSample(event.target.value)}
        error={Boolean(sampleError)}
        helperText={sampleError ?? 'A few records, as a JSON array, to try the split against'}
        size="small"
        fullWidth
        multiline
        minRows={3}
        maxRows={8}
        slotProps={{ input: { sx: { fontFamily: 'monospace', fontSize: 12.5 } } }}
      />

      <Button
        size="small"
        onClick={run}
        disabled={!script.trim() || trial.isPending}
        sx={{ mt: 1 }}
      >
        {trial.isPending ? 'Running…' : 'Try it'}
      </Button>

      {trial.data && !trial.data.ok && (
        <Alert severity="error" sx={{ mt: 1, '& .MuiAlert-message': { fontSize: 12.5 } }}>
          {trial.data.error}
        </Alert>
      )}

      {groups && (
        <Box sx={{ mt: 1 }}>
          <Typography variant="caption" sx={{ display: 'block', color: muted, mb: 0.5 }}>
            {trial.data?.note ?? `${groups.length} call(s), in this order`}
          </Typography>
          <Stack spacing={0.5}>
            {groups.map((group, index) => (
              <Box
                key={`${group.label}-${index}`}
                sx={{ border: 1, borderColor: 'divider', borderRadius: 1, p: 1 }}
              >
                <Typography variant="caption" sx={{ fontWeight: 600 }}>
                  call {index + 1} · label “{group.label || '(no label)'}” · {group.records} record
                  {group.records === 1 ? '' : 's'}
                </Typography>
                <Box
                  component="pre"
                  sx={{ m: 0, mt: 0.5, fontSize: 11.5, overflowX: 'auto', color: muted }}
                >
                  {JSON.stringify(group.payloads, null, 1)}
                </Box>
              </Box>
            ))}
          </Stack>
        </Box>
      )}

      <ErrorPanel error={trial.error} />
    </Box>
  )
}

/** Two groups' worth, so the trial demonstrates a grouping rather than a single call. */
const DEFAULT_SPLIT_SAMPLE = `[
  { "region": "EU", "amount": 10 },
  { "region": "US", "amount": 20 },
  { "region": "EU", "amount": 30 }
]`

/** Filled in when the script option is chosen, so the field is never an empty box. */
const STARTER_SPLIT = `function split(records) {
  // One label per record, same order. Same label = same call.
  return records.map(r => r.region)
}`

function OptionalNumberField({
  label,
  value,
  onChange,
  help,
}: {
  label: string
  value: number | null
  onChange: (value: number | null) => void
  help: string
}) {
  return (
    <TextField
      label={label}
      type="number"
      value={value === null ? '' : value}
      placeholder="no limit"
      onChange={(event) => {
        const raw = event.target.value
        onChange(raw === '' ? null : Math.max(0, Number(raw) || 0))
      }}
      size="small"
      fullWidth
      slotProps={{ inputLabel: { shrink: true } }}
      helperText={<span style={{ color: muted }}>{help}</span>}
    />
  )
}

function NumberField({
  label,
  value,
  onChange,
  help,
  disabled,
}: {
  label: string
  value: number
  onChange: (value: number) => void
  help: string
  disabled?: boolean
}) {
  return (
    <TextField
      label={label}
      type="number"
      value={value}
      disabled={disabled}
      onChange={(event) => onChange(Math.max(0, Number(event.target.value) || 0))}
      size="small"
      fullWidth
      slotProps={{ inputLabel: { shrink: true } }}
      helperText={<span style={{ color: muted }}>{help}</span>}
    />
  )
}

/**
 * Days out of whatever shape the retention arrives in.
 *
 * <p>It is asymmetric on purpose. The API serialises a Duration as a number of seconds
 * (`7776000.0`), and accepts an ISO-8601 string (`PT2160H`) on the way back in — so this reads the
 * number and {@link PolicyDialog} writes the string. Handling only the string looked correct
 * against the default, because an unparsed value fell back to exactly the thirty days the default
 * happens to be; every other setting silently displayed as thirty days too.
 *
 * <p>The ISO branch is kept so a value written by hand, or by an older client, still reads.
 */
function daysOf(retention: string | number): number {
  const FALLBACK_DAYS = 30

  if (typeof retention === 'number' && Number.isFinite(retention)) {
    return Math.max(1, Math.round(retention / 86_400))
  }

  const match = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?)?$/.exec(String(retention ?? ''))
  if (!match) return FALLBACK_DAYS

  const hours = Number(match[1] ?? 0) * 24 + Number(match[2] ?? 0)
  return hours > 0 ? Math.max(1, Math.round(hours / 24)) : FALLBACK_DAYS
}
