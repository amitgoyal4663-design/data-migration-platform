import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import Collapse from '@mui/material/Collapse'
import IconButton from '@mui/material/IconButton'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import { useState } from 'react'
import { useRunStages } from '@/api/hooks'
import { muted, tabular } from '@/theme'
import type { StageLogEntry } from '@/api/types'

/**
 * One run, read as a sequence rather than as a set of lists.
 *
 * <p>The chunk table says what each chunk did. The failures tab says which records were refused.
 * Neither says <em>what the platform actually did, in what order</em> — which read returned
 * nothing, which transform dropped nine records, which call the destination refused. Those are
 * three separate stores answering three separate questions, and the trace id is what makes them
 * one answer.
 *
 * <p>Grouped by trace, because a trace is one cycle: a window of reading, the transforms over it,
 * and the call that carried it out. Ungrouped it is a flat list of timestamps, which is the shape
 * that made this information useless when it was only in the server log.
 */
export function RunTimeline({ runId }: { runId: string }) {
  const [chunkId, setChunkId] = useState('')
  const [stage, setStage] = useState('')

  // Trimmed, because the commonest way to fill this box is pasting an id with a stray space on the
  // end — and a filter that silently matches nothing is indistinguishable from a run that did
  // nothing.
  const stages = useRunStages(runId, chunkId.trim() || undefined, stage || undefined)
  const entries = stages.data?.content ?? []

  return (
    <Stack spacing={2}>
      <Stack direction="row" spacing={2} alignItems="flex-start">
        <TextField
          label="Chunk or trace id"
          size="small"
          value={chunkId}
          onChange={(event) => setChunkId(event.target.value)}
          placeholder="paste a chunk id, or a trace id"
          helperText="A trace id narrows to its chunk — paste whichever you have"
          sx={{ minWidth: 320 }}
          slotProps={{ input: { sx: { ...tabular, fontSize: 13 } } }}
        />
        <TextField
          label="Stage"
          size="small"
          select
          value={stage}
          onChange={(event) => setStage(event.target.value)}
          helperText="One stage across the run compares read time against write time"
          sx={{ minWidth: 200 }}
        >
          <MenuItem value="">All stages</MenuItem>
          <MenuItem value="FETCH">Fetch (calls to the source)</MenuItem>
          <MenuItem value="READ">Read (batches filled)</MenuItem>
          <MenuItem value="TRANSFORM">Transform</MenuItem>
          <MenuItem value="WRITE">Write</MenuItem>
        </TextField>
      </Stack>

      {stages.isLoading && <Typography variant="caption">Loading…</Typography>}

      {!stages.isLoading && entries.length === 0 && (
        <Alert severity="info">
          {/*
            * Two very different things look identical here, and saying so is the whole job of this
            * message: a run that did nothing, and a run whose pipeline was never asked to record
            * what it did. Reporting the second as the first is how somebody concludes a migration
            * failed silently.
            */}
          No stages recorded{chunkId || stage ? ' for this filter' : ''}. Stage logging is off by
          default — switch it on under <strong>Execution settings → Audit → Whether to log the work
          itself</strong>, then run again. This does not mean the run did no work.
        </Alert>
      )}

      {groupByTrace(entries).map((cycle) => (
        <TraceGroup key={cycle.traceId} traceId={cycle.traceId} entries={cycle.entries} />
      ))}

      {stages.data && stages.data.totalElements > entries.length && (
        <Typography variant="caption" sx={{ color: muted }}>
          Showing the first {entries.length} of {stages.data.totalElements.toLocaleString()} stages.
          Narrow by chunk to see the rest.
        </Typography>
      )}
    </Stack>
  )
}

/** One read → transform → write cycle. */
function TraceGroup({ traceId, entries }: { traceId: string; entries: StageLogEntry[] }) {
  return (
    <Box sx={{ border: 1, borderColor: 'divider', borderRadius: 1, overflow: 'hidden' }}>
      <Stack
        direction="row"
        alignItems="center"
        spacing={1}
        sx={{ px: 1.5, py: 0.75, bgcolor: 'action.hover' }}
      >
        <CopyableTrace traceId={traceId} entries={entries} />
        <Typography variant="caption" sx={{ color: muted }}>
          {entries.length} stage{entries.length === 1 ? '' : 's'} ·{' '}
          {entries.reduce((total, entry) => total + entry.durationMs, 0)} ms
        </Typography>
      </Stack>

      <CycleSummary entries={entries} />

      {(() => {
        const { head, deliveries } = splitIntoDeliveries(entries)
        return (
          <>
            {head.map((entry) => (
              <StageRow key={entry.position} entry={entry} />
            ))}
            {deliveries.map((delivery, index) => (
              <Box key={delivery[0]!.position}>
                {/* One heading per delivery, because the flat list could not say which batch
                    transform belonged to which call — and with a script that throws on one group
                    and not the next, that is the only thing worth knowing. */}
                {deliveries.length > 1 && (
                  <Typography
                    variant="caption"
                    sx={{ display: 'block', px: 1.5, py: 0.5, color: muted, borderTop: 1,
                          borderColor: 'divider', bgcolor: 'action.hover' }}
                  >
                    delivery {index + 1} of {deliveries.length}
                  </Typography>
                )}
                {delivery.map((entry) => (
                  <StageRow key={entry.position} entry={entry} />
                ))}
              </Box>
            ))}
          </>
        )
      })()}
    </Box>
  )
}

/**
 * The group's trace id, and the string that finds its chunk in the application logs.
 *
 * <p>Two different ids meet here and conflating them would send somebody to an empty grep. The
 * heading is the <em>cycle</em> trace — {@code chunk#0}, {@code chunk#1} — which is what ties one
 * read, its transforms and its writes together, and what the record index stamps on every record
 * that travelled in them. What the engine stamps on a <em>log line</em> is coarser: the run, the
 * chunk and the attempt, because a log line belongs to a chunk rather than to one cycle of it.
 * So the visible id is the cycle and the copied one is the log's.
 */
function CopyableTrace({ traceId, entries }: { traceId: string; entries: StageLogEntry[] }) {
  const first = entries[0]

  return (
    <Stack direction="row" spacing={2} alignItems="baseline" flexWrap="wrap" useFlexGap>
      {/* Both, labelled, because they are different things and one raw hex string said neither.
          chunkId is the key every store agrees on — the stage log, the record index, and the
          application log line all carry it. traceId is finer: which read-transform-write cycle
          within the chunk, and what the records that travelled in it are stamped with. */}
      {first?.chunkId && <Labelled name="chunkId" value={first.chunkId} />}
      <Labelled name="traceId" value={traceId} />
    </Stack>
  )
}

/** A named id, shown in full and copied whole. */
function Labelled({ name, value }: { name: string; value: string }) {
  const [copied, setCopied] = useState(false)

  return (
    <Stack direction="row" spacing={0.5} alignItems="baseline">
      <Typography variant="caption" sx={{ color: muted }}>
        {name}=
      </Typography>
      <Tooltip title={copied ? 'Copied' : 'Click to copy'}>
        <Typography
          variant="caption"
          onClick={() => {
            void navigator.clipboard.writeText(value)
            setCopied(true)
            window.setTimeout(() => setCopied(false), 1500)
          }}
          sx={{
            ...tabular,
            fontWeight: 600,
            cursor: 'pointer',
            color: copied ? 'success.main' : 'inherit',
            '&:hover': { color: 'primary.main' },
          }}
        >
          {value}
        </Typography>
      </Tooltip>
    </Stack>
  )
}

/**
 * What the whole cycle did, above the steps that did it.
 *
 * <p>No single row can answer "how much reached the destination". A write row carries one call's
 * share, and delivery splits a batch across as many calls as it likes — so a cycle that wrote a
 * thousand records across two calls showed "500 records" twice and no total anywhere. Reading it
 * meant adding rows up by eye, and getting it wrong in the direction of thinking the sink received
 * half of what it did.
 *
 * <p>Silent when the numbers cannot mislead: a cycle with one write and no transform says the same
 * thing on its only row.
 */
function CycleSummary({ entries }: { entries: StageLogEntry[] }) {
  const reads = entries.filter((e) => e.stage === 'READ')
  const transforms = entries.filter((e) => e.stage === 'TRANSFORM')
  const writes = entries.filter((e) => e.stage === 'WRITE')

  if (writes.length === 0 && transforms.length === 0) {
    return null
  }

  const read = reads.reduce((total, e) => total + e.recordsIn, 0)
  // The last transform's output is what the destination was offered: the record stage may filter
  // or split, a batch stage may rewrite, and only the final count is what was handed on.
  const offered = transforms.length > 0 ? transforms[transforms.length - 1]!.recordsOut : read
  const handed = writes.reduce((total, e) => total + e.recordsIn, 0)
  const kept = writes.reduce((total, e) => total + e.recordsOut, 0)
  const refused = handed - kept

  // A single write and no transform is already fully described by its own row.
  if (transforms.length === 0 && writes.length <= 1 && refused === 0) {
    return null
  }

  return (
    <Typography
      variant="caption"
      sx={{ display: 'block', px: 1.5, py: 0.5, color: muted, borderTop: 1,
            borderColor: 'divider', ...tabular }}
    >
      {read.toLocaleString()} read
      {transforms.length > 0 && offered !== read && (
        <> → {offered.toLocaleString()} after transforms</>
      )}
      {writes.length > 0 && (
        <>
          {' '}→ {kept.toLocaleString()} into {writes[0]!.nodeName || 'the destination'}
          {refused > 0 && (
            <Box component="span" sx={{ color: 'error.main' }}>
              {' '}({refused.toLocaleString()} refused)
            </Box>
          )}
          {' · '}
          {writes.length} call{writes.length === 1 ? '' : 's'}
        </>
      )}
    </Typography>
  )
}

/**
 * Splits a cycle into the reading, and then one section per delivery.
 *
 * <p>The engine divides a batch into delivery groups and runs the batch transform per group,
 * immediately before that group's call — so the true shape is a read followed by N deliveries, not
 * the flat list of transforms and writes it was rendered as. With a batch script that throws on
 * one group's records and not the next, the flat list showed a failed transform, a successful one
 * and a write, and nothing said which belonged to which.
 *
 * <p>A delivery opens at a batch transform, or at a write with no transform before it. It closes
 * at the write — or at a batch transform that failed, which ends that delivery without one.
 */
function splitIntoDeliveries(entries: StageLogEntry[]): {
  head: StageLogEntry[]
  deliveries: StageLogEntry[][]
} {
  const head: StageLogEntry[] = []
  const deliveries: StageLogEntry[][] = []
  let current: StageLogEntry[] | null = null

  for (const entry of entries) {
    const batchTransform =
      entry.stage === 'TRANSFORM' &&
      (entry.details as { transformStage?: string } | null)?.transformStage === 'BATCH'

    if (batchTransform) {
      current = [entry]
      deliveries.push(current)
      // A batch script that threw delivered nothing, so no write follows and this one is closed.
      if (entry.outcome === 'FAILED') {
        current = null
      }
      continue
    }

    if (entry.stage === 'WRITE') {
      if (current) {
        current.push(entry)
        current = null
      } else {
        deliveries.push([entry])
      }
      continue
    }

    if (current) {
      current.push(entry)
    } else {
      head.push(entry)
    }
  }

  return { head, deliveries }
}

function StageRow({ entry }: { entry: StageLogEntry }) {
  const [open, setOpen] = useState(false)
  const hasDetail =
    Boolean(entry.query) || Boolean(entry.details) || Boolean(entry.request) ||
    Boolean(entry.response) || Boolean(entry.errorMessage)

  return (
    <Box sx={{ borderTop: 1, borderColor: 'divider' }}>
      <Stack direction="row" alignItems="center" spacing={1.5} sx={{ px: 1.5, py: 1 }}>
        <Chip
          size="small"
          label={entry.stage}
          color={entry.outcome === 'FAILED' ? 'error' : STAGE_COLOUR[entry.stage]}
          variant={entry.outcome === 'FAILED' ? 'filled' : 'outlined'}
          sx={{ minWidth: 92, fontSize: 11 }}
        />

        <Typography variant="body2" sx={{ minWidth: 150 }}>
          {entry.nodeName || entry.nodeId}

        </Typography>

        <Typography variant="caption" sx={{ ...tabular, minWidth: 130 }}>
          <RecordCount entry={entry} />
        </Typography>

        <Typography variant="caption" sx={{ ...tabular, color: muted, minWidth: 70 }}>
          {entry.durationMs} ms
        </Typography>

        {/* Why, then what. A URL is the least legible thing on the row and was winning the
            slot: two fetches against one chunk showed two near-identical ninety-character
            strings, and nothing said one had fetched column names and the other a thousand
            rows. The URL is still there, one click away. */}
        <Typography variant="caption" sx={{ ...tabular, color: muted, flex: 1 }} noWrap>
          {entry.errorMessage ?? reasonOf(entry) ?? entry.query ?? summarise(entry.details)}
        </Typography>

        {hasDetail && (
          <IconButton size="small" onClick={() => setOpen(!open)}>
            <ExpandMoreIcon
              fontSize="small"
              sx={{ transform: open ? 'rotate(180deg)' : 'none', transition: '150ms' }}
            />
          </IconButton>
        )}
      </Stack>

      <Collapse in={open} unmountOnExit>
        <Stack spacing={1} sx={{ px: 1.5, pb: 1.5 }}>
          {entry.errorMessage && (
            <Alert severity="error" sx={{ fontSize: 12.5 }}>
              {entry.errorCode ? `${entry.errorCode} — ` : ''}
              {entry.errorMessage}
            </Alert>
          )}
          <Detail label="Query" value={entry.query} />
          <Detail label="Cursor before" value={entry.cursorIn} />
          <Detail label="Cursor after" value={entry.cursorOut} />
          <Detail label="What the destination reported" value={entry.details} />
          {/* No "Sent" row. It held the whole batch — the records themselves — which are in the
              record index, one document each, searchable by any field they contain. Here they were
              one unsearchable blob per call, usually truncated, and they crowded out the small
              facts this panel exists for. */}
          <Detail label="Received" value={entry.response} />
        </Stack>
      </Collapse>
    </Box>
  )
}

/**
 * How many records, and — at a transform — how many came out.
 *
 * <p>The arrow appears only where the number changed. A transform that passed everything through
 * reads as a plain count, so the runs where something <em>was</em> dropped stand out rather than
 * hiding among identical-looking rows.
 */
function RecordCount({ entry }: { entry: StageLogEntry }) {
  // Any stage where the two differ, not only a transform. A write whose destination refused a
  // third of the batch was showing the batch size — true about the request, wrong about the
  // outcome, and silent about which it meant.
  if (entry.recordsIn === entry.recordsOut) {
    return <>{entry.recordsIn.toLocaleString()} records</>
  }
  const delta = entry.recordsOut - entry.recordsIn
  const lost =
    entry.stage === 'WRITE' ? `${-delta} refused by the destination` : `${-delta} dropped here`
  return (
    <Tooltip title={delta < 0 ? lost : `${delta} added here`}>
      <span>
        {entry.recordsIn.toLocaleString()} → {entry.recordsOut.toLocaleString()}{' '}
        <Box component="span" sx={{ color: delta < 0 ? 'warning.main' : 'info.main' }}>
          ({delta > 0 ? '+' : ''}
          {delta})
        </Box>
      </span>
    </Tooltip>
  )
}

function Detail({ label, value }: { label: string; value: unknown }) {
  if (value === null || value === undefined || value === '') {
    return null
  }
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2)
  return (
    <Box>
      <Typography variant="caption" sx={{ color: muted, display: 'block', mb: 0.25 }}>
        {label}
      </Typography>
      <Box
        component="pre"
        sx={{
          m: 0,
          p: 1,
          borderRadius: 1,
          bgcolor: 'action.hover',
          fontSize: 12,
          fontFamily: 'monospace',
          overflowX: 'auto',
          maxHeight: 260,
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
        }}
      >
        {text}
      </Box>
    </Box>
  )
}

const STAGE_COLOUR: Record<StageLogEntry['stage'], 'primary' | 'secondary' | 'success' | 'info'> = {
  // Distinct from READ rather than sharing its colour, because the pair sitting next to each other
  // is the point: the same colour twice would read as the same thing logged twice.
  FETCH: 'info',
  READ: 'primary',
  TRANSFORM: 'secondary',
  WRITE: 'success',
}

/**
 * Entries grouped into the cycles they belong to, order preserved.
 *
 * <p>The API returns them oldest first, so first appearance is the right order for the groups too
 * — no sort, and none possible: two stages inside one millisecond would otherwise be reordered by
 * a comparator that has nothing to separate them.
 */
function groupByTrace(entries: StageLogEntry[]): { traceId: string; entries: StageLogEntry[] }[] {
  const byTrace = new Map<string, StageLogEntry[]>()
  for (const entry of entries) {
    const key = entry.traceId ?? entry.chunkId
    const existing = byTrace.get(key)
    if (existing) {
      existing.push(entry)
    } else {
      byTrace.set(key, [entry])
    }
  }
  return [...byTrace].map(([traceId, grouped]) => ({ traceId, entries: grouped }))
}

/** A one-line gist of a details object, for the collapsed row. */
/**
 * A connector's own report, on one line.
 *
 * <p>`String(value)` was fine while every destination reported flat scalars — a job id, a status —
 * and became `[object Object]` the moment one reported the reply body. The row it renders is the
 * collapsed summary, so a value that cannot be summarised in a few characters is better named than
 * stringified: `response={6 keys}` tells the reader to expand, `[object Object]` tells them
 * nothing and looks like a defect.
 */
/** The connector's own words for why it made this call, when it gave any. */
function reasonOf(entry: StageLogEntry): string | null {
  const reason = entry.details?.reason
  return typeof reason === 'string' && reason.length > 0 ? reason : null
}

function summarise(details: Record<string, unknown> | null): string {
  if (!details) {
    return ''
  }
  return Object.entries(details)
    .map(([name, value]) => `${name}=${describe(value)}`)
    .join('  ')
}

function describe(value: unknown): string {
  if (value === null || value === undefined) {
    return '—'
  }
  if (Array.isArray(value)) {
    return `[${value.length}]`
  }
  if (typeof value === 'object') {
    const keys = Object.keys(value as object)
    return `{${keys.length} field${keys.length === 1 ? '' : 's'}}`
  }
  const text = String(value)
  return text.length > 60 ? `${text.slice(0, 60)}…` : text
}
