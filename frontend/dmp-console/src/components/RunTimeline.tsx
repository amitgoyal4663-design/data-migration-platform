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
          <MenuItem value="READ">Read</MenuItem>
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
        <Typography variant="caption" sx={{ ...tabular, fontWeight: 600 }}>
          {traceId}
        </Typography>
        <Typography variant="caption" sx={{ color: muted }}>
          {entries.length} stage{entries.length === 1 ? '' : 's'} ·{' '}
          {entries.reduce((total, entry) => total + entry.durationMs, 0)} ms
        </Typography>
      </Stack>
      {entries.map((entry) => (
        <StageRow key={`${entry.stage}-${entry.sequence}`} entry={entry} />
      ))}
    </Box>
  )
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

        <Typography variant="caption" sx={{ ...tabular, color: muted, flex: 1 }} noWrap>
          {entry.errorMessage ?? entry.query ?? summarise(entry.details)}
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
          <Detail label="Sent" value={entry.request} />
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
  if (entry.stage !== 'TRANSFORM' || entry.recordsIn === entry.recordsOut) {
    return <>{entry.recordsIn.toLocaleString()} records</>
  }
  const delta = entry.recordsOut - entry.recordsIn
  return (
    <Tooltip title={delta < 0 ? `${-delta} dropped here` : `${delta} added here`}>
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

const STAGE_COLOUR: Record<StageLogEntry['stage'], 'primary' | 'secondary' | 'success'> = {
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
function summarise(details: Record<string, unknown> | null): string {
  if (!details) {
    return ''
  }
  return Object.entries(details)
    .map(([name, value]) => `${name}=${String(value)}`)
    .join('  ')
}
