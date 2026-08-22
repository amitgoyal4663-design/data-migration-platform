import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import Collapse from '@mui/material/Collapse'
import IconButton from '@mui/material/IconButton'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import { useMemo, useState } from 'react'
import { useRunStages } from '@/api/hooks'
import { muted, tabular } from '@/theme'
import { shortId } from '@/api/ids'
import { MoreOnScroll } from '@/components/MoreOnScroll'
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
  const entries = useMemo(
    () => stages.data?.pages.flatMap((page) => page.content) ?? [],
    [stages.data],
  )
  const total = stages.data?.pages[0]?.totalElements ?? 0

  // Everything fetched is drawn. The page size is what decides how much arrives at a time, so a
  // scroll is a request rather than a request feeding several scrolls — which is what holding
  // entries back to reveal them locally amounted to.
  const visible = useMemo(() => groupByTrace(entries), [entries])

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

      {visible.map((cycle) => (
        <TraceGroup key={cycle.traceId} traceId={cycle.traceId} entries={cycle.entries} />
      ))}

      {/* Loads as it is reached, rather than telling the reader the rest exists somewhere they
          cannot get to — which is what "showing the first 200" amounted to on a run whose stages
          run into the hundreds of thousands. */}
      <MoreOnScroll
        hasMore={stages.hasNextPage}
        loading={stages.isFetchingNextPage}
        onReach={() => void stages.fetchNextPage()}
        shown={entries.length}
        total={total}
      />
    </Stack>
  )
}

/**
 * How many of a cycle's deliveries are drawn before the reader asks for more.
 *
 * <p>Small on purpose. A per-record delivery — one call per record, so one delivery per record —
 * would otherwise put a hundred thousand rows into a single group, and ten is enough to see what
 * a cycle is doing before deciding whether to look further.
 */
const DELIVERY_PAGE = 10

/** One read → transform → write cycle. */
function TraceGroup({ traceId, entries }: { traceId: string; entries: StageLogEntry[] }) {
  const [shownDeliveries, setShownDeliveries] = useState(DELIVERY_PAGE)

  return (
    <Box sx={{ border: 1, borderColor: 'divider', borderRadius: 1, overflow: 'hidden' }}>
      {/* One line. The id, what the cycle did, and how long it took are one fact about one
          cycle, and splitting them across two rows made a four-row cycle six rows tall. */}
      <Stack
        direction="row"
        alignItems="baseline"
        spacing={2}
        sx={{ px: 1.5, py: 0.75, bgcolor: 'action.hover' }}
      >
        <CopyableTrace traceId={traceId} entries={entries} />
        <CycleSummary entries={entries} />
        <Box sx={{ flex: 1 }} />
        <Typography variant="caption" sx={{ ...tabular, color: muted }}>
          {entries.reduce((total, entry) => total + entry.durationMs, 0)} ms
        </Typography>
      </Stack>

      {(() => {
        const { head, deliveries } = splitIntoDeliveries(entries)
        const visible = deliveries.slice(0, shownDeliveries)
        return (
          <>
            {head.map((entry) => (
              <StageRow key={entry.position} entry={entry} />
            ))}
            {visible.map((delivery, index) => (
              // One block: a tint and a single border down its whole height. Drawing the rule per
              // row left disconnected stubs with gaps at every row boundary, and the heading
              // floated free of them. A block cannot come apart, and needs no indent — so nothing
              // looks nested under the transform above it either.
              <Box
                key={delivery[0]!.position}
                sx={{
                  bgcolor: 'action.hover',
                  borderLeft: 2,
                  borderColor: 'primary.main',
                  borderTop: 1,
                  borderTopColor: 'divider',
                }}
              >
                {/* Inside the block, aligned with the stage names beneath it. */}
                <Typography
                  variant="caption"
                  sx={{ display: 'block', pl: 1.5, pt: 0.5, color: 'primary.main',
                        fontWeight: 700, letterSpacing: 0.3 }}
                >
                  delivery{deliveries.length > 1 ? ` ${index + 1} of ${deliveries.length}` : ''}
                </Typography>
                {delivery.map((entry) => (
                  <StageRow key={entry.position} entry={entry} />
                ))}
              </Box>
            ))}

            {/* A cycle delivering one record per call has as many deliveries as it has records.
                Rendering them all put tens of thousands of rows inside one collapsed group and
                made the page unusable before anybody had asked to see them. */}
            {deliveries.length > visible.length && (
              <Stack alignItems="center" sx={{ py: 0.75, borderTop: 1, borderColor: 'divider' }}>
                <Button
                  size="small"
                  onClick={() => setShownDeliveries((shown) => shown + DELIVERY_PAGE)}
                >
                  Show {Math.min(DELIVERY_PAGE, deliveries.length - visible.length)} more of{' '}
                  {deliveries.length.toLocaleString()} deliveries
                </Button>
              </Stack>
            )}
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
  const [copied, setCopied] = useState(false)
  const first = entries[0]
  const chunkId = first?.chunkId
  const cycle = traceId.split('#')[1]

  // Both ids in full was eighty characters of hex for one fact — and the second is the first with
  // a suffix, so most of it was the same string twice. The short form is what a person reads; the
  // whole id is what they need on the clipboard, and both are one click apart.
  return (
    <Tooltip
      title={
        copied
          ? 'Copied'
          : chunkId
            ? `Click to copy the chunk id ${chunkId} — application logs, the record index and this
               timeline all carry it`
            : traceId
      }
    >
      <Stack
        direction="row"
        spacing={1}
        alignItems="baseline"
        onClick={() => {
          void navigator.clipboard.writeText(chunkId ?? traceId)
          setCopied(true)
          window.setTimeout(() => setCopied(false), 1500)
        }}
        sx={{ cursor: 'pointer', '&:hover .id': { color: 'primary.main' } }}
      >
        <Typography variant="caption" sx={{ color: muted }}>
          chunk
        </Typography>
        <Typography
          className="id"
          variant="caption"
          sx={{ ...tabular, fontWeight: 700, color: copied ? 'success.main' : 'inherit' }}
        >
          {chunkId ? shortId(chunkId) : traceId}
        </Typography>
        {cycle !== undefined && (
          <Typography variant="caption" sx={{ color: muted }}>
            · cycle {cycle}
          </Typography>
        )}
      </Stack>
    </Tooltip>
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
 * <p>Shown for every cycle that delivered anything. It was suppressed for simple ones while it
 * cost a row of its own; inline in the heading it costs nothing, and a total is worth having even
 * when a single row happens to carry the same number.
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

  return (
    <Typography variant="caption" sx={{ fontWeight: 600, ...tabular }}>
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
/**
 * An entry's details minus the fields the timeline set for itself.
 *
 * <p>{@code transformStage} is how the rows above were grouped, not something a destination said,
 * and showing it under "what the destination reported" is worse than showing nothing: it is a
 * plausible-looking field that came from us.
 */
function reportedDetails(entry: StageLogEntry): Record<string, unknown> | null {
  if (!entry.details) {
    return null
  }
  const { transformStage: _ignored, ...rest } = entry.details as Record<string, unknown>
  return Object.keys(rest).length > 0 ? rest : null
}

/** Whether an entry is the pass over one delivery group, rather than over every record. */
function isBatchTransform(entry: StageLogEntry): boolean {
  return (
    entry.stage === 'TRANSFORM' &&
    (entry.details as { transformStage?: string } | null)?.transformStage === 'BATCH'
  )
}

function splitIntoDeliveries(entries: StageLogEntry[]): {
  head: StageLogEntry[]
  deliveries: StageLogEntry[][]
} {
  const head: StageLogEntry[] = []
  const deliveries: StageLogEntry[][] = []
  let current: StageLogEntry[] | null = null

  for (const entry of entries) {
    if (isBatchTransform(entry)) {
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
  // transformStage alone is not detail worth an expander — it is how the rows above were grouped.
  const reportable = summarise(entry.details)
  const hasDetail =
    Boolean(entry.query) || Boolean(reportable) || Boolean(entry.request) ||
    Boolean(entry.response) || Boolean(entry.errorMessage)

  return (
    <Box sx={{ borderTop: 1, borderColor: 'divider' }}>
      <Stack direction="row" alignItems="center" spacing={1.5} sx={{ px: 1.5, py: 0.5 }}>
        {/* The column is fixed so the rows line up; the pill inside it is not, so a short word
            sits in a short pill instead of floating in the middle of a wide one. */}
        <Box sx={{ width: 132, flexShrink: 0 }}>
          <Chip
            size="small"
            // Both stages were labelled TRANSFORM, so the pass over every record and the pass over
            // one delivery group looked identical — which is the distinction the grouping draws.
            label={isBatchTransform(entry) ? 'BATCH TRANSFORM' : entry.stage}
            color={entry.outcome === 'FAILED' ? 'error' : STAGE_COLOUR[entry.stage]}
            variant={entry.outcome === 'FAILED' ? 'filled' : 'outlined'}
            sx={{ fontSize: 10.5, height: 20 }}
          />
        </Box>

        <Typography variant="body2" sx={{ width: 190 }} noWrap>
          {entry.nodeName || entry.nodeId}
        </Typography>

        {/* Right-aligned, so counts and durations form columns down the cycle instead of
            starting wherever the name before them happened to end. */}
        <Typography variant="caption" sx={{ ...tabular, width: 150, textAlign: 'right' }}>
          <RecordCount entry={entry} />
        </Typography>

        <Typography
          variant="caption"
          sx={{ ...tabular, color: muted, width: 64, textAlign: 'right' }}
        >
          {entry.durationMs} ms
        </Typography>

        {/* Why, then what. A URL is the least legible thing on the row and was winning the
            slot: two fetches against one chunk showed two near-identical ninety-character
            strings, and nothing said one had fetched column names and the other a thousand
            rows. The URL is still there, one click away. */}
        <Typography variant="caption" sx={{ ...tabular, color: muted, flex: 1 }} noWrap>
          {entry.errorMessage ?? reasonOf(entry) ?? entry.query ?? reportable}
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
          {/* The connector's report, without this component's own grouping hint in it. */}
          <Detail label="What the destination reported" value={reportedDetails(entry)} />
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
  // The word "records" on every row of every cycle is a column of the same eight characters. The
  // number is what varies, and an arrow is what says it changed.
  if (entry.recordsIn === entry.recordsOut) {
    return <>{entry.recordsIn.toLocaleString()}</>
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
    // Internal to how this component groups rows, and it was being rendered as the row's message:
    // "transformStage=RECORD" where the reader expects what the stage actually did.
    .filter(([name]) => name !== 'transformStage')
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
