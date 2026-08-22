import { useMemo } from 'react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import BoltIcon from '@mui/icons-material/Bolt'
import PauseCircleOutlineIcon from '@mui/icons-material/PauseCircleOutline'
import type { ConnectorInstance, ConnectorSpec, PipelineVersion, RateLimit } from '../api/types'

/**
 * What this pipeline will actually do, in one line, without opening a settings dialog.
 *
 * The numbers that decide a migration — how big a chunk is, how many requests it becomes, and how
 * fast the far end has agreed to be called — live in three different dialogs, and their interaction
 * is what nobody can see. A chunk of 500 delivered in groups of 3 is 167 requests, and whether that
 * is fine or hopeless depends on a limit configured on a different screen entirely.
 *
 * So this states the consequence rather than the settings: requests per chunk, the resulting rate,
 * and how long a hundred thousand records would take. The last one is the number people actually
 * need before they start something, and the one they currently discover three hours in.
 */

const PER_100K = 100_000

export function FlowSummary({
  version,
  source,
  sink,
  sinkSpec,
}: {
  version: PipelineVersion
  source?: ConnectorInstance
  sink?: ConnectorInstance
  sinkSpec?: ConnectorSpec
}) {
  const summary = useMemo(
    () => summarise(version, source, sink, sinkSpec),
    [version, source, sink, sinkSpec],
  )

  if (!summary) return null

  return (
    <Paper variant="outlined" sx={{ px: 2, py: 1.25, mb: 1.5 }}>
      <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap" useFlexGap>
        <Step label="Chunk" value={summary.chunk} />
        <Arrow />
        <Step label={summary.callsLabel} value={summary.calls} />
        {summary.rate && (
          <>
            <Arrow />
            <Step label="Rate" value={summary.rate} limited />
          </>
        )}
        {summary.duration && (
          <>
            <Arrow />
            <Step label="Per 100k records" value={summary.duration} />
          </>
        )}

        <Box sx={{ flex: 1 }} />

        {summary.badges.map((badge) => (
          <Chip
            key={badge.label}
            size="small"
            variant="outlined"
            icon={badge.icon}
            label={badge.label}
            sx={{ height: 24 }}
          />
        ))}
      </Stack>

      {summary.impossible && (
        <Alert severity="error" sx={{ mt: 1.25, py: 0 }}>
          {summary.impossible}
        </Alert>
      )}
    </Paper>
  )
}

function Step({ label, value, limited }: { label: string; value: string; limited?: boolean }) {
  return (
    <Box>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', lineHeight: 1.2 }}>
        {label}
      </Typography>
      <Typography
        variant="body2"
        sx={{ fontWeight: 600, lineHeight: 1.3, color: limited ? 'warning.main' : undefined }}
      >
        {value}
      </Typography>
    </Box>
  )
}

function Arrow() {
  return (
    <Typography variant="body2" color="text.disabled" sx={{ px: 0.5 }}>
      →
    </Typography>
  )
}

interface Summary {
  chunk: string
  calls: string
  callsLabel: string
  rate: string | null
  duration: string | null
  badges: { label: string; icon: React.ReactElement }[]
  impossible: string | null
}

function summarise(
  version: PipelineVersion,
  source?: ConnectorInstance,
  sink?: ConnectorInstance,
  sinkSpec?: ConnectorSpec,
): Summary | null {
  const rows = version.executionPolicy?.rowsPerChunk ?? 0
  if (rows <= 0) {
    // Left automatic, so the engine derives it from the read size at run time. Naming a number
    // here would be inventing one, and the whole point of this strip is that its numbers are real.
    return null
  }

  // The chunk is one unit of work for a destination that runs it as a job — created, uploaded,
  // polled to completion — however many requests that takes underneath.
  const perChunk = sinkSpec?.callCost === 'PER_CHUNK'
  const calls = perChunk ? 1 : deliveryCalls(version, rows)

  const sinkLimit = sink?.rateLimit ?? null
  const sourceLimit = source?.rateLimit ?? null

  const recordsPerSecond = limitedRate(sinkLimit, sourceLimit, calls, rows)
  const badges: Summary['badges'] = []

  if (perChunk) {
    badges.push({ label: 'parks & polls', icon: <PauseCircleOutlineIcon /> })
  }
  if (sinkLimit) {
    badges.push({ label: describeLimit(sinkLimit), icon: <BoltIcon /> })
  }
  if (sourceLimit) {
    badges.push({ label: `source ${describeLimit(sourceLimit)}`, icon: <BoltIcon /> })
  }

  return {
    chunk: `${rows.toLocaleString()} records`,
    calls: calls.toLocaleString(),
    callsLabel: perChunk ? 'Job per chunk' : calls === 1 ? 'Request per chunk' : 'Requests per chunk',
    rate: recordsPerSecond ? `${Math.round(recordsPerSecond * 60).toLocaleString()} records/min` : null,
    duration: recordsPerSecond ? humanise(PER_100K / recordsPerSecond) : null,
    badges,
    impossible: impossibility(sinkLimit, rows, calls),
  }
}

/** How many groups the delivery policy makes of one chunk — the same arithmetic the engine uses. */
function deliveryCalls(version: PipelineVersion, rows: number): number {
  const delivery = version.deliveryPolicy
  if (!delivery) return 1
  // A split script's group count cannot be known until the records are read, so this is the
  // engine's own worst case. It reserves this many and hands back what it did not use.
  if (delivery.splitScript) return rows
  const groupSize = delivery.groupSize ?? 0
  if (groupSize <= 0) return 1
  return Math.ceil(rows / groupSize)
}

/**
 * Records per second, once both ends have had their say.
 *
 * Whichever limit bites first decides, which is the answer people get wrong: a generous records
 * limit means nothing if the calls limit only permits two requests a minute.
 */
function limitedRate(
  sinkLimit: RateLimit | null,
  sourceLimit: RateLimit | null,
  callsPerChunk: number,
  rows: number,
): number | null {
  const candidates = [sinkLimit, sourceLimit]
    .filter(Boolean)
    .flatMap((limit) => {
      const l = limit as RateLimit
      const rates: number[] = []
      if (l.records && l.recordsWindow) rates.push(l.records / seconds(l.recordsWindow))
      // A calls limit converts to records by how many records ride in each call.
      if (l.calls && l.callsWindow) {
        rates.push((l.calls / seconds(l.callsWindow)) * (rows / Math.max(1, callsPerChunk)))
      }
      return rates
    })

  return candidates.length ? Math.min(...candidates) : null
}

/** The case that can never run, said at design time rather than discovered on the fourth attempt. */
function impossibility(limit: RateLimit | null, rows: number, calls: number): string | null {
  if (!limit) return null
  if (limit.records && rows > limit.records) {
    return `A chunk of ${rows.toLocaleString()} records cannot be sent: the limit is ${limit.records.toLocaleString()} per ${describeWindow(limit.recordsWindow)}, and a chunk has to fit in one window. Reduce the chunk size.`
  }
  if (limit.calls && calls > limit.calls) {
    return `This chunk makes ${calls.toLocaleString()} requests but the limit is ${limit.calls.toLocaleString()} per ${describeWindow(limit.callsWindow)}. Reduce the chunk size, or send more records per request.`
  }
  return null
}

function describeLimit(limit: RateLimit): string {
  const parts: string[] = []
  if (limit.records) parts.push(`${limit.records.toLocaleString()}/${describeWindow(limit.recordsWindow)}`)
  if (limit.calls) parts.push(`${limit.calls.toLocaleString()} calls/${describeWindow(limit.callsWindow)}`)
  return parts.join(' · ')
}

function describeWindow(window: string | null): string {
  if (!window) return ''
  return window.replace('PT', '').replace('P', '').toLowerCase()
}

/** ISO-8601 period to seconds. Only the shapes the console offers, which is all that reaches here. */
function seconds(window: string): number {
  const match = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$/.exec(window)
  if (!match) return 60
  const [, d, h, m, s] = match
  return Number(d ?? 0) * 86400 + Number(h ?? 0) * 3600 + Number(m ?? 0) * 60 + Number(s ?? 0)
}

function humanise(totalSeconds: number): string {
  if (!Number.isFinite(totalSeconds) || totalSeconds <= 0) return '—'
  if (totalSeconds < 90) return `${Math.round(totalSeconds)}s`
  const minutes = totalSeconds / 60
  if (minutes < 90) return `${Math.round(minutes)}m`
  const hours = Math.floor(minutes / 60)
  const rest = Math.round(minutes % 60)
  return rest ? `${hours}h ${rest}m` : `${hours}h`
}
