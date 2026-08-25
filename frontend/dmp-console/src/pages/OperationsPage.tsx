import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Divider from '@mui/material/Divider'
import LinearProgress from '@mui/material/LinearProgress'
import Link from '@mui/material/Link'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import CheckCircleIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorIcon from '@mui/icons-material/ErrorOutlineOutlined'
import WarningIcon from '@mui/icons-material/WarningAmberOutlined'
import MoreVertIcon from '@mui/icons-material/MoreVert'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import StopIcon from '@mui/icons-material/Stop'
import IconButton from '@mui/material/IconButton'
import Menu from '@mui/material/Menu'
import MenuItem from '@mui/material/MenuItem'
import Tab from '@mui/material/Tab'
import Tabs from '@mui/material/Tabs'
import { useMemo, useState } from 'react'
import { Link as RouterLink, useSearchParams } from 'react-router-dom'
import {
  useMonitorPipeline,
  useOperationsDashboard,
  useRunControl,
  useStartRun,
} from '@/api/hooks'
import { ErrorPanel, Loading } from '@/components/Feedback'
import { OperationsControls, applyFilters } from '@/components/OperationsControls'
import type { OperationsFilters, SortOrder, StatusFilter } from '@/components/OperationsControls'
import { AlertBar, AlertsDrawer } from '@/components/OperationsAlerts'
import { OperationsProduct, ProductTotals } from '@/components/OperationsProduct'
import { PageHeader } from '@/components/PageHeader'
import { RunDialog } from '@/components/RunDialog'
import { RunStateChip } from '@/components/StateChip'
import { muted, tabular } from '@/theme'
import type {
  OperationsAttempt,
  OperationsLiveRun,
  OperationsTotals,
  PipelineHealth,
} from '@/api/types'

/**
 * The screen a support desk works from.
 *
 * Built around what somebody does at nine in the morning with a job that ran overnight: did it
 * run, did it finish, how much moved, was that normal, what failed and why, and is it mine to fix.
 * One card per job answers all six without a click, and every number links to the thing that
 * explains it.
 *
 * A card rather than a table row, because the useful unit is a job and not a cell — and the part
 * that matters most, why records failed, is a list of sentences, which is exactly what a table
 * cannot hold.
 */
type View = 'support' | 'product'

const VIEWS: { value: View; label: string; caption: string }[] = [
  {
    value: 'support',
    label: 'Support',
    caption: 'Is anything wrong right now, and what do I do about it',
  },
  {
    value: 'product',
    label: 'Product',
    caption: 'How much data moved, by pipeline, across the window',
  },
]

export function OperationsPage() {
  const [params, setParams] = useSearchParams()
  const [alertsOpen, setAlertsOpen] = useState(false)

  // Read from the URL rather than from state, so a filtered screen is a link. Half of support work
  // is one person showing another what they are looking at, and "failures first, last seven days,
  // every pipeline" is not a sentence anybody wants to say out loud.
  const view = (params.get('view') as View) || 'support'
  const filters: OperationsFilters = {
    hours: Number(params.get('hours')) || 24,
    watched: params.get('scope') !== 'all',
    search: params.get('q') ?? '',
    status: (params.get('status') as StatusFilter) || 'all',
    sort: (params.get('sort') as SortOrder) || 'worst',
    live: params.get('live') !== '0',
  }

  const change = (next: Partial<OperationsFilters & { view: View }>) => {
    const updated = new URLSearchParams(params)
    const set = (key: string, value: string, fallback: string) =>
      value === fallback ? updated.delete(key) : updated.set(key, value)

    if (next.view !== undefined) set('view', next.view, 'support')
    if (next.hours !== undefined) set('hours', String(next.hours), '24')
    if (next.watched !== undefined) set('scope', next.watched ? 'watched' : 'all', 'watched')
    if (next.search !== undefined) set('q', next.search, '')
    if (next.status !== undefined) set('status', next.status, 'all')
    if (next.sort !== undefined) set('sort', next.sort, 'worst')
    if (next.live !== undefined) set('live', next.live ? '1' : '0', '1')
    setParams(updated, { replace: true })
  }

  const dashboard = useOperationsDashboard(
    filters.hours,
    filters.watched,
    filters.live ? 30_000 : false,
  )

  const shown = useMemo(
    () => (dashboard.data ? applyFilters(dashboard.data.pipelines, filters) : []),
    [dashboard.data, filters],
  )

  if (dashboard.isLoading && !dashboard.data) return <Loading />
  if (dashboard.error) return <ErrorPanel error={dashboard.error} />
  if (!dashboard.data) return null

  const { pipelines, watched, healthy, live, totals, headlines, generatedAt } = dashboard.data
  const attention = shown.filter((pipeline) => !pipeline.healthy)

  return (
    <>
      <PageHeader
        title="Operations"
        subtitle={`${watched} job${watched === 1 ? '' : 's'} · ${healthy} normal · ${
          filters.live ? 'live, ' : ''
        }checked ${new Date(generatedAt).toLocaleTimeString()}`}
      />

      <Tabs
        value={view}
        onChange={(_, next: View) => change({ view: next })}
        sx={{ mb: 1, borderBottom: 1, borderColor: 'divider' }}
      >
        {VIEWS.map((entry) => (
          <Tab key={entry.value} value={entry.value} label={entry.label} />
        ))}
      </Tabs>
      <Typography variant="body2" sx={{ color: muted, mb: 2 }}>
        {VIEWS.find((entry) => entry.value === view)?.caption}
      </Typography>

      <OperationsControls
        filters={filters}
        onChange={change}
        onRefresh={() => void dashboard.refetch()}
        refreshing={dashboard.isFetching}
        showing={shown.length}
        total={pipelines.length}
      />

      {/*
        On every view, not only Support. A product team reading volumes and an engineer reading
        causes are both entitled to know that two jobs are on fire, and one line is small enough to
        say so everywhere without taking the screen.
      */}
      <AlertBar headlines={headlines} jobs={pipelines.length} onOpen={() => setAlertsOpen(true)} />
      <AlertsDrawer
        open={alertsOpen}
        headlines={headlines}
        onClose={() => setAlertsOpen(false)}
      />

      {view === 'support' && (
        <>
          <Highlights totals={totals} />
          {live.length > 0 && <RunningNow runs={live} />}

          <Box sx={{ mt: 3 }}>
            {pipelines.length === 0 ? (
              <Alert severity="info">
                {filters.watched
                  ? 'No jobs are being watched. Open a pipeline and press Watch to put it here — or switch Pipelines to "Every published" to see them all.'
                  : 'No published pipelines have run in this window.'}
              </Alert>
            ) : shown.length === 0 ? (
              <Alert severity="info">Nothing matches these filters.</Alert>
            ) : attention.length === 0 ? (
              <Alert severity="success" icon={<CheckCircleIcon fontSize="inherit" />}>
                All {shown.length} job{shown.length === 1 ? '' : 's'} shown ran, moved roughly what
                they usually move, and lost nothing.
              </Alert>
            ) : (
              <Alert severity={attention.some((p) => p.worst === 'CRITICAL') ? 'error' : 'warning'}>
                <strong>
                  {attention.length} of {shown.length} need attention
                </strong>{' '}
                — {attention.map((pipeline) => pipeline.name).join(', ')}
              </Alert>
            )}
          </Box>

          <Stack spacing={2} sx={{ mt: 2 }}>
            {shown.map((pipeline) => (
              <JobCard key={pipeline.pipelineId} job={pipeline} />
            ))}
          </Stack>
        </>
      )}

      {view === 'product' && (
        <>
          <ProductTotals pipelines={shown} />
          <OperationsProduct pipelines={shown} />
        </>
      )}

    </>
  )
}

/**
 * The screen in sentences, before any of the figures.
 *
 * Everything below this is numbers, and a number has to be interpreted before it means anything —
 * 60,301 is alarming or routine depending on what it is out of and which job it belongs to. These
 * have already done that work: three lines and somebody knows whether to sit down or get up.
 *
 * The first is given the room, because a strip where everything is the same size is a strip
 * where nothing is first.
 */
/** The figures somebody is asked for before they have opened anything. */
function Highlights({ totals }: { totals: OperationsTotals }) {
  return (
    <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
      <Tile label="TRANSFERRED · 24H" value={totals.recordsWritten} tone="success.main" />
      <Tile label="READ" value={totals.recordsRead} />
      <Tile
        label="NOT DELIVERED"
        value={totals.recordsFailed}
        tone={totals.recordsFailed > 0 ? 'error.main' : undefined}
      />
      <Tile label="RUNS COMPLETED" value={totals.completed} />
      <Tile
        label="RUNS FAILED"
        value={totals.failed}
        tone={totals.failed > 0 ? 'error.main' : undefined}
      />
      <Tile
        label="RUNNING NOW"
        value={totals.running}
        tone={totals.running > 0 ? 'info.main' : undefined}
      />
    </Box>
  )
}

function Tile({ label, value, tone }: { label: string; value: number; tone?: string }) {
  return (
    <Paper sx={{ flex: '1 1 150px', px: 2, py: 1.5 }}>
      <Typography variant="caption" sx={{ color: muted, letterSpacing: '0.08em' }}>
        {label}
      </Typography>
      <Typography sx={{ ...tabular, fontSize: 28, fontWeight: 600, lineHeight: 1.2, color: tone }}>
        {value.toLocaleString()}
      </Typography>
    </Paper>
  )
}

/**
 * What is moving right now.
 *
 * Indeterminate until planning produces a chunk count, rather than a bar sitting at zero: a bar
 * that looks stalled and a run that is stalled must not look the same.
 */
function RunningNow({ runs }: { runs: OperationsLiveRun[] }) {
  return (
    <Paper sx={{ mt: 2, p: 2 }}>
      <Typography variant="caption" sx={{ color: muted, letterSpacing: '0.08em' }}>
        RUNNING NOW
      </Typography>
      <Stack spacing={1.5} sx={{ mt: 1 }}>
        {runs.map((run) => (
          <Box key={run.runId}>
            <Box sx={{ display: 'flex', alignItems: 'baseline', gap: 1.5, flexWrap: 'wrap' }}>
              <Link component={RouterLink} to={`/runs/${run.runId}`} sx={{ fontWeight: 500 }}>
                {run.pipeline}
              </Link>
              <RunStateChip state={run.state} />
              <Box sx={{ flex: 1 }} />
              <Typography variant="caption" sx={{ ...tabular, color: muted }}>
                {run.recordsWritten.toLocaleString()} transferred · {formatSeconds(run.seconds)}
                {run.progress !== null && ` · ${Math.round(run.progress * 100)}%`}
              </Typography>
              <StopButton runId={run.runId} />
            </Box>
            <LinearProgress
              variant={run.progress === null ? 'indeterminate' : 'determinate'}
              value={run.progress === null ? undefined : run.progress * 100}
              sx={{ mt: 0.5, height: 6, borderRadius: 1 }}
            />
          </Box>
        ))}
      </Stack>
    </Paper>
  )
}

/**
 * Stops a run from the screen it is being watched on.
 *
 * <p>Chunks already running finish their current batch, so the run stops somewhere it can resume
 * from — which is why this is safe to offer beside a progress bar rather than behind two pages.
 */
function StopButton({ runId }: { runId: string }) {
  const control = useRunControl(runId)
  return (
    <Tooltip title="Stop after the current batch">
      <span>
        <IconButton
          size="small"
          onClick={() => control.stop.mutate()}
          disabled={control.stop.isPending}
        >
          <StopIcon fontSize="small" />
        </IconButton>
      </span>
    </Tooltip>
  )
}

/**
 * What somebody does about a job, on the screen that told them about it.
 *
 * <p>The old card said what had happened and offered one link. Everything a support desk actually
 * does next — run it again for one policy, rehearse it first, take it off the watchlist — meant
 * finding the pipeline somewhere else, which is where the trail went cold.
 */
function JobActions({ job }: { job: PipelineHealth }) {
  const [anchor, setAnchor] = useState<HTMLElement | null>(null)
  const [dialog, setDialog] = useState<'run' | 'dry' | null>(null)
  const start = useStartRun()
  const monitor = useMonitorPipeline()

  return (
    <>
      <Tooltip title="Run this job">
        <span>
          <IconButton size="small" onClick={() => setDialog('run')}>
            <PlayArrowIcon fontSize="small" />
          </IconButton>
        </span>
      </Tooltip>
      <IconButton size="small" onClick={(event) => setAnchor(event.currentTarget)}>
        <MoreVertIcon fontSize="small" />
      </IconButton>

      <Menu anchorEl={anchor} open={Boolean(anchor)} onClose={() => setAnchor(null)}>
        <MenuItem
          onClick={() => {
            setAnchor(null)
            setDialog('dry')
          }}
        >
          Dry run — read everything, write nothing
        </MenuItem>
        {job.latest && (
          <MenuItem component={RouterLink} to={`/runs/${job.latest.id}`} onClick={() => setAnchor(null)}>
            Open the latest run
          </MenuItem>
        )}
        <MenuItem
          component={RouterLink}
          to={`/runs?pipelineId=${job.pipelineId}`}
          onClick={() => setAnchor(null)}
        >
          Every run of this job
        </MenuItem>
        <MenuItem component={RouterLink} to={`/pipelines/${job.pipelineId}`} onClick={() => setAnchor(null)}>
          Open the pipeline
        </MenuItem>
        <MenuItem
          onClick={() => {
            setAnchor(null)
            monitor.mutate({ id: job.pipelineId, watched: !job.watched })
          }}
        >
          {job.watched ? 'Stop watching' : 'Watch this job'}
        </MenuItem>
        <MenuItem
          onClick={() => {
            setAnchor(null)
            void navigator.clipboard?.writeText(summarise(job))
          }}
        >
          Copy a summary
        </MenuItem>
      </Menu>

      <RunDialog
        open={dialog !== null}
        pipelineId={job.pipelineId}
        pending={start.isPending}
        dryRun={dialog === 'dry'}
        onCancel={() => setDialog(null)}
        onStart={(parameters, query) => {
          start.mutate(
            { pipelineId: job.pipelineId, parameters, query, dryRun: dialog === 'dry' },
            { onSuccess: () => setDialog(null) },
          )
        }}
      />
    </>
  )
}

/** The card as a paragraph, for the person who is going to paste it into a chat window. */
function summarise(job: PipelineHealth): string {
  const run = job.latest
  const lines = [
    `${job.name} — ${job.worst === 'INFO' ? 'normal' : job.worst.toLowerCase()}`,
    run
      ? `Last run ${run.state.toLowerCase()} at ${new Date(run.createdAt).toLocaleString()}: ` +
        `${run.metrics.recordsRead.toLocaleString()} read, ` +
        `${run.metrics.recordsWritten.toLocaleString()} transferred, ` +
        `${run.metrics.recordsFailed.toLocaleString()} failed`
      : 'Has never run',
    ...job.findings.filter((f) => f.severity !== 'INFO').map((f) => `- ${f.message}`),
    ...job.reasons.map((r) => `- ${r.count.toLocaleString()} ${r.reason}`),
    run ? `${window.location.origin}/runs/${run.id}` : '',
  ]
  return lines.filter(Boolean).join('\n')
}

function JobCard({ job }: { job: PipelineHealth }) {
  const run = job.latest
  const tone =
    job.worst === 'CRITICAL'
      ? 'error.main'
      : job.worst === 'WARNING'
        ? 'warning.main'
        : 'success.main'

  return (
    <Paper sx={{ borderLeft: 3, borderColor: tone, overflow: 'hidden' }}>
      <Box
        sx={{ display: 'flex', alignItems: 'center', gap: 1.5, px: 2, py: 1.5, flexWrap: 'wrap' }}
      >
        <Verdict worst={job.worst} />
        <Link
          component={RouterLink}
          to={`/pipelines/${job.pipelineId}`}
          sx={{ fontWeight: 600, fontSize: 16 }}
        >
          {job.name}
        </Link>
        {job.schedule && (
          <Tooltip
            title={`${job.schedule.cron} (${job.schedule.timezone})${
              job.schedule.nextDueAt
                ? ` · next due ${new Date(job.schedule.nextDueAt).toLocaleString()}`
                : ''
            }`}
          >
            <Chip
              size="small"
              variant="outlined"
              label={job.schedule.name}
              sx={{ cursor: 'help' }}
            />
          </Tooltip>
        )}
        <Box sx={{ flex: 1 }} />
        {run && (
          <>
            <RunStateChip state={run.state} />
            <Typography variant="caption" sx={{ color: muted }}>
              {new Date(run.createdAt).toLocaleString()}
              {run.durationSeconds != null && ` · ${formatSeconds(run.durationSeconds)}`}
            </Typography>
            <Button size="small" component={RouterLink} to={`/runs/${run.id}`}>
              Open run
            </Button>
          </>
        )}
        <JobActions job={job} />
      </Box>

      <Divider />

      {run ? (
        <Box sx={{ display: 'flex', flexWrap: 'wrap' }}>
          <Numbers job={job} />
          <Box
            sx={{
              flex: '1 1 340px',
              minWidth: 280,
              borderLeft: { md: '1px solid' },
              borderColor: { md: 'divider' },
              p: 2,
            }}
          >
            <Problems job={job} />
          </Box>
        </Box>
      ) : (
        <Typography variant="body2" sx={{ color: muted, p: 2 }}>
          This job has never run.
        </Typography>
      )}

      {job.trend.length > 1 && <Trend trend={job.trend} />}
    </Paper>
  )
}

/** The numbers somebody is actually asked for, with what normal looks like beside them. */
function Numbers({ job }: { job: PipelineHealth }) {
  const metrics = job.latest?.metrics
  if (!metrics) return null

  return (
    <Box sx={{ flex: '1 1 380px', display: 'flex', gap: 3, p: 2, flexWrap: 'wrap' }}>
      <Figure
        label="READ"
        value={metrics.recordsRead}
        note={
          job.typicalRows === null
            ? `no baseline yet (${job.baselineRuns} run${job.baselineRuns === 1 ? '' : 's'})`
            : `usually ${job.typicalRows.toLocaleString()}`
        }
      />
      <Figure label="TRANSFERRED" value={metrics.recordsWritten} tone="success.main" />
      <Figure
        label="NOT DELIVERED"
        value={metrics.recordsFailed}
        tone={metrics.recordsFailed > 0 ? 'error.main' : undefined}
        note={
          metrics.recordsProduced > 0 && metrics.recordsFailed > 0
            ? `${Math.round((metrics.recordsFailed / metrics.recordsProduced) * 100)}% of the batch`
            : undefined
        }
      />
      {metrics.recordsFiltered > 0 && (
        <Figure label="FILTERED" value={metrics.recordsFiltered} note="dropped by a rule" />
      )}
    </Box>
  )
}

function Figure({
  label,
  value,
  tone,
  note,
}: {
  label: string
  value: number
  tone?: string
  note?: string
}) {
  return (
    <Box>
      <Typography variant="caption" sx={{ color: muted, letterSpacing: '0.08em' }}>
        {label}
      </Typography>
      <Typography sx={{ ...tabular, fontSize: 26, fontWeight: 600, lineHeight: 1.2, color: tone }}>
        {value.toLocaleString()}
      </Typography>
      {note && (
        <Typography variant="caption" sx={{ color: muted, display: 'block' }}>
          {note}
        </Typography>
      )}
    </Box>
  )
}

/**
 * What is wrong, and why records failed.
 *
 * The reasons are the point of the whole screen. A count of failures makes somebody open the run;
 * a named reason with a count tells them straight away whether it is a mapping they own or an
 * upstream system that is down — the difference between fixing it and escalating it.
 */
function Problems({ job }: { job: PipelineHealth }) {
  const problems = job.findings.filter((finding) => finding.severity !== 'INFO')

  if (problems.length === 0 && job.reasons.length === 0) {
    return (
      <Typography variant="body2" sx={{ color: 'success.main' }}>
        Nothing to look at — it ran, the volume was normal, and every record is accounted for.
      </Typography>
    )
  }

  return (
    <Stack spacing={1.25}>
      {problems.map((finding, index) => (
        <Typography
          key={index}
          variant="body2"
          sx={{ color: finding.severity === 'CRITICAL' ? 'error.main' : 'warning.main' }}
        >
          {finding.message}
        </Typography>
      ))}

      {job.reasons.length > 0 && (
        <Box>
          <Typography variant="caption" sx={{ color: muted, letterSpacing: '0.08em' }}>
            WHY RECORDS FAILED
          </Typography>
          {job.reasons.map((reason, index) => (
            <Box key={index} sx={{ display: 'flex', gap: 1.5, alignItems: 'baseline', mt: 0.5 }}>
              <Typography sx={{ ...tabular, fontWeight: 600, minWidth: 60, textAlign: 'right' }}>
                {reason.count.toLocaleString()}
              </Typography>
              <Typography variant="body2" sx={{ minWidth: 0 }}>
                {reason.reason}
                <Typography component="span" variant="caption" sx={{ color: muted, ml: 1 }}>
                  {reason.code}
                </Typography>
              </Typography>
            </Box>
          ))}
          {job.latest && (
            <Button
              size="small"
              component={RouterLink}
              to={`/runs/${job.latest.id}`}
              sx={{ mt: 0.5 }}
            >
              See the failed records
            </Button>
          )}
        </Box>
      )}
    </Stack>
  )
}

/**
 * The last seven runs, oldest on the left.
 *
 * Bars rather than figures: the question a trend answers is "is today like the others", and a
 * shape answers that faster than seven numbers somebody has to compare in their head. Failures are
 * stacked on what was transferred, so a run that moved its usual volume while losing a slice of it
 * is obvious without reading anything.
 */
function Trend({ trend }: { trend: OperationsAttempt[] }) {
  const runs = [...trend].reverse()
  const peak = Math.max(...runs.map((attempt) => attempt.read), 1)

  return (
    <Box sx={{ px: 2, py: 1.5, borderTop: '1px solid', borderColor: 'divider' }}>
      <Typography variant="caption" sx={{ color: muted, letterSpacing: '0.08em' }}>
        LAST {runs.length} RUNS
      </Typography>
      <Box sx={{ display: 'flex', alignItems: 'flex-end', gap: 1, height: 44, mt: 0.5 }}>
        {runs.map((attempt) => (
          <Tooltip
            key={attempt.runId}
            title={`${new Date(attempt.at).toLocaleString()} — ${attempt.state}: read ${attempt.read.toLocaleString()}, transferred ${attempt.written.toLocaleString()}, not delivered ${attempt.failed.toLocaleString()}`}
          >
            <Box
              component={RouterLink}
              to={`/runs/${attempt.runId}`}
              sx={{
                flex: 1,
                maxWidth: 48,
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'flex-end',
                height: '100%',
              }}
            >
              <Box
                sx={{
                  height: `${(attempt.failed / peak) * 100}%`,
                  bgcolor: 'error.main',
                  opacity: 0.9,
                }}
              />
              <Box
                sx={{
                  height: `${(attempt.written / peak) * 100}%`,
                  bgcolor: attempt.state === 'COMPLETED' ? 'success.main' : 'text.disabled',
                  opacity: 0.9,
                  minHeight: 2,
                }}
              />
            </Box>
          </Tooltip>
        ))}
      </Box>
    </Box>
  )
}

function Verdict({ worst }: { worst: string }) {
  if (worst === 'CRITICAL') return <ErrorIcon fontSize="small" sx={{ color: 'error.main' }} />
  if (worst === 'WARNING') return <WarningIcon fontSize="small" sx={{ color: 'warning.main' }} />
  return <CheckCircleIcon fontSize="small" sx={{ color: 'success.main' }} />
}

function formatSeconds(seconds: number) {
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  return `${(seconds / 3600).toFixed(1)}h`
}
