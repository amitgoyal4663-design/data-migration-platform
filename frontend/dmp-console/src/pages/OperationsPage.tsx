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
import { Link as RouterLink } from 'react-router-dom'
import { useOperationsDashboard } from '@/api/hooks'
import { ErrorPanel, Loading } from '@/components/Feedback'
import { PageHeader } from '@/components/PageHeader'
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
export function OperationsPage() {
  const dashboard = useOperationsDashboard(30_000)

  if (dashboard.isLoading) return <Loading />
  if (dashboard.error) return <ErrorPanel error={dashboard.error} />
  if (!dashboard.data) return null

  const { pipelines, watched, healthy, live, totals, generatedAt } = dashboard.data
  const attention = pipelines.filter((pipeline) => !pipeline.healthy)

  return (
    <>
      <PageHeader
        title="Operations"
        subtitle={`${watched} job${watched === 1 ? '' : 's'} watched · ${healthy} normal · checked ${new Date(
          generatedAt,
        ).toLocaleTimeString()}`}
      />

      <Highlights totals={totals} />

      {live.length > 0 && <RunningNow runs={live} />}

      <Box sx={{ mt: 3 }}>
        {watched === 0 ? (
          <Alert severity="info">
            No jobs are being watched. Open a pipeline and press <strong>Watch</strong> to put it
            here — a watchlist rather than everything, so the jobs somebody is accountable for do
            not end up among the experiments.
          </Alert>
        ) : attention.length === 0 ? (
          <Alert severity="success" icon={<CheckCircleIcon fontSize="inherit" />}>
            All {healthy} watched job{healthy === 1 ? '' : 's'} ran, moved roughly what they
            usually move, and lost nothing.
          </Alert>
        ) : (
          <Alert severity={attention.some((p) => p.worst === 'CRITICAL') ? 'error' : 'warning'}>
            <strong>
              {attention.length} of {watched} need attention
            </strong>{' '}
            — {attention.map((pipeline) => pipeline.name).join(', ')}
          </Alert>
        )}
      </Box>

      <Stack spacing={2} sx={{ mt: 2 }}>
        {pipelines.map((pipeline) => (
          <JobCard key={pipeline.pipelineId} job={pipeline} />
        ))}
      </Stack>
    </>
  )
}

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
