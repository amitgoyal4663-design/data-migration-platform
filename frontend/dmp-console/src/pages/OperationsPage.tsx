import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import CheckCircleIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorIcon from '@mui/icons-material/ErrorOutlineOutlined'
import WarningIcon from '@mui/icons-material/WarningAmberOutlined'
import { Link as RouterLink } from 'react-router-dom'
import Link from '@mui/material/Link'
import { useOperationsDashboard } from '@/api/hooks'
import { ErrorPanel, Loading } from '@/components/Feedback'
import { PageHeader } from '@/components/PageHeader'
import { RunStateChip } from '@/components/StateChip'
import { muted, tabular } from '@/theme'
import type { PipelineHealth } from '@/api/types'

/**
 * The screen a support team reads every morning.
 *
 * It answers one question — is anything wrong? — which the run list cannot, because a number is
 * only readable beside what that pipeline usually does. Five thousand records is healthy for one
 * and a catastrophe for another.
 *
 * Everything flagged says what it was compared against. A judgement nobody on the desk can restate
 * gets ignored within a fortnight, and takes the real alerts down with it.
 */
export function OperationsPage() {
  // Polled, because this is left open on a wall. The interval is deliberately slow: nothing here
  // changes second to second, and the queries behind it walk every watched pipeline's history.
  const dashboard = useOperationsDashboard(60_000)

  if (dashboard.isLoading) return <Loading />
  if (dashboard.error) return <ErrorPanel error={dashboard.error} />
  if (!dashboard.data) return null

  const { pipelines, watched, healthy, generatedAt } = dashboard.data
  const needsAttention = pipelines.filter((p) => !p.healthy)

  return (
    <>
      <PageHeader
        title="Operations"
        subtitle={`${watched} pipeline${watched === 1 ? '' : 's'} watched · checked ${new Date(
          generatedAt,
        ).toLocaleTimeString()}`}
      />

      {watched === 0 ? (
        <Alert severity="info">
          Nothing is being watched yet. Open a pipeline and press <strong>Watch</strong> to put it
          on this screen. A watchlist rather than everything, so the jobs that matter do not end up
          among the experiments.
        </Alert>
      ) : needsAttention.length === 0 ? (
        <Alert severity="success" icon={<CheckCircleIcon fontSize="inherit" />}>
          All {healthy} watched pipeline{healthy === 1 ? '' : 's'} look normal — they ran, they
          moved roughly what they usually move, and nothing was lost.
        </Alert>
      ) : (
        <Alert severity={needsAttention.some(worst('CRITICAL')) ? 'error' : 'warning'}>
          <strong>
            {needsAttention.length} of {watched} need attention
          </strong>
          <Box sx={{ mt: 1 }}>
            {needsAttention.map((pipeline) =>
              pipeline.findings
                .filter((finding) => finding.severity !== 'INFO')
                .map((finding, index) => (
                  <Typography key={`${pipeline.pipelineId}-${index}`} variant="body2">
                    <Link component={RouterLink} to={`/pipelines/${pipeline.pipelineId}`}>
                      {pipeline.name}
                    </Link>{' '}
                    — {finding.message}
                  </Typography>
                )),
            )}
          </Box>
        </Alert>
      )}

      {pipelines.length > 0 && (
        <Paper sx={{ overflow: 'auto', mt: 3 }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell />
                <TableCell>PIPELINE</TableCell>
                <TableCell>LAST RUN</TableCell>
                <TableCell align="right">READ</TableCell>
                <TableCell align="right">USUALLY</TableCell>
                <TableCell align="right">WRITTEN</TableCell>
                <TableCell align="right">NOT DELIVERED</TableCell>
                <TableCell align="right">TOOK</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {pipelines.map((pipeline) => (
                <Row key={pipeline.pipelineId} pipeline={pipeline} />
              ))}
            </TableBody>
          </Table>
        </Paper>
      )}

      <Typography variant="caption" sx={{ color: muted, display: 'block', mt: 2 }}>
        &ldquo;Usually&rdquo; is the median of this pipeline&apos;s last ten completed runs, and a
        run is flagged when it reads less than half or more than double that. Deliberately blunt: a
        daily load varies with the business, and a band tight enough to catch a 10% drift would
        flag most Mondays.
      </Typography>
    </>
  )
}

const worst = (severity: string) => (pipeline: PipelineHealth) => pipeline.worst === severity

function Row({ pipeline }: { pipeline: PipelineHealth }) {
  const run = pipeline.latest
  const metrics = run?.metrics

  return (
    <TableRow hover>
      <TableCell sx={{ width: 36 }}>
        <Health pipeline={pipeline} />
      </TableCell>
      <TableCell>
        <Link
          component={RouterLink}
          to={`/pipelines/${pipeline.pipelineId}`}
          sx={{ fontWeight: 500 }}
        >
          {pipeline.name}
        </Link>
        {pipeline.findings
          .filter((finding) => finding.severity !== 'INFO')
          .map((finding, index) => (
            <Typography
              key={index}
              variant="caption"
              sx={{ display: 'block', color: finding.severity === 'CRITICAL' ? 'error.main' : 'warning.main' }}
            >
              {finding.message}
            </Typography>
          ))}
      </TableCell>
      <TableCell>
        {run ? (
          <Stack direction="row" spacing={1} alignItems="center">
            <RunStateChip state={run.state} />
            <Link component={RouterLink} to={`/runs/${run.id}`} variant="caption">
              {new Date(run.createdAt).toLocaleString()}
            </Link>
          </Stack>
        ) : (
          // A watched pipeline that has never run is worth seeing, not hiding behind a dash.
          <Typography variant="caption" sx={{ color: muted }}>
            never run
          </Typography>
        )}
      </TableCell>
      <TableCell align="right" sx={tabular}>
        {metrics ? metrics.recordsRead.toLocaleString() : '—'}
      </TableCell>
      <TableCell align="right" sx={{ ...tabular, color: muted }}>
        {pipeline.typicalRows === null ? (
          // Said, rather than shown as a dash that reads like zero. A comparison from two runs is
          // not a comparison.
          <Tooltip title={`Only ${pipeline.baselineRuns} completed run(s) to compare against`}>
            <span>not yet</span>
          </Tooltip>
        ) : (
          <Tooltip title={`Median of the last ${pipeline.baselineRuns} completed runs`}>
            <span>{pipeline.typicalRows.toLocaleString()}</span>
          </Tooltip>
        )}
      </TableCell>
      <TableCell align="right" sx={{ ...tabular, color: 'success.main' }}>
        {metrics ? metrics.recordsWritten.toLocaleString() : '—'}
      </TableCell>
      <TableCell
        align="right"
        sx={{ ...tabular, color: metrics && metrics.recordsFailed > 0 ? 'error.main' : muted }}
      >
        {metrics ? metrics.recordsFailed.toLocaleString() : '—'}
      </TableCell>
      <TableCell align="right" sx={{ ...tabular, color: muted }}>
        {run?.durationSeconds != null ? formatSeconds(run.durationSeconds) : '—'}
      </TableCell>
    </TableRow>
  )
}

function Health({ pipeline }: { pipeline: PipelineHealth }) {
  const reasons = pipeline.findings.map((finding) => finding.message).join('\n')

  if (pipeline.worst === 'CRITICAL') {
    return (
      <Tooltip title={reasons}>
        <ErrorIcon fontSize="small" sx={{ color: 'error.main' }} />
      </Tooltip>
    )
  }
  if (pipeline.worst === 'WARNING') {
    return (
      <Tooltip title={reasons}>
        <WarningIcon fontSize="small" sx={{ color: 'warning.main' }} />
      </Tooltip>
    )
  }
  return (
    <Tooltip title={reasons || 'Ran, moved a normal amount, lost nothing'}>
      <CheckCircleIcon fontSize="small" sx={{ color: 'success.main' }} />
    </Tooltip>
  )
}

function formatSeconds(seconds: number) {
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  return `${(seconds / 3600).toFixed(1)}h`
}

/** A chip for the pipeline page, so somebody can see at a glance that a pipeline is watched. */
export function MonitoredChip({ monitored }: { monitored: boolean }) {
  if (!monitored) return null
  return <Chip size="small" label="Watched" variant="outlined" color="info" />
}
