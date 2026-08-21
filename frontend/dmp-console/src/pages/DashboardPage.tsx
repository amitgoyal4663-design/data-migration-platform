import Grid from '@mui/material/Grid2'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import LinearProgress from '@mui/material/LinearProgress'
import Button from '@mui/material/Button'
import AccountTreeIcon from '@mui/icons-material/AccountTreeOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { usePipelines, useRuns } from '@/api/hooks'
import { PageHeader } from '@/components/PageHeader'
import { StatTile } from '@/components/StatTile'
import { RunStateChip } from '@/components/StateChip'
import { EmptyState, ErrorPanel, Loading } from '@/components/Feedback'
import { muted, tabular } from '@/theme'

export function DashboardPage() {
  const pipelines = usePipelines()
  const runs = useRuns()

  if (pipelines.isLoading || runs.isLoading) return <Loading label="Loading" />
  if (pipelines.error) return <ErrorPanel error={pipelines.error} />
  if (runs.error) return <ErrorPanel error={runs.error} />

  const allRuns = runs.data?.content ?? []
  const active = allRuns.filter((run) => run.active)

  // Summed across runs, which is not the same as distinct records migrated: a pipeline run
  // twice writes the same records twice, and an idempotent sink collapses them into one row.
  // The label has to say so, or this reads as "we moved 40,000 things" when 20,000 exist.
  const recordsWritten = allRuns.reduce((sum, run) => sum + run.metrics.recordsWritten, 0)
  const failedRuns = allRuns.filter((run) => run.state === 'FAILED').length
  // Records read but neither written nor rejected. Anything above zero means rows went missing,
  // which deserves to be visible on the front page rather than buried in a run detail.
  const unaccounted = allRuns.reduce((sum, run) => sum + run.metrics.unaccountedRecords, 0)

  return (
    <>
      <PageHeader
        title="Dashboard"
        subtitle="What is running right now, and what recently finished"
      />

      <Grid container spacing={2}>
        <Grid size={{ xs: 6, md: 3 }}>
          <StatTile label="Active runs" value={active.length} emphasis />
        </Grid>
        <Grid size={{ xs: 6, md: 3 }}>
          <StatTile
            label="Records written"
            value={recordsWritten}
            unit={allRuns.length === 1 ? 'in 1 run' : `in ${allRuns.length} runs`}
            hint={
              'Total write operations across the runs listed below — not distinct records. ' +
              'Running the same pipeline twice counts its records twice, even though an ' +
              'idempotent destination stores them once.'
            }
          />
        </Grid>
        <Grid size={{ xs: 6, md: 3 }}>
          <StatTile
            label="Failed runs"
            value={failedRuns}
            tone={failedRuns > 0 ? 'critical' : 'default'}
          />
        </Grid>
        <Grid size={{ xs: 6, md: 3 }}>
          <StatTile
            label="Unaccounted records"
            value={unaccounted}
            tone={unaccounted > 0 ? 'critical' : 'good'}
            hint="Records read but neither written nor rejected. Anything other than zero means rows went missing."
          />
        </Grid>
      </Grid>

      <Typography variant="h2" sx={{ mt: 5, mb: 2 }}>
        In flight
      </Typography>

      {active.length === 0 ? (
        <EmptyState
          icon={<AccountTreeIcon />}
          title="Nothing running"
          description={
            pipelines.data?.content.length
              ? 'Start a run from a pipeline to see it here.'
              : 'Create a connector and a pipeline to move some data.'
          }
          action={
            <Button component={RouterLink} to="/pipelines" variant="contained">
              Go to pipelines
            </Button>
          }
        />
      ) : (
        <Paper sx={{ overflow: 'hidden' }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>RUN</TableCell>
                <TableCell>STATE</TableCell>
                <TableCell sx={{ width: 220 }}>PROGRESS</TableCell>
                <TableCell align="right">READ</TableCell>
                <TableCell align="right">WRITTEN</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {active.map((run) => (
                <TableRow
                  key={run.id}
                  hover
                  component={RouterLink}
                  to={`/runs/${run.id}`}
                  sx={{ textDecoration: 'none', cursor: 'pointer' }}
                >
                  <TableCell>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      v{run.versionNumber}
                    </Typography>
                    <Typography variant="caption">{run.id.slice(0, 8)}</Typography>
                  </TableCell>
                  <TableCell>
                    <RunStateChip state={run.state} />
                  </TableCell>
                  <TableCell>
                    <ProgressBar run={run} />
                  </TableCell>
                  <TableCell align="right" sx={tabular}>
                    {run.metrics.recordsRead.toLocaleString()}
                  </TableCell>
                  <TableCell align="right" sx={tabular}>
                    {run.metrics.recordsWritten.toLocaleString()}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Paper>
      )}

      <Typography variant="h2" sx={{ mt: 5, mb: 2 }}>
        Recent runs
      </Typography>
      <RecentRuns runs={allRuns.slice(0, 10)} />
    </>
  )
}

function ProgressBar({ run }: { run: { progress: number | null; waitingOnExternalSystem: boolean } }) {
  if (run.waitingOnExternalSystem) {
    return (
      <Stack spacing={0.5}>
        <LinearProgress sx={{ height: 6, borderRadius: 3 }} />
        <Typography variant="caption">Waiting on the source system</Typography>
      </Stack>
    )
  }
  if (run.progress === null) {
    return <Typography variant="caption">Planning</Typography>
  }
  return (
    <Stack spacing={0.5}>
      <LinearProgress
        variant="determinate"
        value={run.progress * 100}
        sx={{ height: 6, borderRadius: 3 }}
      />
      <Typography variant="caption" sx={tabular}>
        {Math.round(run.progress * 100)}%
      </Typography>
    </Stack>
  )
}

function RecentRuns({ runs }: { runs: import('@/api/types').Run[] }) {
  if (runs.length === 0) {
    return (
      <Typography variant="body2" sx={{ color: muted }}>
        No runs yet.
      </Typography>
    )
  }

  return (
    <Paper sx={{ overflow: 'hidden' }}>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>RUN</TableCell>
            <TableCell>STATE</TableCell>
            <TableCell align="right">WRITTEN</TableCell>
            <TableCell align="right">FAILED</TableCell>
            <TableCell align="right">DURATION</TableCell>
            <TableCell>STARTED</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {runs.map((run) => (
            <TableRow
              key={run.id}
              hover
              component={RouterLink}
              to={`/runs/${run.id}`}
              sx={{ textDecoration: 'none', cursor: 'pointer' }}
            >
              <TableCell>
                <Typography variant="body2">v{run.versionNumber}</Typography>
                <Typography variant="caption">{run.id.slice(0, 8)}</Typography>
              </TableCell>
              <TableCell>
                <RunStateChip state={run.state} />
              </TableCell>
              <TableCell align="right" sx={tabular}>
                {run.metrics.recordsWritten.toLocaleString()}
              </TableCell>
              <TableCell align="right" sx={tabular}>
                {run.metrics.recordsFailed > 0 ? (
                  <Typography variant="body2" color="error.main" sx={tabular}>
                    {run.metrics.recordsFailed.toLocaleString()}
                  </Typography>
                ) : (
                  '—'
                )}
              </TableCell>
              <TableCell align="right" sx={tabular}>
                {run.durationSeconds === null ? '—' : formatDuration(run.durationSeconds)}
              </TableCell>
              <TableCell>
                <Typography variant="caption">
                  {run.startedAt ? new Date(run.startedAt).toLocaleString() : '—'}
                </Typography>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Paper>
  )
}

export function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`
}
