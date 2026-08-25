import Box from '@mui/material/Box'
import Link from '@mui/material/Link'
import LinearProgress from '@mui/material/LinearProgress'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import { Link as RouterLink } from 'react-router-dom'
import type { PipelineHealth } from '@/api/types'
import { muted, tabular } from '@/theme'

/**
 * How much data moved, by pipeline, across the whole window.
 *
 * <p>The support view answers "is it broken". This one answers the other question people bring to
 * the same screen — *did the policies migrate this week* — and no single run can answer it: seven
 * nightly runs are seven numbers, and the useful figure is their sum next to how many of them
 * finished.
 *
 * <p>A table rather than cards, because this view is read by comparing rows. Written against read
 * is the column that matters: they are equal on a healthy job, and the gap between them is
 * precisely the data that did not arrive.
 */
export function OperationsProduct({ pipelines }: { pipelines: PipelineHealth[] }) {
  if (pipelines.length === 0) {
    return (
      <Paper variant="outlined" sx={{ p: 3 }}>
        <Typography variant="body2" sx={{ color: muted }}>
          Nothing matches these filters.
        </Typography>
      </Paper>
    )
  }

  return (
    <Paper sx={{ overflow: 'auto' }}>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Pipeline</TableCell>
            <TableCell align="right">Runs</TableCell>
            <TableCell align="right">Finished</TableCell>
            <TableCell align="right">Read</TableCell>
            <TableCell align="right">Transferred</TableCell>
            <TableCell align="right">Did not arrive</TableCell>
            <TableCell sx={{ width: 160 }}>Delivered</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {pipelines.map((pipeline) => (
            <Row key={pipeline.pipelineId} pipeline={pipeline} />
          ))}
        </TableBody>
      </Table>
    </Paper>
  )
}

function Row({ pipeline }: { pipeline: PipelineHealth }) {
  const { volume } = pipeline
  // Of what was read, what reached the destination. The bar is this rather than the success rate
  // of runs: a run that "succeeded" having dropped a thousand records is the failure this screen
  // exists to make visible, and counting runs would call it a hundred per cent.
  const delivered = volume.read === 0 ? null : volume.written / volume.read
  const lost = volume.read - volume.written

  return (
    <TableRow hover>
      <TableCell>
        <Link component={RouterLink} to={`/pipelines/${pipeline.pipelineId}`}>
          {pipeline.name}
        </Link>
        {!pipeline.watched && (
          <Typography variant="caption" sx={{ color: muted, ml: 1 }}>
            not watched
          </Typography>
        )}
      </TableCell>
      <TableCell align="right" sx={tabular}>
        {volume.runs.toLocaleString()}
      </TableCell>
      <TableCell align="right" sx={tabular}>
        {volume.runs === 0 ? (
          <Typography variant="caption" sx={{ color: muted }}>
            —
          </Typography>
        ) : (
          <Tooltip title={`${volume.completed} completed, ${volume.failed} failed`}>
            <span>
              {volume.completed}
              {volume.failed > 0 && (
                <Typography component="span" variant="caption" sx={{ color: 'error.main', ml: 0.5 }}>
                  +{volume.failed} failed
                </Typography>
              )}
            </span>
          </Tooltip>
        )}
      </TableCell>
      <TableCell align="right" sx={tabular}>
        {volume.read.toLocaleString()}
      </TableCell>
      <TableCell align="right" sx={tabular}>
        {volume.written.toLocaleString()}
      </TableCell>
      <TableCell align="right" sx={tabular}>
        {lost > 0 ? (
          <Typography variant="body2" sx={{ ...tabular, color: 'error.main' }}>
            {lost.toLocaleString()}
          </Typography>
        ) : (
          <Typography variant="caption" sx={{ color: muted }}>
            —
          </Typography>
        )}
      </TableCell>
      <TableCell>
        {delivered === null ? (
          <Typography variant="caption" sx={{ color: muted }}>
            nothing read
          </Typography>
        ) : (
          <Stack spacing={0.25}>
            <LinearProgress
              variant="determinate"
              value={Math.min(100, delivered * 100)}
              color={delivered >= 0.999 ? 'success' : delivered >= 0.95 ? 'warning' : 'error'}
              sx={{ height: 6, borderRadius: 1 }}
            />
            <Typography variant="caption" sx={{ ...tabular, color: muted }}>
              {(delivered * 100).toFixed(delivered >= 0.999 ? 0 : 1)}%
            </Typography>
          </Stack>
        )}
      </TableCell>
    </TableRow>
  )
}

/** The window's totals, summed from the rows on screen rather than fetched separately. */
export function ProductTotals({ pipelines }: { pipelines: PipelineHealth[] }) {
  const read = pipelines.reduce((sum, pipeline) => sum + pipeline.volume.read, 0)
  const written = pipelines.reduce((sum, pipeline) => sum + pipeline.volume.written, 0)
  const runs = pipelines.reduce((sum, pipeline) => sum + pipeline.volume.runs, 0)
  const failed = pipelines.reduce((sum, pipeline) => sum + pipeline.volume.failed, 0)

  return (
    <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', mb: 2 }}>
      <Figure label="Runs" value={runs.toLocaleString()} />
      <Figure label="Failed runs" value={failed.toLocaleString()} tone={failed > 0 ? 'error.main' : undefined} />
      <Figure label="Records read" value={read.toLocaleString()} />
      <Figure label="Transferred" value={written.toLocaleString()} />
      <Figure
        label="Did not arrive"
        value={(read - written).toLocaleString()}
        tone={read - written > 0 ? 'error.main' : undefined}
      />
    </Box>
  )
}

function Figure({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <Paper variant="outlined" sx={{ px: 2, py: 1.5, flex: '1 1 150px' }}>
      <Typography variant="caption" sx={{ color: muted, letterSpacing: '0.06em' }}>
        {label.toUpperCase()}
      </Typography>
      <Typography variant="h2" sx={{ ...tabular, color: tone, mt: 0.5 }}>
        {value}
      </Typography>
    </Paper>
  )
}
