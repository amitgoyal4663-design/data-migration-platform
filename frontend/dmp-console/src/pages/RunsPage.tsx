import Paper from '@mui/material/Paper'
import Table from '@mui/material/Table'
import Chip from '@mui/material/Chip'
import IconButton from '@mui/material/IconButton'
import Tooltip from '@mui/material/Tooltip'
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown'
import KeyboardArrowRightIcon from '@mui/icons-material/KeyboardArrowRight'
import { toChains } from '@/components/runChains'
import { RunChainRows, type ChainRowProps } from '@/components/RunChainRows'
import type { Run } from '@/api/types'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import ToggleButton from '@mui/material/ToggleButton'
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup'
import Typography from '@mui/material/Typography'
import HistoryIcon from '@mui/icons-material/HistoryOutlined'
import { useState } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import { useRuns } from '@/api/hooks'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState, ErrorPanel, Loading } from '@/components/Feedback'
import { RunStateChip } from '@/components/StateChip'
import { formatDuration } from '@/lib/duration'
import { RunParameters } from '@/components/RunParameters'
import { tabular } from '@/theme'
import type { RunState } from '@/api/types'
import { shortId } from '@/api/ids'

const FILTERS: { key: string; label: string; states?: RunState[] }[] = [
  { key: 'all', label: 'All' },
  { key: 'active', label: 'In flight', states: ['PREPARING', 'RUNNING', 'PAUSED', 'STOPPING', 'FINALIZING'] },
  { key: 'failed', label: 'Failed', states: ['FAILED'] },
  { key: 'done', label: 'Completed', states: ['COMPLETED'] },
]

export function RunsPage() {
  const [filter, setFilter] = useState('all')
  const active = FILTERS.find((entry) => entry.key === filter)
  const runs = useRuns({ state: active?.states })

  return (
    <>
      <PageHeader title="Runs" subtitle="Every execution, live and historical" />

      <ToggleButtonGroup
        size="small"
        value={filter}
        exclusive
        onChange={(_, next) => next && setFilter(next)}
        sx={{ mb: 2 }}
      >
        {FILTERS.map((entry) => (
          <ToggleButton key={entry.key} value={entry.key} sx={{ textTransform: 'none', px: 2 }}>
            {entry.label}
          </ToggleButton>
        ))}
      </ToggleButtonGroup>

      <ErrorPanel error={runs.error} />

      {runs.isLoading ? (
        <Loading />
      ) : (runs.data?.content.length ?? 0) === 0 ? (
        <EmptyState
          icon={<HistoryIcon />}
          title="No runs to show"
          description="Start a pipeline and its execution will appear here, updating live."
        />
      ) : (
        <Paper sx={{ overflow: 'hidden' }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell sx={{ width: 40 }} />
                <TableCell>RUN</TableCell>
                <TableCell>STATE</TableCell>
                <TableCell>TRIGGER</TableCell>
                <TableCell>COVERED</TableCell>
                <TableCell align="right">READ</TableCell>
                <TableCell align="right">WRITTEN</TableCell>
                <TableCell align="right">FAILED</TableCell>
                <TableCell align="right">CHUNKS</TableCell>
                <TableCell align="right">DURATION</TableCell>
                <TableCell>STARTED</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {toChains(runs.data?.content ?? []).map((chain) => (
                <RunChainRows
                  key={chain.root.id}
                  chain={chain}
                  render={(run, props) => <RunRow key={run.id} run={run} {...props} />}
                />
              ))}
            </TableBody>
          </Table>
        </Paper>
      )}
    </>
  )
}

/** One row of the runs table, whether it is a migration's first attempt or one of its resumes. */
function RunRow({ run, chain, expanded, onToggle }: ChainRowProps & { run: Run }) {
  // A chain's row stands for the migration, so it reports the latest attempt's state and the
  // whole chain's totals. Reporting the first attempt's STOPPED for a migration that later
  // finished answers a question nobody asked.
  const state = chain ? chain.latest.state : run.state
  const written = chain ? chain.totalWritten : run.metrics.recordsWritten
  const read = chain ? chain.totalRead : run.metrics.recordsRead

  return (
                <TableRow
                  key={run.id}
                  hover
                  component={RouterLink}
                  to={`/runs/${run.id}`}
                  sx={{ textDecoration: 'none', cursor: 'pointer' }}
                >
                  <TableCell sx={{ width: 40 }}>
                    {onToggle && (
                      <Tooltip title={expanded ? 'Hide attempts' : `${(chain?.attempts.length ?? 0) + 1} attempts`}>
                        <IconButton
                          size="small"
                          onClick={(event) => {
                            // The row is a link; expanding is not navigating.
                            event.preventDefault()
                            event.stopPropagation()
                            onToggle()
                          }}
                        >
                          {expanded ? (
                            <KeyboardArrowDownIcon fontSize="small" />
                          ) : (
                            <KeyboardArrowRightIcon fontSize="small" />
                          )}
                        </IconButton>
                      </Tooltip>
                    )}
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      v{run.versionNumber}
                      {chain && (
                        <Chip
                          size="small"
                          variant="outlined"
                          label={`${chain.attempts.length + 1} attempts`}
                          sx={{ ml: 1, height: 18, fontSize: 10 }}
                        />
                      )}
                    </Typography>
                    <Typography variant="caption">{shortId(run.id)}</Typography>
                  </TableCell>
                  <TableCell>
                    <RunStateChip state={state} />
                  </TableCell>
                  {/*
                    What each run actually covered. The run page has always shown this; the list
                    did not, which is where it matters most — a window nobody migrated is invisible
                    in a single run and obvious in a column of them.
                  */}
                  <TableCell>
                    <RunParameters parameters={run.parameters} />
                  </TableCell>
                  <TableCell align="right" sx={tabular}>
                    {read.toLocaleString()}
                  </TableCell>
                  <TableCell align="right" sx={tabular}>
                    {written.toLocaleString()}
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
                    {/*
                      A ratio only while the denominator is final. A lazily chunked run creates its
                      next chunk when the current one finds more data, so mid-flight its total is
                      merely what has been discovered so far — and "20/21" then reads as almost
                      finished when there may be five hundred chunks still to come. Once the run is
                      terminal the total is real and the ratio is worth showing.
                    */}
                    {run.terminal
                      ? `${run.metrics.chunksCompleted}/${run.metrics.chunksTotal}`
                      : `${run.metrics.chunksCompleted} done`}
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
  )
}


