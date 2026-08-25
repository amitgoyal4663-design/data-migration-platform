import RefreshIcon from '@mui/icons-material/Refresh'
import SearchIcon from '@mui/icons-material/Search'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import FormControlLabel from '@mui/material/FormControlLabel'
import InputAdornment from '@mui/material/InputAdornment'
import MenuItem from '@mui/material/MenuItem'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Switch from '@mui/material/Switch'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import { muted } from '@/theme'
import type { PipelineHealth } from '@/api/types'

/**
 * What the screen is showing, and how to change it.
 *
 * <p>Every control here answers a question somebody asked out loud in front of the old screen —
 * *only the last day?*, *where are the ones nobody watches?*, *just show me the broken ones*, *stop
 * moving while I read this*. A dashboard that cannot answer those is a poster: correct, and of no
 * use to the person standing in front of it.
 *
 * <p>The state lives in the URL, so a filtered view is a link. Half of support work is one person
 * showing another what they are looking at, and "sort by failures, last 7 days, all pipelines" is
 * not something anybody wants to say out loud.
 */
export interface OperationsFilters {
  hours: number
  watched: boolean
  search: string
  status: StatusFilter
  sort: SortOrder
  live: boolean
}

export type StatusFilter = 'all' | 'attention' | 'failing' | 'healthy' | 'never'
export type SortOrder = 'worst' | 'name' | 'records' | 'failures' | 'slowest'

const WINDOWS = [
  { hours: 6, label: 'Last 6 hours' },
  { hours: 24, label: 'Last 24 hours' },
  { hours: 24 * 7, label: 'Last 7 days' },
  { hours: 24 * 30, label: 'Last 30 days' },
]

const STATUSES: { value: StatusFilter; label: string }[] = [
  { value: 'all', label: 'Any state' },
  { value: 'attention', label: 'Needs attention' },
  { value: 'failing', label: 'Last run failed' },
  { value: 'healthy', label: 'Healthy' },
  { value: 'never', label: 'Never run' },
]

const SORTS: { value: SortOrder; label: string }[] = [
  { value: 'worst', label: 'Worst first' },
  { value: 'name', label: 'Name' },
  { value: 'records', label: 'Most records' },
  { value: 'failures', label: 'Most failures' },
  { value: 'slowest', label: 'Slowest' },
]

export function OperationsControls({
  filters,
  onChange,
  onRefresh,
  refreshing,
  showing,
  total,
}: {
  filters: OperationsFilters
  onChange: (change: Partial<OperationsFilters>) => void
  onRefresh: () => void
  refreshing: boolean
  showing: number
  total: number
}) {
  return (
    <Paper variant="outlined" sx={{ p: 1.5, mb: 2 }}>
      <Stack direction="row" spacing={1.5} flexWrap="wrap" useFlexGap alignItems="center">
        <TextField
          select
          size="small"
          label="Window"
          value={filters.hours}
          onChange={(event) => onChange({ hours: Number(event.target.value) })}
          sx={{ minWidth: 150 }}
        >
          {WINDOWS.map((window) => (
            <MenuItem key={window.hours} value={window.hours}>
              {window.label}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          select
          size="small"
          label="Pipelines"
          value={filters.watched ? 'watched' : 'all'}
          onChange={(event) => onChange({ watched: event.target.value === 'watched' })}
          sx={{ minWidth: 150 }}
        >
          <MenuItem value="watched">Watchlist</MenuItem>
          <MenuItem value="all">Every published</MenuItem>
        </TextField>

        <TextField
          select
          size="small"
          label="State"
          value={filters.status}
          onChange={(event) => onChange({ status: event.target.value as StatusFilter })}
          sx={{ minWidth: 160 }}
        >
          {STATUSES.map((status) => (
            <MenuItem key={status.value} value={status.value}>
              {status.label}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          select
          size="small"
          label="Sort"
          value={filters.sort}
          onChange={(event) => onChange({ sort: event.target.value as SortOrder })}
          sx={{ minWidth: 140 }}
        >
          {SORTS.map((sort) => (
            <MenuItem key={sort.value} value={sort.value}>
              {sort.label}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          size="small"
          placeholder="Job name, or a failure reason"
          value={filters.search}
          onChange={(event) => onChange({ search: event.target.value })}
          sx={{ minWidth: 260, flexGrow: 1 }}
          slotProps={{
            input: {
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" sx={{ color: muted }} />
                </InputAdornment>
              ),
            },
          }}
        />

        <Box sx={{ flexGrow: 1 }} />

        <FormControlLabel
          control={
            <Switch
              size="small"
              checked={filters.live}
              onChange={(event) => onChange({ live: event.target.checked })}
            />
          }
          label={
            <Typography variant="body2" sx={{ color: muted }}>
              Live
            </Typography>
          }
        />
        <Button size="small" startIcon={<RefreshIcon />} onClick={onRefresh} disabled={refreshing}>
          Refresh
        </Button>
      </Stack>

      {showing !== total && (
        <Typography variant="caption" sx={{ color: muted, mt: 1, display: 'block' }}>
          Showing {showing} of {total}. The totals above describe the whole window, not this
          filter — a figure that moved when somebody typed in a search box would mean nothing.
        </Typography>
      )}
    </Paper>
  )
}

/** Applies the search, the state filter and the ordering, in that order. */
export function applyFilters(
  pipelines: PipelineHealth[],
  filters: OperationsFilters,
): PipelineHealth[] {
  const needle = filters.search.trim().toLowerCase()

  const matched = pipelines.filter((pipeline) => {
    if (needle) {
      // Searched across the failure reasons too, because "who else is failing on
      // Policy_Number__c" is a question about a message and not about a job name.
      const haystack = [
        pipeline.name,
        ...pipeline.reasons.map((reason) => `${reason.code} ${reason.reason}`),
        ...pipeline.findings.map((finding) => finding.message),
      ]
        .join(' ')
        .toLowerCase()
      if (!haystack.includes(needle)) {
        return false
      }
    }

    switch (filters.status) {
      case 'attention':
        return !pipeline.healthy
      case 'failing':
        return pipeline.latest?.state === 'FAILED'
      case 'healthy':
        return pipeline.healthy && pipeline.latest !== null
      case 'never':
        return pipeline.latest === null
      default:
        return true
    }
  })

  const severity = (pipeline: PipelineHealth) =>
    pipeline.worst === 'CRITICAL' ? 2 : pipeline.worst === 'WARNING' ? 1 : 0

  return [...matched].sort((left, right) => {
    switch (filters.sort) {
      case 'name':
        return left.name.localeCompare(right.name)
      case 'records':
        return right.volume.read - left.volume.read
      case 'failures':
        return right.volume.recordsFailed - left.volume.recordsFailed
      case 'slowest':
        return right.volume.seconds - left.volume.seconds
      default:
        return severity(right) - severity(left) || left.name.localeCompare(right.name)
    }
  })
}
