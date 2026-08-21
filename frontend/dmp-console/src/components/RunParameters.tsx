import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import { muted, tabular } from '@/theme'

/**
 * The window a run covered, short enough for a table cell.
 *
 * <p>A run's parameters are what make a list of runs a coverage log rather than a list of events.
 * Whether last Tuesday was migrated is a question about one run; whether <em>every</em> day was is
 * a question about the column, and it can only be answered if each row says what it took.
 *
 * <p>Timestamps are trimmed to what distinguishes one run's window from the next. The seconds and
 * the offset are identical on every row of a daily schedule, so showing them costs the width that
 * would otherwise show the value — and the full text is one hover away.
 */
export function RunParameters({
  parameters,
}: {
  parameters: Record<string, unknown> | null | undefined
}) {
  const entries = Object.entries(parameters ?? {})

  if (entries.length === 0) {
    return (
      <Typography variant="caption" sx={{ color: muted }}>
        —
      </Typography>
    )
  }

  const full = entries.map(([name, value]) => `${name}: ${String(value)}`).join('\n')
  const short = entries.map(([, value]) => shorten(String(value))).join(' → ')

  return (
    <Tooltip title={<span style={{ whiteSpace: 'pre-line' }}>{full}</span>}>
      <Typography variant="caption" sx={{ ...tabular, color: muted, whiteSpace: 'nowrap' }}>
        {short}
      </Typography>
    </Tooltip>
  )
}

/**
 * An ISO timestamp as the part that varies between runs.
 *
 * <p>Anything that does not parse as one is left alone: a parameter may be an id, a status or a
 * region, and truncating those would hide the whole value rather than its noise.
 */
function shorten(value: string): string {
  const match = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})/.exec(value)
  const date = match?.[1]
  const time = match?.[2]
  if (!date || !time) {
    return value.length > 24 ? `${value.slice(0, 23)}…` : value
  }
  // Midnight is the common case for a daily window and says nothing; a time that is not midnight
  // is the whole point of the row.
  return time === '00:00' ? date : `${date} ${time}`
}
