import { useState } from 'react'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'

/**
 * When to look, in the shape people already know from a log tool.
 *
 * <p>A support desk works from a ticket that says "yesterday afternoon", so the quick ranges are
 * the answer almost every time. The custom range exists for the case they are reconciling a
 * specific migration window, and it is deliberately behind the quick ones rather than beside them:
 * two date pickers on screen for a question usually answered by "last 24 hours" is three clicks
 * charged to every search to serve one.
 *
 * <p>An unbounded search is offered and is not the default. Without a bound, a busy index answers
 * the least useful version of the question — every entry ever written, newest first.
 */

export interface TimeWindow {
  after?: string
  before?: string
}

/** Minutes back from now. Zero means no lower bound at all. */
const QUICK: { label: string; minutes: number }[] = [
  { label: 'Last 15 minutes', minutes: 15 },
  { label: 'Last hour', minutes: 60 },
  { label: 'Last 24 hours', minutes: 60 * 24 },
  { label: 'Last 7 days', minutes: 60 * 24 * 7 },
  { label: 'Last 30 days', minutes: 60 * 24 * 30 },
  { label: 'Any time', minutes: 0 },
]

const CUSTOM = 'custom'

export function TimeRange({ onChange }: { onChange: (next: TimeWindow) => void }) {
  const [choice, setChoice] = useState<string>(String(60 * 24 * 7))
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')

  const applyQuick = (minutes: number) => {
    onChange(
      minutes === 0
        ? {}
        : { after: new Date(Date.now() - minutes * 60_000).toISOString() },
    )
  }

  const applyCustom = (nextFrom: string, nextTo: string) => {
    onChange({
      // datetime-local has no zone, so it is read as the browser's — which is the zone the person
      // typing it is thinking in. Converting to an instant here keeps the API on UTC throughout.
      after: nextFrom ? new Date(nextFrom).toISOString() : undefined,
      before: nextTo ? new Date(nextTo).toISOString() : undefined,
    })
  }

  return (
    <Stack direction="row" spacing={1}>
      <TextField
        select
        size="small"
        label="When"
        value={choice}
        onChange={(event) => {
          const next = event.target.value
          setChoice(next)
          if (next === CUSTOM) applyCustom(from, to)
          else applyQuick(Number(next))
        }}
        sx={{ minWidth: 160 }}
      >
        {QUICK.map((entry) => (
          <MenuItem key={entry.label} value={String(entry.minutes)}>
            {entry.label}
          </MenuItem>
        ))}
        <MenuItem value={CUSTOM}>Custom range…</MenuItem>
      </TextField>

      {choice === CUSTOM && (
        <>
          <TextField
            type="datetime-local"
            size="small"
            label="From"
            value={from}
            onChange={(event) => {
              setFrom(event.target.value)
              applyCustom(event.target.value, to)
            }}
            InputLabelProps={{ shrink: true }}
          />
          <TextField
            type="datetime-local"
            size="small"
            label="To"
            value={to}
            onChange={(event) => {
              setTo(event.target.value)
              applyCustom(from, event.target.value)
            }}
            InputLabelProps={{ shrink: true }}
          />
        </>
      )}
    </Stack>
  )
}
