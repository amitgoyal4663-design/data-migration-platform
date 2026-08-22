import { useMemo } from 'react'
import Box from '@mui/material/Box'
import FormControlLabel from '@mui/material/FormControlLabel'
import Radio from '@mui/material/Radio'
import RadioGroup from '@mui/material/RadioGroup'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import type { RateLimit } from '../api/types'

/**
 * What the client said they will accept, in the two units clients actually use.
 *
 * Both optional, because most clients give exactly one number. Leaving a row empty means no limit
 * on that unit — there is no switch to throw and no mode to choose, which is deliberate: a mode
 * selector would be more UI than the two boxes it guards, and would leave the client who quoted
 * both numbers unable to enter them.
 */

/** Periods a client is likely to have named. ISO-8601, because that is what the API stores. */
const PERIODS = [
  { value: 'PT1S', label: 'second' },
  { value: 'PT1M', label: 'minute' },
  { value: 'PT5M', label: '5 minutes' },
  { value: 'PT15M', label: '15 minutes' },
  { value: 'PT1H', label: 'hour' },
  { value: 'P1D', label: 'day' },
]

const EMPTY: RateLimit = {
  records: null,
  recordsWindow: null,
  calls: null,
  callsWindow: null,
  pacing: 'BURST',
}

export function RateLimitFields({
  value,
  onChange,
}: {
  value: RateLimit | null
  onChange: (next: RateLimit | null) => void
}) {
  const limit = value ?? EMPTY

  const set = (patch: Partial<RateLimit>) => {
    const next = { ...limit, ...patch }
    // An entirely empty form is "no limit", which the API expects as null rather than as an object
    // of nulls. Keeping those two the same state is what stops a connector from looking limited
    // while limiting nothing.
    const empty = !next.records && !next.calls
    onChange(empty ? null : next)
  }

  const summary = useMemo(() => describe(value), [value])

  return (
    <Box>
      <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
        What the far end allows
      </Typography>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1.5 }}>
        Leave blank if they never gave you a number. The budget belongs to this connection, so every
        pipeline using it shares one — and it is shared across workers, so scaling out does not
        multiply the rate they agreed to.
      </Typography>

      <Stack spacing={1.5}>
        <Row
          label="Records"
          amount={limit.records}
          window={limit.recordsWindow}
          placeholder="10000"
          onAmount={(records) => set({ records, recordsWindow: records ? limit.recordsWindow ?? 'PT5M' : null })}
          onWindow={(recordsWindow) => set({ recordsWindow })}
        />
        <Row
          label="Calls"
          amount={limit.calls}
          window={limit.callsWindow}
          placeholder="100"
          onAmount={(calls) => set({ calls, callsWindow: calls ? limit.callsWindow ?? 'PT1M' : null })}
          onWindow={(callsWindow) => set({ callsWindow })}
        />
      </Stack>

      {(limit.records || limit.calls) && (
        <Box sx={{ mt: 2 }}>
          <Typography variant="body2" sx={{ mb: 0.5 }}>
            How they count it
          </Typography>
          <RadioGroup
            value={limit.pacing ?? 'BURST'}
            onChange={(event) => set({ pacing: event.target.value as 'BURST' | 'EVEN' })}
          >
            <FormControlLabel
              value="BURST"
              control={<Radio size="small" />}
              label={
                <Box>
                  <Typography variant="body2">The window resets</Typography>
                  <Typography variant="caption" color="text.secondary">
                    Send a whole window at once, then wait for the next. What most clients mean.
                  </Typography>
                </Box>
              }
            />
            <FormControlLabel
              value="EVEN"
              control={<Radio size="small" />}
              label={
                <Box>
                  <Typography variant="body2">They count the last {periodLabel(limit.recordsWindow ?? limit.callsWindow)}, continuously</Typography>
                  <Typography variant="caption" color="text.secondary">
                    Never exceeds the limit in any window. Slower, and the smaller the chunk the
                    less it costs — a chunk of a tenth of the limit keeps about ninety percent of
                    the rate.
                  </Typography>
                </Box>
              }
            />
          </RadioGroup>
        </Box>
      )}

      {summary && (
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1.5 }}>
          {summary}
        </Typography>
      )}
    </Box>
  )
}

function Row({
  label,
  amount,
  window,
  placeholder,
  onAmount,
  onWindow,
}: {
  label: string
  amount: number | null
  window: string | null
  placeholder: string
  onAmount: (value: number | null) => void
  onWindow: (value: string) => void
}) {
  return (
    <Stack direction="row" spacing={1} alignItems="center">
      <TextField
        label={label}
        value={amount ?? ''}
        onChange={(event) => {
          const raw = event.target.value.trim()
          const parsed = Number(raw)
          onAmount(raw === '' || Number.isNaN(parsed) || parsed <= 0 ? null : Math.floor(parsed))
        }}
        placeholder={placeholder}
        size="small"
        sx={{ flex: 1 }}
        inputProps={{ inputMode: 'numeric' }}
      />
      <Typography variant="body2" color="text.secondary">
        per
      </Typography>
      <TextField
        select
        value={window ?? ''}
        onChange={(event) => onWindow(event.target.value)}
        size="small"
        // Meaningless without an amount, and an enabled dropdown next to an empty box invites
        // somebody to set a period and believe they have set a limit.
        disabled={!amount}
        sx={{ width: 140 }}
      >
        {PERIODS.map((period) => (
          <MenuItem key={period.value} value={period.value}>
            {period.label}
          </MenuItem>
        ))}
      </TextField>
    </Stack>
  )
}

/** Reads the limit back in the shape a person would say it, as a check on what was typed. */
function describe(limit: RateLimit | null): string | null {
  if (!limit || (!limit.records && !limit.calls)) {
    return 'No limit — work goes as fast as the connection allows.'
  }
  const parts: string[] = []
  if (limit.records) parts.push(`${limit.records.toLocaleString()} records`)
  if (limit.calls) parts.push(`${limit.calls.toLocaleString()} calls`)

  const windows = new Set([limit.recordsWindow, limit.callsWindow].filter(Boolean))
  if (windows.size === 1) {
    return `At most ${parts.join(' and ')} per ${periodLabel([...windows][0] as string)}.`
  }
  const spoken = [
    limit.records ? `${limit.records.toLocaleString()} records per ${periodLabel(limit.recordsWindow)}` : null,
    limit.calls ? `${limit.calls.toLocaleString()} calls per ${periodLabel(limit.callsWindow)}` : null,
  ].filter(Boolean)
  return `At most ${spoken.join(', and ')}. Whichever runs out first is what the run waits on.`
}

function periodLabel(value: string | null): string {
  return PERIODS.find((period) => period.value === value)?.label ?? value ?? ''
}
