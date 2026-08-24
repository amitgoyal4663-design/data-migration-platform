import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import { useState } from 'react'
import { muted, tabular } from '@/theme'

/**
 * Which records this run was asked for: the query it used, and the values it was given.
 *
 * <p>Its own panel rather than a line beside the state, because the values are the answer to the
 * question people bring to a finished run — *was POL-44219 in this one?* A run started with two
 * hundred policy numbers has two hundred values worth keeping, and a subtitle can hold neither
 * them nor the question.
 *
 * <p>Long lists are folded rather than trimmed. A list cut off at ten with no count reads as a run
 * that covered ten, and the difference between "ten" and "the first ten of two hundred" is the
 * whole reason somebody is looking.
 */
export function RunSelection({
  query,
  parameters,
}: {
  query: string | null | undefined
  parameters: Record<string, unknown> | null | undefined
}) {
  const entries = Object.entries(parameters ?? {})

  if (!query && entries.length === 0) {
    return null
  }

  return (
    <Paper variant="outlined" sx={{ p: 2, mb: 3 }}>
      <Typography variant="h3" sx={{ mb: 1.5 }}>
        What this run selected
      </Typography>

      <Stack spacing={1.5}>
        {query && (
          <Row name="Query">
            <Typography variant="body2">{query}</Typography>
          </Row>
        )}
        {entries.map(([name, value]) => (
          <Row key={name} name={name}>
            <Value value={value} />
          </Row>
        ))}
        {entries.length === 0 && (
          <Typography variant="body2" sx={{ color: muted }}>
            This query takes no values — it selects the same records every run.
          </Typography>
        )}
      </Stack>
    </Paper>
  )
}

function Row({ name, children }: { name: string; children: React.ReactNode }) {
  return (
    <Stack direction={{ xs: 'column', sm: 'row' }} spacing={{ xs: 0.25, sm: 2 }}>
      <Typography
        variant="body2"
        sx={{ color: muted, minWidth: 140, flexShrink: 0, pt: 0.25 }}
      >
        {name}
      </Typography>
      <Box sx={{ minWidth: 0, flexGrow: 1 }}>{children}</Box>
    </Stack>
  )
}

/** How many of a list are shown before it folds. Enough to recognise, few enough to scan. */
const SHOWN = 12

function Value({ value }: { value: unknown }) {
  const [expanded, setExpanded] = useState(false)
  const [copied, setCopied] = useState(false)

  if (!Array.isArray(value)) {
    return (
      <Typography variant="body2" sx={{ ...tabular, wordBreak: 'break-word' }}>
        {value === null || value === undefined || value === '' ? '—' : String(value)}
      </Typography>
    )
  }

  const values = value.map((entry) => String(entry))
  const shown = expanded ? values : values.slice(0, SHOWN)
  const hidden = values.length - shown.length

  const copy = () => {
    // One per line, which is the shape the Run dialog takes them in — so the answer to "run those
    // same ones again" is a copy and a paste rather than a transcription.
    void navigator.clipboard?.writeText(values.join('\n')).then(
      () => setCopied(true),
      () => setCopied(false),
    )
  }

  return (
    <Stack spacing={0.75} sx={{ minWidth: 0 }}>
      <Stack direction="row" spacing={1} alignItems="center">
        <Typography variant="body2" sx={tabular}>
          {values.length.toLocaleString()} value{values.length === 1 ? '' : 's'}
        </Typography>
        <Button size="small" startIcon={<ContentCopyIcon fontSize="small" />} onClick={copy}>
          {copied ? 'Copied' : 'Copy'}
        </Button>
      </Stack>

      <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
        {shown.map((entry, index) => (
          <Chip key={`${entry}-${index}`} label={entry} size="small" variant="outlined" />
        ))}
        {hidden > 0 && (
          <Chip
            label={`+${hidden.toLocaleString()} more`}
            size="small"
            onClick={() => setExpanded(true)}
          />
        )}
        {expanded && values.length > SHOWN && (
          <Chip label="Show fewer" size="small" onClick={() => setExpanded(false)} />
        )}
      </Stack>
    </Stack>
  )
}
