import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogTitle from '@mui/material/DialogTitle'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import { useEffect, useState } from 'react'
import { useRunParameters, useRunQueries } from '@/api/hooks'
import { muted } from '@/theme'

/**
 * What a run needs before it can start: which query finds the records, and the values it wants.
 *
 * <p>The query picker is the part worth explaining. A connector may declare several named ways to
 * select records — "By date range" for the nightly load, "By policy number" for a support desk
 * holding one policy and no idea when it was last touched. Choosing one changes which boxes appear,
 * because each declares its own placeholders.
 *
 * <p>Support never writes a query. They pick a name somebody else wrote and fill in a box, which is
 * the whole distance between a safe operation and an arbitrary query tool.
 */
export function RunDialog({
  open,
  pipelineId,
  pending,
  dryRun,
  onCancel,
  onStart,
}: {
  open: boolean
  pipelineId: string
  pending: boolean
  /**
   * Rehearsing rather than delivering. Said in the dialog, because the button that opened it and
   * the button that confirms it are on different screens by the time somebody reads them.
   */
  dryRun?: boolean
  onCancel: () => void
  onStart: (parameters: Record<string, unknown>, query: string | null) => void
}) {
  const queries = useRunQueries(open ? pipelineId : undefined)
  const [query, setQuery] = useState<string>('')
  const parameters = useRunParameters(open ? pipelineId : undefined, query || undefined)

  const names = parameters.data?.names ?? []
  const lists = parameters.data?.lists ?? []
  const options = queries.data?.names ?? []

  const [values, setValues] = useState<Record<string, string>>({})

  // Cleared when the query changes, and deliberately: the boxes are different boxes. Carrying a
  // value across would leave ":from" filled in on a query that has no from, and the run would be
  // started with a parameter nothing binds.
  useEffect(() => {
    setValues({})
  }, [query])

  // The first named query, because a connector's author put it first — and because a dialog whose
  // picker opens on nothing makes somebody choose before they can see what the choice does.
  useEffect(() => {
    if (open && options.length > 0 && !query) {
      setQuery(options[0] ?? '')
    }
  }, [open, options, query])

  // Every placeholder must have a value: the query cannot run with one missing, and the backend
  // refuses by name rather than quietly reading the whole table.
  const incomplete = names.some((name) => (values[name] ?? '').trim() === '')

  const start = () => {
    const bound: Record<string, unknown> = {}
    for (const name of names) {
      const raw = (values[name] ?? '').trim()
      bound[name] = lists.includes(name)
        ? // One per line or comma-separated, whichever somebody pasted. Sent as an array because
          // the query uses it inside IN (…) — a single string there matches one literal value
          // containing commas, and matches nothing.
          raw
            .split(/[\n,]/)
            .map((entry) => entry.trim())
            .filter(Boolean)
        : raw
    }
    onStart(bound, query || null)
  }

  return (
    <Dialog open={open} onClose={onCancel} maxWidth="sm" fullWidth>
      <DialogTitle>{dryRun ? 'Start dry run' : 'Start run'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2.5} sx={{ mt: 0.5 }}>
          {options.length > 0 && (
            <TextField
              select
              label="Find records"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              size="small"
              fullWidth
              helperText="Each way of finding records asks for different values"
            >
              {options.map((option) => (
                <MenuItem key={option} value={option}>
                  {option}
                </MenuItem>
              ))}
            </TextField>
          )}

          {dryRun && (
            <Typography variant="body2" sx={{ color: 'warning.main' }}>
              Nothing will be written. The source is read in full and every script runs, but the
              destination is never opened — so this cannot tell you it would have accepted the
              records, only what would have been sent and which never got that far.
            </Typography>
          )}

          {names.length === 0 && !parameters.isLoading && (
            <Typography variant="body2" sx={{ color: muted }}>
              This query needs no values. Start the run when you are ready.
            </Typography>
          )}

          {names.map((name) => {
            const isList = lists.includes(name)
            const entered = isList
              ? (values[name] ?? '').split(/[\n,]/).filter((entry) => entry.trim()).length
              : 0

            return (
              <TextField
                key={name}
                label={name}
                value={values[name] ?? ''}
                onChange={(event) =>
                  setValues((current) => ({ ...current, [name]: event.target.value }))
                }
                size="small"
                fullWidth
                autoFocus={name === names[0]}
                multiline={isList}
                minRows={isList ? 3 : undefined}
                maxRows={isList ? 8 : undefined}
                placeholder={
                  isList ? 'POL-44219\nPOL-91002' : '5000, or 2026-08-01T00:00:00Z'
                }
                helperText={
                  isList
                    ? `${entered} value${entered === 1 ? '' : 's'} — one per line, or separated by commas`
                    : undefined
                }
              />
            )
          })}

          <Typography variant="caption" sx={{ color: muted }}>
            Recorded on the run, so it stays an account of exactly which records were covered — and
            a retry repeats that same selection rather than a freshly computed one.
          </Typography>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onCancel}>Cancel</Button>
        <Button variant="contained" disabled={incomplete || pending} onClick={start}>
          {dryRun ? 'Start dry run' : 'Start run'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
