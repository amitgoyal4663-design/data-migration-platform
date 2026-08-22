import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogTitle from '@mui/material/DialogTitle'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import { useState } from 'react'
import { muted } from '@/theme'

/**
 * Collects the values a pipeline's source query expects before a run starts.
 *
 * <p>The names come from the backend, which asks the connector — only it knows that a Databricks
 * query writes its placeholders as `:from`. Parsing SQL here would put a second copy of that rule
 * in the frontend, and the copy that drifts is always the one nobody is testing.
 *
 * <p>A pipeline whose query takes no parameters never sees this dialog: the caller starts the run
 * directly, exactly as before.
 */
export function RunDialog({
  open,
  names,
  pending,
  dryRun,
  onCancel,
  onStart,
}: {
  open: boolean
  names: string[]
  pending: boolean
  /** Rehearsing rather than delivering. Said in the dialog, because the button that opened it
   *  and the button that confirms it are on different screens by the time somebody reads them. */
  dryRun?: boolean
  onCancel: () => void
  onStart: (parameters: Record<string, string>) => void
}) {
  const [values, setValues] = useState<Record<string, string>>({})

  // Every named placeholder must have a value: the query cannot run with one missing, and the
  // backend refuses by name rather than quietly reading the whole table.
  const incomplete = names.some((name) => (values[name] ?? '').trim() === '')

  return (
    <Dialog open={open} onClose={onCancel} maxWidth="sm" fullWidth>
      <DialogTitle>{dryRun ? 'Start dry run' : 'Start run'}</DialogTitle>
      <DialogContent>
        <Typography variant="body2" sx={{ color: muted, mb: 2.5 }}>
          This pipeline&apos;s query expects {names.length === 1 ? 'a value' : 'values'} for{' '}
          {names.map((name) => (
            <code key={name}>:{name} </code>
          ))}
          — supply {names.length === 1 ? 'it' : 'them'} for this run. They are recorded on the run,
          so it stays a record of exactly which range was covered, and a retry repeats that same
          range rather than a newly computed one.
        </Typography>

        {dryRun && (
          <Typography variant="body2" sx={{ color: 'warning.main', mb: 2.5 }}>
            Nothing will be written. The source is read in full and every script runs, but the
            destination is never opened — so this cannot tell you it would have accepted the
            records, only what would have been sent and which never got that far.
          </Typography>
        )}

        <Stack spacing={2}>
          {names.map((name) => (
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
              // Both shapes, because the same two boxes serve an id range and a time window. The
              // type is inferred from what is typed, so nothing has to be declared anywhere.
              placeholder="5000, or 2026-08-01T00:00:00Z"
            />
          ))}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onCancel}>Cancel</Button>
        <Button
          variant="contained"
          disabled={incomplete || pending}
          onClick={() => onStart(values)}
        >
          {dryRun ? 'Start dry run' : 'Start run'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
