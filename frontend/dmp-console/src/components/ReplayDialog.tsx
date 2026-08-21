import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  Radio,
  RadioGroup,
  Typography,
} from '@mui/material'
import { useEffect, useState } from 'react'
import type { ReplayRequest } from '@/api/types'
import { muted } from '@/theme'

/**
 * Chooses how to re-deliver the records a run rejected.
 *
 * The two options are not a preference — they answer different situations, and picking the wrong
 * one either wastes the attempt or quietly runs records through logic the rest of the migration
 * never saw. So they are stated as the two circumstances rather than as a setting.
 */
export function ReplayDialog({
  open,
  recordCount,
  versionNumber,
  pending,
  error,
  onClose,
  onConfirm,
}: {
  open: boolean
  recordCount: number
  versionNumber: number
  pending: boolean
  error: unknown
  onClose: () => void
  onConfirm: (request: ReplayRequest) => void
}) {
  const [throughLatest, setThroughLatest] = useState(false)
  const [acknowledgeRedaction, setAcknowledgeRedaction] = useState(false)

  // The server refuses a redacted replay until it is acknowledged, and names the fields when it
  // does. Offering the acknowledgement before that refusal would be asking the user to agree to
  // something neither of us has told them yet.
  const message = error instanceof Error ? error.message : null
  const redactionRefused = Boolean(message?.includes('redacts'))

  useEffect(() => {
    if (open) {
      setThroughLatest(false)
      setAcknowledgeRedaction(false)
    }
  }, [open])

  return (
    <Dialog open={open} onClose={pending ? undefined : onClose} maxWidth="sm" fullWidth>
      <DialogTitle>
        Replay {recordCount.toLocaleString()} rejected record
        {recordCount === 1 ? '' : 's'}
      </DialogTitle>

      <DialogContent>
        <Typography variant="body2" sx={{ color: muted, mb: 2 }}>
          What was stored is the record as the destination saw it, after any transforms had run. The
          source is not read, so nothing that already arrived is written twice.
        </Typography>

        <RadioGroup
          value={throughLatest ? 'latest' : 'original'}
          onChange={(event) => setThroughLatest(event.target.value === 'latest')}
        >
          <FormControlLabel
            value="original"
            control={<Radio size="small" />}
            label={
              <>
                <Typography variant="body2">
                  Send them exactly as they were stored (version {versionNumber})
                </Typography>
                <Typography variant="caption" sx={{ color: muted }}>
                  The fix was at the destination — a picklist value added, a constraint relaxed, a
                  permission granted. The records were always correct, so they go straight to the
                  sink: no transform runs, because the stored copy is already a transform's output
                  and running it again would apply it twice.
                </Typography>
              </>
            }
            sx={{ alignItems: 'flex-start', mb: 1.5 }}
          />
          <FormControlLabel
            value="latest"
            control={<Radio size="small" />}
            label={
              <>
                <Typography variant="body2">
                  Put them through the published version's transforms first
                </Typography>
                <Typography variant="caption" sx={{ color: muted }}>
                  The fix was in the pipeline — a transform that now maps the value the target
                  refused. Note that the published transforms are applied to the stored record,
                  which the old version had already transformed. That is right for a mapping added
                  on top, and wrong for a rewrite of what came before.
                </Typography>
              </>
            }
            sx={{ alignItems: 'flex-start' }}
          />
        </RadioGroup>

        {message && (
          <Alert severity={redactionRefused ? 'warning' : 'error'} sx={{ mt: 2 }}>
            {message}
          </Alert>
        )}

        {redactionRefused && (
          <FormControlLabel
            sx={{ mt: 1 }}
            control={
              <Radio
                size="small"
                checked={acknowledgeRedaction}
                onClick={() => setAcknowledgeRedaction((was) => !was)}
              />
            }
            label={
              <Typography variant="body2">
                Write the redacted values anyway — the destination does not need those fields
              </Typography>
            }
          />
        )}
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose} disabled={pending}>
          Cancel
        </Button>
        <Button
          variant="contained"
          disabled={pending || (redactionRefused && !acknowledgeRedaction)}
          onClick={() =>
            onConfirm({
              throughLatestVersion: throughLatest,
              acknowledgeRedaction,
            })
          }
        >
          {pending ? 'Starting…' : 'Replay'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
