import Alert from '@mui/material/Alert'
import AlertTitle from '@mui/material/AlertTitle'
import Button from '@mui/material/Button'
import Checkbox from '@mui/material/Checkbox'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogTitle from '@mui/material/DialogTitle'
import FormControlLabel from '@mui/material/FormControlLabel'
import FormControl from '@mui/material/FormControl'
import FormLabel from '@mui/material/FormLabel'
import Radio from '@mui/material/Radio'
import RadioGroup from '@mui/material/RadioGroup'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import { useEffect, useState } from 'react'
import type { RetryFrom, RetryScope } from '@/api/types'
import { ErrorPanel } from '@/components/Feedback'
import { muted } from '@/theme'

export interface RetryTarget {
  /** A single chunk, or the whole run when absent. */
  chunkId?: string
  label: string
  /** Chunks that will be re-attempted. */
  chunkCount: number
  /** Chunks that never started, offered separately because including them is a real choice. */
  cancelledCount: number
  /** Records these chunks already wrote — what a restart would send a second time. */
  recordsAtRisk: number
  /**
   * The run was stopped rather than having failed, so this is a continuation.
   *
   * Set from the run's own state rather than inferred from the chunk counts. A stopped run that
   * also had a failure is still a run somebody stopped, and "resume" is still what they mean.
   */
  resuming?: boolean
}

/**
 * Asks the two questions the platform genuinely cannot answer on the user's behalf.
 *
 * Whether to resume a chunk or run it again depends on why it failed, and whether the resulting
 * duplicates matter depends on the destination. Both are asked rather than assumed — but the
 * defaults are chosen so that pressing straight through is always the safe option.
 */
export function RetryDialog({
  open,
  target,
  pending,
  error,
  onClose,
  onConfirm,
}: {
  open: boolean
  target: RetryTarget | null
  pending: boolean
  error: unknown
  onClose: () => void
  onConfirm: (options: {
    from: RetryFrom
    scope: RetryScope
    acknowledgeDuplicates: boolean
  }) => void
}) {
  const [from, setFrom] = useState<RetryFrom>('CHECKPOINT')
  const [includeCancelled, setIncludeCancelled] = useState(false)
  const [acknowledged, setAcknowledged] = useState(false)

  // Reset each time the dialog opens. Carrying a previous acknowledgement across would let someone
  // confirm duplicates once and silently keep confirming them for every retry afterwards.
  useEffect(() => {
    if (open) {
      setFrom('CHECKPOINT')
      // On by default only when it is the entire point — a stopped run with nothing failed. On a
      // run that genuinely failed, including the chunks that never started is a real choice and
      // stays the user's to make.
      setIncludeCancelled(Boolean(target?.resuming) && !target?.chunkId)
      setAcknowledged(false)
    }
  }, [open])

  if (!target) return null

  // Mechanically identical to a retry — same new run, same pinned version, same checkpoints — but
  // named for what the user is doing. Calling it "retry 0 chunks", with the part that matters
  // defaulted off, made the one thing they came here for the thing they had to go and find.
  const resuming = Boolean(target.resuming) && !target.chunkId

  const restarting = from === 'CHUNK_START'
  const wouldResend = restarting && target.recordsAtRisk > 0
  const blocked = wouldResend && !acknowledged

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{target.label}</DialogTitle>

      <DialogContent>
        <Typography variant="body2" sx={{ color: muted, mb: 3 }}>
          {resuming
            ? `This creates a new run that picks up the ${target.cancelledCount} chunk${
                target.cancelledCount === 1 ? '' : 's'
              } the stop left unfinished, each from its saved position. Chunks that completed are
               never re-run, and the original run's record is left exactly as it is.`
            : `A retry creates a new run that re-attempts only these chunks. Chunks that completed
               are never re-run, and the original run's record is left exactly as it is.`}
        </Typography>

        <FormControl sx={{ mb: 2 }}>
          <FormLabel sx={{ mb: 1 }}>Where should each chunk pick up?</FormLabel>
          <RadioGroup value={from} onChange={(event) => setFrom(event.target.value as RetryFrom)}>
            <FormControlLabel
              value="CHECKPOINT"
              control={<Radio />}
              label={
                <Stack>
                  <Typography variant="body2">Resume where it stopped</Typography>
                  <Typography variant="caption" sx={{ color: muted }}>
                    Continues from the last saved position. Nothing already written is sent again.
                  </Typography>
                </Stack>
              }
            />
            <FormControlLabel
              value="CHUNK_START"
              control={<Radio />}
              label={
                <Stack>
                  <Typography variant="body2">Start the chunk over</Typography>
                  <Typography variant="caption" sx={{ color: muted }}>
                    Discards the saved position and runs the whole chunk again. The right choice
                    when the earlier partial result is not trusted.
                  </Typography>
                </Stack>
              }
            />
          </RadioGroup>
        </FormControl>

        {target.cancelledCount > 0 && !target.chunkId && (
          <FormControlLabel
            sx={{ mb: 1, display: 'flex', alignItems: 'flex-start' }}
            control={
              <Checkbox
                checked={includeCancelled}
                onChange={(event) => setIncludeCancelled(event.target.checked)}
              />
            }
            label={
              <Stack sx={{ pt: 1 }}>
                <Typography variant="body2">
                  Also run the {target.cancelledCount} chunk
                  {target.cancelledCount === 1 ? '' : 's'} that never started
                </Typography>
                <Typography variant="caption" sx={{ color: muted }}>
                  These were cancelled when the run was stopped. Including them finishes the
                  migration rather than only repairing what failed.
                </Typography>
              </Stack>
            }
          />
        )}

        {wouldResend && (
          <Alert severity="warning" sx={{ mt: 2 }}>
            <AlertTitle>
              This will send {target.recordsAtRisk.toLocaleString()} record
              {target.recordsAtRisk === 1 ? '' : 's'} a second time
            </AlertTitle>
            <Typography variant="body2" sx={{ mb: 1 }}>
              These chunks had already written them before they failed. If the destination
              overwrites by key that is harmless. If it appends or inserts, you get{' '}
              {target.recordsAtRisk.toLocaleString()} duplicate
              {target.recordsAtRisk === 1 ? '' : 's'}.
            </Typography>
            <FormControlLabel
              control={
                <Checkbox
                  checked={acknowledged}
                  onChange={(event) => setAcknowledged(event.target.checked)}
                />
              }
              label={<Typography variant="body2">I understand, start them over anyway</Typography>}
            />
          </Alert>
        )}

        <ErrorPanel error={error} />
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={pending || blocked}
          onClick={() =>
            onConfirm({
              from,
              scope: includeCancelled ? 'FAILED_AND_CANCELLED' : 'FAILED',
              acknowledgeDuplicates: acknowledged,
            })
          }
        >
          {resuming ? 'Resume' : 'Retry'}{' '}
          {target.chunkId
            ? 'chunk'
            : `${target.chunkCount + (includeCancelled ? target.cancelledCount : 0)} chunks`}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
