import { useState } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import Box from '@mui/material/Box'
import Stack from '@mui/material/Stack'
import TableCell from '@mui/material/TableCell'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import { RunStateChip } from '@/components/StateChip'
import type { Run } from '@/api/types'
import { attemptReason, type RunChain } from './runChains'
import { shortId } from '@/api/ids'

/**
 * One migration in a table of runs: a single row, expanding into the attempts behind it.
 *
 * <p><b>The attempts are not rows.</b> They were, and it read badly: a child row's columns line up
 * under headings that do not describe it — the same version, the same window, a duration that means
 * something different — so the eye tries to compare numbers that are not comparable. What the
 * attempts actually are is a sequence in time, so they are drawn as one: a timeline in a panel
 * beneath the row, spanning the full width, borrowing none of the table's columns.
 *
 * <p><b>The row is the migration, not the first attempt.</b> Its state is the latest attempt's, and
 * its total is the whole chain's — which is why the panel does not repeat them: the same number in
 * two places a centimetre apart is one to keep in agreement for no benefit. A migration that was stopped and then resumed to completion used
 * to be listed as STOPPED, which is true of one attempt and false of the thing somebody is asking
 * about.
 */

export interface ChainRowProps {
  /** Present when this row stands for a chain, carrying the migration's state and totals. */
  chain?: RunChain
  expanded?: boolean
  onToggle?: () => void
}

export function RunChainRows({
  chain,
  render,
}: {
  chain: RunChain
  render: (run: Run, props: ChainRowProps) => React.ReactNode
}) {
  const [open, setOpen] = useState(false)
  const chained = chain.attempts.length > 0

  return (
    <>
      {render(chain.root, {
        chain: chained ? chain : undefined,
        expanded: open,
        onToggle: chained ? () => setOpen((was) => !was) : undefined,
      })}

      {chained && open && (
        <TableRow>
          {/*
            Deliberately more columns than any table here has. A browser clamps a colSpan to the
            width of the table, so this always reaches the right-hand edge — whereas a number
            counted by hand was wrong on both tables the first time and would go wrong again the
            next time somebody adds a column.
          */}
          <TableCell colSpan={99} sx={{ p: 0, borderBottom: 0 }}>
            <AttemptTimeline chain={chain} />
          </TableCell>
        </TableRow>
      )}
    </>
  )
}

function AttemptTimeline({ chain }: { chain: RunChain }) {
  const all = [chain.root, ...chain.attempts]

  return (
    <Box
      sx={{
        // A panel that belongs to the row above it: same surface, an accent down the left edge to
        // tie it to its parent, and a real border rather than a wash of grey. The washed-out
        // version read as disabled content, which is the opposite of what it is.
        bgcolor: 'background.default',
        borderLeft: 3,
        borderColor: 'primary.main',
        // Indented past the expander column and the row's first cell, so an attempt reads as
        // sitting inside the migration above it. Level with the parent — which is where the
        // padding started — it reads as a sibling of the row, not a part of it.
        pl: 9,
        pr: 3,
        py: 2,
      }}
    >
      <Stack spacing={0}>
        {all.map((run, index) => (
          <Attempt
            key={run.id}
            run={run}
            number={index + 1}
            reason={index === 0 ? 'started' : attemptReason(run, all[index - 1])}
            last={index === all.length - 1}
          />
        ))}
      </Stack>
    </Box>
  )
}

/** The dot's colour, so the spine carries the same meaning as the chips beside it. */
function dotColour(state: Run['state']): string {
  if (state === 'COMPLETED') return 'success.main'
  if (state === 'FAILED') return 'error.main'
  if (state === 'RUNNING' || state === 'PREPARING') return 'primary.main'
  return 'text.disabled'
}

function Attempt({
  run,
  number,
  reason,
  last,
}: {
  run: Run
  number: number
  reason: string
  last: boolean
}) {
  return (
    <Stack direction="row" spacing={2} alignItems="stretch">
      {/* The spine: a dot per attempt, coloured by outcome, joined by a line so the order reads as
          a sequence rather than being inferred from the timestamps. */}
      <Stack alignItems="center" sx={{ pt: 1.25, width: 10, flexShrink: 0 }}>
        <Box
          sx={{
            width: 10,
            height: 10,
            borderRadius: '50%',
            bgcolor: dotColour(run.state),
            flexShrink: 0,
          }}
        />
        {!last && <Box sx={{ width: 2, flex: 1, bgcolor: 'divider' }} />}
      </Stack>

      <Stack
        direction="row"
        spacing={2}
        alignItems="center"
        component={RouterLink}
        to={`/runs/${run.id}`}
        sx={{
          textDecoration: 'none',
          color: 'inherit',
          flex: 1,
          minWidth: 0,
          py: 0.75,
          px: 1,
          mb: last ? 0 : 0.5,
          borderRadius: 1,
          '&:hover': { bgcolor: 'action.hover' },
        }}
      >
        <Typography variant="body2" sx={{ width: 72, flexShrink: 0, fontWeight: 600 }}>
          attempt {number}
        </Typography>

        <Box sx={{ width: 104, flexShrink: 0 }}>
          <RunStateChip state={run.state} />
        </Box>

        {/* The number and its unit together. A bare figure in a column with no heading — the panel
            has none, deliberately — is a number nobody can name. */}
        <Stack direction="row" spacing={0.75} alignItems="baseline" sx={{ width: 150, flexShrink: 0 }}>
          <Typography variant="body2" sx={{ fontWeight: 700, fontVariantNumeric: 'tabular-nums' }}>
            {run.metrics.recordsWritten.toLocaleString()}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            written
          </Typography>
        </Stack>

        <Typography variant="body2" color="text.secondary" sx={{ flex: 1, minWidth: 0 }}>
          {reason}
        </Typography>

        <Typography variant="body2" color="text.secondary" sx={{ flexShrink: 0 }}>
          {run.startedAt ? new Date(run.startedAt).toLocaleTimeString() : '—'}
        </Typography>

        <Typography
          variant="caption"
          color="text.disabled"
          sx={{ width: 72, textAlign: 'right', flexShrink: 0 }}
        >
          {shortId(run.id)}
        </Typography>
      </Stack>
    </Stack>
  )
}
