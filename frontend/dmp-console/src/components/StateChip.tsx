import Chip from '@mui/material/Chip'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ErrorIcon from '@mui/icons-material/Error'
import PauseCircleIcon from '@mui/icons-material/PauseCircle'
import PlayCircleIcon from '@mui/icons-material/PlayCircle'
import HourglassTopIcon from '@mui/icons-material/HourglassTop'
import StopCircleIcon from '@mui/icons-material/StopCircle'
import RadioButtonUncheckedIcon from '@mui/icons-material/RadioButtonUnchecked'
import CloudSyncIcon from '@mui/icons-material/CloudSync'
import type { ChunkState, RunState } from '@/api/types'
import { status, muted } from '@/theme'

/**
 * A run or chunk state, shown as colour **plus an icon plus the word**.
 *
 * The redundancy is required, not decorative. Two of these colours sit in the CVD warn band
 * against each other, and `warning` falls below 3:1 on the light surface — so colour alone would
 * leave some readers unable to tell a paused run from a completed one. The icon and the label are
 * the mitigation, which is why there is no icon-only variant of this component.
 */

type Appearance = { color: string; Icon: typeof CheckCircleIcon; label: string }

const RUN_APPEARANCE: Record<RunState, Appearance> = {
  CREATED: { color: muted, Icon: RadioButtonUncheckedIcon, label: 'Created' },
  VALIDATED: { color: muted, Icon: RadioButtonUncheckedIcon, label: 'Validated' },
  // Distinct from RUNNING on purpose: "waiting on Salesforce" and "moving data" are different
  // situations, and collapsing them makes every duration metric misleading.
  PREPARING: { color: status.warning, Icon: CloudSyncIcon, label: 'Preparing' },
  RUNNING: { color: '#3987e5', Icon: PlayCircleIcon, label: 'Running' },
  PAUSED: { color: status.warning, Icon: PauseCircleIcon, label: 'Paused' },
  STOPPING: { color: status.warning, Icon: HourglassTopIcon, label: 'Stopping' },
  FINALIZING: { color: '#3987e5', Icon: HourglassTopIcon, label: 'Finalizing' },
  COMPLETED: { color: status.good, Icon: CheckCircleIcon, label: 'Completed' },
  FAILED: { color: status.critical, Icon: ErrorIcon, label: 'Failed' },
  STOPPED: { color: muted, Icon: StopCircleIcon, label: 'Stopped' },
  ARCHIVED: { color: muted, Icon: RadioButtonUncheckedIcon, label: 'Archived' },
}

const CHUNK_APPEARANCE: Record<ChunkState, Appearance> = {
  PENDING: { color: muted, Icon: RadioButtonUncheckedIcon, label: 'Pending' },
  RUNNING: { color: '#3987e5', Icon: PlayCircleIcon, label: 'Running' },
  // The same colour as PREPARING on a run, and for the same reason: this chunk is not moving
  // data, it is waiting on somebody else's system to finish deciding. Reading it as "running"
  // would make every duration here mean two different things.
  WAITING_EXTERNAL: { color: status.warning, Icon: CloudSyncIcon, label: 'Waiting on destination' },
  COMPLETED: { color: status.good, Icon: CheckCircleIcon, label: 'Completed' },
  FAILED: { color: status.warning, Icon: ErrorIcon, label: 'Failed, will retry' },
  ABANDONED: { color: status.critical, Icon: ErrorIcon, label: 'Abandoned' },
  CANCELLED: { color: muted, Icon: StopCircleIcon, label: 'Cancelled' },
}

/**
 * What to show for a state this build has never heard of.
 *
 * The engine gains states over time and the console is deployed separately, so the two are
 * routinely a version apart. Looking one up and destructuring the miss threw a TypeError out of
 * render, and React unmounted the entire route — a whole page replaced by "Unexpected Application
 * Error!" because one chip in a table did not recognise one word.
 *
 * Showing the raw name is a worse chip and a far better outcome: the run stays readable and the
 * unknown state is legible enough to act on.
 */
function unknownState(state: string): Appearance {
  return { color: muted, Icon: RadioButtonUncheckedIcon, label: state }
}

export function RunStateChip({ state, size = 'small' }: { state: RunState; size?: 'small' | 'medium' }) {
  return <StateChip appearance={RUN_APPEARANCE[state] ?? unknownState(state)} size={size} />
}

export function ChunkStateChip({ state }: { state: ChunkState }) {
  return <StateChip appearance={CHUNK_APPEARANCE[state] ?? unknownState(state)} size="small" />
}

function StateChip({ appearance, size }: { appearance: Appearance; size: 'small' | 'medium' }) {
  const { color, Icon, label } = appearance
  return (
    <Chip
      size={size}
      icon={<Icon sx={{ fontSize: 16, color: `${color} !important` }} />}
      label={label}
      variant="outlined"
      sx={{
        color,
        borderColor: color,
        // A tinted fill rather than a solid one, so the label stays legible against it in both
        // modes without needing a per-state text colour.
        backgroundColor: `${color}14`,
      }}
    />
  )
}
