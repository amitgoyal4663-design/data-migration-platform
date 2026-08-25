import CheckCircleIcon from '@mui/icons-material/CheckCircleOutlined'
import ChevronRightIcon from '@mui/icons-material/ChevronRight'
import CloseIcon from '@mui/icons-material/Close'
import ErrorIcon from '@mui/icons-material/ErrorOutlineOutlined'
import WarningIcon from '@mui/icons-material/WarningAmberOutlined'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Divider from '@mui/material/Divider'
import Drawer from '@mui/material/Drawer'
import IconButton from '@mui/material/IconButton'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import { Link as RouterLink } from 'react-router-dom'
import type { OperationsHeadline } from '@/api/types'
import { muted, tabular } from '@/theme'

/**
 * Alerts as one line, with the detail a click away.
 *
 * <p>The strip that preceded this printed every alert in full at the top of the screen. Three
 * problems took three hundred pixels — the whole first screenful — to say what a count says in one
 * line, and pushed the figures somebody actually came for below the fold. Worse, two of its lines
 * were restating the tiles immediately beneath it.
 *
 * <p>So: a bar that says how many and how bad, and a drawer holding the same alerts in full, with
 * the buttons that act on them. Nothing is hidden — a count of two criticals is not less
 * informative than two sentences about them, it is the same fact in the space a heading deserves.
 */
export function AlertBar({
  headlines,
  jobs,
  onOpen,
}: {
  headlines: OperationsHeadline[]
  jobs: number
  onOpen: () => void
}) {
  const alerts = problems(headlines)
  const critical = alerts.filter((alert) => alert.severity === 'CRITICAL').length
  const warning = alerts.length - critical

  if (alerts.length === 0) {
    return (
      <Paper
        variant="outlined"
        sx={{
          px: 2,
          py: 1,
          mb: 2,
          display: 'flex',
          alignItems: 'center',
          gap: 1.5,
          borderLeft: 3,
          borderLeftColor: 'success.main',
        }}
      >
        <CheckCircleIcon fontSize="small" sx={{ color: 'success.main' }} />
        <Typography variant="body2">
          All {jobs} job{jobs === 1 ? '' : 's'} normal
        </Typography>
        <Typography variant="body2" sx={{ color: muted }}>
          — nothing failed, nothing moved an unusual amount, nothing was lost.
        </Typography>
      </Paper>
    )
  }

  return (
    <Paper
      variant="outlined"
      onClick={onOpen}
      sx={{
        px: 2,
        py: 1,
        mb: 2,
        display: 'flex',
        alignItems: 'center',
        gap: 2,
        cursor: 'pointer',
        borderLeft: 3,
        borderLeftColor: critical > 0 ? 'error.main' : 'warning.main',
        '&:hover': { bgcolor: 'action.hover' },
      }}
    >
      {critical > 0 && (
        <Count icon={<ErrorIcon fontSize="small" />} tone="error.main" n={critical} word="critical" />
      )}
      {warning > 0 && (
        <Count icon={<WarningIcon fontSize="small" />} tone="warning.main" n={warning} word="warning" />
      )}

      {/* The names, because "2 critical" is a number and "M to Kafka" is the thing somebody owns. */}
      <Typography
        variant="body2"
        sx={{
          color: muted,
          flex: 1,
          minWidth: 0,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
      >
        {alerts
          .map((alert) => alert.subject)
          .filter(Boolean)
          .join(' · ')}
      </Typography>

      <Button size="small" endIcon={<ChevronRightIcon />}>
        View alerts
      </Button>
    </Paper>
  )
}

function Count({
  icon,
  tone,
  n,
  word,
}: {
  icon: React.ReactNode
  tone: string
  n: number
  word: string
}) {
  return (
    <Stack direction="row" spacing={0.75} alignItems="center" sx={{ color: tone, flexShrink: 0 }}>
      {icon}
      <Typography variant="body2" sx={{ fontWeight: 600 }}>
        {n} {word}
        {n === 1 ? '' : 's'}
      </Typography>
    </Stack>
  )
}

/**
 * Every alert in full, and what to do about each.
 *
 * <p>A drawer rather than a page, because reading an alert is not leaving the screen — the answer
 * to "what is wrong" is usually followed by a look back at the numbers it came from.
 */
export function AlertsDrawer({
  open,
  headlines,
  onClose,
}: {
  open: boolean
  headlines: OperationsHeadline[]
  onClose: () => void
}) {
  const alerts = problems(headlines)

  return (
    <Drawer anchor="right" open={open} onClose={onClose}>
      <Box sx={{ width: { xs: '100vw', sm: 520 }, display: 'flex', flexDirection: 'column' }}>
        <Stack direction="row" alignItems="center" sx={{ px: 2, py: 1.5 }}>
          <Typography variant="h3" sx={{ flex: 1 }}>
            Alerts
          </Typography>
          <Typography variant="body2" sx={{ color: muted, mr: 1 }}>
            {alerts.length}
          </Typography>
          <IconButton size="small" onClick={onClose}>
            <CloseIcon fontSize="small" />
          </IconButton>
        </Stack>
        <Divider />

        {alerts.length === 0 ? (
          <Typography variant="body2" sx={{ color: muted, p: 3 }}>
            Nothing is wrong with the jobs in view.
          </Typography>
        ) : (
          <Stack divider={<Divider />}>
            {alerts.map((alert, index) => (
              <Alert key={index} alert={alert} onNavigate={onClose} />
            ))}
          </Stack>
        )}

        <Divider />
        <Typography variant="caption" sx={{ color: muted, p: 2 }}>
          One alert per job — its worst finding. Everything else it raised is on that job&apos;s card,
          which is where the numbers behind these sit.
        </Typography>
      </Box>
    </Drawer>
  )
}

function Alert({ alert, onNavigate }: { alert: OperationsHeadline; onNavigate: () => void }) {
  const tone = alert.severity === 'CRITICAL' ? 'error.main' : 'warning.main'

  return (
    <Box sx={{ p: 2, borderLeft: 3, borderLeftColor: tone }}>
      <Stack direction="row" spacing={1} alignItems="baseline">
        <Typography
          variant="caption"
          sx={{ color: tone, fontWeight: 700, letterSpacing: '0.08em' }}
        >
          {alert.severity}
        </Typography>
        <Box sx={{ flex: 1 }} />
        {alert.at && (
          <Typography variant="caption" sx={{ ...tabular, color: muted }}>
            {ago(alert.at)}
          </Typography>
        )}
      </Stack>

      {alert.subject && (
        <Typography sx={{ fontWeight: 600, mt: 0.25 }}>{alert.subject}</Typography>
      )}
      <Typography variant="body2" sx={{ color: tone, mt: 0.25 }}>
        {alert.headline}
      </Typography>
      <Typography variant="body2" sx={{ color: muted, mt: 0.5 }}>
        {alert.detail}
      </Typography>

      <Stack direction="row" spacing={1} sx={{ mt: 1 }}>
        {alert.runId && (
          <Button size="small" component={RouterLink} to={`/runs/${alert.runId}`} onClick={onNavigate}>
            Open the run
          </Button>
        )}
        {alert.pipelineId && (
          <Button
            size="small"
            component={RouterLink}
            to={`/pipelines/${alert.pipelineId}`}
            onClick={onNavigate}
          >
            Open the job
          </Button>
        )}
      </Stack>
    </Box>
  )
}

/**
 * The alerts, which is everything that is not merely informational.
 *
 * <p>The INFO lines said "8 records transferred in the last 24 hours" and "2 jobs running now" —
 * both of which the tiles and the progress bars directly below were already saying, in a form built
 * for it. A summary that repeats what is visible underneath is not a summary.
 */
function problems(headlines: OperationsHeadline[]): OperationsHeadline[] {
  return headlines.filter((headline) => headline.severity !== 'INFO')
}

function ago(iso: string): string {
  const seconds = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 90) return 'just now'
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`
  return `${Math.round(seconds / 86400)}d ago`
}
