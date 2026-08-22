import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Checkbox from '@mui/material/Checkbox'
import Chip from '@mui/material/Chip'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogTitle from '@mui/material/DialogTitle'
import FormControlLabel from '@mui/material/FormControlLabel'
import IconButton from '@mui/material/IconButton'
import MenuItem from '@mui/material/MenuItem'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import TextField from '@mui/material/TextField'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import AddIcon from '@mui/icons-material/Add'
import DeleteIcon from '@mui/icons-material/DeleteOutline'
import EditIcon from '@mui/icons-material/EditOutlined'
import SendIcon from '@mui/icons-material/SendOutlined'
import { useState } from 'react'
import {
  useDeleteNotifier,
  useNotifiers,
  usePipelines,
  useSaveNotifier,
  useTestNotifier,
} from '@/api/hooks'
import { ErrorPanel, Loading } from '@/components/Feedback'
import { PageHeader } from '@/components/PageHeader'
import { muted, tabular } from '@/theme'
import type { Notifier, NotifierEvent } from '@/api/types'

/**
 * Which outcomes are offered, and why somebody would want each.
 *
 * The descriptions matter more than usual here: the difference between a completed run and a
 * completed run that lost four thousand records is the whole reason the second exists, and it is
 * not evident from the name.
 */
const EVENTS: { value: NotifierEvent; label: string; note: string }[] = [
  { value: 'RUN_FAILED', label: 'Run failed', note: 'Stopped with an error. Everyone wants this.' },
  {
    value: 'RUN_COMPLETED_WITH_FAILURES',
    label: 'Finished, but records were lost',
    note: 'The run is green, the dashboard is green, and the records are not there.',
  },
  { value: 'RUN_COMPLETED', label: 'Finished cleanly', note: 'To watch the nightly load land.' },
  { value: 'RUN_STOPPED', label: 'Stopped', note: 'Somebody stopped it, or a chunk failure did.' },
]

export function NotifiersPage() {
  const notifiers = useNotifiers()
  const remove = useDeleteNotifier()
  const test = useTestNotifier()
  const [editing, setEditing] = useState<Notifier | null>(null)
  const [creating, setCreating] = useState(false)

  if (notifiers.isLoading) return <Loading />
  if (notifiers.error) return <ErrorPanel error={notifiers.error} />

  const rows = notifiers.data ?? []

  return (
    <>
      <PageHeader
        title="Notifications"
        subtitle="Where to send word when a run ends. Without one, a pipeline that fails at two in the morning is discovered at eleven."
        actions={
          <Button startIcon={<AddIcon />} variant="contained" onClick={() => setCreating(true)}>
            Add
          </Button>
        }
      />

      <ErrorPanel error={remove.error ?? test.error} />

      {test.data && (
        <Alert severity={test.data.delivered ? 'success' : 'error'} sx={{ mb: 2 }}>
          {test.data.delivered
            ? 'Delivered. The endpoint accepted it.'
            : `Not delivered — ${test.data.error}`}
        </Alert>
      )}

      {rows.length === 0 ? (
        <Alert severity="info">
          Nothing is watching. A scheduled pipeline that fails overnight will not tell anyone.
        </Alert>
      ) : (
        <Paper sx={{ overflow: 'auto' }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>NAME</TableCell>
                <TableCell>WATCHES</TableCell>
                <TableCell>ON</TableCell>
                <TableCell>LAST DELIVERY</TableCell>
                <TableCell align="right" />
              </TableRow>
            </TableHead>
            <TableBody>
              {rows.map((notifier) => (
                <TableRow key={notifier.id} hover>
                  <TableCell>
                    <Box sx={{ fontWeight: 500 }}>{notifier.name}</Box>
                    <Typography variant="caption" sx={{ ...tabular, color: muted }}>
                      {notifier.url}
                    </Typography>
                    {!notifier.enabled && (
                      <Chip label="disabled" size="small" sx={{ ml: 1 }} />
                    )}
                  </TableCell>
                  <TableCell sx={{ color: notifier.pipelineId ? undefined : muted }}>
                    {notifier.pipelineId ? 'one pipeline' : 'every pipeline'}
                  </TableCell>
                  <TableCell>
                    <Stack direction="row" spacing={0.5} sx={{ flexWrap: 'wrap', gap: 0.5 }}>
                      {notifier.events.map((event) => (
                        <Chip
                          key={event}
                          size="small"
                          label={EVENTS.find((e) => e.value === event)?.label ?? event}
                          color={event === 'RUN_FAILED' ? 'error' : 'default'}
                          variant="outlined"
                        />
                      ))}
                    </Stack>
                  </TableCell>
                  <TableCell>
                    <LastDelivery notifier={notifier} />
                  </TableCell>
                  <TableCell align="right" sx={{ whiteSpace: 'nowrap' }}>
                    <Button
                      size="small"
                      startIcon={<SendIcon />}
                      disabled={test.isPending}
                      onClick={() => test.mutate(notifier.id)}
                    >
                      Test
                    </Button>
                    <Button size="small" startIcon={<EditIcon />} onClick={() => setEditing(notifier)}>
                      Edit
                    </Button>
                    <Tooltip title="Delete">
                      <IconButton size="small" onClick={() => remove.mutate(notifier.id)}>
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Paper>
      )}

      {(creating || editing) && (
        <NotifierDialog
          editing={editing}
          onClose={() => {
            setCreating(false)
            setEditing(null)
          }}
        />
      )}
    </>
  )
}

/**
 * Whether this actually works, stated rather than assumed.
 *
 * A notifier that has never fired and one whose URL was rotated away look identical until somebody
 * needs it, which is the worst moment to find out.
 */
function LastDelivery({ notifier }: { notifier: Notifier }) {
  if (!notifier.lastAttemptAt) {
    return (
      <Typography variant="caption" sx={{ color: muted }}>
        Never fired — send a test
      </Typography>
    )
  }
  return (
    <Tooltip title={notifier.lastAttemptError ?? ''}>
      <Box>
        <Typography
          variant="caption"
          sx={{ color: notifier.lastAttemptSucceeded ? 'success.main' : 'error.main' }}
        >
          {notifier.lastAttemptSucceeded ? 'Delivered' : 'Failed'}
        </Typography>
        <Typography variant="caption" sx={{ color: muted, display: 'block' }}>
          {new Date(notifier.lastAttemptAt).toLocaleString()}
        </Typography>
      </Box>
    </Tooltip>
  )
}

function NotifierDialog({ editing, onClose }: { editing: Notifier | null; onClose: () => void }) {
  const save = useSaveNotifier()
  const pipelines = usePipelines()

  const [name, setName] = useState(editing?.name ?? '')
  const [url, setUrl] = useState(editing?.url ?? '')
  const [pipelineId, setPipelineId] = useState(editing?.pipelineId ?? '')
  const [events, setEvents] = useState<NotifierEvent[]>(editing?.events ?? ['RUN_FAILED'])
  const [secretHeader, setSecretHeader] = useState(editing?.secretHeader ?? '')
  // Seeded from what is stored, so editing a URL cannot silently strip the credential.
  const [secretRef, setSecretRef] = useState(editing?.secretRef ?? '')
  const [enabled, setEnabled] = useState(editing?.enabled ?? true)
  const [description, setDescription] = useState(editing?.description ?? '')

  const submit = () =>
    save.mutate(
      {
        id: editing?.id,
        name,
        url,
        pipelineId: pipelineId || null,
        events,
        secretHeader: secretHeader || null,
        secretRef: secretRef || null,
        enabled,
        description: description || null,
      },
      { onSuccess: onClose },
    )

  return (
    <Dialog open onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{editing ? 'Edit notification' : 'Add a notification'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          <ErrorPanel error={save.error} />

          <TextField
            label="Name"
            value={name}
            onChange={(event) => setName(event.target.value)}
            size="small"
            fullWidth
            autoFocus
          />

          <TextField
            label="URL"
            value={url}
            onChange={(event) => setUrl(event.target.value)}
            size="small"
            fullWidth
            placeholder="https://hooks.example.com/services/…"
            helperText="Any endpoint that accepts a JSON POST — a chat webhook, an alerting system, your own service."
          />

          <TextField
            select
            label="Watches"
            value={pipelineId}
            onChange={(event) => setPipelineId(event.target.value)}
            size="small"
            fullWidth
          >
            <MenuItem value="">Every pipeline</MenuItem>
            {(pipelines.data?.content ?? []).map((pipeline) => (
              <MenuItem key={pipeline.id} value={pipeline.id}>
                {pipeline.name}
              </MenuItem>
            ))}
          </TextField>

          <Box>
            <Typography variant="body2" sx={{ mb: 0.5 }}>
              Tell me when
            </Typography>
            {EVENTS.map((event) => (
              <Box key={event.value}>
                <FormControlLabel
                  control={
                    <Checkbox
                      size="small"
                      checked={events.includes(event.value)}
                      onChange={(_, checked) =>
                        setEvents((current) =>
                          checked
                            ? [...current, event.value]
                            : current.filter((e) => e !== event.value),
                        )
                      }
                    />
                  }
                  label={event.label}
                />
                <Typography variant="caption" sx={{ color: muted, display: 'block', ml: 4, mt: -0.5 }}>
                  {event.note}
                </Typography>
              </Box>
            ))}
          </Box>

          <Stack direction="row" spacing={1}>
            <TextField
              label="Auth header"
              value={secretHeader}
              onChange={(event) => setSecretHeader(event.target.value)}
              size="small"
              placeholder="Authorization"
              sx={{ flex: 1 }}
            />
            <TextField
              label="Credential reference"
              value={secretRef}
              onChange={(event) => setSecretRef(event.target.value)}
              size="small"
              placeholder="env:SLACK_WEBHOOK_TOKEN"
              sx={{ flex: 2 }}
            />
          </Stack>
          <Typography variant="caption" sx={{ color: muted, mt: -1 }}>
            A reference, never the value — the platform resolves it when sending. Optional: most
            chat webhooks carry their own token in the URL.
          </Typography>

          <TextField
            label="Description"
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            size="small"
            fullWidth
          />

          <FormControlLabel
            control={
              <Checkbox
                size="small"
                checked={enabled}
                onChange={(_, checked) => setEnabled(checked)}
              />
            }
            label="Enabled"
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          onClick={submit}
          disabled={!name.trim() || !url.trim() || events.length === 0 || save.isPending}
        >
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
