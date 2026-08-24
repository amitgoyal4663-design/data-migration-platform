import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import CircularProgress from '@mui/material/CircularProgress'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogTitle from '@mui/material/DialogTitle'
import IconButton from '@mui/material/IconButton'
import PreviewIcon from '@mui/icons-material/TableRowsOutlined'
import { PreviewDialog } from '@/components/PreviewDialog'
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
import EditIcon from '@mui/icons-material/EditOutlined'
import PlugIcon from '@mui/icons-material/PowerSettingsNewOutlined'
import CableIcon from '@mui/icons-material/CableOutlined'
import DeleteIcon from '@mui/icons-material/DeleteOutline'
import { useMemo, useState } from 'react'
import {
  useConnectorCatalogue,
  useConnectorInstances,
  useCreateConnectorInstance,
  useTestConnection,
  useUpdateConnectorInstance,
  useDeleteConnectorInstance,
} from '@/api/hooks'
import Divider from '@mui/material/Divider'
import { RateLimitFields } from '@/components/RateLimitFields'
import type { ConnectorInstance, RateLimit } from '@/api/types'
import { PageHeader } from '@/components/PageHeader'
import { QueryVariantsEditor } from '@/components/QueryVariantsEditor'
import { SchemaForm } from '@/components/SchemaForm'
import { EmptyState, ErrorPanel, Loading } from '@/components/Feedback'
import { info, muted, status } from '@/theme'
import type { ConnectorStatus } from '@/api/types'

export function ConnectorsPage() {
  const catalogue = useConnectorCatalogue()
  const instances = useConnectorInstances()
  const remove = useDeleteConnectorInstance()
  const test = useTestConnection()
  const [editing, setEditing] = useState<ConnectorInstance | null>(null)
  const [previewing, setPreviewing] = useState<ConnectorInstance | null>(null)
  const [creating, setCreating] = useState(false)

  if (catalogue.isLoading || instances.isLoading) return <Loading label="Loading connectors" />
  if (catalogue.error) return <ErrorPanel error={catalogue.error} />

  const rows = instances.data?.content ?? []

  return (
    <>
      <PageHeader
        title="Connectors"
        subtitle="Configured connections to the systems you move data between"
        actions={
          <Button startIcon={<AddIcon />} variant="contained" onClick={() => setCreating(true)}>
            New connection
          </Button>
        }
      />

      <ErrorPanel error={instances.error} />
      <ErrorPanel error={remove.error} />

      {rows.length === 0 ? (
        <EmptyState
          icon={<CableIcon />}
          title="No connections yet"
          description={`${catalogue.data?.length ?? 0} connector type(s) are installed. Create a connection to point one at a real system.`}
          action={
            <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreating(true)}>
              New connection
            </Button>
          }
        />
      ) : (
        <Paper sx={{ overflow: 'hidden' }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>NAME</TableCell>
                <TableCell>TYPE</TableCell>
                <TableCell>DIRECTION</TableCell>
                <TableCell>STATE</TableCell>
                <TableCell />
              </TableRow>
            </TableHead>
            <TableBody>
              {rows.map((instance) => (
                <TableRow key={instance.id} hover>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      {instance.name}
                    </Typography>
                    {instance.description && (
                      <Typography variant="caption">{instance.description}</Typography>
                    )}
                  </TableCell>
                  <TableCell>
                    <Chip label={instance.connectorType} size="small" variant="outlined" />
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" sx={{ color: muted }}>
                      {instance.direction}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <ConnectorStatusChip
                      status={instance.status}
                      error={instance.lastTestError}
                      lastTestedAt={instance.lastTestedAt}
                      testing={test.isPending && test.variables === instance.id}
                    />
                  </TableCell>
                  <TableCell align="right">
                    <Button
                      size="small"
                      startIcon={<PlugIcon />}
                      disabled={test.isPending}
                      onClick={() => test.mutate(instance.id)}
                    >
                      Test
                    </Button>
                    {/* Only where there is something to read. A sink has no rows to show, and an
                        enabled button that always answers "this is a destination" is worse than
                        an absent one. */}
                    {instance.direction !== 'SINK' && (
                      <Button
                        size="small"
                        startIcon={<PreviewIcon />}
                        onClick={() => setPreviewing(instance)}
                      >
                        Preview
                      </Button>
                    )}
                    <Button size="small" startIcon={<EditIcon />} onClick={() => setEditing(instance)}>
                      Edit
                    </Button>
                    <Tooltip title="Delete">
                      <IconButton size="small" onClick={() => remove.mutate(instance.id)}>
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

      <Typography variant="h2" sx={{ mt: 5, mb: 1 }}>
        Installed connector types
      </Typography>
      <Typography variant="body2" sx={{ color: muted, mb: 2 }}>
        Discovered from the workers at startup. This list reflects what is actually loaded, not a
        hardcoded catalogue — a type missing here is one no pipeline can use.
      </Typography>

      <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
        {catalogue.data?.map((spec) => (
          <Paper key={spec.type} sx={{ p: 2, minWidth: 240, flex: '1 1 240px' }}>
            <Typography variant="h3">{spec.displayName}</Typography>
            <Typography variant="caption" sx={{ display: 'block', mb: 1 }}>
              {spec.type} · v{spec.version} · {spec.direction}
            </Typography>
            <Typography variant="body2" sx={{ color: muted }}>
              {spec.description}
            </Typography>
          </Paper>
        ))}
      </Stack>

      {creating && <CreateConnectorDialog onClose={() => setCreating(false)} />}
      {editing && (
        <CreateConnectorDialog editing={editing} onClose={() => setEditing(null)} />
      )}
      {previewing && (
        <PreviewDialog
          open
          connectorInstanceId={previewing.id}
          name={previewing.name}
          onClose={() => setPreviewing(null)}
        />
      )}
    </>
  )
}

function ConnectorStatusChip({
  status: state,
  error,
  lastTestedAt,
  testing,
}: {
  status: ConnectorStatus
  error: string | null
  /**
   * When the status was last established.
   *
   * Shown because the status is a claim about the past, not about now. ACTIVE means a test passed
   * at some point — possibly weeks ago, against a system that has since moved, had its credentials
   * rotated, or been firewalled off. Nothing re-verifies it, so a green chip on its own invites
   * exactly the wrong conclusion: that the connection works.
   */
  lastTestedAt?: string | null
  /**
   * A test is in flight right now.
   *
   * Deliberately not a persisted status. A stored TESTING would survive a restart, and a process
   * that died mid-test would leave a row claiming to be testing forever with nothing left to
   * finish it. This is transient state about a request, so it belongs to the request.
   */
  testing?: boolean
}) {
  if (testing) {
    return (
      <Chip
        size="small"
        variant="outlined"
        icon={<CircularProgress size={12} thickness={6} />}
        label="Testing…"
        sx={{ color: info.light, borderColor: info.light }}
      />
    )
  }

  // Colour plus the word, never colour alone.
  const appearance: Record<ConnectorStatus, { color: string; label: string }> = {
    UNTESTED: { color: muted, label: 'Untested' },
    ACTIVE: { color: status.good, label: 'Active' },
    FAILED: { color: status.critical, label: 'Failed' },
    DISABLED: { color: muted, label: 'Disabled' },
  }
  let { color, label } = appearance[state]

  // A pass old enough to be worthless is drawn as unverified rather than green. Six hours is not
  // a meaningful boundary in itself — nothing changes at six hours — but any boundary is better
  // than presenting a three-week-old success as a statement about the present.
  const testedAt = lastTestedAt ? new Date(lastTestedAt) : null
  const ageHours = testedAt ? (Date.now() - testedAt.getTime()) / 3_600_000 : null
  const stale = state === 'ACTIVE' && ageHours !== null && ageHours > 6

  if (stale) {
    color = muted
    label = 'Active'
  }

  const chip = (
    <Chip
      size="small"
      label={label}
      variant="outlined"
      sx={{ color, borderColor: color, backgroundColor: `${color}14` }}
    />
  )

  const when = testedAt
    ? `Last tested ${testedAt.toLocaleString()}`
    : 'Never tested — the status is a guess'

  // Both, when there is both. The error alone left "when did this break" unanswerable, and the
  // timestamp alone left the reason a click away.
  const explanation = error ? `${when}. ${error}` : when

  return (
    <Tooltip title={explanation}>
      <Box sx={{ display: 'inline-flex', flexDirection: 'column', gap: 0.25 }}>
        {chip}
        {testedAt && (
          <Typography variant="caption" sx={{ color: muted, fontSize: 10.5, lineHeight: 1.2 }}>
            {relativeAge(testedAt)}
          </Typography>
        )}
      </Box>
    </Tooltip>
  )
}

/** "4m ago", "3h ago", "12d ago" — enough to judge whether the status still means anything. */
function relativeAge(when: Date) {
  const minutes = Math.floor((Date.now() - when.getTime()) / 60_000)
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}

function CreateConnectorDialog({
  onClose,
  editing,
}: {
  onClose: () => void
  editing?: ConnectorInstance
}) {
  const catalogue = useConnectorCatalogue()
  const create = useCreateConnectorInstance()
  const update = useUpdateConnectorInstance()

  const [type, setType] = useState(editing?.connectorType ?? '')
  const [name, setName] = useState(editing?.name ?? '')
  const [description, setDescription] = useState(editing?.description ?? '')
  const [direction, setDirection] = useState(editing?.direction ?? 'BOTH')
  const [config, setConfig] = useState<Record<string, unknown>>(editing?.config ?? {})
  // Seeded from the connector being edited, like every other field here. It was not, and the
  // consequence was silent and severe: opening a connection to rename it, or to correct a URL,
  // submitted an empty secret map and deleted every credential reference it had. Nothing said so.
  // The next run failed with "no 'token' secret is configured" — naming a field the person had
  // never touched, on a connection that had worked ten minutes earlier.
  //
  // Safe to seed because these are references and not values: the store holds `env:DBX_TOKEN`,
  // never the token. There is nothing secret here to put into a form.
  const [secrets, setSecrets] = useState<Record<string, string>>(editing?.secretRefs ?? {})
  const [rateLimit, setRateLimit] = useState<RateLimit | null>(editing?.rateLimit ?? null)

  const spec = useMemo(
    () => catalogue.data?.find((candidate) => candidate.type === type),
    [catalogue.data, type],
  )

  const submit = () => {
    if (editing) {
      // The connector type is deliberately absent: a different type has a different configuration
      // shape, so every stored value would become meaningless. That is a new connection.
      update.mutate(
        { id: editing.id, name, direction, config, secretRefs: secrets, description, rateLimit },
        { onSuccess: onClose },
      )
      return
    }
    create.mutate(
      { name, connectorType: type, direction, config, secretRefs: secrets, description, rateLimit },
      { onSuccess: onClose },
    )
  }

  return (
    <Dialog open onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{editing ? `Edit ${editing.name}` : 'New connection'}</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2.5} sx={{ pt: 1 }}>
          <TextField
            select
            label="Connector type"
            value={type}
            onChange={(event) => {
              setType(event.target.value)
              // Configuration is schema-specific, so carrying it across a type change would leave
              // fields the new connector has never heard of.
              setConfig({})
              setSecrets({})
            }}
            size="small"
            fullWidth
          >
            {catalogue.data?.map((candidate) => (
              <MenuItem key={candidate.type} value={candidate.type}>
                {candidate.displayName}
              </MenuItem>
            ))}
          </TextField>

          {spec && (
            <>
              <TextField
                label="Name *"
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="finance-postgres-replica"
                helperText="How you will recognise this connection when wiring a pipeline"
                size="small"
                fullWidth
              />
              <TextField
                label="Description"
                value={description}
                onChange={(event) => setDescription(event.target.value)}
                size="small"
                fullWidth
              />
              <TextField
                select
                label="Direction"
                value={direction}
                onChange={(event) => setDirection(event.target.value as 'SOURCE' | 'SINK' | 'BOTH')}
                size="small"
                fullWidth
                helperText="Whether this connection may be used to read, to write, or both"
              >
                {spec.direction === 'BOTH' && <MenuItem value="BOTH">Source and sink</MenuItem>}
                {spec.direction !== 'SINK' && <MenuItem value="SOURCE">Source only</MenuItem>}
                {spec.direction !== 'SOURCE' && <MenuItem value="SINK">Sink only</MenuItem>}
              </TextField>

              <Box sx={{ pt: 1 }}>
                <SchemaForm
                  direction={direction as 'SOURCE' | 'SINK' | 'BOTH'}
                  schema={spec.configSchema}
                  secretFields={spec.secretFields}
                  config={config}
                  secrets={secrets}
                  onConfigChange={(field, value) =>
                    setConfig((current) => ({ ...current, [field]: value }))
                  }
                  onSecretChange={(field, value) =>
                    setSecrets((current) => ({ ...current, [field]: value }))
                  }
                  errors={create.error instanceof Error ? undefined : undefined}
                />
              </Box>

              {/*
                Only for an instance that will read. A sink has no selection to name, and offering
                to write queries for one would be an invitation to configure something that never
                runs.
              */}
              {direction !== 'SINK' && (
                <>
                  <Divider />
                  <QueryVariantsEditor
                    schema={spec.configSchema}
                    config={config}
                    onChange={(patch) => setConfig((current) => ({ ...current, ...patch }))}
                  />
                </>
              )}

              <Divider />

              <RateLimitFields value={rateLimit} onChange={setRateLimit} />
            </>
          )}

          <ErrorPanel error={create.error ?? update.error} />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          onClick={submit}
          disabled={!type || !name || create.isPending || update.isPending}
        >
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
