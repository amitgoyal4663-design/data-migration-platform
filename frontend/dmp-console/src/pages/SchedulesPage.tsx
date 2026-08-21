import { useThemeMode } from '@/store'
import PlayIcon from '@mui/icons-material/PlayArrowOutlined'
import Editor from '@monaco-editor/react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogTitle from '@mui/material/DialogTitle'
import IconButton from '@mui/material/IconButton'
import MenuItem from '@mui/material/MenuItem'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Switch from '@mui/material/Switch'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import TextField from '@mui/material/TextField'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import AddIcon from '@mui/icons-material/AddOutlined'
import DeleteIcon from '@mui/icons-material/DeleteOutlined'
import EditIcon from '@mui/icons-material/EditOutlined'
import { useState } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import {
  useCreateSchedule,
  usePreviewWindow,
  useRunParameterNames,
  useDeleteSchedule,
  usePipelines,
  useSchedules,
  useSetScheduleEnabled,
  useUpdateSchedule,
} from '@/api/hooks'
import { PageHeader } from '@/components/PageHeader'
import { ErrorPanel, Loading } from '@/components/Feedback'
import { muted, tabular } from '@/theme'
import type { Schedule } from '@/api/types'

/**
 * Common rules, offered so nobody has to remember Quartz's field order.
 *
 * <p>Six fields, not the five of Unix cron — the difference is the leading seconds field, and
 * pasting a Unix expression here is the mistake people make first.
 */
const PRESETS: { label: string; cron: string }[] = [
  { label: 'Every 5 minutes', cron: '0 0/5 * * * ?' },
  { label: 'Every hour, on the hour', cron: '0 0 * * * ?' },
  { label: 'Every day at 03:00', cron: '0 0 3 * * ?' },
  { label: 'Every weekday at 02:30', cron: '0 30 2 ? * MON-FRI' },
  { label: 'Every Monday at 06:00', cron: '0 0 6 ? * MON' },
  { label: 'First of the month at 01:00', cron: '0 0 1 1 * ?' },
  { label: 'Every 15 minutes', cron: '0 0/15 * * * ?' },
]

/** The browser's own zone first, because it is what most people mean. */
const LOCAL_ZONE = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'

const ZONES: string[] = Array.from(
  new Set([
    LOCAL_ZONE,
    'UTC',
    'Asia/Kolkata',
    'Europe/London',
    'America/New_York',
    'America/Los_Angeles',
    'Europe/Berlin',
    'Asia/Singapore',
    'Australia/Sydney',
  ]),
)

/**
 * Common windows, as scripts.
 *
 * <p>The presets exist because almost every schedule wants one of these four, and asking someone
 * to write date arithmetic for "the previous day" is friction with no payoff. The editor exists
 * because the fifth case always arrives — the previous business day, the month so far, a date in
 * a format one warehouse insists on — and a fixed list of four would send that person back here
 * asking for a fifth.
 *
 * <p>Choosing a preset writes it into the editor rather than hiding it. What runs at 3am is then
 * the thing on screen, and adjusting it is editing a line rather than escaping a mode.
 */
const WINDOW_PRESETS = [
  {
    key: 'day',
    label: 'The previous whole day',
    detail: 'Fires whenever you like; covers midnight to midnight of the day before.',
    script: "const to = fireTime.startOf('day')\nreturn { from: to.minus({ days: 1 }), to: to }",
  },
  {
    key: 'hour',
    label: 'The previous whole hour',
    detail: 'Covers the hour that had just finished when the run was due.',
    script: "const to = fireTime.startOf('hour')\nreturn { from: to.minus({ hours: 1 }), to: to }",
  },
  {
    key: 'overlap',
    label: 'The previous hour, with 10 minutes of overlap',
    detail:
      'Re-reads the last ten minutes so rows written late are still picked up. Needs an upsert ' +
      'sink, or the overlap duplicates.',
    script:
      "const to = fireTime.startOf('hour')\n" +
      "return { from: to.minus({ hours: 1, minutes: 10 }), to: to }",
  },
  {
    key: 'week',
    label: 'The previous whole week',
    detail: 'Monday to Monday, in the schedule’s timezone.',
    script: "const to = fireTime.startOf('week')\nreturn { from: to.minus({ weeks: 1 }), to: to }",
  },
] as const

export function SchedulesPage() {
  const schedules = useSchedules()
  const pipelines = usePipelines()
  const setEnabled = useSetScheduleEnabled()
  const remove = useDeleteSchedule()

  const [editing, setEditing] = useState<Schedule | null>(null)
  const [creating, setCreating] = useState(false)

  if (schedules.isLoading) return <Loading />
  if (schedules.error) return <ErrorPanel error={schedules.error} />

  const rows = schedules.data ?? []
  const pipelineName = (id: string) =>
    pipelines.data?.content.find((pipeline) => pipeline.id === id)?.name ?? id.slice(0, 8)

  return (
    <>
      <PageHeader
        title="Schedules"
        subtitle="Run a pipeline automatically, on a recurring rule"
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreating(true)}>
            New schedule
          </Button>
        }
      />

      <ErrorPanel error={setEnabled.error ?? remove.error} />

      {rows.length === 0 ? (
        <Alert severity="info">
          No schedules yet. A schedule starts a run of a published pipeline on a recurring rule —
          the run itself behaves exactly as if you had pressed Run.
        </Alert>
      ) : (
        <Paper sx={{ overflow: 'auto' }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>NAME</TableCell>
                <TableCell>PIPELINE</TableCell>
                <TableCell>RULE</TableCell>
                <TableCell>NEXT RUN</TableCell>
                <TableCell>LAST RUN</TableCell>
                <TableCell align="center">ON</TableCell>
                <TableCell align="right" />
              </TableRow>
            </TableHead>
            <TableBody>
              {rows.map((schedule) => (
                <TableRow key={schedule.id} hover>
                  <TableCell>
                    <Typography variant="body2">{schedule.name}</Typography>
                    {schedule.description && (
                      <Typography variant="caption" sx={{ color: muted }}>
                        {schedule.description}
                      </Typography>
                    )}
                  </TableCell>

                  <TableCell>
                    <RouterLink
                      to={`/pipelines/${schedule.pipelineId}`}
                      style={{ color: 'inherit' }}
                    >
                      {pipelineName(schedule.pipelineId)}
                    </RouterLink>
                  </TableCell>

                  <TableCell>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Chip size="small" label={schedule.cronExpression} sx={tabular} />
                      <Typography variant="caption" sx={{ color: muted }}>
                        {schedule.timezone}
                      </Typography>
                    </Stack>
                  </TableCell>

                  {/*
                    The number people open this screen to read. A cron expression is not something
                    most of us can evaluate in our heads, and seeing "tomorrow 03:00" is what
                    catches a mistyped rule before it costs a missed nightly load.
                  */}
                  <TableCell sx={tabular}>
                    {schedule.enabled && schedule.nextFireAt ? (
                      <Tooltip title={new Date(schedule.nextFireAt).toString()} arrow>
                        <span>{formatWhen(schedule.nextFireAt)}</span>
                      </Tooltip>
                    ) : (
                      <Typography variant="caption" sx={{ color: muted }}>
                        paused
                      </Typography>
                    )}
                  </TableCell>

                  <TableCell sx={tabular}>
                    {schedule.lastFiredAt ? (
                      formatWhen(schedule.lastFiredAt)
                    ) : (
                      <Typography variant="caption" sx={{ color: muted }}>
                        never
                      </Typography>
                    )}
                  </TableCell>

                  <TableCell align="center">
                    {/*
                      A switch rather than delete-and-recreate. Pausing during an incident is
                      common, and deleting would lose the rule, its history and whatever the
                      description said about why it exists.
                    */}
                    <Switch
                      size="small"
                      checked={schedule.enabled}
                      onChange={(event) =>
                        setEnabled.mutate({ id: schedule.id, enabled: event.target.checked })
                      }
                    />
                  </TableCell>

                  <TableCell align="right">
                    <IconButton size="small" onClick={() => setEditing(schedule)}>
                      <EditIcon fontSize="small" />
                    </IconButton>
                    <IconButton
                      size="small"
                      color="error"
                      onClick={() => {
                        if (window.confirm(`Delete the schedule "${schedule.name}"?`)) {
                          remove.mutate(schedule.id)
                        }
                      }}
                    >
                      <DeleteIcon fontSize="small" />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Paper>
      )}

      {(creating || editing) && (
        <ScheduleDialog
          schedule={editing}
          onClose={() => {
            setCreating(false)
            setEditing(null)
          }}
        />
      )}
    </>
  )
}

function ScheduleDialog({
  schedule,
  onClose,
}: {
  schedule: Schedule | null
  onClose: () => void
}) {
  const pipelines = usePipelines()
  const create = useCreateSchedule()
  const update = useUpdateSchedule(schedule?.id ?? '')

  const [pipelineId, setPipelineId] = useState(schedule?.pipelineId ?? '')
  const [name, setName] = useState(schedule?.name ?? '')
  const [cron, setCron] = useState(schedule?.cronExpression ?? '0 0 3 * * ?')
  const [timezone, setTimezone] = useState(schedule?.timezone ?? LOCAL_ZONE)
  // A new schedule opens with the commonest window already written, so the shape is visible
  // rather than something to be discovered from documentation. Editing an existing one shows what
  // it actually has, including nothing.
  const [windowScript, setWindowScript] = useState(
    schedule ? (schedule.windowScript ?? '') : WINDOW_PRESETS[0].script,
  )
  const [description, setDescription] = useState(schedule?.description ?? '')

  const editingExisting = schedule !== null

  // What the chosen pipeline's query actually asks for. Everything about the script field follows
  // from this: whether to prefill it, what to suggest, and whether what it returns matches.
  const expected = useRunParameterNames(pipelineId || undefined)
  const expectedNames = expected.data?.names ?? []

  const mutation = editingExisting ? update : create
  const runnable = (pipelines.data?.content ?? []).filter((pipeline) => pipeline.runnable)

  const submit = () => {
    const body = {
      name,
      cronExpression: cron,
      timezone,
      // Empty means no parameters, which is how every schedule behaved before window scripts
      // existed. Sending "" rather than null would store a script that returns nothing.
      windowScript: windowScript.trim() || null,
      description: description || null,
    }
    if (editingExisting) {
      update.mutate(body, { onSuccess: onClose })
    } else {
      create.mutate({ ...body, pipelineId }, { onSuccess: onClose })
    }
  }

  const valid = name.trim() && cron.trim() && timezone && (editingExisting || pipelineId)

  return (
    <Dialog open onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{editingExisting ? 'Edit schedule' : 'New schedule'}</DialogTitle>

      <DialogContent dividers>
        <Stack spacing={2.5} sx={{ pt: 1 }}>
          <ErrorPanel error={mutation.error} />

          {!editingExisting && (
            <TextField
              select
              label="Pipeline"
              value={pipelineId}
              onChange={(event) => setPipelineId(event.target.value)}
              size="small"
              fullWidth
              helperText={
                runnable.length === 0
                  ? 'No pipeline has a published version yet. A schedule on one would fail every time it fired.'
                  : 'Only pipelines with a published version can be scheduled'
              }
              disabled={runnable.length === 0}
            >
              {runnable.map((pipeline) => (
                <MenuItem key={pipeline.id} value={pipeline.id}>
                  {pipeline.name} (v{pipeline.publishedVersion})
                </MenuItem>
              ))}
            </TextField>
          )}

          <TextField
            label="Name"
            value={name}
            onChange={(event) => setName(event.target.value)}
            size="small"
            fullWidth
            helperText="How you will recognise it in the list"
          />

          <Box>
            <TextField
              select
              label="How often"
              value={PRESETS.find((preset) => preset.cron === cron)?.cron ?? 'custom'}
              onChange={(event) => {
                if (event.target.value !== 'custom') setCron(event.target.value)
              }}
              size="small"
              fullWidth
              sx={{ mb: 1.5 }}
            >
              {PRESETS.map((preset) => (
                <MenuItem key={preset.cron} value={preset.cron}>
                  {preset.label}
                </MenuItem>
              ))}
              <MenuItem value="custom">Custom — write the expression myself</MenuItem>
            </TextField>

            <TextField
              label="Cron expression"
              value={cron}
              onChange={(event) => setCron(event.target.value)}
              size="small"
              fullWidth
              slotProps={{ input: { sx: tabular } }}
              helperText={
                <>
                  Six fields: second, minute, hour, day of month, month, day of week. This is{' '}
                  <strong>not</strong> five-field Unix cron — <code>0 3 * * *</code> will be
                  rejected; write <code>0 0 3 * * ?</code> for 03:00 daily.
                </>
              }
            />
          </Box>

          {/*
            Required, never defaulted to the server's zone. "Every day at 03:00" is not a fact
            until a zone is named: the same rule fires at different moments in different zones,
            and twice or never on the day the clocks change.
          */}
          <TextField
            select
            label="Timezone"
            value={timezone}
            onChange={(event) => setTimezone(event.target.value)}
            size="small"
            fullWidth
            helperText="The hour above is interpreted in this zone, including across daylight saving"
          >
            {ZONES.map((zone) => (
              <MenuItem key={zone} value={zone}>
                {zone}
              </MenuItem>
            ))}
          </TextField>

          <WindowScriptField
            value={windowScript}
            onChange={setWindowScript}
            cron={cron}
            timezone={timezone}
            expected={expectedNames}
          />

          <TextField
            label="Why this exists"
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            size="small"
            fullWidth
            multiline
            minRows={2}
            helperText="Optional, and worth writing — the person who finds this at 3am may not be you"
          />

          <Alert severity="info" sx={{ '& .MuiAlert-message': { fontSize: 12.5 } }}>
            If the platform is down when this is due, the run is skipped rather than fired late.
            Several catch-up migrations starting at once on recovery is worse than one missed load.
          </Alert>
        </Stack>
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          onClick={submit}
          disabled={!valid || mutation.isPending}
        >
          {mutation.isPending ? 'Saving…' : editingExisting ? 'Save' : 'Create'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}

/** Relative for anything close, absolute beyond a day — "in 4 hours" beats a timestamp to decode. */
function formatWhen(iso: string) {
  const when = new Date(iso)
  const deltaMinutes = Math.round((when.getTime() - Date.now()) / 60_000)
  const magnitude = Math.abs(deltaMinutes)

  if (magnitude < 1) return 'now'
  if (magnitude < 60) return relative(deltaMinutes, magnitude, 'minute')
  if (magnitude < 60 * 24) return relative(deltaMinutes, Math.round(magnitude / 60), 'hour')

  return when.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function relative(delta: number, amount: number, unit: string) {
  const plural = amount === 1 ? unit : `${unit}s`
  return delta > 0 ? `in ${amount} ${plural}` : `${amount} ${plural} ago`
}


/**
 * The script that decides which range of data each firing covers.
 *
 * <p>The preview is the important half. A window script is only wrong in ways you discover the
 * following morning, once a run has already covered the wrong day — seeing the next four firings
 * before saving turns that into a five-second check.
 */
function WindowScriptField({
  value,
  onChange,
  cron,
  timezone,
  expected,
}: {
  value: string
  onChange: (value: string) => void
  cron: string
  timezone: string
  expected: string[]
}) {
  const mode = useThemeMode((state) => state.mode)
  const preview = usePreviewWindow()
  const enabled = value.trim() !== ''

  // The query wants :since and :until; the script returns from and to. Both halves are individually
  // valid and the run fails at 3am with a message about a missing parameter. Comparing the two
  // lists is free once a preview has been run, and turns that into a warning while you are looking.
  const produced = preview.data?.firings.find((firing) => !firing.error)?.parameters
  const unmet = produced ? expected.filter((name) => !(name in produced)) : []

  return (
    <Box>
      <Typography variant="caption" sx={{ display: 'block', fontWeight: 600 }}>
        What each run covers
      </Typography>
      <Typography variant="caption" sx={{ display: 'block', color: muted, mb: 1 }}>
        {expected.length > 0 ? (
          <>
            This pipeline&apos;s query expects{' '}
            {expected.map((name) => (
              <code key={name}>:{name} </code>
            ))}
            — return {expected.length === 1 ? 'that' : 'those'} below. A starting point is filled in;
            adjust it or pick another shape above.
          </>
        ) : (
          <>
            Sets the values your query&apos;s <code>:placeholders</code> are given. This
            pipeline&apos;s query has none yet, so what this returns is recorded on each run and
            otherwise ignored — add <code>:from</code> and <code>:to</code> to the query to use it,
            or clear this to read everything every time.
          </>
        )}
      </Typography>

      <Stack direction="row" spacing={1} sx={{ mb: 1 }} flexWrap="wrap" useFlexGap>
        {WINDOW_PRESETS.map((preset) => (
          <Tooltip key={preset.key} title={preset.detail}>
            <Chip
              size="small"
              label={preset.label}
              variant={value.trim() === preset.script ? 'filled' : 'outlined'}
              onClick={() => {
                onChange(preset.script)
                preview.reset()
              }}
            />
          </Tooltip>
        ))}
        {enabled && (
          <Chip
            size="small"
            label="Clear"
            variant="outlined"
            color="error"
            onClick={() => {
              onChange('')
              preview.reset()
            }}
          />
        )}
      </Stack>

      <Box sx={{ border: 1, borderColor: 'divider', borderRadius: 1, overflow: 'hidden' }}>
        <Editor
          height="120px"
          defaultLanguage="javascript"
          theme={mode === 'dark' ? 'vs-dark' : 'light'}
          value={value}
          onChange={(next) => onChange(next ?? '')}
          options={{
            minimap: { enabled: false },
            fontSize: 13,
            lineNumbers: 'off',
            scrollBeyondLastLine: false,
            tabSize: 2,
            automaticLayout: true,
            folding: false,
            // The editor is three lines of arithmetic, not a program. Chrome left on for a script
            // this short reads as clutter and makes the field look bigger than the job it does.
            glyphMargin: false,
            lineDecorationsWidth: 0,
            overviewRulerLanes: 0,
          }}
        />
      </Box>

      <Typography variant="caption" sx={{ display: 'block', color: muted, mt: 0.75 }}>
        <code>fireTime</code> is when the run was due. There is no clock available, so the same run
        always computes the same range — which is what lets a retry repeat a window rather than
        move it.
      </Typography>

      <Stack direction="row" spacing={1} alignItems="center" sx={{ mt: 1 }}>
        <Button
          size="small"
          startIcon={<PlayIcon />}
          onClick={() => preview.mutate({ cronExpression: cron, timezone, windowScript: value })}
          disabled={!enabled || !cron.trim() || preview.isPending}
        >
          {preview.isPending ? 'Checking…' : 'Preview next runs'}
        </Button>
      </Stack>

      <ErrorPanel error={preview.error} />

      {unmet.length > 0 && (
        <Alert severity="warning" sx={{ mt: 1, '& .MuiAlert-message': { fontSize: 12.5 } }}>
          The query expects {unmet.map((name) => `:${name}`).join(', ')}, which this script does not
          return. Runs would fail asking for {unmet.length === 1 ? 'it' : 'them'} — rename what the
          script returns, or change the placeholder in the query.
        </Alert>
      )}

      {preview.data && (
        <Paper variant="outlined" sx={{ mt: 1, p: 1.25 }}>
          <Stack spacing={0.75}>
            {preview.data.firings.map((firing) => (
              <Stack key={firing.firesAt} direction="row" spacing={1} alignItems="baseline">
                <Typography variant="caption" sx={{ color: muted, ...tabular, minWidth: 150 }}>
                  {new Date(firing.firesAt).toLocaleString()}
                </Typography>
                {firing.error ? (
                  // Per firing rather than for the whole preview: a script can be right on most
                  // days and wrong on the first of the month, and seeing which is the whole point.
                  <Typography variant="caption" sx={{ color: 'error.main' }}>
                    {firing.error}
                  </Typography>
                ) : (
                  <Typography variant="caption" sx={tabular}>
                    {Object.entries(firing.parameters)
                      .map(([name, bound]) => `${name} = ${bound}`)
                      .join('   ')}
                  </Typography>
                )}
              </Stack>
            ))}
          </Stack>
        </Paper>
      )}
    </Box>
  )
}
