import Alert from '@mui/material/Alert'
import AlertTitle from '@mui/material/AlertTitle'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Grid from '@mui/material/Grid2'
import LinearProgress from '@mui/material/LinearProgress'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Tab from '@mui/material/Tab'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Tabs from '@mui/material/Tabs'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import PauseIcon from '@mui/icons-material/PauseOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import Menu from '@mui/material/Menu'
import MenuItem from '@mui/material/MenuItem'
import ListItemText from '@mui/material/ListItemText'
import Divider from '@mui/material/Divider'
import IconButton from '@mui/material/IconButton'
import MoreVertIcon from '@mui/icons-material/MoreVert'
import ListItemIcon from '@mui/material/ListItemIcon'
import ReplayIcon from '@mui/icons-material/ReplayOutlined'
import DownloadIcon from '@mui/icons-material/DownloadOutlined'
import RestartAltIcon from '@mui/icons-material/RestartAltOutlined'
import StopIcon from '@mui/icons-material/StopOutlined'
import { useState } from 'react'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import Link from '@mui/material/Link'
import {
  useChunkSummary,
  useChunks,
  useErrorGroups,
  useReplayRun,
  useRetryRun,
  useRun,
  useRunControl,
  useRunErrors,
  useStartRun,
  useVersion,
} from '@/api/hooks'
import { api } from '@/api/client'
import { RetryDialog, type RetryTarget } from '@/components/RetryDialog'
import { ReplayDialog } from '@/components/ReplayDialog'
import { ReconciliationPanel } from '@/components/ReconciliationPanel'
import { RunTimeline } from '@/components/RunTimeline'
import { PageHeader } from '@/components/PageHeader'
import { StatTile } from '@/components/StatTile'
import { ChunkStateChip, RunStateChip } from '@/components/StateChip'
import { ErrorPanel, Loading } from '@/components/Feedback'
import { formatDuration } from './DashboardPage'
import { muted, tabular } from '@/theme'
import { MoreOnScroll } from '@/components/MoreOnScroll'

/**
 * A run's parameters in the shape the start endpoint takes.
 *
 * <p>They arrive as arbitrary JSON — a window is two strings, but a parameter could be a number or
 * a boolean — and go back out as strings, which is what the source binds. Converting here rather
 * than widening the request type keeps the asymmetry in one place.
 */
function asStringMap(parameters: Record<string, unknown> | null | undefined) {
  if (!parameters) return undefined
  return Object.fromEntries(
    Object.entries(parameters).map(([name, value]) => [name, String(value)]),
  )
}

export function RunDetailPage() {
  const { runId = '' } = useParams()
  const run = useRun(runId)
  const [tab, setTab] = useState(0)

  // The frozen version this run executed. Fetched here because a run's numbers are unreadable
  // without the settings that produced them: 40 chunks is expected or alarming depending entirely
  // on the rows-per-chunk it ran with, and that was previously nowhere on this page.
  const version = useVersion(run.data?.pipelineId ?? '', run.data?.pipelineVersionId ?? '')

  const live = Boolean(run.data && !run.data.terminal)
  const chunks = useChunks(runId, live)
  const summary = useChunkSummary(runId, live)
  const errors = useRunErrors(runId)
  const groups = useErrorGroups(runId)
  const control = useRunControl(runId)
  const retry = useRetryRun(runId)
  const replay = useReplayRun(runId)
  const startRun = useStartRun()
  const navigate = useNavigate()
  const [retryTarget, setRetryTarget] = useState<RetryTarget | null>(null)
  const [replayOpen, setReplayOpen] = useState(false)

  if (run.isLoading) return <Loading />
  if (run.error) return <ErrorPanel error={run.error} />
  if (!run.data) return null

  const current = run.data
  const metrics = current.metrics

  // A transform changed the record count if anything was dropped, or if more came out than went
  // in. Both are normal; both make the plain read/written comparison misleading without saying so.
  const transformed =
    metrics.recordsFiltered > 0 || metrics.recordsProduced !== metrics.recordsRead

  // The pages loaded so far, which is what the table draws. Everything below that needs to be
  // true of the whole run comes from the summary instead — deriving it from these would count only
  // what happened to be on screen.
  // Not memoised: this sits below the early returns above, and a hook there runs on some renders
  // and not others — which is what "rendered more hooks than during the previous render" means.
  // Flattening a handful of ten-row pages is not worth the hazard.
  const loadedChunks = chunks.data?.pages.flatMap((page) => page.content) ?? []
  const totalChunks = chunks.data?.pages[0]?.totalElements ?? 0

  // Chunks generated as the run proceeds carry no range of their own. There is no denominator to
  // report a percentage against until the source is exhausted. A property of how the run was
  // planned, so the first chunk answers it for all of them.
  const lazilyChunked =
    (loadedChunks[0]?.spec as { _dmpOpenEnded?: boolean } | undefined)?._dmpOpenEnded === true

  const failedCount = summary.data?.failed ?? 0
  const unfinishedCount = summary.data?.unfinished ?? 0

  // Only a finished run can be retried, and only one with something left to do. A run where every
  // chunk succeeded has nothing to offer, and showing the button anyway would invite a click that
  // can only fail.
  const retryable =
    current.terminal &&
    current.state !== 'ARCHIVED' &&
    failedCount + unfinishedCount > 0

  // Replay is the other half, and answers a different question. Retry re-reads chunks that failed;
  // replay re-sends records rejected inside chunks that succeeded. A run can offer both, either, or
  // neither, so the two are judged separately rather than sharing a flag.
  //
  // Counted on payloads stored, not on records rejected, and the two are usually different. The
  // audit policy keeps a few examples per distinct fault, so twenty thousand records failing one
  // rule leave ten payloads behind — and only a stored payload can be sent anywhere. Offering to
  // replay the rejection count would promise nineteen thousand records that no longer exist.
  const rejectedCount = (groups.data ?? []).reduce((sum, group) => sum + group.count, 0)
  const replayableCount = (groups.data ?? []).reduce((sum, group) => sum + group.samplesStored, 0)
  const replayable = current.terminal && current.state !== 'ARCHIVED' && replayableCount > 0

  // A run somebody stopped is resumed, not retried. Same machinery underneath — a new run over the
  // chunks that did not finish, each from its checkpoint — but the word has to match the intent, or
  // the only button offered after a stop reads as though it were about repairing a failure.
  const stopped = current.state === 'STOPPED'

  const openRunRetry = () =>
    setRetryTarget({
      label: stopped
        ? `Resume ${unfinishedCount} unfinished chunk${unfinishedCount === 1 ? '' : 's'}`
        : `Retry ${failedCount} failed chunk${failedCount === 1 ? '' : 's'}`,
      chunkCount: failedCount,
      cancelledCount: unfinishedCount,
      recordsAtRisk: summary.data?.recordsAtRisk ?? 0,
      resuming: stopped,
    })

  const confirmRetry = (options: {
    from: 'CHECKPOINT' | 'CHUNK_START'
    scope: 'FAILED' | 'FAILED_AND_CANCELLED'
    acknowledgeDuplicates: boolean
  }) => {
    const chunkId = retryTarget?.chunkId
    const mutation = chunkId ? retry.chunk : retry.run
    const payload = chunkId ? { ...options, chunkId } : options

    // Navigating to the new run is the point: the retry is a separate run, and leaving the user
    // on the old page would look as though nothing happened.
    ;(mutation.mutate as (v: typeof payload, o: { onSuccess: (run: { id: string }) => void }) => void)(
      payload,
      {
        onSuccess: (created) => {
          setRetryTarget(null)
          navigate(`/runs/${created.id}`)
        },
      },
    )
  }

  return (
    <>
      <PageHeader
        breadcrumbs={[{ label: 'Runs', to: '/runs' }, { label: runId.slice(0, 8) }]}
        title={`Run of v${current.versionNumber}`}
        subtitle={
          <Stack direction="row" spacing={1} alignItems="center">
            <RunStateChip state={current.state} size="medium" />
            <Typography variant="body2" sx={{ color: muted }}>
              triggered {current.trigger.toLowerCase()}
              {current.triggeredBy ? ` by ${current.triggeredBy}` : ''}
            </Typography>
            {/*
              What this run actually covered. Shown beside the state rather than buried, because
              with a range on every run the run list becomes a coverage log — and a missing window
              is only visible if each run says which one it took.
            */}
            {current.parameters && Object.keys(current.parameters).length > 0 && (
              <Typography variant="body2" sx={{ color: muted }}>
                · covered{' '}
                {Object.entries(current.parameters)
                  .map(([name, value]) => `${name} ${String(value)}`)
                  .join(' → ')}
              </Typography>
            )}
            {current.retryOf && (
              <Typography variant="body2" sx={{ color: muted }}>
                · {current.trigger === 'REPLAY' ? 'replay of' : 'retry of'}{' '}
                <Link component={RouterLink} to={`/runs/${current.retryOf}`}>
                  run {current.retryOf.slice(0, 8)}
                </Link>
              </Typography>
            )}
          </Stack>
        }
        actions={
          <>
            {current.state === 'RUNNING' && (
              <Button
                startIcon={<PauseIcon />}
                onClick={() => control.pause.mutate()}
                disabled={control.pause.isPending}
              >
                Pause
              </Button>
            )}
            {current.state === 'PAUSED' && (
              <Button
                startIcon={<PlayArrowIcon />}
                onClick={() => control.resume.mutate()}
                disabled={control.resume.isPending}
              >
                Resume
              </Button>
            )}
            {current.active && (
              <Button
                startIcon={<StopIcon />}
                color="error"
                onClick={() => control.stop.mutate()}
                disabled={control.stop.isPending}
              >
                Stop
              </Button>
            )}
            {/*
              A fresh run of the same window, which is a different thing from Retry and from
              Replay and was previously not offered at all.

                Retry   re-runs the chunks that failed, inside the original run
                Replay  re-sends the records a successful chunk rejected
                Again   starts a new run, same parameters, current published version

              The last is what you want after fixing the pipeline or the source data: the earlier
              run is finished and its chunks are settled, so there is nothing in it to resume — but
              the window it covered still has to be migrated. Without this the parameters had to be
              copied out of one screen and typed into another.
            */}
            {current.terminal && current.state !== 'ARCHIVED' && (
              <Button
                startIcon={<RestartAltIcon />}
                onClick={() =>
                  startRun.mutate(
                    {
                      pipelineId: current.pipelineId,
                      parameters: asStringMap(current.parameters),
                    },
                    { onSuccess: (created) => navigate(`/runs/${created.id}`) },
                  )
                }
                disabled={startRun.isPending}
              >
                {startRun.isPending ? 'Starting…' : 'Run again'}
              </Button>
            )}
            {retryable && (
              <Button
                variant="contained"
                startIcon={stopped ? <PlayArrowIcon /> : <ReplayIcon />}
                onClick={openRunRetry}
              >
                {stopped ? 'Resume' : 'Retry'}
              </Button>
            )}
          </>
        }
      />

      <ErrorPanel
        error={
          control.stop.error ?? control.pause.error ?? control.resume.error ?? startRun.error
        }
      />

      {current.state === 'FAILED' && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <AlertTitle>{current.errorCode ?? 'Run failed'}</AlertTitle>
          {current.errorMessage}
        </Alert>
      )}

      {current.waitingOnExternalSystem && (
        <Alert severity="info" sx={{ mb: 3 }}>
          Waiting on the source system to prepare the data. Nothing is moving yet — this is normal
          for bulk exports, which can take minutes or hours.
        </Alert>
      )}

      {current.state === 'STOPPING' && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          Stopping. Chunks already running are finishing their current batch so the run stops at a
          point it can resume from cleanly.
        </Alert>
      )}

      {/* Records read but neither written nor rejected. This should be impossible, so it is shown
          loudly rather than as a metric among others. */}
      {metrics.unaccountedRecords > 0 && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <AlertTitle>{metrics.unaccountedRecords.toLocaleString()} records unaccounted for</AlertTitle>
          These reached the destination stage but were neither written nor recorded as rejected.
          Records a transform dropped are counted separately as filtered, so this is not that — it
          means records went missing. Do not treat this migration as complete.
        </Alert>
      )}

      {!current.terminal && (lazilyChunked || current.progress !== null) && (
        <Box sx={{ mb: 3 }}>
          {/*
            A lazily chunked run does not know how many chunks it will have — the next one is
            created when the current one finds more data. Its total therefore grows as it goes,
            which would make a percentage climb 1/2, 2/3, 3/4 and read as 50% done when it is
            five. An indeterminate bar and an honest count beat a confident wrong number.
          */}
          <LinearProgress
            variant={lazilyChunked ? 'indeterminate' : 'determinate'}
            value={lazilyChunked ? undefined : (current.progress ?? 0) * 100}
            sx={{ height: 8, borderRadius: 4 }}
          />
          <Typography variant="caption" sx={{ mt: 0.5, display: 'block', ...tabular }}>
            {lazilyChunked ? (
              <>
                {metrics.chunksCompleted} chunk{metrics.chunksCompleted === 1 ? '' : 's'} done ·{' '}
                {metrics.recordsRead.toLocaleString()} records read · total not known until the
                source runs dry
              </>
            ) : (
              <>
                {metrics.chunksCompleted} of {metrics.chunksTotal} chunks ·{' '}
                {Math.round((current.progress ?? 0) * 100)}%
              </>
            )}
          </Typography>
        </Box>
      )}

      {/*
        Filtered earns a tile only when a transform actually changed the count. Showing a permanent
        zero on every ordinary migration would train people to ignore the row that matters on the
        one run where a script dropped more than they meant it to.
      */}
      <Grid container spacing={2} sx={{ mb: 4 }}>
        <Grid size={{ xs: 6, md: transformed ? 2.4 : 3 }}>
          <StatTile label="Read" value={metrics.recordsRead} emphasis />
        </Grid>

        {transformed && (
          <Grid size={{ xs: 6, md: 2.4 }}>
            <StatTile
              label="Filtered"
              value={metrics.recordsFiltered}
              hint="Records a transform chose to drop. Deliberate, not lost — they are excluded from the destination on purpose."
            />
          </Grid>
        )}

        <Grid size={{ xs: 6, md: transformed ? 2.4 : 3 }}>
          <StatTile label="Written" value={metrics.recordsWritten} tone="good" emphasis />
        </Grid>
        <Grid size={{ xs: 6, md: transformed ? 2.4 : 3 }}>
          <StatTile
            label="Failed"
            value={metrics.recordsFailed}
            tone={metrics.recordsFailed > 0 ? 'critical' : 'default'}
            hint="Records that did not reach the destination and were not meant to be dropped — a script threw on them, or the destination refused them. Both are failures; only Filtered is deliberate. Listed under Rejected records."
          />
        </Grid>
        <Grid size={{ xs: 6, md: transformed ? 2.4 : 3 }}>
          <StatTile
            label="Throughput"
            value={
              metrics.throughputPerSecond === null
                ? '—'
                : Math.round(metrics.throughputPerSecond).toLocaleString()
            }
            unit="rec/s"
          />
        </Grid>
      </Grid>

      {transformed && <Reconciliation metrics={metrics} />}

      <SettingsUsed version={version.data} />

      <Typography variant="body2" sx={{ color: muted, mb: 3 }}>
        {current.durationSeconds !== null && <>Ran for {formatDuration(current.durationSeconds)}. </>}
        Started {current.startedAt ? new Date(current.startedAt).toLocaleString() : 'not yet'}.
      </Typography>

      <Tabs value={tab} onChange={(_, next) => setTab(next)} sx={{ mb: 2 }}>
        <Tab label={`Chunks (${totalChunks})`} sx={{ textTransform: 'none' }} />
        <Tab
          label={`Failures (${groups.data?.length ?? 0})`}
          sx={{ textTransform: 'none' }}
        />
        <Tab
          label={`Sample records (${errors.data?.length ?? 0})`}
          sx={{ textTransform: 'none' }}
        />
        <Tab label="Timeline" sx={{ textTransform: 'none' }} />
        <Tab label="Reconciliation" sx={{ textTransform: 'none' }} />
      </Tabs>

      {tab === 0 && (
        <ChunkTable
          runId={runId}
          chunks={loadedChunks}
          hasMore={chunks.hasNextPage}
          loadingMore={chunks.isFetchingNextPage}
          onLoadMore={() => void chunks.fetchNextPage()}
          total={totalChunks}
          onRetry={
            retryable
              ? (chunk) =>
                  setRetryTarget({
                    chunkId: chunk.id,
                    label: `Retry chunk ${chunk.index}`,
                    chunkCount: 1,
                    cancelledCount: 0,
                    recordsAtRisk: chunk.recordsWritten,
                  })
              : undefined
          }
        />
      )}
      {tab === 1 && (
        <FailureGroups
          groups={groups.data ?? []}
          onReplay={replayable ? () => setReplayOpen(true) : undefined}
          storedCount={replayableCount}
          rejectedCount={rejectedCount}
        />
      )}
      {tab === 2 && <RejectedRecords errors={errors.data ?? []} />}

      {/*
        * Loaded only when opened. A run of any size has far more stages than chunks, and fetching
        * two hundred of them behind a tab nobody clicked would slow the page that is actually
        * being looked at.
        */}
      {tab === 3 && <RunTimeline runId={runId} />}

      {/* Same reasoning as the timeline: an aggregation over the whole index, behind a tab. */}
      {tab === 4 && <ReconciliationPanel runId={runId} live={live} />}

      <RetryDialog
        open={retryTarget !== null}
        target={retryTarget}
        pending={retry.run.isPending || retry.chunk.isPending}
        error={retry.run.error ?? retry.chunk.error}
        onClose={() => setRetryTarget(null)}
        onConfirm={confirmRetry}
      />

      <ReplayDialog
        open={replayOpen}
        recordCount={rejectedCount}
        versionNumber={current.versionNumber}
        pending={replay.isPending}
        error={replay.error}
        onClose={() => {
          setReplayOpen(false)
          replay.reset()
        }}
        onConfirm={(request) =>
          replay.mutate(request, {
            onSuccess: (created) => {
              setReplayOpen(false)
              replay.reset()
              navigate(`/runs/${created.id}`)
            },
          })
        }
      />
    </>
  )
}

/**
 * A run's rejections, collapsed to the distinct faults behind them.
 *
 * Twenty thousand records failing one rule are one row here with a count beside it. The flat list
 * of sample payloads is still a tab away — but it is the fault, not the four-hundredth example of
 * it, that tells someone what to change.
 */
function FailureGroups({
  groups,
  onReplay,
  storedCount,
  rejectedCount,
}: {
  groups: import('@/api/types').ErrorGroup[]
  onReplay?: () => void
  storedCount: number
  rejectedCount: number
}) {
  // Only a stored payload can be sent again. When sampling kept fewer than were rejected, the
  // difference is stated rather than left for someone to discover after the replay finishes with a
  // number they were not expecting.
  const sampledAway = rejectedCount - storedCount
  if (groups.length === 0) {
    return (
      <Typography variant="body2" sx={{ color: muted, py: 3 }}>
        Nothing was rejected.
      </Typography>
    )
  }

  return (
    <>
      {onReplay && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
          <Button variant="outlined" size="small" onClick={onReplay}>
            Replay {storedCount.toLocaleString()} record{storedCount === 1 ? '' : 's'}
          </Button>
          <Typography variant="caption" sx={{ color: muted }}>
            {sampledAway > 0 ? (
              <>
                {rejectedCount.toLocaleString()} were rejected but only {storedCount.toLocaleString()}{' '}
                payload{storedCount === 1 ? ' was' : 's were'} kept — the audit policy samples them.
                Set samples per fault to 0 on this pipeline to make every rejection replayable.
              </>
            ) : (
              <>Sends them to the sink again through the same transforms. The source is not read.</>
            )}
          </Typography>
        </Box>
      )}
      <Paper sx={{ overflow: 'auto' }}>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell align="right">RECORDS</TableCell>
            <TableCell>CODE</TableCell>
            <TableCell>CAUSE</TableCell>
            <TableCell align="right">SAMPLES KEPT</TableCell>
            <TableCell>LAST SEEN</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {groups.map((group) => (
            <TableRow key={group.signature} hover>
              <TableCell align="right" sx={tabular}>
                {group.count.toLocaleString()}
              </TableCell>
              <TableCell>
                <Typography variant="caption" sx={tabular}>
                  {group.code}
                </Typography>
              </TableCell>
              <TableCell sx={{ maxWidth: 520 }}>
                <Tooltip
                  title={
                    <Typography variant="body2" sx={{ color: 'inherit' }}>
                      {group.message}
                    </Typography>
                  }
                >
                  <Typography variant="caption" sx={{ ...ellipsis, cursor: 'help' }}>
                    {group.message}
                  </Typography>
                </Tooltip>
              </TableCell>
              <TableCell align="right" sx={tabular}>
                <Tooltip
                  title={
                    group.samplesStored < group.count
                      ? 'The count is exact. Payloads are sampled per the pipeline audit policy, ' +
                        'so a fault hit by millions of records still costs only a few kilobytes.'
                      : ''
                  }
                >
                  <span>{group.samplesStored.toLocaleString()}</span>
                </Tooltip>
              </TableCell>
              <TableCell>
                <Typography variant="caption" sx={{ color: muted }}>
                  {new Date(group.lastSeenAt).toLocaleString()}
                </Typography>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      </Paper>
    </>
  )
}

/**
 * The settings this run actually executed with.
 *
 * <p>A run's numbers cannot be read without them. "40 chunks" is unremarkable or alarming entirely
 * depending on the rows-per-chunk it ran with, and "20,000 read, 15,000 written" is a bug or a
 * working filter depending on whether a transform was in the graph. All of it was already frozen
 * on the version and none of it was on this page, so the only way to find out was to open the
 * designer and hope you were looking at the same version.
 */
function SettingsUsed({ version }: { version?: import('@/api/types').PipelineVersion }) {
  if (!version) {
    return null
  }

  const chunking = version.chunkingPolicy
  const execution = version.executionPolicy

  const transforms = version.definition.nodes.filter(
    (node) => node.type === 'TRANSFORM' || node.type === 'BATCH_TRANSFORM',
  )

  const scheduling =
    execution.maxConcurrentChunks === 0
      ? 'In parallel'
      : execution.maxConcurrentChunks === 1
        ? 'One at a time'
        : `Up to ${execution.maxConcurrentChunks} at once`

  const settings: { label: string; value: string; hint?: string }[] = [
    { label: 'Version', value: `v${version.versionNumber}` },
    { label: 'Mode', value: version.mode.replace('_', ' ').toLowerCase() },
    {
      label: 'Read size',
      value: chunking.readFetchSize.toLocaleString(),
      hint: 'Records fetched per round trip to the source',
    },
    {
      label: 'Rows per chunk',
      value:
        execution.rowsPerChunk > 0
          ? execution.rowsPerChunk.toLocaleString()
          : `${(chunking.readFetchSize * 10).toLocaleString()} (derived)`,
      hint: 'How much source data one chunk covers',
    },
    { label: 'Chunks run', value: scheduling },
    {
      label: 'Attempts per chunk',
      value: String(execution.maxAttemptsPerChunk),
      hint: '1 means no retry after a failure',
    },
  ]

  return (
    <Paper variant="outlined" sx={{ p: 2, mb: 4 }}>
      <Typography variant="caption" sx={{ fontWeight: 700, letterSpacing: '0.06em' }}>
        SETTINGS THIS RUN USED
      </Typography>

      <Grid container spacing={2} sx={{ mt: 0.5 }}>
        {settings.map((setting) => (
          <Grid key={setting.label} size={{ xs: 6, sm: 4, md: 12 / 7 }}>
            <Tooltip title={setting.hint ?? ''} placement="top" arrow disableHoverListener={!setting.hint}>
              <Box>
                <Typography variant="caption" sx={{ color: muted, display: 'block' }}>
                  {setting.label}
                </Typography>
                <Typography variant="body2" sx={{ ...tabular, textTransform: 'none' }}>
                  {setting.value}
                </Typography>
              </Box>
            </Tooltip>
          </Grid>
        ))}
      </Grid>

      {transforms.length > 0 && (
        <Typography variant="caption" sx={{ color: muted, display: 'block', mt: 1.5 }}>
          Transformations applied:{' '}
          {transforms
            .map((node) => `${node.name}${node.type === 'BATCH_TRANSFORM' ? ' (per batch)' : ''}`)
            .join(', ')}
        </Typography>
      )}
    </Paper>
  )
}

/**
 * Download what the destination itself recorded about a chunk.
 *
 * <p>Shown only for a destination that decided asynchronously and still holds its own result
 * file — in practice a Salesforce bulk job. Every other sink answers while the batch is being
 * written, so there is nothing to fetch and no button appears.
 *
 * <p>The platform keeps none of this. It keeps the counts, which are permanent; the file lives in
 * the target system for as long as that system keeps it, which for Salesforce is about a week.
 * When it has gone the download says so rather than returning an empty file — "the org no longer
 * holds it" and "this chunk had no failures" are different sentences and must not look alike.
 */
/**
 * Everything you can do to one chunk, in one menu.
 *
 * <p>Retrying it and reading what the destination recorded about it are the two questions somebody
 * has when a chunk catches their eye, and they were two columns — one of which was empty for most
 * rows, because a retry is offered on failures and results only exist for an asynchronous
 * destination. Two mostly-blank columns cost width on every row to serve a few.
 *
 * <p>Items appear only when they mean something. A completed chunk cannot be retried — the engine
 * refuses it, since re-running it would rewrite records that are already correct — and a chunk
 * written synchronously has no remote job and therefore no file to fetch.
 */
function ChunkActions({
  runId,
  chunk,
  onRetry,
}: {
  runId: string
  chunk: import('@/api/types').Chunk
  onRetry?: (chunk: import('@/api/types').Chunk) => void
}) {
  const [anchor, setAnchor] = useState<HTMLElement | null>(null)
  const [busy, setBusy] = useState<string | null>(null)
  const [gone, setGone] = useState(false)

  const canRetry = Boolean(onRetry) && chunk.state !== 'COMPLETED'
  const hasResults = chunk.hasDestinationResults

  if (!canRetry && !hasResults) {
    return null
  }

  const download = async (kind: 'failed' | 'successful') => {
    setAnchor(null)
    setBusy(kind)
    setGone(false)
    try {
      const blob = await api.download(
        `/api/v1/runs/${runId}/chunks/${chunk.id}/results?kind=${kind}`,
      )
      if (!blob) {
        setGone(true)
        return
      }
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `${chunk.destinationJobId ?? chunk.id}-${kind}.csv`
      link.click()
      URL.revokeObjectURL(url)
    } finally {
      setBusy(null)
    }
  }

  return (
    <>
      <Tooltip title={gone ? 'The destination no longer holds that file' : ''}>
        <IconButton
          size="small"
          onClick={(event) => setAnchor(event.currentTarget)}
          disabled={busy !== null}
          color={gone ? 'warning' : 'default'}
        >
          <MoreVertIcon fontSize="small" />
        </IconButton>
      </Tooltip>

      <Menu anchorEl={anchor} open={Boolean(anchor)} onClose={() => setAnchor(null)}>
        {canRetry && (
          <MenuItem
            onClick={() => {
              setAnchor(null)
              onRetry?.(chunk)
            }}
          >
            <ListItemIcon>
              <ReplayIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText secondary="Read from the checkpoint and send again">
              Retry this chunk
            </ListItemText>
          </MenuItem>
        )}

        {canRetry && hasResults && <Divider />}

        {hasResults && (
          <MenuItem disabled sx={{ opacity: 1 }}>
            <Typography variant="caption" sx={{ ...tabular, color: muted }}>
              {chunk.destinationJobId}
            </Typography>
          </MenuItem>
        )}
        {hasResults && (
          <MenuItem onClick={() => download('failed')}>
            <ListItemIcon>
              <DownloadIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText secondary="Records it refused, with its own error">
              Failed records
            </ListItemText>
          </MenuItem>
        )}
        {hasResults && (
          <MenuItem onClick={() => download('successful')}>
            <ListItemIcon>
              <DownloadIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText secondary="Records it confirmed it accepted">
              Successful records
            </ListItemText>
          </MenuItem>
        )}

        {/*
          * Said in the menu rather than discovered on a click. Salesforce keeps job results for
          * about a week; the counts on the chunk are permanent. An empty download and "there were
          * no failures" look identical, so the difference has to be stated.
          */}
        {hasResults && (
          <MenuItem disabled sx={{ opacity: 1, maxWidth: 300, whiteSpace: 'normal' }}>
            <Typography variant="caption" sx={{ color: muted }}>
              Fetched from the destination, not stored here. Salesforce keeps these about a week.
            </Typography>
          </MenuItem>
        )}
      </Menu>
    </>
  )
}

/**
 * The chunk's number, which is also how its id is copied.
 *
 * <p>The id is what finds a chunk in the application log, in the stage log and in the record
 * index, and it was on no screen at all. Shown as a column of its own it wrapped a UUID onto three
 * lines in a cell sized for a single digit; given an icon it widened a table that has no width to
 * spare. So the number itself is the target: identical layout, identical alignment, one more thing
 * the cell does.
 */
function ChunkIndex({ chunk }: { chunk: import('@/api/types').Chunk }) {
  const [copied, setCopied] = useState(false)

  return (
    <Tooltip title={copied ? 'Copied' : `Click to copy chunk id ${chunk.id}`}>
      <Box
        component="span"
        onClick={() => {
          void navigator.clipboard.writeText(chunk.id)
          setCopied(true)
          window.setTimeout(() => setCopied(false), 1500)
        }}
        sx={{
          cursor: 'pointer',
          color: copied ? 'success.main' : 'inherit',
          '&:hover': { color: 'primary.main' },
        }}
      >
        {chunk.index}
      </Box>
    </Tooltip>
  )
}

function ChunkTable({
  runId,
  chunks,
  hasMore,
  loadingMore,
  onLoadMore,
  total,
  onRetry,
}: {
  runId: string
  chunks: import('@/api/types').Chunk[]
  hasMore: boolean
  loadingMore: boolean
  onLoadMore: () => void
  total: number
  onRetry?: (chunk: import('@/api/types').Chunk) => void
}) {
  if (chunks.length === 0) {
    return (
      <Typography variant="body2" sx={{ color: muted, py: 3 }}>
        No chunks planned yet.
      </Typography>
    )
  }

  return (
    <Paper sx={{ overflow: 'auto' }}>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>#</TableCell>
            <TableCell>STATE</TableCell>
            <TableCell>RANGE</TableCell>
            <TableCell>WORKER</TableCell>
            <TableCell align="right">WRITTEN</TableCell>
            <TableCell align="right">FILTERED</TableCell>
            <TableCell align="right">FAILED</TableCell>
            <TableCell align="right">ATTEMPT</TableCell>
            <TableCell>ERROR</TableCell>
            <TableCell sx={{ width: 56 }} />
          </TableRow>
        </TableHead>
        <TableBody>
          {chunks.map((chunk) => (
            <TableRow key={chunk.id} hover>
              <TableCell sx={tabular}>
                <ChunkIndex chunk={chunk} />
              </TableCell>
              <TableCell>
                <ChunkStateChip state={chunk.state} />
              </TableCell>
              <TableCell sx={{ maxWidth: 220 }}>
                <SpecCell spec={chunk.spec} />
              </TableCell>
              <TableCell>
                {chunk.assignedTo ? (
                  <Tooltip
                    title={
                      chunk.leaseExpiresAt
                        ? `Claim expires ${new Date(chunk.leaseExpiresAt).toLocaleTimeString()}`
                        : ''
                    }
                  >
                    <Typography variant="caption">{chunk.assignedTo}</Typography>
                  </Tooltip>
                ) : (
                  <Typography variant="caption" sx={{ color: muted }}>
                    —
                  </Typography>
                )}
              </TableCell>
              <TableCell align="right" sx={tabular}>
                {chunk.recordsWritten > 0 ? chunk.recordsWritten.toLocaleString() : '—'}
              </TableCell>
              {/* Between written and failed on purpose. It is neither, and the reader's eye
                  should not have to travel past a failure count to find out that a chunk's
                  missing records were dropped deliberately. */}
              <TableCell align="right" sx={tabular}>
                {chunk.recordsFiltered > 0 ? (
                  <Tooltip title="Records a transform dropped on purpose. Not a failure — they were excluded from the destination deliberately, and they are not counted in the percentage beside them.">
                    <Typography variant="caption" sx={{ ...tabular, color: muted }}>
                      {chunk.recordsFiltered.toLocaleString()}
                    </Typography>
                  </Tooltip>
                ) : (
                  '—'
                )}
              </TableCell>
              <TableCell align="right" sx={tabular}>
                {chunk.recordsFailed > 0 ? (
                  // The bare percentage was unreadable: a number with no denominator beside a
                  // different number. It is the share of what this chunk actually attempted to
                  // deliver — records it filtered on purpose are not in it — and saying so is the
                  // difference between a figure somebody can act on and one they ignore.
                  <Tooltip
                    title={`${chunk.rejectionPercent}% of the records this chunk attempted to deliver. Records dropped by a filter are excluded.`}
                  >
                    <Typography variant="caption" color="error.main" sx={tabular}>
                      {chunk.recordsFailed.toLocaleString()}
                      {chunk.rejectionPercent !== null && ` (${chunk.rejectionPercent}% of attempted)`}
                    </Typography>
                  </Tooltip>
                ) : (
                  '—'
                )}
              </TableCell>
              <TableCell align="right" sx={tabular}>
                {chunk.attempt > 0 ? chunk.attempt : '—'}
              </TableCell>
              <TableCell sx={{ maxWidth: 180 }}>
                {chunk.errorMessage ? (
                  <Tooltip title={<FailureDetail message={chunk.errorMessage} />}>
                    <Typography
                      variant="caption"
                      color="error.main"
                      sx={{ ...ellipsis, cursor: 'help' }}
                    >
                      {firstSentence(chunk.errorMessage)}
                    </Typography>
                  </Tooltip>
                ) : (
                  <Typography variant="caption" sx={{ color: muted }}>
                    —
                  </Typography>
                )}
              </TableCell>
              <TableCell align="right" sx={{ py: 0 }}>
                <ChunkActions runId={runId} chunk={chunk} onRetry={onRetry} />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {/* Fetches the next page as it is reached. The endpoint pages now, so this asks the server
          for more rather than revealing rows the browser was already holding. */}
      <MoreOnScroll
        hasMore={hasMore}
        loading={loadingMore}
        onReach={onLoadMore}
        shown={chunks.length}
        total={total}
        noun="chunks"
      />
    </Paper>
  )
}

/**
 * The dead-letter queue.
 *
 * Shows the record itself, not just a count. "5,000 records failed" tells someone they have a
 * problem; the payload and the destination's own error message tell them how to fix it.
 */
/**
 * Where every record read went, as a sum that has to balance.
 *
 * <p>Written as an equation rather than a sentence because the question it answers is arithmetic:
 * a thousand records were read and four hundred and sixty-six were written, so what happened to
 * the other five hundred and thirty-four? Prose invites the reader to skim past the gap; a total
 * that does not add up is visible immediately.
 *
 * <p><b>Filtered sits on its own side of the line.</b> It is the one number here that is not a
 * loss — a transform dropped those records because it was told to — and grouping it with failures
 * makes a working filter look like an incident.
 */
function Reconciliation({ metrics }: { metrics: import('@/api/types').RunMetrics }) {
  const accounted =
    metrics.recordsWritten + metrics.recordsFailed + metrics.recordsFiltered
  // Never expected to be non-zero, and shown rather than hidden if it is. A silent discrepancy in
  // the one place people come to reconcile counts is worse than an ugly one.
  const unaccounted = metrics.recordsRead - accounted

  return (
    <Box sx={{ mb: 3 }}>
      <Stack direction="row" spacing={1} alignItems="baseline" flexWrap="wrap" useFlexGap
             sx={{ ...tabular }}>
        <Typography variant="body2" sx={{ fontWeight: 700 }}>
          {metrics.recordsRead.toLocaleString()} read
        </Typography>
        <Typography variant="body2" sx={{ color: muted }}>=</Typography>

        <Typography variant="body2" sx={{ color: 'success.main', fontWeight: 600 }}>
          {metrics.recordsWritten.toLocaleString()} written
        </Typography>
        <Typography variant="body2" sx={{ color: muted }}>+</Typography>

        <Typography
          variant="body2"
          sx={{ color: metrics.recordsFailed > 0 ? 'error.main' : muted, fontWeight: 600 }}
        >
          {metrics.recordsFailed.toLocaleString()} failed
        </Typography>
        <Typography variant="body2" sx={{ color: muted }}>+</Typography>

        <Typography variant="body2" sx={{ color: muted, fontWeight: 600 }}>
          {metrics.recordsFiltered.toLocaleString()} filtered on purpose
        </Typography>

        {unaccounted !== 0 && (
          <>
            <Typography variant="body2" sx={{ color: muted }}>+</Typography>
            <Typography variant="body2" sx={{ color: 'warning.main', fontWeight: 700 }}>
              {unaccounted.toLocaleString()} unaccounted for
            </Typography>
          </>
        )}
      </Stack>

      <Typography variant="caption" sx={{ color: muted }}>
        Failed covers both a script throwing on a record and the destination refusing it — neither
        arrived. Filtered is a transform dropping records deliberately, and is not a loss.
      </Typography>
    </Box>
  )
}

function RejectedRecords({ errors }: { errors: import('@/api/types').RecordError[] }) {
  if (errors.length === 0) {
    return (
      <Typography variant="body2" sx={{ color: muted, py: 3 }}>
        No records were rejected.
      </Typography>
    )
  }

  return (
    <Stack spacing={1}>
      <Typography variant="caption" sx={{ color: muted }}>
        Payloads are redacted according to this pipeline’s audit policy.
      </Typography>

      {errors.map((error, index) => (
        <Paper key={`${error.chunkId}-${error.seq}-${index}`} sx={{ p: 2 }}>
          <Stack direction="row" spacing={2} alignItems="baseline" sx={{ mb: 1 }}>
            <Typography variant="body2" sx={{ fontWeight: 600 }} color="error.main">
              {error.code}
            </Typography>
            <Typography variant="caption">
              record #{error.seq}
              {error.key ? ` · key ${error.key}` : ''} · node {error.nodeId}
            </Typography>
          </Stack>

          <Typography variant="body2" sx={{ mb: 1.5 }}>
            {error.message}
          </Typography>

          <Box
            component="pre"
            sx={{
              m: 0,
              p: 1.5,
              borderRadius: 1,
              bgcolor: 'action.hover',
              fontSize: 12,
              overflowX: 'auto',
            }}
          >
            {JSON.stringify(error.payload, null, 2)}
          </Box>
        </Paper>
      ))}
    </Stack>
  )
}

/**
 * The range a chunk covers, with the connector's own spec behind it.
 *
 * <p>The spec is worth showing where it says something — a key range, a result-chunk index, a file
 * path. It is not worth showing when everything in it is the engine's own bookkeeping: a chunk
 * generated as the run proceeds has no range, so its spec holds a single internal marker saying
 * exactly that, and a tooltip reading {@code {"_dmpOpenEnded": true}} tells the reader less than
 * the words already in the cell.
 */
function SpecCell({ spec }: { spec: Record<string, unknown> }) {
  const readable = Object.fromEntries(
    Object.entries((spec ?? {}) as Record<string, unknown>).filter(
      ([key]) => !key.startsWith('_dmp'),
    ),
  )
  const label = (
    <Typography variant="caption" sx={{ ...tabular, ...ellipsis }}>
      {describeRange(spec)}
    </Typography>
  )

  if (Object.keys(readable).length === 0) {
    return label
  }

  return (
    <Tooltip
      title={
        <Box component="pre" sx={{ m: 0, font: 'inherit', fontFamily: 'ui-monospace, monospace' }}>
          {JSON.stringify(readable, null, 2)}
        </Box>
      }
    >
      <Box sx={{ cursor: 'help' }}>{label}</Box>
    </Tooltip>
  )
}

/** Renders a connector-defined chunk spec as something readable. */
/**
 * One line, with the rest a hover away.
 *
 * A table cell that grows to fit its content is fine until the content is a paragraph, at which
 * point one failed chunk is a screenful and the columns either side become unreadable. The full
 * text is never lost — it moves to the tooltip, which is where a wall of explanation belongs.
 */
const ellipsis = {
  display: 'block',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
} as const

/** The claim, without the paragraph explaining it. */
function firstSentence(message: string): string {
  const end = message.indexOf('. ')
  return end === -1 ? message : message.slice(0, end + 1)
}

/**
 * A failure message as two things rather than one paragraph.
 *
 * What happened and why it happened get read at different moments — the first while scanning for
 * the broken chunk, the second once it is found. Running them together as prose means the reader
 * parses four lines of reasoning before reaching the number they came for.
 *
 * Colour is inherited deliberately: the theme mutes `caption`, and muted text on the tooltip's own
 * surface is the low-contrast smudge this markup exists to avoid.
 */
function FailureDetail({ message }: { message: string }) {
  const claim = firstSentence(message)
  const reasoning = message.slice(claim.length).trim()

  return (
    <Stack spacing={reasoning ? 0.75 : 0}>
      <Typography variant="body2" sx={{ color: 'inherit', fontWeight: 600 }}>
        {claim}
      </Typography>
      {reasoning && (
        <Typography variant="body2" sx={{ color: 'inherit', opacity: 0.82 }}>
          {reasoning}
        </Typography>
      )}
    </Stack>
  )
}

/**
 * A chunk's boundaries, in whatever vocabulary the connector planned them with.
 *
 * Paired keys are matched by name rather than enumerated, so a connector that splits on `fromSeq`
 * or `fromPartition` reads properly without this function knowing it exists. Anything unrecognised
 * still renders as JSON — truncated in the cell, complete in the tooltip.
 */
function describeRange(spec: Record<string, unknown>): string {
  // An engine-generated chunk with no range of its own. Its marker is an implementation detail and
  // has no business being shown as though it were a key range the user could reason about.
  if (spec._dmpOpenEnded === true) {
    return 'continues from the previous chunk'
  }

  const keys = Object.keys(spec)
  const fromKey = keys.find((key) => key.toLowerCase().startsWith('from'))
  const toKey = fromKey ? 'to' + fromKey.slice(4) : undefined

  if (fromKey && toKey && spec[toKey] !== undefined) {
    return `${shorten(spec[fromKey])} – ${shorten(spec[toKey])}`
  }

  const rendered = JSON.stringify(spec)
  return rendered === '{}' ? 'whole source' : rendered
}

/**
 * Object ids differ only in their tail — a shared prefix is what a timestamp-led id looks like.
 * Showing the leading characters of two adjacent chunks would print the same string twice.
 */
function shorten(value: unknown): string {
  const text = String(value).replace(/^oid:/, '')
  return text.length > 12 ? '…' + text.slice(-8) : text
}
