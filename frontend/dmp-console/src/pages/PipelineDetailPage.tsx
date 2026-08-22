import MoreVertIcon from '@mui/icons-material/MoreVert'
import ListItemIcon from '@mui/material/ListItemIcon'
import ListItemText from '@mui/material/ListItemText'
import Menu from '@mui/material/Menu'
import Divider from '@mui/material/Divider'
import MenuItem from '@mui/material/MenuItem'
import HistoryIcon from '@mui/icons-material/HistoryOutlined'
import ViewIcon from '@mui/icons-material/VisibilityOutlined'
import CopyIcon from '@mui/icons-material/ContentCopyOutlined'
import IconButton from '@mui/material/IconButton'
import Tooltip from '@mui/material/Tooltip'
import Alert from '@mui/material/Alert'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown'
import KeyboardArrowRightIcon from '@mui/icons-material/KeyboardArrowRight'
import { toChains } from '@/components/runChains'
import { RunChainRows, type ChainRowProps } from '@/components/RunChainRows'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TablePagination from '@mui/material/TablePagination'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import AddIcon from '@mui/icons-material/Add'
import EditIcon from '@mui/icons-material/EditOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PublishIcon from '@mui/icons-material/PublishOutlined'
import DeleteIcon from '@mui/icons-material/DeleteOutlined'
import { useState } from 'react'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import {
  useCopyVersion,
  useCreateVersion,
  useDeleteVersion,
  usePipeline,
  usePublishVersion,
  useRunParameterNames,
  useRuns,
  useStartRun,
  useVersions,
} from '@/api/hooks'
import { PageHeader } from '@/components/PageHeader'
import { RunDialog } from '@/components/RunDialog'
import { ErrorPanel, Loading } from '@/components/Feedback'
import RestartAltIcon from '@mui/icons-material/RestartAltOutlined'
import { RunParameters } from '@/components/RunParameters'
import { RunStateChip } from '@/components/StateChip'
import { muted, status } from '@/theme'
import type { PipelineVersionSummary, VersionStatus } from '@/api/types'
import { shortId } from '@/api/ids'

/**
 * A run's parameters in the shape the start endpoint takes.
 *
 * <p>They arrive as arbitrary JSON and go back out as strings, which is what a source binds.
 */
function asStringMap(parameters: Record<string, unknown> | null | undefined) {
  if (!parameters) return undefined
  return Object.fromEntries(
    Object.entries(parameters).map(([name, value]) => [name, String(value)]),
  )
}

/**
 * A timestamp as the part that distinguishes one row from the next.
 *
 * <p>"20/08/2026, 21:25:46" is nineteen characters of which four matter in a list where every
 * version was created the same week. The year and the seconds go to the tooltip.
 */
function shortDate(value: string): string {
  const at = new Date(value)
  return `${at.toLocaleDateString(undefined, { day: 'numeric', month: 'short' })}, `
    + `${at.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}`
}

export function PipelineDetailPage() {
  const { pipelineId = '' } = useParams()
  const navigate = useNavigate()

  const [versionPage, setVersionPage] = useState(0)
  const [versionRows, setVersionRows] = useState(5)
  const [runPage, setRunPage] = useState(0)
  const [runRows, setRunRows] = useState(5)

  const pipeline = usePipeline(pipelineId)
  const versions = useVersions(pipelineId)
  const runs = useRuns({ pipelineId, page: runPage, size: runRows })
  const createVersion = useCreateVersion(pipelineId)
  const deleteVersion = useDeleteVersion(pipelineId)
  const publish = usePublishVersion(pipelineId)
  const startRun = useStartRun()
  const copyVersion = useCopyVersion(pipelineId)
  const runParameters = useRunParameterNames(pipelineId)
  const [askingParameters, setAskingParameters] = useState(false)

  if (pipeline.isLoading) return <Loading />
  if (pipeline.error) return <ErrorPanel error={pipeline.error} />
  if (!pipeline.data) return null

  const current = pipeline.data
  const versionList = versions.data ?? []
  // From the whole list, not the visible page: a draft on page three is still the draft that the
  // header button should open, and still the one that stops "New version" making another.
  const latestDraft = versionList.find((version) => version.status !== 'PUBLISHED')

  // Versions are paged in the browser. They are bounded by how many times a person pressed "New
  // version" — tens, not millions — so one request and a slice beats a paged endpoint here. Runs
  // are not bounded that way, which is why those are paged on the server instead.
  //
  // Clamped rather than reset in an effect: deleting the last draft on the final page would
  // otherwise leave the table showing an empty page with no way to tell why.
  const versionMaxPage = Math.max(0, Math.ceil(versionList.length / versionRows) - 1)
  const versionSafePage = Math.min(versionPage, versionMaxPage)
  const visibleVersions = versionList.slice(
    versionSafePage * versionRows,
    versionSafePage * versionRows + versionRows,
  )

  const runPageData = runs.data
  const runList = runPageData?.content ?? []

  const newVersion = () => {
    if (latestDraft) {
      // A published version cannot be edited, so a draft is needed to make any change — but only
      // one. Creating another each time leaves a trail of empty versions nobody can tell apart,
      // which is exactly what makes "when was this made, and by whom" a question worth asking.
      navigate(`/pipelines/${pipelineId}/versions/${latestDraft.id}/design`)
      return
    }
    createVersion.mutate(
      { changeNote: 'Created from console' },
      {
        onSuccess: (version) =>
          navigate(`/pipelines/${pipelineId}/versions/${version.id}/design`),
      },
    )
  }

  return (
    <>
      <PageHeader
        breadcrumbs={[{ label: 'Pipelines', to: '/pipelines' }, { label: current.name }]}
        title={current.name}
        subtitle={
          <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" useFlexGap>
            {current.publishedVersion !== null ? (
              <Chip
                size="small"
                label={`v${current.publishedVersion} live`}
                sx={{
                  color: status.good,
                  borderColor: status.good,
                  backgroundColor: `${status.good}14`,
                  fontWeight: 600,
                }}
                variant="outlined"
              />
            ) : (
              <Chip size="small" variant="outlined" label="Nothing published" sx={{ color: muted }} />
            )}
            {current.latestVersion > (current.publishedVersion ?? 0) && (
              <Typography variant="body2" sx={{ color: muted }}>
                · v{current.latestVersion} is the newest draft
              </Typography>
            )}
            {current.description && (
              <Typography variant="body2" sx={{ color: muted }}>
                · {current.description}
              </Typography>
            )}
          </Stack>
        }
        actions={
          <>
            <Button startIcon={<AddIcon />} onClick={newVersion} disabled={createVersion.isPending}>
              {latestDraft ? `Edit draft v${latestDraft.versionNumber}` : 'New version'}
            </Button>
            <Button
              startIcon={<PlayArrowIcon />}
              variant="contained"
              disabled={!current.runnable || startRun.isPending}
              onClick={() => {
                // A pipeline whose query takes no parameters starts immediately, as it always
                // has. Only one that needs values stops to ask for them.
                if ((runParameters.data?.names.length ?? 0) > 0) {
                  setAskingParameters(true)
                  return
                }
                startRun.mutate(
                  { pipelineId },
                  { onSuccess: (run) => navigate(`/runs/${run.id}`) },
                )
              }}
            >
              Run now
            </Button>
          </>
        }
      />

      <RunDialog
        open={askingParameters}
        names={runParameters.data?.names ?? []}
        pending={startRun.isPending}
        onCancel={() => setAskingParameters(false)}
        onStart={(parameters) =>
          startRun.mutate(
            { pipelineId, parameters },
            {
              onSuccess: (run) => {
                setAskingParameters(false)
                navigate(`/runs/${run.id}`)
              },
            },
          )
        }
      />

      <ErrorPanel error={createVersion.error ?? deleteVersion.error ?? copyVersion.error} />
      <ErrorPanel error={publish.error} />
      <ErrorPanel error={startRun.error} />

      {!current.runnable && (
        <Alert severity="info" sx={{ mb: 3 }}>
          This pipeline has no published version, so it cannot run yet. Design a version and publish
          it — publishing freezes it, which is what lets a run months from now still say exactly what
          it executed.
        </Alert>
      )}

      {(current.folder || current.tags.length > 0) && (
        <Stack direction="row" spacing={1} sx={{ mb: 2 }} flexWrap="wrap" useFlexGap>
          {current.folder && <Chip size="small" variant="outlined" label={current.folder} />}
          {current.tags.map((tag) => (
            <Chip key={tag} size="small" variant="outlined" label={tag} />
          ))}
        </Stack>
      )}

      <Typography variant="h2" sx={{ mb: 1.5 }}>
        Versions
      </Typography>

      <Paper sx={{ overflow: 'hidden', mb: 3 }}>
        {/*
          Fixed layout with equal columns.

          Under the default automatic layout a browser distributes width by content, which put a
          version number in a third of the table beside a change note reading "Cre…". Fixed layout
          and one width for every column removes the negotiation entirely: nothing competes,
          nothing collapses, and every row divides on the same boundaries.

          The actions column is the one exception, at a fixed 56px — a single icon button given a
          fifth of the table would sit in the middle of its own cell, a long way from the row it
          belongs to.
        */}
        <Table size="small" sx={{ tableLayout: 'fixed' }}>
          <TableHead>
            <TableRow>
              {/*
                Four columns, not six. Mode and node count moved under the version number and the
                created cell lost two of its three facts to a tooltip — because at six columns the
                note and the timestamp were each about eighty pixels wide and wrapped one word per
                line, which made every row six lines tall and threw the actions out of alignment.
                Density was the cause of the mess, not the cure for it.
              */}
              <TableCell sx={{ width: '25%' }}>VERSION</TableCell>
              <TableCell sx={{ width: '25%' }}>STATE</TableCell>
              <TableCell sx={{ width: '25%' }}>NOTE</TableCell>
              <TableCell sx={{ width: '25%' }}>CREATED</TableCell>
              <TableCell sx={{ width: 56 }} />
            </TableRow>
          </TableHead>
          <TableBody>
            {visibleVersions.map((version) => {
              const isPublished = current.publishedVersion === version.versionNumber
              return (
                <TableRow key={version.id} hover>
                  {/*
                    Mode and node count are on the tooltip rather than under the number. As a
                    caption they read as detail and behaved as width: "FULL_LOAD · 3 nodes" is
                    wider than anything else this column holds, so it set the column's size and
                    opened a gap between the version and its state. They are also identical on
                    every row of most pipelines, which is the definition of something that should
                    not cost a column.
                  */}
                  <TableCell sx={{ whiteSpace: 'nowrap' }}>
                    <Tooltip
                      title={`${version.mode} · ${version.nodeCount} node`
                        + `${version.nodeCount === 1 ? '' : 's'}`}
                    >
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>
                          v{version.versionNumber}
                        </Typography>
                        {isPublished && (
                          <Chip
                            size="small"
                            label="Live"
                            sx={{
                              color: status.good,
                              borderColor: status.good,
                              backgroundColor: `${status.good}14`,
                            }}
                            variant="outlined"
                          />
                        )}
                      </Stack>
                    </Tooltip>
                  </TableCell>
                  <TableCell>
                    <VersionStatusChip status={version.status} />
                  </TableCell>
                  {/*
                    The note is the only thing here worth the width, so it gets whatever is left
                    and is cut with an ellipsis rather than wrapped. A change note is written to be
                    scanned down the column; six words stacked vertically cannot be.
                  */}
                  <TableCell>
                    <Tooltip title={version.changeNote ?? ''} disableHoverListener={!version.changeNote}>
                      <Typography
                        variant="caption"
                        sx={{
                          display: 'block',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                        }}
                      >
                        {version.changeNote ?? '—'}
                      </Typography>
                    </Tooltip>
                  </TableCell>
                  {/*
                    One line. Who created it and when it was published are worth keeping but not
                    worth a column each — they are questions asked about one version, not compared
                    down a list, so they belong on hover.
                  */}
                  <TableCell sx={{ whiteSpace: 'nowrap' }}>
                    <Tooltip
                      title={
                        <span style={{ whiteSpace: 'pre-line' }}>
                          {`Created ${new Date(version.createdAt).toLocaleString()}\n`
                            + `by ${version.createdBy ?? 'unknown'}`
                            + (version.publishedAt
                              ? `\nPublished ${new Date(version.publishedAt).toLocaleString()}`
                              : '')}
                        </span>
                      }
                    >
                      <Typography variant="caption" sx={{ color: muted }}>
                        {shortDate(version.createdAt)}
                      </Typography>
                    </Tooltip>
                  </TableCell>
                  {/*
                    One button, not four. Text buttons for View, Copy and Roll back cost more
                    width than the note they were squeezing, and the set differs between a draft
                    and a published version — so no two rows had the same buttons in the same
                    place and nothing lined up. A menu is a fixed-width anchor whatever it holds.
                  */}
                  <TableCell align="right" sx={{ whiteSpace: 'nowrap', py: 0.5 }}>
                    <VersionActions
                      version={version}
                      pipelineId={pipelineId}
                      isLive={isPublished}
                      blockedByDraft={
                        latestDraft ? `v${latestDraft.versionNumber}` : null
                      }
                      busy={
                        publish.isPending || copyVersion.isPending || deleteVersion.isPending
                      }
                      onPublish={() => publish.mutate(version.versionNumber)}
                      onCopy={() =>
                        copyVersion.mutate(version, {
                          onSuccess: (created) =>
                            navigate(`/pipelines/${pipelineId}/versions/${created.id}/design`),
                        })
                      }
                      onDelete={() => deleteVersion.mutate(version.id)}
                    />
                  </TableCell>
                </TableRow>
              )
            })}

            {versionList.length === 0 && (
              <TableRow>
                <TableCell colSpan={5}>
                  <Typography variant="body2" sx={{ color: muted, py: 2 }}>
                    No versions yet. Create one to start designing.
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>

        {versionList.length > 0 && (
          <TablePagination
            component="div"
            count={versionList.length}
            page={versionSafePage}
            rowsPerPage={versionRows}
            rowsPerPageOptions={ROWS_PER_PAGE}
            onPageChange={(_, next) => setVersionPage(next)}
            onRowsPerPageChange={(event) => {
              setVersionRows(Number(event.target.value))
              setVersionPage(0)
            }}
            labelRowsPerPage="Versions per page"
          />
        )}
      </Paper>

      {latestDraft && (
        <Alert severity="info" sx={{ mb: 3 }}>
          v{latestDraft.versionNumber} is a draft. Editing it changes nothing about what is running —
          published versions can never be modified.
        </Alert>
      )}

      <Typography variant="h2" sx={{ mb: 1.5 }}>
        Recent runs
      </Typography>

      <Paper sx={{ overflow: 'hidden' }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell sx={{ width: 40 }} />
              <TableCell>RUN</TableCell>
              <TableCell>VERSION</TableCell>
              <TableCell>STATE</TableCell>
              <TableCell>COVERED</TableCell>
              <TableCell align="right">WRITTEN</TableCell>
              <TableCell>STARTED</TableCell>
              <TableCell />
            </TableRow>
          </TableHead>
          <TableBody>
            {toChains(runList).map((chain) => (
              <RunChainRows
                key={chain.root.id}
                chain={chain}
                render={(run: import('@/api/types').Run, props: ChainRowProps) => (
              <TableRow
                key={run.id}
                hover
                component={RouterLink}
                to={`/runs/${run.id}`}
                sx={{ textDecoration: 'none', cursor: 'pointer' }}
              >
                <TableCell sx={{ width: 40 }}>
                  {props.onToggle && (
                    <Tooltip
                      title={props.expanded ? 'Hide attempts' : `${(props.chain?.attempts.length ?? 0) + 1} attempts`}
                    >
                      <IconButton
                        size="small"
                        onClick={(event) => {
                          // The row is a link; expanding is not navigating.
                          event.preventDefault()
                          event.stopPropagation()
                          props.onToggle?.()
                        }}
                      >
                        {props.expanded ? (
                          <KeyboardArrowDownIcon fontSize="small" />
                        ) : (
                          <KeyboardArrowRightIcon fontSize="small" />
                        )}
                      </IconButton>
                    </Tooltip>
                  )}
                </TableCell>
                <TableCell>
                  <Typography variant="caption">{shortId(run.id)}</Typography>
                  {props.chain && (
                    <Chip
                      size="small"
                      variant="outlined"
                      label={`${props.chain.attempts.length + 1} attempts`}
                      sx={{ ml: 1, height: 18, fontSize: 10 }}
                    />
                  )}
                </TableCell>
                <TableCell>v{run.versionNumber}</TableCell>
                <TableCell>
                  {/* The migration's state, which is the latest attempt's — not the first one's. */}
                  <RunStateChip state={props.chain ? props.chain.latest.state : run.state} />
                </TableCell>
                {/*
                  Which window each run took. On a scheduled pipeline this column is the whole
                  point of the table: a gap in it is a day nobody migrated, and that is invisible
                  when the range only appears inside one run.
                */}
                <TableCell>
                  <RunParameters parameters={run.parameters} />
                </TableCell>
                <TableCell align="right">
                  {(props.chain ? props.chain.totalWritten : run.metrics.recordsWritten).toLocaleString()}
                </TableCell>
                <TableCell>
                  <Typography variant="caption">
                    {run.startedAt ? new Date(run.startedAt).toLocaleString() : '—'}
                  </Typography>
                </TableCell>
                {/*
                  Repeating a window from the row it is listed on. The same action exists on the
                  run page, but this is the table somebody scans to find the day that did not
                  migrate — and having found it, opening the run only to come back and press
                  another button is a step with no purpose.
                */}
                <TableCell align="right" sx={{ py: 0 }}>
                  {run.terminal && run.state !== 'ARCHIVED' && (
                    <Tooltip
                      title={
                        run.parameters && Object.keys(run.parameters).length > 0
                          ? 'Run again, covering the same window'
                          : 'Run again'
                      }
                    >
                      <span>
                        <IconButton
                          size="small"
                          disabled={startRun.isPending}
                          onClick={(event) => {
                            // The row is a link. Without both of these, re-running also navigates
                            // to the run being repeated rather than the one just created.
                            event.preventDefault()
                            event.stopPropagation()
                            startRun.mutate(
                              {
                                pipelineId,
                                parameters: asStringMap(run.parameters),
                              },
                              { onSuccess: (created) => navigate(`/runs/${created.id}`) },
                            )
                          }}
                        >
                          <RestartAltIcon fontSize="small" />
                        </IconButton>
                      </span>
                    </Tooltip>
                  )}
                </TableCell>
              </TableRow>
                )}
              />
            ))}
            {runList.length === 0 && (
              <TableRow>
                <TableCell colSpan={7}>
                  <Typography variant="body2" sx={{ color: muted, py: 2 }}>
                    {runPage === 0
                      ? 'This pipeline has not run yet.'
                      : 'No runs on this page.'}
                  </Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>

        {(runList.length > 0 || runPage > 0) && (
          <TablePagination
            component="div"
            // -1 when the store cannot count cheaply, which MUI renders as "of more than n".
            // Honest, and better than a total that would cost a full scan to produce.
            count={runPageData?.totalElements ?? -1}
            page={runPage}
            rowsPerPage={runRows}
            rowsPerPageOptions={ROWS_PER_PAGE}
            onPageChange={(_, next) => setRunPage(next)}
            onRowsPerPageChange={(event) => {
              setRunRows(Number(event.target.value))
              setRunPage(0)
            }}
            // The server knows whether another page exists even when it will not count the whole
            // set, so the arrow follows that rather than arithmetic on an unknown total.
            nextIconButtonProps={{ disabled: !runPageData?.hasNext }}
            labelRowsPerPage="Runs per page"
          />
        )}
      </Paper>
    </>
  )
}

/** Small by default: this page is read to find one version or one run, not to browse them all. */
const ROWS_PER_PAGE = [5, 10, 25, 50]

/**
 * A version's status, in one word.
 *
 * <p>"Published — frozen" said two things and cost the width of both. Frozen is what published
 * means here — every published version is immutable, so the qualifier repeated the noun for every
 * row of every pipeline. It moved to the tooltip, where it explains rather than labels.
 */
function VersionStatusChip({ status: state }: { status: VersionStatus }) {
  const label = { DRAFT: 'Draft', VALIDATED: 'Validated', PUBLISHED: 'Published' }[state]
  const meaning = {
    DRAFT: 'Editable. Not used by any run until it is published.',
    VALIDATED: 'Checked and editable. Not used by any run until it is published.',
    PUBLISHED: 'Frozen — it can never be edited, which is what lets an old run say what it ran.',
  }[state]

  return (
    <Tooltip title={meaning}>
      <Chip size="small" variant="outlined" label={label} sx={{ color: muted }} />
    </Tooltip>
  )
}

/**
 * A version's actions, behind one button.
 *
 * <p>Four text buttons in a table cell cost more width than the change note they were squeezing,
 * and the set differs between a draft and a published version — Design, Publish and Delete against
 * View and Roll back — so no two rows had the same controls in the same place and the column never
 * lined up. A menu is one fixed-width anchor whatever it contains.
 *
 * <p>The destructive item is last, separated, and still asks. A menu makes every item one click
 * from the same place, which is precisely why Delete must not sit where Copy was a moment ago.
 */
function VersionActions({
  version,
  pipelineId,
  isLive,
  blockedByDraft,
  busy,
  onPublish,
  onCopy,
  onDelete,
}: {
  version: PipelineVersionSummary
  pipelineId: string
  isLive: boolean
  /** The draft already open, if any — only one may exist at a time. */
  blockedByDraft: string | null
  busy: boolean
  onPublish: () => void
  onCopy: () => void
  onDelete: () => void
}) {
  const [anchor, setAnchor] = useState<HTMLElement | null>(null)
  const close = () => setAnchor(null)

  const act = (run: () => void) => () => {
    close()
    run()
  }

  const isDraft = version.status !== 'PUBLISHED'
  const designPath = `/pipelines/${pipelineId}/versions/${version.id}/design`

  return (
    <>
      <IconButton size="small" onClick={(event) => setAnchor(event.currentTarget)} disabled={busy}>
        <MoreVertIcon fontSize="small" />
      </IconButton>

      <Menu anchorEl={anchor} open={Boolean(anchor)} onClose={close}>
        {/*
          A draft's Design and a published version's View open the same screen — the designer,
          which refuses edits on a frozen version. Two names because the two are different acts:
          one is where you work, the other is where you look.
        */}
        <MenuItem component={RouterLink} to={designPath} onClick={close}>
          <ListItemIcon>
            {isDraft ? <EditIcon fontSize="small" /> : <ViewIcon fontSize="small" />}
          </ListItemIcon>
          <ListItemText>{isDraft ? 'Design' : 'View'}</ListItemText>
        </MenuItem>

        <MenuItem onClick={act(onCopy)} disabled={Boolean(blockedByDraft)}>
          <ListItemIcon>
            <CopyIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText
            secondary={
              blockedByDraft
                ? `Draft ${blockedByDraft} is already open`
                : 'New draft from this version'
            }
          >
            Copy
          </ListItemText>
        </MenuItem>

        {isDraft && (
          <MenuItem onClick={act(onPublish)}>
            <ListItemIcon>
              <PublishIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText>Publish</ListItemText>
          </MenuItem>
        )}

        {!isDraft && !isLive && (
          <MenuItem onClick={act(onPublish)}>
            <ListItemIcon>
              <HistoryIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText secondary="Make this the version new runs use">Roll back</ListItemText>
          </MenuItem>
        )}

        {isDraft && <Divider />}

        {isDraft && (
          <MenuItem
            onClick={() => {
              close()
              // Confirmed because it cannot be undone, and named because "delete this version?"
              // on a page listing nine of them is not a question anyone can answer correctly.
              if (
                window.confirm(
                  `Delete draft v${version.versionNumber}? This cannot be undone. ` +
                    'Published versions are never affected.',
                )
              ) {
                onDelete()
              }
            }}
          >
            <ListItemIcon>
              <DeleteIcon fontSize="small" color="error" />
            </ListItemIcon>
            <ListItemText sx={{ color: 'error.main' }}>Delete</ListItemText>
          </MenuItem>
        )}
      </Menu>
    </>
  )
}
