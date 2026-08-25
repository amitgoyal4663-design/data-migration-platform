import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Link from '@mui/material/Link'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import { Link as RouterLink } from 'react-router-dom'
import type { OperationsFinding, PipelineHealth } from '@/api/types'
import { muted, tabular } from '@/theme'

/**
 * The same window, arranged by cause instead of by job.
 *
 * <p>A support desk asks which job is broken. Whoever fixes it asks the opposite question — what is
 * breaking, and is it one thing or twelve. Those are different arrangements of the same facts, and
 * the second one cannot be read off a list of jobs: a validation rule failing on four pipelines is
 * four cards saying a little each, or one line saying it once with four names on it.
 */
export function OperationsEngineering({ pipelines }: { pipelines: PipelineHealth[] }) {
  const causes = groupByCause(pipelines)
  const findings = groupFindings(pipelines)
  const slowest = [...pipelines]
    .filter((pipeline) => pipeline.volume.seconds > 0)
    .sort((left, right) => right.volume.seconds - left.volume.seconds)
    .slice(0, 5)

  return (
    <Stack spacing={2}>
      <Section
        title="Why records failed"
        caption="Across every job on screen, from each one's latest run. The same reason on four
                 pipelines is one problem, and reading it four times is how it stays one problem
                 for a fortnight."
      >
        {causes.length === 0 ? (
          <Nothing>No records failed on any of these jobs' latest runs.</Nothing>
        ) : (
          <Stack divider={<Box sx={{ borderTop: 1, borderColor: 'divider' }} />}>
            {causes.map((cause) => (
              <Box key={cause.key} sx={{ py: 1.25 }}>
                <Stack direction="row" spacing={1.5} alignItems="baseline" flexWrap="wrap">
                  <Typography variant="h3" sx={{ ...tabular, color: 'error.main', minWidth: 80 }}>
                    {cause.count.toLocaleString()}
                  </Typography>
                  <Typography variant="body2" sx={{ flexGrow: 1, minWidth: 240 }}>
                    {cause.reason}
                  </Typography>
                  <Chip size="small" variant="outlined" label={cause.code} />
                </Stack>
                <Stack direction="row" spacing={0.75} flexWrap="wrap" useFlexGap sx={{ mt: 0.75, pl: { sm: 10 } }}>
                  {cause.pipelines.map((pipeline) => (
                    <Button
                      key={pipeline.pipelineId}
                      size="small"
                      component={RouterLink}
                      to={pipeline.runId ? `/runs/${pipeline.runId}` : `/pipelines/${pipeline.pipelineId}`}
                    >
                      {pipeline.name} · {pipeline.count.toLocaleString()}
                    </Button>
                  ))}
                </Stack>
              </Box>
            ))}
          </Stack>
        )}
      </Section>

      <Section
        title="What the platform noticed"
        caption="Every finding raised across these jobs, grouped by kind. A finding is a judgement
                 against that pipeline's own history, so the same code on two jobs is two
                 independent measurements agreeing."
      >
        {findings.length === 0 ? (
          <Nothing>Nothing was flagged on any of these jobs.</Nothing>
        ) : (
          <Stack spacing={1}>
            {findings.map((group) => (
              <Box key={group.code}>
                <Stack direction="row" spacing={1} alignItems="baseline" flexWrap="wrap">
                  <Chip
                    size="small"
                    label={group.code}
                    color={
                      group.severity === 'CRITICAL'
                        ? 'error'
                        : group.severity === 'WARNING'
                          ? 'warning'
                          : 'default'
                    }
                  />
                  <Typography variant="body2" sx={{ color: muted }}>
                    {group.entries.length} job{group.entries.length === 1 ? '' : 's'}
                  </Typography>
                </Stack>
                <Stack spacing={0.25} sx={{ mt: 0.5, pl: 1 }}>
                  {group.entries.map((entry) => (
                    <Stack key={entry.pipelineId} direction="row" spacing={1} flexWrap="wrap">
                      <Link
                        component={RouterLink}
                        to={`/pipelines/${entry.pipelineId}`}
                        variant="body2"
                        sx={{ minWidth: 180 }}
                      >
                        {entry.name}
                      </Link>
                      <Typography variant="body2" sx={{ color: muted, flexGrow: 1 }}>
                        {entry.finding.message}
                        {entry.finding.detail && ` — ${entry.finding.detail}`}
                      </Typography>
                    </Stack>
                  ))}
                </Stack>
              </Box>
            ))}
          </Stack>
        )}
      </Section>

      <Section
        title="Where the time went"
        caption="Total run time over the window, which is the figure that decides whether a nightly
                 job still fits in its night."
      >
        {slowest.length === 0 ? (
          <Nothing>Nothing ran long enough to measure.</Nothing>
        ) : (
          <Stack spacing={0.75}>
            {slowest.map((pipeline) => (
              <Stack key={pipeline.pipelineId} direction="row" spacing={2} alignItems="baseline">
                <Link
                  component={RouterLink}
                  to={`/pipelines/${pipeline.pipelineId}`}
                  variant="body2"
                  sx={{ flexGrow: 1 }}
                >
                  {pipeline.name}
                </Link>
                <Typography variant="body2" sx={{ ...tabular, color: muted }}>
                  {pipeline.volume.runs} run{pipeline.volume.runs === 1 ? '' : 's'}
                </Typography>
                <Typography variant="body2" sx={{ ...tabular, minWidth: 90, textAlign: 'right' }}>
                  {duration(pipeline.volume.seconds)}
                </Typography>
                <Typography variant="body2" sx={{ ...tabular, color: muted, minWidth: 130, textAlign: 'right' }}>
                  {pipeline.volume.seconds > 0
                    ? `${Math.round(pipeline.volume.read / pipeline.volume.seconds).toLocaleString()}/s`
                    : '—'}
                </Typography>
              </Stack>
            ))}
          </Stack>
        )}
      </Section>
    </Stack>
  )
}

function Section({
  title,
  caption,
  children,
}: {
  title: string
  caption: string
  children: React.ReactNode
}) {
  return (
    <Paper sx={{ p: 2 }}>
      <Typography variant="h3">{title}</Typography>
      <Typography variant="body2" sx={{ color: muted, mt: 0.5, mb: 1.5 }}>
        {caption}
      </Typography>
      {children}
    </Paper>
  )
}

function Nothing({ children }: { children: React.ReactNode }) {
  return (
    <Typography variant="body2" sx={{ color: muted }}>
      {children}
    </Typography>
  )
}

interface Cause {
  key: string
  code: string
  reason: string
  count: number
  pipelines: { pipelineId: string; name: string; runId: string | null; count: number }[]
}

/** The same reason on several jobs, counted once with every job that has it named beside it. */
function groupByCause(pipelines: PipelineHealth[]): Cause[] {
  const causes = new Map<string, Cause>()

  for (const pipeline of pipelines) {
    for (const reason of pipeline.reasons) {
      const key = `${reason.code}::${reason.reason}`
      const cause = causes.get(key) ?? {
        key,
        code: reason.code,
        reason: reason.reason,
        count: 0,
        pipelines: [],
      }
      cause.count += reason.count
      cause.pipelines.push({
        pipelineId: pipeline.pipelineId,
        name: pipeline.name,
        runId: pipeline.latest?.id ?? null,
        count: reason.count,
      })
      causes.set(key, cause)
    }
  }

  return [...causes.values()].sort((left, right) => right.count - left.count)
}

interface FindingGroup {
  code: string
  severity: string
  entries: { pipelineId: string; name: string; finding: OperationsFinding }[]
}

function groupFindings(pipelines: PipelineHealth[]): FindingGroup[] {
  const groups = new Map<string, FindingGroup>()
  const rank = (severity: string) => (severity === 'CRITICAL' ? 2 : severity === 'WARNING' ? 1 : 0)

  for (const pipeline of pipelines) {
    for (const finding of pipeline.findings) {
      if (finding.severity === 'INFO') {
        continue
      }
      const group = groups.get(finding.code) ?? {
        code: finding.code,
        severity: finding.severity,
        entries: [],
      }
      if (rank(finding.severity) > rank(group.severity)) {
        group.severity = finding.severity
      }
      group.entries.push({
        pipelineId: pipeline.pipelineId,
        name: pipeline.name,
        finding,
      })
      groups.set(finding.code, group)
    }
  }

  return [...groups.values()].sort(
    (left, right) =>
      rank(right.severity) - rank(left.severity) || right.entries.length - left.entries.length,
  )
}

function duration(seconds: number) {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`
}
