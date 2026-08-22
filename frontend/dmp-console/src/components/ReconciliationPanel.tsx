import Alert from '@mui/material/Alert'
import AlertTitle from '@mui/material/AlertTitle'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import CheckCircleIcon from '@mui/icons-material/CheckCircleOutlined'
import DownloadIcon from '@mui/icons-material/DownloadOutlined'
import ErrorIcon from '@mui/icons-material/ErrorOutlineOutlined'
import HourglassIcon from '@mui/icons-material/HourglassEmptyOutlined'
import { useState } from 'react'
import { api } from '@/api/client'
import { useReconciliation } from '@/api/hooks'
import { ErrorPanel, Loading } from '@/components/Feedback'
import { muted, tabular } from '@/theme'
import type { Reconciliation, ReconciliationKind } from '@/api/types'

/**
 * The page a migration is signed off on.
 *
 * Everything here already existed as a counter somewhere; what was missing was the arithmetic in
 * one place with a verdict on it. The sheet is rendered from what the server sends rather than
 * from field names known here, so a line added on the server appears without a change to this file
 * — and, more to the point, the CSV export and this table can never drift apart.
 */
export function ReconciliationPanel({ runId, live }: { runId: string; live: boolean }) {
  const report = useReconciliation(runId, live)

  if (report.isLoading) return <Loading />
  if (report.error) return <ErrorPanel error={report.error} />
  if (!report.data) return null

  const data = report.data

  return (
    <Stack spacing={2}>
      {data.dryRun && (
        <Alert severity="warning">
          <AlertTitle>These are rehearsal numbers</AlertTitle>
          Nothing was written. “Written” below means handed to a destination that was never opened,
          so it counts what would have been sent — not what was accepted.
        </Alert>
      )}
      <Verdict data={data} runId={runId} />
      <Sheet data={data} />
      {data.checks.length > 0 && <Checks data={data} />}
      {!data.indexed && !data.dryRun && (
        <Typography variant="caption" sx={{ color: muted }}>
          This pipeline does not index records, so the sheet is checked only against itself. Set
          the audit level to INDEXED to have every record counted a second time, independently.
        </Typography>
      )}
    </Stack>
  )
}

/** The one line most people read, and the download beside it. */
function Verdict({ data, runId }: { data: Reconciliation; runId: string }) {
  const [saving, setSaving] = useState(false)

  // Fetched rather than navigated to. The tenant travels as a header, so a plain link to the same
  // URL arrives without one and is refused — which would look like a broken button.
  const download = async () => {
    setSaving(true)
    try {
      const blob = await api.download(`/api/v1/runs/${runId}/reconciliation.csv`)
      if (!blob) return
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `reconciliation-${runId}.csv`
      link.click()
      URL.revokeObjectURL(url)
    } finally {
      setSaving(false)
    }
  }

  const { severity, icon, title, detail } = verdictCopy(data)

  return (
    <Alert
      severity={severity}
      icon={icon}
      action={
        <Button size="small" startIcon={<DownloadIcon />} onClick={download} disabled={saving}>
          CSV
        </Button>
      }
    >
      <AlertTitle sx={{ mb: 0.25 }}>{title}</AlertTitle>
      <Typography variant="body2">{detail}</Typography>
    </Alert>
  )
}

function verdictCopy(data: Reconciliation) {
  if (data.verdict === 'INCOMPLETE') {
    return {
      severity: 'info' as const,
      icon: <HourglassIcon fontSize="inherit" />,
      title: 'Run has not finished',
      detail:
        'The numbers below are current but not final, so no verdict is given yet — a balance ' +
        'struck while chunks are still writing is arithmetic about a moving target.',
    }
  }
  if (data.verdict === 'BALANCED') {
    return {
      severity: 'success' as const,
      icon: <CheckCircleIcon fontSize="inherit" />,
      title: 'Balanced',
      detail: data.indexed
        ? 'Every record read is accounted for, and the record index agrees with the run’s own counters.'
        : 'Every record read is accounted for.',
    }
  }
  return {
    severity: 'error' as const,
    icon: <ErrorIcon fontSize="inherit" />,
    title: 'Does not balance',
    detail:
      'Something below does not add up. This is not always data loss — an audit policy changed ' +
      'mid-run will disagree legitimately — but it is always worth reading before signing anything.',
  }
}

/** The balance itself, read top to bottom, closing on a figure that must be zero. */
function Sheet({ data }: { data: Reconciliation }) {
  return (
    <Paper sx={{ overflow: 'auto' }}>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>{data.pipelineName}</TableCell>
            <TableCell align="right" sx={{ width: 140 }}>
              RECORDS
            </TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {data.sheet.map((line) => {
            const style = lineStyle(line.kind, line.count)
            return (
              <TableRow key={line.label} hover>
                <TableCell sx={{ ...style.cell, borderTop: style.rule }}>
                  <Tooltip title={line.note} placement="right">
                    <Box component="span" sx={{ pl: style.indent, cursor: 'help' }}>
                      {line.label}
                    </Box>
                  </Tooltip>
                </TableCell>
                <TableCell
                  align="right"
                  sx={{ ...tabular, ...style.cell, borderTop: style.rule }}
                >
                  {style.sign}
                  {line.count.toLocaleString()}
                </TableCell>
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
    </Paper>
  )
}

/**
 * How a row is drawn, from its kind alone.
 *
 * Deductions are indented and signed so the sheet reads as a subtraction rather than as a list of
 * unrelated numbers — the indentation is what makes it obvious at a glance which lines come out of
 * the line above them.
 */
function lineStyle(kind: ReconciliationKind, count: number) {
  switch (kind) {
    case 'TOTAL':
      return { indent: 0, sign: '', cell: { fontWeight: 600 }, rule: undefined }
    case 'DEDUCTION':
      return {
        indent: 2,
        sign: count > 0 ? '−' : '',
        cell: { color: count > 0 ? undefined : muted },
        rule: undefined,
      }
    case 'SUBTOTAL':
      return { indent: 0, sign: '', cell: { fontWeight: 600 }, rule: '1px solid' as const }
    case 'RESULT':
      return {
        indent: 2,
        sign: '',
        cell: { color: 'success.main', fontWeight: 600 },
        rule: undefined,
      }
    case 'PENDING':
      return { indent: 2, sign: '', cell: { color: 'warning.main' }, rule: undefined }
    case 'BALANCE':
      return {
        indent: 0,
        sign: '',
        cell: {
          fontWeight: 700,
          color: count === 0 ? 'success.main' : 'error.main',
        },
        rule: '2px solid' as const,
      }
  }
}

/**
 * The two counts side by side.
 *
 * The run's counters are incremented by workers as chunks finish; the index is written per record,
 * by different code, to a different store. This is the only part of the report capable of catching
 * a defect in the counting itself, which is why it is a separate table rather than a footnote.
 */
function Checks({ data }: { data: Reconciliation }) {
  const failing = data.checks.filter((check) => !check.passed).length

  return (
    <Paper sx={{ overflow: 'auto' }}>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>
              CHECKED AGAINST THE RECORD INDEX
              {failing > 0 && (
                <Typography component="span" variant="caption" sx={{ color: 'error.main', ml: 1 }}>
                  {failing} of {data.checks.length} disagree
                </Typography>
              )}
            </TableCell>
            <TableCell align="right">THIS RUN SAYS</TableCell>
            <TableCell align="right">THE INDEX SAYS</TableCell>
            <TableCell align="right" sx={{ width: 110 }}>
              DIFFERENCE
            </TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {data.checks.map((check) => (
            <TableRow key={check.label} hover>
              <TableCell>
                <Tooltip title={check.note} placement="right">
                  <Box component="span" sx={{ cursor: 'help' }}>
                    {check.label}
                  </Box>
                </Tooltip>
              </TableCell>
              <TableCell align="right" sx={tabular}>
                {check.expected.toLocaleString()}
              </TableCell>
              <TableCell align="right" sx={tabular}>
                {check.actual.toLocaleString()}
              </TableCell>
              <TableCell
                align="right"
                sx={{
                  ...tabular,
                  color: check.passed ? 'success.main' : 'error.main',
                  fontWeight: check.passed ? 400 : 600,
                }}
              >
                {check.passed
                  ? '—'
                  : `${check.difference > 0 ? '+' : ''}${check.difference.toLocaleString()}`}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Paper>
  )
}
