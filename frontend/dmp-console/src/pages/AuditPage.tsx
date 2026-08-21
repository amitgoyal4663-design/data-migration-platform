import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
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
import Button from '@mui/material/Button'
import { useState } from 'react'
import { useAuditTrail } from '@/api/hooks'
import { PageHeader } from '@/components/PageHeader'
import { ErrorPanel, Loading } from '@/components/Feedback'
import { muted, tabular } from '@/theme'

/** Resource types the platform records against. Free text would only invite typos. */
const RESOURCE_TYPES = ['', 'pipeline', 'pipeline-version', 'connector', 'schedule', 'run']

/**
 * Who changed what, and when.
 *
 * <p>The trail has been written since the platform's first commit, inside the same transaction as
 * each change, and until this page existed there was no way to read it short of opening a psql
 * session. Evidence nobody can examine is worth no more than evidence nobody collected — and worse,
 * because it looks as though the question is covered.
 */
export function AuditPage() {
  const [resourceType, setResourceType] = useState('')
  const [actor, setActor] = useState('')
  const [page, setPage] = useState(0)

  const trail = useAuditTrail({
    resourceType: resourceType || undefined,
    actor: actor || undefined,
    page,
  })

  const entries = trail.data?.content ?? []

  return (
    <>
      <PageHeader
        title="Audit trail"
        subtitle="Every change to a definition, and every run command. Append-only — the database refuses updates and deletes."
      />

      <Stack direction="row" spacing={2} sx={{ mb: 3, flexWrap: 'wrap' }}>
        <TextField
          select
          size="small"
          label="Resource"
          value={resourceType}
          onChange={(event) => {
            setResourceType(event.target.value)
            setPage(0)
          }}
          sx={{ minWidth: 200 }}
        >
          {RESOURCE_TYPES.map((type) => (
            <MenuItem key={type || 'all'} value={type}>
              {type || 'Everything'}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          size="small"
          label="Actor"
          placeholder="console, system:scheduler"
          value={actor}
          onChange={(event) => {
            setActor(event.target.value)
            setPage(0)
          }}
          sx={{ minWidth: 220 }}
        />
      </Stack>

      {trail.isLoading && <Loading />}
      {trail.error && <ErrorPanel error={trail.error} />}

      {entries.length === 0 && !trail.isLoading && (
        <Typography variant="body2" sx={{ color: muted, py: 3 }}>
          Nothing recorded for these filters.
        </Typography>
      )}

      {entries.length > 0 && (
        <>
          <Paper sx={{ overflow: 'auto' }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>WHEN</TableCell>
                  <TableCell>ACTOR</TableCell>
                  <TableCell>ACTION</TableCell>
                  <TableCell>RESOURCE</TableCell>
                  <TableCell>WHAT CHANGED</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {entries.map((entry) => (
                  <TableRow key={entry.id} hover>
                    <TableCell sx={{ whiteSpace: 'nowrap' }}>
                      <Typography variant="caption" sx={tabular}>
                        {new Date(entry.occurredAt).toLocaleString()}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="caption">{entry.actor}</Typography>
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={entry.action}
                        size="small"
                        variant="outlined"
                        color={toneOf(entry.action)}
                      />
                    </TableCell>
                    <TableCell>
                      <Typography variant="caption" sx={{ color: muted }}>
                        {entry.resourceType}
                      </Typography>
                    </TableCell>
                    <TableCell sx={{ maxWidth: 520 }}>
                      {/* The before/after diff is on the entry; the tooltip is where it belongs
                          rather than as a column nobody can read at a glance. */}
                      <Tooltip
                        title={
                          entry.before || entry.after ? (
                            <Box
                              component="pre"
                              sx={{ m: 0, fontSize: 11, maxHeight: 320, overflow: 'auto' }}
                            >
                              {JSON.stringify(entry.after ?? entry.before, null, 2).slice(0, 2000)}
                            </Box>
                          ) : (
                            ''
                          )
                        }
                      >
                        <Typography
                          variant="caption"
                          sx={{
                            cursor: entry.before || entry.after ? 'help' : 'default',
                            display: 'block',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                          }}
                        >
                          {entry.summary ?? '—'}
                        </Typography>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Paper>

          <Stack direction="row" spacing={2} alignItems="center" sx={{ mt: 2 }}>
            <Button size="small" disabled={page === 0} onClick={() => setPage(page - 1)}>
              Previous
            </Button>
            <Typography variant="caption" sx={{ color: muted, ...tabular }}>
              {trail.data && trail.data.totalElements >= 0
                ? `${page * 50 + 1}–${page * 50 + entries.length} of ${trail.data.totalElements.toLocaleString()}`
                : `page ${page + 1}`}
            </Typography>
            <Button
              size="small"
              disabled={!trail.data?.hasNext}
              onClick={() => setPage(page + 1)}
            >
              Next
            </Button>
          </Stack>
        </>
      )}
    </>
  )
}

function toneOf(action: string): 'error' | 'success' | 'warning' | 'default' {
  if (action === 'DELETE' || action === 'RUN_STOP') return 'error'
  if (action === 'PUBLISH' || action === 'RUN_START') return 'success'
  if (action === 'ROLLBACK' || action === 'ARCHIVE' || action === 'DISABLE') return 'warning'
  return 'default'
}
