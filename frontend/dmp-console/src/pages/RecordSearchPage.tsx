import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import InputAdornment from '@mui/material/InputAdornment'
import Link from '@mui/material/Link'
import Paper from '@mui/material/Paper'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import SearchIcon from '@mui/icons-material/SearchOutlined'
import { useState } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import { usePipelines, useRecordSearch } from '@/api/hooks'
import { PageHeader } from '@/components/PageHeader'
import { ErrorPanel, Loading } from '@/components/Feedback'
import { muted, tabular } from '@/theme'

/**
 * "Was this record transferred, and what happened to it?"
 *
 * The question a support engineer is actually asked — months after a cutover, about one identifier
 * out of crores. Neither the run counters nor the destination can answer it: counters say how many,
 * and the destination says whether the record is there now, not whether this platform put it there.
 */
export function RecordSearchPage() {
  const pipelines = usePipelines()
  const [pipelineId, setPipelineId] = useState('')
  const [typed, setTyped] = useState('')
  const [key, setKey] = useState('')
  const results = useRecordSearch(pipelineId, key)

  const searched = Boolean(pipelineId) && key.trim().length > 0
  const found = results.data?.content ?? []
  const chosen = pipelines.data?.content.find((p) => p.id === pipelineId)

  return (
    <>
      <PageHeader
        title="Find a record"
        subtitle="Pick the pipeline that moved it, then search by the source's own identifier — a MongoDB _id, a primary key, a message key."
      />

      {/*
        Pipeline first, and not optional. A record key is only unique within the source it came
        from — two pipelines moving different systems can both hold a record numbered 88291 — so an
        unscoped search would answer a question nobody asked. It is also what makes the query
        authorizable once access is granted per pipeline.
      */}
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} sx={{ mb: 3, maxWidth: 900 }}>
        <TextField
          select
          size="small"
          label="Pipeline"
          value={pipelineId}
          onChange={(event) => {
            setPipelineId(event.target.value)
            setKey('')
          }}
          sx={{ minWidth: 260 }}
          helperText={
            pipelines.data && pipelines.data.content.length === 0
              ? 'No pipelines yet'
              : 'Which migration moved this record'
          }
        >
          {(pipelines.data?.content ?? []).map((pipeline) => (
            <MenuItem key={pipeline.id} value={pipeline.id}>
              {pipeline.name}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          fullWidth
          size="small"
          disabled={!pipelineId}
          placeholder="oid:6a74f8d822f41ebe40a1e179"
          label="Record key"
          value={typed}
          onChange={(event) => setTyped(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') setKey(typed)
          }}
          slotProps={{
            input: {
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" />
                </InputAdornment>
              ),
            },
          }}
          helperText={
            pipelineId
              ? 'Press Enter to search. The key must match exactly, as the source reported it.'
              : 'Choose a pipeline first'
          }
        />
      </Stack>

      {results.isLoading && searched && <Loading />}
      {results.error && <ErrorPanel error={results.error} />}

      {/*
        An empty result is stated as two possibilities rather than one. "Not found" would be a
        wrong answer for a pipeline that simply does not index its records, and the difference —
        "we have no record of this" versus "this was never migrated" — is exactly the difference
        somebody would act on.
      */}
      {searched && !results.isLoading && found.length === 0 && (
        <Alert severity="info">
          <Typography variant="body2" sx={{ mb: 1 }}>
            Nothing indexed under <strong>{key}</strong> in{' '}
            <strong>{chosen?.name ?? 'this pipeline'}</strong>.
          </Typography>
          <Typography variant="caption">
            That means either no run of this pipeline handled the record, or this pipeline does not
            index its records. Only pipelines whose audit level is <strong>indexed</strong> write
            these entries — check the version's Settings before concluding the record never arrived.
            If another pipeline may have moved it, search that one too: the index is scoped to the
            pipeline, because the same key can exist in more than one source.
          </Typography>
        </Alert>
      )}

      {found.length > 0 && (
        <>
          <Typography variant="body2" sx={{ color: muted, mb: 1.5 }}>
            {found.length} run{found.length === 1 ? '' : 's'} of {chosen?.name} handled this
            record, newest first.
          </Typography>

          <Paper sx={{ overflow: 'auto' }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>OUTCOME</TableCell>
                  <TableCell>WHEN</TableCell>
                  <TableCell>RUN</TableCell>
                  <TableCell>CHUNK</TableCell>
                  <TableCell>ERROR</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {found.map((entry) => (
                  // Keyed by the engine's coordinates, not the record's own key. Searching for a
                  // key a source holds twice returns two rows, and keying on it would give React
                  // two identical keys — the same collision that used to lose one of them.
                  <TableRow key={`${entry.chunkId}:${entry.seq}:${entry.ordinal}`} hover>
                    <TableCell>
                      <OutcomeChip outcome={entry.outcome} />
                    </TableCell>
                    <TableCell>
                      <Typography variant="caption" sx={tabular}>
                        {new Date(entry.occurredAt).toLocaleString()}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Link
                        component={RouterLink}
                        to={`/runs/${entry.runId}`}
                        variant="caption"
                        sx={tabular}
                      >
                        {entry.runId.slice(0, 8)}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <Typography variant="caption" sx={{ ...tabular, color: muted }}>
                        {entry.chunkId.slice(0, 8)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="caption" sx={tabular}>
                        {entry.errorCode ?? '—'}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Paper>

          <Box sx={{ mt: 2 }}>
            <Typography variant="caption" sx={{ color: muted }}>
              To search by content rather than by key — every record whose email ends @acme.com, or
              everything with status cancelled — use the search cluster's own dashboards. That
              needs the pipeline to be indexing record content, not identities alone.
            </Typography>
          </Box>
        </>
      )}
    </>
  )
}

function OutcomeChip({ outcome }: { outcome: string }) {
  const tone =
    outcome === 'WRITTEN' ? 'success' : outcome === 'REJECTED' ? 'error' : 'default'

  return <Chip label={outcome} size="small" color={tone} variant="outlined" />
}
