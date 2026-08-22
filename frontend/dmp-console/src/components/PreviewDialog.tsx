import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogTitle from '@mui/material/DialogTitle'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Tab from '@mui/material/Tab'
import TextField from '@mui/material/TextField'
import Tabs from '@mui/material/Tabs'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import { useEffect, useState } from 'react'
import { usePreviewSource, useSourceParameters } from '@/api/hooks'
import { ErrorPanel } from '@/components/Feedback'
import { muted, tabular } from '@/theme'

/**
 * What a record from this source actually looks like.
 *
 * The question every mapping starts with, and one the console could not answer — so field names
 * went into scripts from memory, and the first thing that ever compared them with reality was a
 * run. A numeric field typed as a string and a nested object assumed to be flat are both obvious
 * here and both expensive to discover on chunk zero.
 *
 * Two views because two questions are being asked. The table answers "what columns are there";
 * the JSON answers "what is the exact shape", which a table flattens away and which is what a
 * script is actually handed.
 */
export function PreviewDialog({
  open,
  connectorInstanceId,
  name,
  onClose,
  onUseRecord,
}: {
  open: boolean
  connectorInstanceId: string
  name: string
  onClose: () => void
  /** Offered when the caller has somewhere to put a record — the script editor's sample box. */
  onUseRecord?: (record: Record<string, unknown>) => void
}) {
  const preview = usePreviewSource()
  const parameters = useSourceParameters(open ? connectorInstanceId : undefined)
  const [view, setView] = useState(0)
  const [selected, setSelected] = useState(0)
  const [values, setValues] = useState<Record<string, string>>({})

  const names = parameters.data?.names ?? []
  const needsValues = names.some((name) => (values[name] ?? '').trim() === '')

  // Fetched on open rather than on mount: this makes a real call to somebody else's system, and a
  // dialog mounted behind a closed flag would make it on a page nobody asked anything of. Held
  // back entirely while the query still has unfilled placeholders — reading with them missing is
  // a refusal from the connector, which reads as a broken button rather than a missing value.
  useEffect(() => {
    if (!open || parameters.isLoading || needsValues) return
    setSelected(0)
    preview.mutate({ connectorInstanceId, limit: 10, parameters: values })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, connectorInstanceId, parameters.isLoading])

  const data = preview.data

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>
      <DialogTitle sx={{ display: 'flex', alignItems: 'baseline', gap: 1.5 }}>
        {name}
        {data && (
          <Typography variant="caption" sx={{ color: muted }}>
            {data.records.length} record{data.records.length === 1 ? '' : 's'}
            {data.more && ' (more available)'} · {data.fields.length} field
            {data.fields.length === 1 ? '' : 's'} · {data.durationMillis.toLocaleString()} ms
          </Typography>
        )}
      </DialogTitle>

      <DialogContent>
        <Stack spacing={2}>
          {names.length > 0 && (
            <Stack spacing={1.5}>
              <Typography variant="body2" sx={{ color: muted }}>
                This source&apos;s query expects {names.length === 1 ? 'a value' : 'values'} for{' '}
                {names.map((name) => (
                  <code key={name}>:{name} </code>
                ))}
                — the same ones a run would be started with.
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                {names.map((name) => (
                  <TextField
                    key={name}
                    label={name}
                    value={values[name] ?? ''}
                    onChange={(event) =>
                      setValues((current) => ({ ...current, [name]: event.target.value }))
                    }
                    size="small"
                    placeholder="5000, or 2026-08-01T00:00:00Z"
                    sx={{ flex: 1, minWidth: 180 }}
                  />
                ))}
                <Button
                  variant="outlined"
                  disabled={needsValues || preview.isPending}
                  onClick={() =>
                    preview.mutate({ connectorInstanceId, limit: 10, parameters: values })
                  }
                >
                  Read
                </Button>
              </Box>
            </Stack>
          )}

          {preview.isPending && (
            <Typography variant="body2" sx={{ color: muted }}>
              Reading from the source…
            </Typography>
          )}

          <ErrorPanel error={preview.error} />

          {data && data.records.length === 0 && (
            <Alert severity="warning">
              The source returned nothing. That is an answer, not a failure — the query below is
              what was actually asked, and it is almost always the explanation.
              {data.query && (
                <Box
                  component="pre"
                  sx={{ ...tabular, mt: 1, mb: 0, whiteSpace: 'pre-wrap', fontSize: 12 }}
                >
                  {data.query}
                </Box>
              )}
            </Alert>
          )}

          {data && data.records.length > 0 && (
            <>
              <Tabs value={view} onChange={(_, next) => setView(next)}>
                <Tab label="Table" sx={{ textTransform: 'none' }} />
                <Tab label="JSON" sx={{ textTransform: 'none' }} />
              </Tabs>

              {view === 0 && <RecordTable records={data.records} fields={data.fields} />}

              {view === 1 && (
                <>
                  <Stack direction="row" spacing={0.5} sx={{ flexWrap: 'wrap', gap: 0.5 }}>
                    {data.records.map((_, index) => (
                      <Button
                        key={index}
                        size="small"
                        variant={index === selected ? 'contained' : 'outlined'}
                        onClick={() => setSelected(index)}
                        sx={{ minWidth: 36, px: 1 }}
                      >
                        {index + 1}
                      </Button>
                    ))}
                  </Stack>
                  <Paper
                    component="pre"
                    sx={{
                      ...tabular,
                      p: 1.5,
                      m: 0,
                      fontSize: 12.5,
                      maxHeight: 380,
                      overflow: 'auto',
                    }}
                  >
                    {JSON.stringify(data.records[selected], null, 2)}
                  </Paper>
                </>
              )}

              {data.query && (
                <Typography variant="caption" sx={{ ...tabular, color: muted, fontSize: 11.5 }}>
                  {data.query}
                </Typography>
              )}
            </>
          )}

          <Typography variant="caption" sx={{ color: muted }}>
            Read from the real source with the real credentials. Not a run — nothing is planned,
            nothing is written, and this does not appear in the run history.
          </Typography>
        </Stack>
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        {onUseRecord && data && data.records.length > 0 && (
          <Button
            variant="contained"
            onClick={() => {
              const record = data.records[view === 1 ? selected : 0]
              if (record) onUseRecord(record)
              onClose()
            }}
          >
            Use record {view === 1 ? selected + 1 : 1}
          </Button>
        )}
      </DialogActions>
    </Dialog>
  )
}

/**
 * The records as a grid.
 *
 * Nested values are shown as their JSON rather than as [object Object], because a field being an
 * object is the single most useful thing this view can tell somebody about to write `record.city`.
 */
function RecordTable({
  records,
  fields,
}: {
  records: Record<string, unknown>[]
  fields: string[]
}) {
  return (
    <Paper sx={{ overflow: 'auto', maxHeight: 420 }}>
      <Table size="small" stickyHeader>
        <TableHead>
          <TableRow>
            {fields.map((field) => (
              <TableCell key={field} sx={{ whiteSpace: 'nowrap' }}>
                {field}
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {records.map((record, index) => (
            <TableRow key={index} hover>
              {fields.map((field) => (
                <TableCell key={field} sx={{ ...tabular, fontSize: 12, maxWidth: 260 }}>
                  <Cell value={record[field]} />
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Paper>
  )
}

function Cell({ value }: { value: unknown }) {
  if (value === null || value === undefined) {
    // Absent and null are different — a script guarding on one will not catch the other — so this
    // says which, rather than leaving an empty cell that could be either.
    return (
      <Typography component="span" variant="caption" sx={{ color: muted, fontStyle: 'italic' }}>
        {value === null ? 'null' : '—'}
      </Typography>
    )
  }

  const text = typeof value === 'object' ? JSON.stringify(value) : String(value)
  return (
    <Tooltip title={text.length > 60 ? text : ''} placement="top">
      <Box
        component="span"
        sx={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
      >
        {text}
      </Box>
    </Tooltip>
  )
}
