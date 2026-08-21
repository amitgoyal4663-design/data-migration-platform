import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
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
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import AccountTreeIcon from '@mui/icons-material/AccountTreeOutlined'
import AddIcon from '@mui/icons-material/Add'
import { useState } from 'react'
import { Link as RouterLink, useNavigate } from 'react-router-dom'
import { useCreatePipeline, usePipelines } from '@/api/hooks'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState, ErrorPanel, Loading } from '@/components/Feedback'
import { muted, status } from '@/theme'

export function PipelinesPage() {
  const [search, setSearch] = useState('')
  const pipelines = usePipelines({ name: search || undefined })
  const [creating, setCreating] = useState(false)

  return (
    <>
      <PageHeader
        title="Pipelines"
        subtitle="A pipeline is the recipe. Each version is a frozen copy of it."
        actions={
          <Button startIcon={<AddIcon />} variant="contained" onClick={() => setCreating(true)}>
            New pipeline
          </Button>
        }
      />

      <TextField
        placeholder="Search by name"
        value={search}
        onChange={(event) => setSearch(event.target.value)}
        size="small"
        sx={{ mb: 2, width: { xs: '100%', sm: 320 } }}
      />

      <ErrorPanel error={pipelines.error} />

      {pipelines.isLoading ? (
        <Loading />
      ) : (pipelines.data?.content.length ?? 0) === 0 ? (
        <EmptyState
          icon={<AccountTreeIcon />}
          title={search ? 'No pipelines match that search' : 'No pipelines yet'}
          description="A pipeline describes where data comes from, what happens to it, and where it goes."
          action={
            <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreating(true)}>
              New pipeline
            </Button>
          }
        />
      ) : (
        <Paper sx={{ overflow: 'hidden' }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>NAME</TableCell>
                <TableCell>FOLDER</TableCell>
                <TableCell>TAGS</TableCell>
                <TableCell>PUBLISHED</TableCell>
                <TableCell>STATE</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {pipelines.data?.content.map((pipeline) => (
                <TableRow
                  key={pipeline.id}
                  hover
                  component={RouterLink}
                  to={`/pipelines/${pipeline.id}`}
                  sx={{ textDecoration: 'none', cursor: 'pointer' }}
                >
                  <TableCell>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      {pipeline.name}
                    </Typography>
                    {pipeline.description && (
                      <Typography variant="caption">{pipeline.description}</Typography>
                    )}
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" sx={{ color: muted }}>
                      {pipeline.folder ?? '—'}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
                      {pipeline.tags.map((tag) => (
                        <Chip key={tag} label={tag} size="small" variant="outlined" />
                      ))}
                    </Stack>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {pipeline.publishedVersion !== null ? (
                        <>
                          v{pipeline.publishedVersion}
                          {pipeline.latestVersion > pipeline.publishedVersion && (
                            <Typography component="span" variant="caption" sx={{ ml: 0.5 }}>
                              (v{pipeline.latestVersion} draft)
                            </Typography>
                          )}
                        </>
                      ) : (
                        <Typography component="span" variant="body2" sx={{ color: muted }}>
                          Not published
                        </Typography>
                      )}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    {/* Runnable is the property that matters day to day, not the status enum:
                        a pipeline is either ready to run or it needs something first. */}
                    <Chip
                      size="small"
                      variant="outlined"
                      label={pipeline.runnable ? 'Ready to run' : pipeline.status}
                      sx={{
                        color: pipeline.runnable ? status.good : muted,
                        borderColor: pipeline.runnable ? status.good : muted,
                        backgroundColor: pipeline.runnable ? `${status.good}14` : 'transparent',
                      }}
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Paper>
      )}

      {creating && <CreatePipelineDialog onClose={() => setCreating(false)} />}
    </>
  )
}

function CreatePipelineDialog({ onClose }: { onClose: () => void }) {
  const create = useCreatePipeline()
  const navigate = useNavigate()

  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [folder, setFolder] = useState('')
  const [tags, setTags] = useState('')

  const submit = () => {
    create.mutate(
      {
        name,
        description: description || undefined,
        folder: folder || undefined,
        tags: tags
          .split(',')
          .map((tag) => tag.trim())
          .filter(Boolean),
      },
      // Straight into the new pipeline: creating one is never the goal, designing it is.
      { onSuccess: (pipeline) => navigate(`/pipelines/${pipeline.id}`) },
    )
  }

  return (
    <Dialog open onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>New pipeline</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={2.5} sx={{ pt: 1 }}>
          <TextField
            label="Name *"
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="Orders to Databricks"
            size="small"
            fullWidth
            autoFocus
          />
          <TextField
            label="Description"
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            size="small"
            fullWidth
            multiline
            rows={2}
          />
          <TextField
            label="Folder"
            value={folder}
            onChange={(event) => setFolder(event.target.value)}
            placeholder="/finance/daily"
            helperText="Optional. Groups related pipelines together."
            size="small"
            fullWidth
          />
          <TextField
            label="Tags"
            value={tags}
            onChange={(event) => setTags(event.target.value)}
            placeholder="finance, daily"
            helperText="Comma separated. Lowercased automatically."
            size="small"
            fullWidth
          />
          <ErrorPanel error={create.error} />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" onClick={submit} disabled={!name || create.isPending}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
