import Editor from '@monaco-editor/react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import PlayIcon from '@mui/icons-material/PlayArrowOutlined'
import { useState } from 'react'
import { useTestTransform } from '@/api/hooks'
import { useThemeMode } from '@/store'
import type { TransformStage } from '@/api/types'

/**
 * Write a transformation script and see what it does to one record.
 *
 * <p>The Try button is the point of this component. A script is the one part of a pipeline whose
 * behaviour cannot be read off a form, and the alternative to trying it here is discovering the
 * mistake in a nightly run that has already written half a table in the wrong shape.
 */
export function ScriptEditor({
  stage,
  script,
  onChange,
  readOnly = false,
}: {
  stage: TransformStage
  script: string
  onChange: (script: string) => void
  /**
   * Readable but not editable, for a published version.
   *
   * The Try button stays: running a frozen script against a sample changes nothing and is exactly
   * what somebody looking at a production version wants to do — see what it actually does to a
   * record without having to copy it somewhere first.
   */
  readOnly?: boolean
}) {
  const mode = useThemeMode((state) => state.mode)
  const test = useTestTransform()

  const [sample, setSample] = useState(DEFAULT_SAMPLE)
  const [sampleError, setSampleError] = useState<string | null>(null)

  const run = () => {
    let parsed: unknown
    try {
      parsed = JSON.parse(sample)
    } catch {
      setSampleError('That is not valid JSON')
      return
    }
    setSampleError(null)
    test.mutate({ script, stage, sample: parsed })
  }

  const result = test.data

  return (
    <Stack spacing={2}>
      <Box>
        <Typography variant="caption" sx={{ display: 'block', mb: 0.5, fontWeight: 600 }}>
          {stage === 'BATCH' ? 'function transformBatch(records)' : 'function transform(record)'}
        </Typography>
        <Typography variant="caption" sx={{ display: 'block', mb: 1 }}>
          {stage === 'BATCH'
            ? 'Runs once per batch, on the list about to be written. Return the payload to send. ' +
              'It cannot add or remove records — only a per-record transform can do that. Its ' +
              'position on the canvas makes no difference: a batch step always runs at write time.'
            : 'Runs on every record, before batching. Return the record to keep it, null to drop ' +
              'it, or an array to turn it into several.'}
        </Typography>

        <Box
          sx={{
            border: 1,
            borderColor: 'divider',
            borderRadius: 1,
            overflow: 'hidden',
          }}
        >
          <Editor
            height="260px"
            defaultLanguage="javascript"
            theme={mode === 'dark' ? 'vs-dark' : 'light'}
            value={script}
            onChange={(value) => onChange(value ?? '')}
            options={{
              readOnly,
              minimap: { enabled: false },
              fontSize: 13,
              lineNumbers: 'on',
              scrollBeyondLastLine: false,
              tabSize: 2,
              automaticLayout: true,
            }}
          />
        </Box>
      </Box>

      <Alert severity="info" sx={{ '& .MuiAlert-message': { fontSize: 12.5 } }}>
        Scripts run in a sandbox with no network, no filesystem and no access to the platform.
        That is not only for safety: a chunk that fails is retried from its last checkpoint, so a
        script with side effects would produce them twice.
      </Alert>

      <TextField
        label="Sample record"
        value={sample}
        onChange={(event) => setSample(event.target.value)}
        error={Boolean(sampleError)}
        helperText={sampleError ?? 'One record, as JSON, to try the script against'}
        multiline
        minRows={3}
        maxRows={8}
        size="small"
        fullWidth
        slotProps={{ input: { sx: { fontFamily: 'monospace', fontSize: 12.5 } } }}
      />

      <Button
        variant="outlined"
        startIcon={<PlayIcon />}
        onClick={run}
        disabled={!script.trim() || test.isPending}
      >
        {test.isPending ? 'Running…' : 'Try it'}
      </Button>

      {result && !result.ok && (
        <Alert severity="error" sx={{ '& .MuiAlert-message': { fontSize: 12.5 } }}>
          {result.error}
        </Alert>
      )}

      {result?.ok && (
        <Stack spacing={1}>
          <Stack direction="row" spacing={1} alignItems="center">
            <Chip size="small" color="success" label="Ran" />
            <Typography variant="caption">{result.elapsedMillis} ms</Typography>
          </Stack>

          {/*
            A script returning nothing is the most confusing possible outcome: it looks broken and
            is usually a working filter. Saying so beats showing an empty box.
          */}
          {result.note && <Alert severity="warning" sx={{ fontSize: 12.5 }}>{result.note}</Alert>}

          {result.output != null && (
            <Box
              component="pre"
              sx={{
                m: 0,
                p: 1.5,
                borderRadius: 1,
                bgcolor: 'action.hover',
                fontSize: 12,
                fontFamily: 'monospace',
                overflowX: 'auto',
                maxHeight: 240,
              }}
            >
              {JSON.stringify(result.output, null, 2)}
            </Box>
          )}
        </Stack>
      )}
    </Stack>
  )
}

const DEFAULT_SAMPLE = JSON.stringify(
  { orderId: 'A-1001', quantity: 3, price: 250.5, status: 'NEW' },
  null,
  2,
)

/**
 * What a new transform node starts with.
 *
 * <p>Bare, because the panel above the editor already explains the return shapes and the guidance
 * belongs there rather than as comments the user has to delete before writing anything.
 *
 * <p>The per-record starter passes records through unchanged. That matters more than a clever
 * default: a node whose opening script silently rewrote data would mean dragging a step onto the
 * canvas and publishing changed the migration in a way nobody asked for. A batch node has no such
 * neutral position — reshaping the payload is the only reason it exists — so it starts with the
 * commonest shape an API asks for.
 */
export const STARTER_SCRIPTS: Record<TransformStage, string> = {
  RECORD: `function transform(record) {
  return record;
}
`,
  BATCH: `function transformBatch(records) {
  return { items: records };
}
`,
  // Not offered on the canvas: a split belongs to delivery rather than to the graph, and is edited
  // in the execution settings. Present here only because the stage is one of three, and a map
  // missing a key is a compile error waiting for whoever adds the fourth.
  SPLIT: `function split(records) {
  return records.map(r => r.region);
}
`,
}
