import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline'
import StarOutlineIcon from '@mui/icons-material/StarOutline'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import IconButton from '@mui/material/IconButton'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import { useEffect, useRef, useState } from 'react'
import type { JsonSchema } from '@/api/types'
import { muted } from '@/theme'

/**
 * The named ways this connection can find records.
 *
 * <p>A connection has always described exactly one selection — a filter, a predicate, a SELECT.
 * That is right for the job it was built for and useless for the question people actually arrive
 * with: *what happened to policy POL-44219?* Nobody knows when it was last touched, which is why
 * they are asking, so a date range cannot answer it.
 *
 * <p>So the selection can be written several times over, each with a name. Whoever configures the
 * connection writes them — they are queries against production, in the source's own language. On
 * the Run dialog support sees only the names, picks one, and fills in the boxes it asks for. That
 * distance is the whole point: a named list is a safe operation, a query box is a query tool.
 *
 * <p>Which field a query replaces is the connector's own answer, read from its schema
 * (`x-dmp-selection`) rather than guessed from a list of likely names. A connector that marks none
 * — Kafka, a file drop — shows nothing here, correctly: its selection is not one setting.
 */
export function QueryVariantsEditor({
  schema,
  config,
  onChange,
}: {
  schema: JsonSchema
  config: Record<string, unknown>
  onChange: (queries: Record<string, Record<string, unknown>> | undefined) => void
}) {
  const fields = Object.entries(schema.properties ?? {}).filter(
    ([, field]) => field['x-dmp-selection'],
  )

  // Held as a list rather than as the object it is stored as, because a name is edited a letter at
  // a time and an object cannot hold two variants briefly sharing a name — nor an unnamed one, which
  // is what every new variant is for the first keystroke.
  const [variants, setVariants] = useState<Variant[]>(() => read(config))

  // Order carries meaning: the first is what a run gets when it names none, including every
  // schedule written before this existed. Storing the list in order is what makes that stable.
  const published = useRef<string | null>(null)
  useEffect(() => {
    const named = variants.filter((variant) => variant.name.trim() !== '')
    const queries: Record<string, Record<string, unknown>> = {}
    for (const variant of named) {
      queries[variant.name.trim()] = variant.values
    }
    const next = named.length === 0 ? null : JSON.stringify(queries)
    if (next === published.current) {
      return
    }
    published.current = next
    onChange(named.length === 0 ? undefined : queries)
  }, [variants, onChange])

  if (fields.length === 0) {
    return null
  }

  const update = (index: number, change: Partial<Variant>) =>
    setVariants((current) =>
      current.map((variant, at) => (at === index ? { ...variant, ...change } : variant)),
    )

  const add = () =>
    setVariants((current) => [
      ...current,
      {
        name: '',
        // The first one starts as whatever this connection reads today, so writing the second is
        // an addition rather than a retyping — and so the behaviour nothing has changed yet stays
        // reachable by the name somebody gives it.
        values:
          current.length === 0
            ? Object.fromEntries(
                fields
                  .filter(([name]) => config[name] !== undefined && config[name] !== '')
                  .map(([name]) => [name, config[name]]),
              )
            : {},
      },
    ])

  const remove = (index: number) =>
    setVariants((current) => current.filter((_, at) => at !== index))

  const makeDefault = (index: number) =>
    setVariants((current) => [
      current[index]!,
      ...current.filter((_, at) => at !== index),
    ])

  return (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h3">Ways to find records</Typography>
        <Typography variant="body2" sx={{ color: muted, mt: 0.5 }}>
          A run picks one of these by name. Write one for the load that runs on a schedule and one
          for the question a support desk arrives with — the same pipeline, the same mapping, the
          same destination, a different set of records.
        </Typography>
      </Box>

      {variants.length === 0 && (
        <Typography variant="body2" sx={{ color: muted }}>
          None yet, so every run reads what this connection is configured to read. Adding the first
          one starts from exactly that, and changes nothing until you add a second.
        </Typography>
      )}

      {variants.map((variant, index) => {
        const text = fields.map(([name]) => asText(variant.values[name])).join('\n')
        const placeholders = placeholdersIn(text)

        return (
          <Paper key={index} variant="outlined" sx={{ p: 2 }}>
            <Stack spacing={1.5}>
              <Stack direction="row" spacing={1} alignItems="center">
                <TextField
                  label="Name"
                  value={variant.name}
                  onChange={(event) => update(index, { name: event.target.value })}
                  size="small"
                  fullWidth
                  placeholder="By policy number"
                  helperText={
                    index === 0
                      ? 'Used when a run or a schedule names none'
                      : 'What support sees in the Run dialog'
                  }
                />
                {index === 0 ? (
                  <Chip label="Default" size="small" sx={{ mb: 2.5 }} />
                ) : (
                  <Tooltip title="Make this the default">
                    <IconButton
                      size="small"
                      onClick={() => makeDefault(index)}
                      sx={{ mb: 2.5 }}
                    >
                      <StarOutlineIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                )}
                <Tooltip title="Remove">
                  <IconButton size="small" onClick={() => remove(index)} sx={{ mb: 2.5 }}>
                    <DeleteOutlineIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Stack>

              {fields.map(([name, field]) => (
                <TextField
                  key={name}
                  label={name}
                  value={asText(variant.values[name])}
                  onChange={(event) =>
                    update(index, {
                      values: { ...variant.values, [name]: event.target.value },
                    })
                  }
                  size="small"
                  fullWidth
                  multiline
                  minRows={2}
                  maxRows={10}
                  helperText={field.description}
                  slotProps={{ htmlInput: { style: { fontFamily: 'monospace', fontSize: 13 } } }}
                />
              ))}

              {placeholders.length === 0 ? (
                <Typography variant="caption" sx={{ color: muted }}>
                  Asks for nothing — this query is the same every run. Write{' '}
                  <code>:policyNos</code> where a run should supply a value.
                </Typography>
              ) : (
                <Stack direction="row" spacing={0.75} flexWrap="wrap" useFlexGap alignItems="center">
                  <Typography variant="caption" sx={{ color: muted }}>
                    Asks for
                  </Typography>
                  {placeholders.map((placeholder) => (
                    <Chip
                      key={placeholder.name}
                      size="small"
                      variant="outlined"
                      label={placeholder.list ? `${placeholder.name} (a list)` : placeholder.name}
                    />
                  ))}
                </Stack>
              )}
            </Stack>
          </Paper>
        )
      })}

      <Box>
        <Button size="small" startIcon={<AddIcon />} onClick={add}>
          {variants.length === 0 ? 'Add a way to find records' : 'Add another'}
        </Button>
      </Box>
    </Stack>
  )
}

interface Variant {
  name: string
  /** The configuration this query replaces — the selection field, and nothing else. */
  values: Record<string, unknown>
}

function read(config: Record<string, unknown>): Variant[] {
  const queries = config.queries
  if (!queries || typeof queries !== 'object' || Array.isArray(queries)) {
    return []
  }
  return Object.entries(queries as Record<string, unknown>).map(([name, values]) => ({
    name,
    values:
      values && typeof values === 'object' && !Array.isArray(values)
        ? (values as Record<string, unknown>)
        : {},
  }))
}

/**
 * A filter written by hand is a JSON object; one saved through the schema form is a string holding
 * the same JSON. Both are accepted on the way in, so both have to be shown.
 */
function asText(value: unknown): string {
  if (value === undefined || value === null) {
    return ''
  }
  return typeof value === 'string' ? value : JSON.stringify(value, null, 2)
}

const PLACEHOLDER = /:([A-Za-z_][A-Za-z0-9_]*)/g

/** `IN (:policyNos)` in SQL, `"$in": ":policyNos"` in a Mongo filter. */
const SQL_LIST = /\bIN\s*\(\s*:([A-Za-z_][A-Za-z0-9_]*)\s*\)/gi
const MONGO_LIST = /"\$(?:in|nin|all)"\s*:\s*"\s*:([A-Za-z_][A-Za-z0-9_]*)\s*"/gi

/**
 * What a run will be asked for, worked out the way the backend works it out.
 *
 * <p>A hint, not the authority — the connector decides, and the Run dialog asks it. Shown here so
 * that a mistyped placeholder is visible while it is being written rather than at two in the
 * morning when the query it belongs to did not ask for the value it needed.
 */
function placeholdersIn(text: string): { name: string; list: boolean }[] {
  const lists = new Set<string>()
  for (const pattern of [SQL_LIST, MONGO_LIST]) {
    for (const match of text.matchAll(pattern)) {
      lists.add(match[1]!)
    }
  }

  const seen = new Map<string, boolean>()
  for (const match of text.matchAll(PLACEHOLDER)) {
    const name = match[1]!
    seen.set(name, lists.has(name))
  }
  return [...seen].map(([name, list]) => ({ name, list }))
}
