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
import { label } from '@/components/SchemaForm'
import { muted } from '@/theme'

/**
 * Which records this connection reads — as one selection, or as several with names.
 *
 * <p>This is the only place the selection is written. The connector's schema marks the field that
 * holds it (`x-dmp-selection`: a Mongo filter, a SQL predicate, a whole SELECT) and the schema form
 * deliberately skips it, because a filter and a named query are the same sentence written twice —
 * and when both existed only one of them was read, with nothing on the form to say which.
 *
 * <p>Left unnamed it is exactly what it always was: a standing narrowing of what this connection
 * means, stored on the connection itself. Named, it becomes one of several a run can choose
 * between — "By date range" for the nightly load, "By policy number" for the question a support
 * desk actually arrives with, holding a policy number and no idea when it was last touched.
 *
 * <p>Whoever configures the connection writes them; they are queries against production in the
 * source's own language. Support sees the names, picks one, and fills in the boxes it asks for.
 * That distance is the point: a named list is a safe operation, a query box is a query tool.
 */
export function QueryVariantsEditor({
  schema,
  config,
  onChange,
}: {
  schema: JsonSchema
  config: Record<string, unknown>
  /** A patch merged over the configuration: the named queries, and the field they replace. */
  onChange: (patch: Record<string, unknown>) => void
}) {
  const fields = Object.entries(schema.properties ?? {}).filter(
    ([, field]) => field['x-dmp-selection'],
  )

  // Held as a list rather than as the object it is stored as, because a name is edited a letter at
  // a time and an object cannot hold two entries briefly sharing a name — nor an unnamed one, which
  // is what every connection written before names existed has.
  const [variants, setVariants] = useState<Variant[]>(() => read(config, fields.map(([n]) => n)))

  const named = variants.filter((variant) => variant.name.trim() !== '')

  // One rule, and the form is arranged so it cannot be broken: the connection's own selection is
  // read when there are no named queries, and only then. Naming one moves the selection into the
  // queries and clears the field, so there is never a second copy sitting behind a heading, still
  // editable, no longer read by anything.
  const published = useRef<string | null>(null)
  useEffect(() => {
    const patch: Record<string, unknown> = {}
    if (named.length > 0) {
      const queries: Record<string, Record<string, unknown>> = {}
      for (const variant of named) {
        queries[variant.name.trim()] = variant.values
      }
      patch.queries = queries
      for (const [name] of fields) {
        patch[name] = undefined
      }
    } else {
      patch.queries = undefined
      for (const [name] of fields) {
        const value = variants[0]?.values[name]
        patch[name] = value === '' ? undefined : value
      }
    }

    const next = JSON.stringify(patch)
    if (next === published.current) {
      return
    }
    published.current = next
    onChange(patch)
  }, [variants, named, fields, onChange])

  if (fields.length === 0) {
    return null
  }

  const update = (index: number, change: Partial<Variant>) =>
    setVariants((current) =>
      current.map((variant, at) => (at === index ? { ...variant, ...change } : variant)),
    )

  const add = () => setVariants((current) => [...current, { name: '', values: {} }])

  const remove = (index: number) =>
    setVariants((current) => {
      const left = current.filter((_, at) => at !== index)
      // Never none: there would then be no box to write a selection in, and a connection that had
      // one would have lost it with no way to type it back.
      return left.length === 0 ? [{ name: '', values: {} }] : left
    })

  const makeDefault = (index: number) =>
    setVariants((current) => [current[index]!, ...current.filter((_, at) => at !== index)])

  return (
    <Stack spacing={2}>
      <Box>
        <Typography variant="h3">Which records to read</Typography>
        <Typography variant="body2" sx={{ color: muted, mt: 0.5 }}>
          {named.length > 1
            ? 'A run picks one of these by name. The first is what a schedule gets when it names none.'
            : 'Leave the name blank and every run reads this. Name it, and add a second, to let a ' +
              'run choose — the nightly window, or one policy number somebody is asking about.'}
        </Typography>
      </Box>

      {variants.map((variant, index) => {
        const text = fields.map(([name]) => asText(variant.values[name])).join('\n')
        const placeholders = placeholdersIn(text)
        const isDefault = named.length > 1 && index === 0

        return (
          <Paper key={index} variant="outlined" sx={{ p: 2 }}>
            <Stack spacing={1.5}>
              <Stack direction="row" spacing={1} alignItems="flex-start">
                <TextField
                  label="Name"
                  value={variant.name}
                  onChange={(event) => update(index, { name: event.target.value })}
                  size="small"
                  fullWidth
                  placeholder="By policy number"
                  helperText={
                    isDefault
                      ? 'Used when a run or a schedule names none'
                      : variant.name.trim() === '' && variants.length === 1
                        ? 'Optional — without one this is simply what the connection reads'
                        : 'What support sees in the Run dialog'
                  }
                />
                {isDefault && <Chip label="Default" size="small" sx={{ mt: 0.75 }} />}
                {!isDefault && named.length > 1 && variant.name.trim() !== '' && (
                  <Tooltip title="Make this the default">
                    <IconButton size="small" onClick={() => makeDefault(index)} sx={{ mt: 0.25 }}>
                      <StarOutlineIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                )}
                <Tooltip title="Remove">
                  <IconButton size="small" onClick={() => remove(index)} sx={{ mt: 0.25 }}>
                    <DeleteOutlineIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Stack>

              {fields.map(([name, field]) => (
                <TextField
                  key={name}
                  label={label(name)}
                  value={asText(variant.values[name])}
                  onChange={(event) =>
                    update(index, { values: { ...variant.values, [name]: event.target.value } })
                  }
                  size="small"
                  fullWidth
                  multiline
                  minRows={2}
                  maxRows={12}
                  helperText={field.description}
                  slotProps={{ htmlInput: { style: { fontFamily: 'monospace', fontSize: 13 } } }}
                />
              ))}

              {placeholders.length === 0 ? (
                <Typography variant="caption" sx={{ color: muted }}>
                  Asks for nothing — the same records every run. Write <code>:policyNos</code> where
                  a run should supply the value.
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
        <Button
          size="small"
          startIcon={<AddIcon />}
          onClick={add}
          disabled={named.length === 0 && variants.length === 1}
        >
          Add another way to find records
        </Button>
        {named.length === 0 && variants.length === 1 && (
          <Typography variant="caption" sx={{ color: muted, ml: 1.5 }}>
            Name the one above first — a run chooses between them by name.
          </Typography>
        )}
      </Box>
    </Stack>
  )
}

interface Variant {
  name: string
  /** The configuration this replaces — the selection field, and nothing else. */
  values: Record<string, unknown>
}

/**
 * The connection's selection as a list of entries: its named queries, or — for a connection that
 * has none, which is every one written before names existed — the single unnamed one it has always
 * had, carried over unchanged so that opening this form changes nothing.
 */
function read(config: Record<string, unknown>, fields: string[]): Variant[] {
  const queries = config.queries
  if (queries && typeof queries === 'object' && !Array.isArray(queries)) {
    const entries = Object.entries(queries as Record<string, unknown>).map(([name, values]) => ({
      name,
      values:
        values && typeof values === 'object' && !Array.isArray(values)
          ? (values as Record<string, unknown>)
          : {},
    }))
    if (entries.length > 0) {
      return entries
    }
  }

  const values: Record<string, unknown> = {}
  for (const field of fields) {
    if (config[field] !== undefined && config[field] !== null) {
      values[field] = config[field]
    }
  }
  return [{ name: '', values }]
}

/**
 * A filter written by hand is a JSON object; one saved through this form is a string holding the
 * same JSON. Both are accepted on the way in, so both have to be shown.
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
 * morning, when the query it belongs to did not ask for the value it needed.
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
