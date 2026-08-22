import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Checkbox from '@mui/material/Checkbox'
import FormControlLabel from '@mui/material/FormControlLabel'
import IconButton from '@mui/material/IconButton'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import AddIcon from '@mui/icons-material/Add'
import DeleteIcon from '@mui/icons-material/DeleteOutline'
import TableRowsIcon from '@mui/icons-material/TableRowsOutlined'
import { useState } from 'react'
import { PreviewDialog } from '@/components/PreviewDialog'
import { muted } from '@/theme'

/** The conversions the mapper can do, and when each is the answer. */
const TYPES = [
  { value: 'AS_IS', label: 'As-is', note: 'Carry the value through untouched' },
  { value: 'STRING', label: 'Text', note: '' },
  { value: 'NUMBER', label: 'Number', note: 'Turns "50.0" into 50.0' },
  { value: 'INTEGER', label: 'Whole number', note: 'Refuses 3.5' },
  { value: 'BOOLEAN', label: 'True/false', note: 'Accepts true, 1, yes, y and their opposites' },
  { value: 'DATE', label: 'Date', note: 'Normalised to ISO-8601' },
]

export interface Mapping {
  /** One field, or several to join. Absent with a default means a constant. */
  from?: string | string[]
  to: string
  type?: string
  required?: boolean
  default?: unknown
  join?: string
  /** Source value → destination value, for two systems with different words for one state. */
  values?: Record<string, string>
  otherwise?: string
  trim?: boolean
  case?: 'UPPER' | 'LOWER'
  prefix?: string
  suffix?: string
  replace?: { find: string; with: string }
  maxLength?: number
  /** OMIT leaves the key out; NULL writes an explicit null, which clears a field on an upsert. */
  onMissing?: 'OMIT' | 'NULL'
}

/**
 * Field mapping as a two-column list.
 *
 * The reason this exists rather than a script: renaming fields is the most common task in the
 * platform and it was behind the one skill most people doing migrations do not have. It is also
 * information — twelve rows can be read, diffed and checked against a real record, where twelve
 * lines of JavaScript are an opaque blob.
 */
export function MapperEditor({
  config,
  onChange,
  readOnly,
  sourceInstanceId,
  sourceName,
}: {
  config: Record<string, unknown>
  onChange: (config: Record<string, unknown>) => void
  readOnly: boolean
  sourceInstanceId?: string | null
  sourceName?: string
}) {
  const mappings = (config.mappings as Mapping[] | undefined) ?? []
  const keepUnmapped = Boolean(config.keepUnmapped)
  const [picking, setPicking] = useState(false)

  const set = (next: Mapping[], keep = keepUnmapped) =>
    onChange({ ...config, mappings: next, keepUnmapped: keep })

  const update = (index: number, changes: Partial<Mapping>) =>
    set(mappings.map((mapping, i) => (i === index ? { ...mapping, ...changes } : mapping)))

  return (
    <Stack spacing={1.5}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Typography variant="body2" sx={{ flex: 1 }}>
          {mappings.length} field{mappings.length === 1 ? '' : 's'}
        </Typography>
        {!readOnly && sourceInstanceId && (
          <Button size="small" startIcon={<TableRowsIcon />} onClick={() => setPicking(true)}>
            From a real record
          </Button>
        )}
        {!readOnly && (
          <Button
            size="small"
            startIcon={<AddIcon />}
            onClick={() => set([...mappings, { from: '', to: '', type: 'AS_IS' }])}
          >
            Add
          </Button>
        )}
      </Box>

      {mappings.length === 0 && (
        <Alert severity="info" sx={{ '& .MuiAlert-message': { fontSize: 13 } }}>
          Nothing is mapped, so this step would send an empty record. Add a field, or read one from
          the source and start from its actual field names.
        </Alert>
      )}

      {mappings.map((mapping, index) => (
        <MappingRow
          key={index}
          mapping={mapping}
          readOnly={readOnly}
          onChange={(changes) => update(index, changes)}
          onRemove={() => set(mappings.filter((_, i) => i !== index))}
        />
      ))}

      <FormControlLabel
        control={
          <Checkbox
            size="small"
            checked={keepUnmapped}
            disabled={readOnly}
            onChange={(_, checked) => set(mappings, checked)}
          />
        }
        label={<Typography variant="body2">Also carry through fields not listed above</Typography>}
      />
      <Typography variant="caption" sx={{ color: muted, mt: -1 }}>
        Off by default. A mapper says what the destination receives — carrying everything through
        means a source column added months from now silently starts arriving somewhere nobody
        declared it.
      </Typography>

      {picking && sourceInstanceId && (
        <PreviewDialog
          open
          connectorInstanceId={sourceInstanceId}
          name={sourceName ?? 'Source'}
          onClose={() => setPicking(false)}
          onUseRecord={(record) => {
            // Seeds a row per field, from the record's real shape rather than from memory. Targets
            // start equal to sources: renaming three of twelve is quicker than typing twelve.
            const existing = new Set(
              mappings.map((mapping) =>
                Array.isArray(mapping.from) ? mapping.from.join(',') : (mapping.from ?? ''),
              ),
            )
            const added = Object.keys(record)
              .filter((field) => !existing.has(field))
              .map((field) => ({ from: field, to: field, type: 'AS_IS' }))
            set([...mappings, ...added])
          }}
        />
      )}
    </Stack>
  )
}

/**
 * One mapping.
 *
 * The four things every mapping needs are on one line; everything else is behind "More", because a
 * pipeline with twenty mappings that each showed ten inputs would be unreadable — and nineteen of
 * the twenty are a rename and a type.
 */
function MappingRow({
  mapping,
  readOnly,
  onChange,
  onRemove,
}: {
  mapping: Mapping
  readOnly: boolean
  onChange: (changes: Partial<Mapping>) => void
  onRemove: () => void
}) {
  const extras =
    Boolean(mapping.default) ||
    Boolean(mapping.values) ||
    Boolean(mapping.trim) ||
    Boolean(mapping.case) ||
    Boolean(mapping.prefix) ||
    Boolean(mapping.suffix) ||
    Boolean(mapping.replace) ||
    typeof mapping.maxLength === 'number' ||
    mapping.onMissing === 'NULL'

  const [open, setOpen] = useState(extras)
  const fromText = Array.isArray(mapping.from) ? mapping.from.join(', ') : (mapping.from ?? '')

  return (
    <Box sx={{ pb: 1, borderBottom: '1px solid', borderColor: 'divider' }}>
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start', flexWrap: 'wrap' }}>
        <TextField
          label="From"
          value={fromText}
          onChange={(event) => {
            const raw = event.target.value
            // A comma means "join these". Typed rather than a separate mode, because the moment
            // somebody wants two fields they already know which two.
            onChange({
              from: raw.includes(',')
                ? raw.split(',').map((part) => part.trim()).filter(Boolean)
                : raw,
            })
          }}
          size="small"
          disabled={readOnly}
          placeholder="customer.address.city"
          helperText={
            Array.isArray(mapping.from)
              ? `joined with "${mapping.join ?? ' '}"`
              : fromText.trim() === ''
                ? 'empty = a constant, set a default below'
                : undefined
          }
          sx={{ flex: 1, minWidth: 150 }}
        />
        <TextField
          label="To"
          value={mapping.to}
          onChange={(event) => onChange({ to: event.target.value })}
          size="small"
          disabled={readOnly}
          sx={{ flex: 1, minWidth: 150 }}
        />
        <TextField
          select
          label="As"
          value={mapping.type ?? 'AS_IS'}
          onChange={(event) => onChange({ type: event.target.value })}
          size="small"
          disabled={readOnly}
          sx={{ width: 130 }}
        >
          {TYPES.map((type) => (
            <MenuItem key={type.value} value={type.value}>
              {type.label}
            </MenuItem>
          ))}
        </TextField>
        <Tooltip title="Fail the record if this ends up with no value, naming this field">
          <FormControlLabel
            control={
              <Checkbox
                size="small"
                checked={Boolean(mapping.required)}
                disabled={readOnly}
                onChange={(_, checked) => onChange({ required: checked })}
              />
            }
            label={<Typography variant="caption">Required</Typography>}
            sx={{ mr: 0 }}
          />
        </Tooltip>
        <Button size="small" onClick={() => setOpen(!open)} sx={{ minWidth: 64 }}>
          {open ? 'Less' : extras ? 'More •' : 'More'}
        </Button>
        {!readOnly && (
          <IconButton size="small" onClick={onRemove}>
            <DeleteIcon fontSize="small" />
          </IconButton>
        )}
      </Box>

      {open && (
        <Box sx={{ pl: 1, pt: 1, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
          <TextField
            label="Default"
            value={String(mapping.default ?? '')}
            onChange={(event) => onChange({ default: event.target.value || undefined })}
            size="small"
            disabled={readOnly}
            helperText="Used when the source has no value, or an empty one"
            sx={{ minWidth: 170 }}
          />
          <TextField
            label="Translate values"
            value={
              mapping.values
                ? Object.entries(mapping.values).map(([k, v]) => `${k}=${v}`).join(', ')
                : ''
            }
            onChange={(event) => {
              const table: Record<string, string> = {}
              for (const pair of event.target.value.split(',')) {
                const [key, ...rest] = pair.split('=')
                if (key?.trim() && rest.length) table[key.trim()] = rest.join('=').trim()
              }
              onChange({ values: Object.keys(table).length ? table : undefined })
            }}
            size="small"
            disabled={readOnly}
            placeholder="NEW=Open, SHIPPED=Closed"
            helperText="Source value = destination value"
            sx={{ flex: 1, minWidth: 230 }}
          />
          <TextField
            label="Unlisted becomes"
            value={mapping.otherwise ?? ''}
            onChange={(event) => onChange({ otherwise: event.target.value || undefined })}
            size="small"
            disabled={readOnly}
            helperText="Blank = pass through unchanged"
            sx={{ minWidth: 160 }}
          />
          <TextField
            label="Prefix"
            value={mapping.prefix ?? ''}
            onChange={(event) => onChange({ prefix: event.target.value || undefined })}
            size="small"
            disabled={readOnly}
            sx={{ width: 110 }}
          />
          <TextField
            label="Suffix"
            value={mapping.suffix ?? ''}
            onChange={(event) => onChange({ suffix: event.target.value || undefined })}
            size="small"
            disabled={readOnly}
            sx={{ width: 110 }}
          />
          <TextField
            label="Remove text"
            value={mapping.replace?.find ?? ''}
            onChange={(event) =>
              onChange({
                replace: event.target.value
                  ? { find: event.target.value, with: mapping.replace?.with ?? '' }
                  : undefined,
              })
            }
            size="small"
            disabled={readOnly}
            placeholder="DBX-"
            helperText="Exact text, not a pattern"
            sx={{ width: 150 }}
          />
          <TextField
            label="Replace with"
            value={mapping.replace?.with ?? ''}
            onChange={(event) =>
              onChange({
                replace: { find: mapping.replace?.find ?? '', with: event.target.value },
              })
            }
            size="small"
            disabled={readOnly || !mapping.replace?.find}
            sx={{ width: 140 }}
          />
          <TextField
            label="Max length"
            value={mapping.maxLength ?? ''}
            onChange={(event) =>
              onChange({
                maxLength: event.target.value ? Number(event.target.value) : undefined,
              })
            }
            size="small"
            disabled={readOnly}
            helperText="Truncates rather than being refused"
            sx={{ width: 130 }}
          />
          <TextField
            select
            label="Case"
            value={mapping.case ?? ''}
            onChange={(event) =>
              onChange({ case: (event.target.value || undefined) as 'UPPER' | 'LOWER' | undefined })
            }
            size="small"
            disabled={readOnly}
            sx={{ width: 120 }}
          >
            <MenuItem value="">Leave as-is</MenuItem>
            <MenuItem value="UPPER">UPPERCASE</MenuItem>
            <MenuItem value="LOWER">lowercase</MenuItem>
          </TextField>
          <FormControlLabel
            control={
              <Checkbox
                size="small"
                checked={Boolean(mapping.trim)}
                disabled={readOnly}
                onChange={(_, checked) => onChange({ trim: checked || undefined })}
              />
            }
            label={<Typography variant="caption">Trim spaces</Typography>}
          />
          <TextField
            select
            label="When there is no value"
            value={mapping.onMissing ?? 'OMIT'}
            onChange={(event) =>
              onChange({ onMissing: event.target.value as 'OMIT' | 'NULL' })
            }
            size="small"
            disabled={readOnly}
            helperText="An explicit null clears the field on an upsert"
            sx={{ minWidth: 230 }}
          >
            <MenuItem value="OMIT">Leave the field out</MenuItem>
            <MenuItem value="NULL">Send an explicit null</MenuItem>
          </TextField>
        </Box>
      )}
    </Box>
  )
}

/** The checks a rule can make, and what each needs beside it. */
const CHECKS = [
  { value: 'REQUIRED', label: 'Must be present', needs: 'none' },
  { value: 'NOT_BLANK', label: 'Must not be blank', needs: 'none' },
  { value: 'IS_NUMBER', label: 'Must be a number', needs: 'none' },
  { value: 'MIN', label: 'At least', needs: 'number' },
  { value: 'MAX', label: 'At most', needs: 'number' },
  { value: 'MAX_LENGTH', label: 'No longer than', needs: 'number' },
  { value: 'ONE_OF', label: 'One of', needs: 'list' },
  { value: 'MATCHES', label: 'Matches pattern', needs: 'text' },
] as const

export interface Rule {
  name?: string
  field: string
  check: string
  value?: unknown
}

/**
 * Named rules, which is the whole reason this is not a script.
 *
 * A hand-written check throws whatever its author typed, so the console groups every failing
 * record under a stack frame. A rule named "email must be present" produces an error group with
 * that name and a count beside it — a sentence somebody can act on.
 */
export function ValidationEditor({
  config,
  onChange,
  readOnly,
}: {
  config: Record<string, unknown>
  onChange: (config: Record<string, unknown>) => void
  readOnly: boolean
}) {
  const rules = (config.rules as Rule[] | undefined) ?? []
  const onFail = (config.onFail as string) ?? 'REJECT'

  const set = (next: Rule[], fail = onFail) =>
    onChange({ ...config, rules: next, onFail: fail })

  const update = (index: number, changes: Partial<Rule>) =>
    set(rules.map((rule, i) => (i === index ? { ...rule, ...changes } : rule)))

  return (
    <Stack spacing={1.5}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Typography variant="body2" sx={{ flex: 1 }}>
          {rules.length} rule{rules.length === 1 ? '' : 's'}
        </Typography>
        {!readOnly && (
          <Button
            size="small"
            startIcon={<AddIcon />}
            onClick={() => set([...rules, { name: '', field: '', check: 'REQUIRED' }])}
          >
            Add
          </Button>
        )}
      </Box>

      {rules.map((rule, index) => {
        const needs = CHECKS.find((check) => check.value === rule.check)?.needs ?? 'none'
        return (
          <Stack key={index} spacing={1} sx={{ pb: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <TextField
                label="Rule name"
                value={rule.name ?? ''}
                onChange={(event) => update(index, { name: event.target.value })}
                size="small"
                disabled={readOnly}
                placeholder="email must be present"
                helperText={index === 0 ? 'What the failure report will say' : undefined}
                sx={{ flex: 1 }}
              />
              {!readOnly && (
                <IconButton size="small" onClick={() => set(rules.filter((_, i) => i !== index))}>
                  <DeleteIcon fontSize="small" />
                </IconButton>
              )}
            </Box>
            <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
              <TextField
                label="Field"
                value={rule.field}
                onChange={(event) => update(index, { field: event.target.value })}
                size="small"
                disabled={readOnly}
                sx={{ flex: 1, minWidth: 140 }}
              />
              <TextField
                select
                label="Check"
                value={rule.check}
                onChange={(event) => update(index, { check: event.target.value })}
                size="small"
                disabled={readOnly}
                sx={{ width: 175 }}
              >
                {CHECKS.map((check) => (
                  <MenuItem key={check.value} value={check.value}>
                    {check.label}
                  </MenuItem>
                ))}
              </TextField>
              {needs !== 'none' && (
                <TextField
                  label={needs === 'list' ? 'Allowed values' : 'Value'}
                  value={
                    Array.isArray(rule.value) ? rule.value.join(', ') : String(rule.value ?? '')
                  }
                  onChange={(event) => {
                    const raw = event.target.value
                    update(index, {
                      value:
                        needs === 'number'
                          ? Number(raw)
                          : needs === 'list'
                            ? raw.split(',').map((entry) => entry.trim()).filter(Boolean)
                            : raw,
                    })
                  }}
                  size="small"
                  disabled={readOnly}
                  placeholder={needs === 'list' ? 'EU, UK, US' : undefined}
                  sx={{ flex: 1, minWidth: 140 }}
                />
              )}
            </Box>
          </Stack>
        )
      })}

      <TextField
        select
        label="A record that fails"
        value={onFail}
        onChange={(event) => set(rules, event.target.value)}
        size="small"
        disabled={readOnly}
        fullWidth
      >
        <MenuItem value="REJECT">
          Goes to the dead-letter queue, named by the rule it broke
        </MenuItem>
        <MenuItem value="DROP">Is dropped silently, and counts as filtered</MenuItem>
      </TextField>
      <Typography variant="caption" sx={{ color: muted, mt: -1 }}>
        Rejected and filtered are different lines on a run&apos;s balance sheet. A row that broke a
        business rule is worth investigating; a row that is simply out of scope is not.
      </Typography>
    </Stack>
  )
}
