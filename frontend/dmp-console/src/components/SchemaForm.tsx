import Accordion from '@mui/material/Accordion'
import AccordionDetails from '@mui/material/AccordionDetails'
import AccordionSummary from '@mui/material/AccordionSummary'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import InputAdornment from '@mui/material/InputAdornment'
import Tooltip from '@mui/material/Tooltip'
import KeyIcon from '@mui/icons-material/KeyOutlined'
import type { JsonSchema } from '@/api/types'
import { muted } from '@/theme'

/**
 * Renders a form from a connector's JSON Schema.
 *
 * This component is the reason the plugin system is worth having. It knows nothing about JDBC,
 * Salesforce or Databricks — it renders whatever schema the backend reports. Dropping a connector
 * jar into the plugin directory and restarting a worker produces a complete, validated
 * configuration form here with no frontend change and no release.
 *
 * Fields named in `secretFields` are rendered as secret **references**, never as password inputs.
 * A password box invites someone to type the actual credential, which would then be stored in the
 * definition database in plain text. Asking for `env:PG_PASSWORD` instead makes the safe thing the
 * obvious thing.
 */
export function SchemaForm({
  schema,
  secretFields,
  config,
  secrets,
  onConfigChange,
  onSecretChange,
  errors,
  direction,
}: {
  schema: JsonSchema
  secretFields: string[]
  config: Record<string, unknown>
  secrets: Record<string, string>
  onConfigChange: (name: string, value: unknown) => void
  onSecretChange: (name: string, value: string) => void
  errors?: Record<string, string>
  /** Which role this instance will fill. Fields belonging to the other role are hidden. */
  direction?: 'SOURCE' | 'SINK' | 'BOTH'
}) {
  const required = new Set(schema.required ?? [])

  // A field declaring a role only applies to instances filling that role. Asking a Kafka sink
  // where reading should start, or a MongoDB source how to write, is not merely noise: it invites
  // someone to set a value that will be silently ignored, and then to wonder why it had no effect.
  const roleOf = (field: unknown) => (field as { 'x-dmp-role'?: string })['x-dmp-role']
  const isAdvanced = (field: unknown) => Boolean((field as { 'x-dmp-advanced'?: boolean })['x-dmp-advanced'])
  const allEntries = Object.entries(schema.properties ?? {})

  // Split before grouping by role, so an advanced source field lands under Advanced rather than
  // under "When reading". A form asking eight questions to connect to a database buries the two
  // that are about this connection among six that have a right answer already filled in.
  const entries = allEntries.filter(([, field]) => !isAdvanced(field))
  const advanced = allEntries.filter(
    ([, field]) =>
      isAdvanced(field) &&
      (direction === 'BOTH' || !roleOf(field) || roleOf(field) === direction),
  )

  const shared = entries.filter(([, field]) => !roleOf(field))
  const sourceOnly = entries.filter(([, field]) => roleOf(field) === 'SOURCE')
  const sinkOnly = entries.filter(([, field]) => roleOf(field) === 'SINK')

  // Chosen a role, so the other role's fields are irrelevant and simply go. Chosen both, so they
  // are all relevant but not all of the time — grouped under headings rather than hidden, because
  // an instance that reads and writes genuinely needs every one of them and the only useful thing
  // left to say is which ones apply when.
  const groups: { heading: string | null; fields: typeof entries }[] =
    direction === 'SOURCE'
      ? [{ heading: null, fields: [...shared, ...sourceOnly] }]
      : direction === 'SINK'
        ? [{ heading: null, fields: [...shared, ...sinkOnly] }]
        : [
            { heading: null, fields: shared },
            { heading: 'When reading', fields: sourceOnly },
            { heading: 'When writing', fields: sinkOnly },
          ]

  return (
    <Stack spacing={2.5}>
      {/*
        Said once at the top rather than on every field. Anything the cluster owns — a connection
        string, a host, a bootstrap server list — can be a reference instead of a literal, which is
        what lets the same connector instance be promoted between environments rather than rebuilt
        by hand in each of them.
      */}
      <Typography variant="body2" sx={{ color: muted }}>
        Any field can hold a reference instead of a value — <code>env:MONGO_URI</code> for the whole
        field, or <code>{'mongodb://${MONGO_HOST}:27017/orders'}</code> to substitute part of one.
        References are resolved on the worker at run time from the deployment&apos;s environment, so
        nothing environment-specific is stored here and the same connection works in every
        environment.
      </Typography>

      {groups.map((group) =>
        group.fields.length === 0 ? null : (
          <Stack key={group.heading ?? 'shared'} spacing={2.5}>
            {group.heading && (
              <Typography variant="h3" sx={{ pt: 1 }}>
                {group.heading}
              </Typography>
            )}
            {group.fields.map(([name, field]) => (
              <SchemaField
                key={name}
                name={name}
                field={field}
                required={required.has(name)}
                value={config[name]}
                error={errors?.[name]}
                onChange={(value) => onConfigChange(name, value)}
              />
            ))}
          </Stack>
        ),
      )}

      {advanced.length > 0 && (
        <Accordion
          disableGutters
          elevation={0}
          sx={{ '&:before': { display: 'none' }, border: 1, borderColor: 'divider' }}
        >
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Stack>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Advanced
              </Typography>
              <Typography variant="caption" sx={{ color: muted }}>
                {advanced.length} setting{advanced.length === 1 ? '' : 's'} with a working default —
                open only if this connection needs something different.
              </Typography>
            </Stack>
          </AccordionSummary>
          <AccordionDetails>
            <Stack spacing={2.5}>
              {advanced.map(([name, field]) => (
                <SchemaField
                  key={name}
                  name={name}
                  field={field}
                  required={required.has(name)}
                  value={config[name]}
                  error={errors?.[name]}
                  onChange={(value) => onConfigChange(name, value)}
                />
              ))}
            </Stack>
          </AccordionDetails>
        </Accordion>
      )}

      {secretFields.length > 0 && (
        <>
          <Typography variant="h3" sx={{ pt: 1 }}>
            Credentials
          </Typography>
          <Typography variant="body2" sx={{ color: muted, mt: -1.5 }}>
            These fields hold the <strong>name</strong> of a credential your platform team provides,
            not the credential itself. It is looked up on the worker each time the connection is
            used, so nothing secret is stored here, returned by the API, or written to a log.
          </Typography>

          {secretFields.map((name) => (
            <SecretField
              key={name}
              name={name}
              value={secrets[name] ?? ''}
              onChange={(next) => onSecretChange(name, next)}
            />
          ))}
        </>
      )}
    </Stack>
  )
}

/**
 * Where a credential comes from, and what to call the box that names it.
 *
 * <p>The wire format is `scheme:name`. Nobody should have to know that. Choosing the store from a
 * list and typing only the name removes the guess that a single text box forces — "is it the
 * password, the variable name, or the variable name with some prefix?" — which is the guess that
 * ends with a real credential pasted into a field that gets stored and returned by the API.
 *
 * <p>Only stores that something actually resolves are listed. Offering a Vault option while no
 * provider answers to it would let someone save a connection that passes validation and then fails
 * at run time — the same trap as a canvas offering a step the executor ignores. A second entry
 * belongs here the day a second provider is registered, and not before.
 */
const SECRET_STORES = [
  {
    scheme: 'env',
    label: 'Environment variable',
    nameLabel: 'Variable name',
    hint: 'Set on the pod by whoever manages the deployment — usually from a Kubernetes secret.',
  },
] as const

/**
 * One credential, expressed as a store and a name rather than as a string in a format to memorise.
 *
 * <p>The composed reference is shown underneath rather than hidden, so what gets stored is never a
 * mystery — and so it can be read out to whoever has to create it at the other end.
 */
function SecretField({
  name,
  value,
  onChange,
}: {
  name: string
  value: string
  onChange: (value: string) => void
}) {
  const match = /^([a-z][a-z0-9-]*):(.*)$/.exec(value.trim())
  const store =
    SECRET_STORES.find((candidate) => candidate.scheme === match?.[1]) ?? SECRET_STORES[0]

  // Anything that is not a well-formed reference is shown in the name box as typed, so a value
  // pasted here before this existed stays visible and correctable rather than vanishing.
  const reference = value.trim()
  const referenceName = match && store.scheme === match[1] ? match[2] : reference
  const suggestion = envName(name)
  const invalid = reference !== '' && !SECRET_REFERENCE.test(reference)

  return (
    <Stack spacing={0.75}>
      <Stack direction="row" spacing={1} alignItems="flex-start">
        <TextField
          select
          label="Source"
          size="small"
          value={store.scheme}
          onChange={(event) => onChange(`${event.target.value}:${referenceName}`)}
          sx={{ width: 200, flexShrink: 0 }}
        >
          {SECRET_STORES.map((candidate) => (
            <MenuItem key={candidate.scheme} value={candidate.scheme}>
              {candidate.label}
            </MenuItem>
          ))}
        </TextField>

        <TextField
          label={`${label(name)} — ${store.nameLabel.toLowerCase()}`}
          value={referenceName}
          onChange={(event) => onChange(`${store.scheme}:${event.target.value}`)}
          placeholder={store.scheme === 'env' ? suggestion : `path/to/secret#${name}`}
          size="small"
          fullWidth
          error={invalid}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <Tooltip title="A name, not the credential — resolved at run time">
                  <KeyIcon sx={{ fontSize: 18, color: muted }} />
                </Tooltip>
              </InputAdornment>
            ),
          }}
        />
      </Stack>

      <Typography variant="caption" sx={{ color: invalid ? 'error.main' : muted, pl: 0.5 }}>
        {invalid
          ? `That looks like a credential rather than a name. Ask for it to be published as ${suggestion} and put that name here — a value typed in is refused, because this record is readable by anyone who can list connections.`
          : reference === ''
            ? `${store.hint} Leave blank if this connection does not use one.`
            : `Stored as ${reference} — nothing secret is kept here.`}
      </Typography>
    </Stack>
  )
}

/**
 * A field whose value belongs to the deployment — a connection string, a broker list, a workspace.
 *
 * <p>Two modes rather than a free text box. The text box gave no clue that naming a variable was
 * even possible, so the only discoverable path was pasting the production connection string into a
 * record that is stored as written and returned by the API in full.
 */
function EnvironmentField({
  name,
  field,
  required,
  value,
  error,
  onChange,
}: {
  name: string
  field: JsonSchema
  required: boolean
  value: string
  error?: string
  onChange: (value: unknown) => void
}) {
  const asReference = /^env:(.*)$/.exec(value.trim())
  // Empty defaults to the reference mode: it is the first thing a new connection sees, and the
  // path we want taken by default.
  const mode = asReference || value.trim() === '' ? 'env' : 'literal'
  const suggestion = envName(name)

  return (
    <Stack spacing={0.75}>
      <Stack direction="row" spacing={1} alignItems="flex-start">
        <TextField
          select
          label="Source"
          size="small"
          value={mode}
          // Switching modes deliberately clears the value rather than trying to convert it. A
          // variable name is not a connection string and pretending otherwise would leave a
          // half-translated value that looks deliberate.
          onChange={(event) => onChange(event.target.value === 'env' ? 'env:' : '')}
          sx={{ width: 200, flexShrink: 0 }}
        >
          <MenuItem value="env">Environment variable</MenuItem>
          <MenuItem value="literal">Literal value</MenuItem>
        </TextField>

        <TextField
          label={
            (mode === 'env' ? `${label(name)} — variable name` : label(name)) +
            (required ? ' *' : '')
          }
          value={mode === 'env' ? (asReference?.[1] ?? '') : value}
          onChange={(event) =>
            onChange(mode === 'env' ? `env:${event.target.value}` : event.target.value)
          }
          placeholder={mode === 'env' ? suggestion : undefined}
          size="small"
          fullWidth
          error={Boolean(error)}
        />
      </Stack>

      <Typography variant="caption" sx={{ color: error ? 'error.main' : muted, pl: 0.5 }}>
        {error ??
          (mode === 'env'
            ? `Set on the pod by whoever manages the deployment. Stored as the name only, so this connection works unchanged in every environment.`
            : `Stored exactly as written and returned by the API, so it must not contain a password. ${field.description ?? ''}`)}
      </Typography>
    </Stack>
  )
}

function SchemaField({
  name,
  field,
  required,
  value,
  error,
  onChange,
}: {
  name: string
  field: JsonSchema
  required: boolean
  value: unknown
  error?: string
  onChange: (value: unknown) => void
}) {
  const common = {
    label: label(name) + (required ? ' *' : ''),
    helperText: error ?? field.description,
    error: Boolean(error),
    size: 'small' as const,
    fullWidth: true,
  }

  // A field the deployment owns is asked for as a variable name by default, because that is the
  // answer that survives being promoted to the next environment. Typing a literal stays available
  // — on a laptop mongodb://localhost:27017 is the right answer — but it is now the deliberate
  // choice rather than the only one on offer.
  if ((field as { 'x-dmp-environment'?: boolean })['x-dmp-environment']) {
    return (
      <EnvironmentField
        name={name}
        field={field}
        required={required}
        value={(value as string) ?? ''}
        error={error}
        onChange={onChange}
      />
    )
  }

  if (field.enum) {
    return (
      <TextField
        {...common}
        select
        value={(value as string) ?? ''}
        onChange={(event) => onChange(event.target.value)}
      >
        {field.enum.map((option) => (
          <MenuItem key={option} value={option}>
            {option}
          </MenuItem>
        ))}
      </TextField>
    )
  }

  if (field.type === 'integer' || field.type === 'number') {
    return (
      <TextField
        {...common}
        type="number"
        value={(value as number) ?? ''}
        onChange={(event) =>
          onChange(event.target.value === '' ? undefined : Number(event.target.value))
        }
      />
    )
  }

  if (field.type === 'array') {
    // Comma-separated rather than a tag editor: these are column lists, and typing
    // "id, name, email" is faster than clicking through a chip input.
    const asText = Array.isArray(value) ? (value as string[]).join(', ') : ''
    return (
      <TextField
        {...common}
        value={asText}
        placeholder="comma, separated, values"
        onChange={(event) => {
          const items = event.target.value
            .split(',')
            .map((item) => item.trim())
            .filter(Boolean)
          onChange(items.length > 0 ? items : undefined)
        }}
      />
    )
  }

  if (field.type === 'boolean') {
    return (
      <TextField
        {...common}
        select
        value={value === undefined ? '' : String(value)}
        onChange={(event) => onChange(event.target.value === 'true')}
      >
        <MenuItem value="true">Yes</MenuItem>
        <MenuItem value="false">No</MenuItem>
      </TextField>
    )
  }

  return (
    <TextField
      {...common}
      value={(value as string) ?? ''}
      onChange={(event) => onChange(event.target.value || undefined)}
    />
  )
}

/** "splitColumn" → "Split column". Schemas use camelCase; people read prose. */
/**
 * The shape the backend accepts for a credential, mirrored here so the mistake is caught while
 * typing rather than on submit.
 *
 * The scheme list is an allow-list on purpose. "Anything with a colon" would accept
 * `mongodb://user:password@host`, which is a credential and is exactly what this keeps out.
 */
const SECRET_REFERENCE = /^(env|vault):[^\s/][^\s]*$/

/** `clientSecret` becomes `CLIENT_SECRET`, so the suggested variable name is a usable one. */
function envName(field: string): string {
  return field.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toUpperCase()
}

function label(name: string): string {
  const spaced = name.replace(/([A-Z])/g, ' $1').toLowerCase()
  return spaced.charAt(0).toUpperCase() + spaced.slice(1)
}
