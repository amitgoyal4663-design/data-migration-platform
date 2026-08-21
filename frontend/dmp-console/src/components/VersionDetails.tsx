import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import Divider from '@mui/material/Divider'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import type { ConnectorInstance, DeliveryPolicy, PipelineVersion } from '@/api/types'
import { muted, tabular } from '@/theme'

/**
 * Everything a published version actually says, beside its diagram.
 *
 * <p>A frozen version could only be looked at as a picture. The graph is the part people draw, but
 * it is rarely the part that explains a run: chunk size, concurrency, the rejection threshold, what
 * is kept about each record and for how long all live in policies reached through a dialog that is
 * only offered while editing. So the answer to "what is actually running in production" was a
 * diagram and a shrug.
 *
 * <p>Read-only on purpose. A published version cannot change — that is what makes a run months old
 * still able to say what it executed — so this shows and does not offer.
 */
export function VersionDetails({
  version,
  connectors,
}: {
  version: PipelineVersion
  connectors: ConnectorInstance[]
}) {
  const nameOf = (id: string | null | undefined) =>
    connectors.find((c) => c.id === id)?.name ?? (id ? 'unknown connection' : '—')

  const execution = version.executionPolicy
  const chunking = version.chunkingPolicy
  const audit = version.auditPolicy

  return (
    <Paper sx={{ width: 300, flexShrink: 0, overflowY: 'auto', p: 1.75 }}>
      <Stack spacing={0.5}>
        <Typography variant="caption" sx={{ fontWeight: 700, letterSpacing: '0.06em' }}>
          THIS VERSION
        </Typography>
        <Stack direction="row" spacing={0.75} sx={{ pt: 0.5 }} flexWrap="wrap" useFlexGap>
          <Chip size="small" variant="outlined" label={`v${version.versionNumber}`} />
          <Chip size="small" variant="outlined" label={version.mode} />
          <Chip size="small" variant="outlined" label={version.channelType} />
        </Stack>
      </Stack>

      <Field label="Change note" value={version.changeNote ?? '—'} />
      <Field
        label="Created"
        value={`${new Date(version.createdAt).toLocaleString()} by ${version.createdBy ?? 'unknown'}`}
      />
      {version.publishedAt && (
        <Field label="Published" value={new Date(version.publishedAt).toLocaleString()} />
      )}

      <Section title="STEPS" />
      {version.definition.nodes.map((node) => (
        <Field
          key={node.id}
          label={node.name || node.type}
          value={
            node.type === 'SOURCE' || node.type === 'SINK'
              ? `${node.type} · ${nameOf(node.connectorInstanceId)}`
              : node.type
          }
        />
      ))}

      <Section title="SIZE AND PACE" />
      <Field label="Rows per chunk" value={execution.rowsPerChunk || 'from the read size'} />
      <Field
        label="Read fetch size"
        value={chunking.readFetchSize || 'platform default'}
      />
      <Field label="Max batch bytes" value={chunking.maxBatchBytes.toLocaleString()} />
      <Field label="Sink delivery" value={describeDelivery(version.deliveryPolicy)} />
      <Field
        label="Concurrent chunks"
        value={
          execution.maxConcurrentChunks === 0
            ? 'unlimited'
            : execution.maxConcurrentChunks === 1
              ? '1 — strictly sequential'
              : execution.maxConcurrentChunks
        }
      />
      <Field label="Attempts per chunk" value={execution.maxAttemptsPerChunk} />

      <Section title="WHEN RECORDS ARE REJECTED" />
      <Field
        label="Failure threshold"
        value={
          execution.maxFailedPercent === null && execution.maxFailedRecords === null
            ? 'no limit — a chunk completes however many are refused'
            : [
                execution.maxFailedPercent !== null ? `${execution.maxFailedPercent}%` : null,
                execution.maxFailedRecords !== null ? `${execution.maxFailedRecords} records` : null,
              ]
                .filter(Boolean)
                .join(' or ')
        }
      />
      <Field
        label="On a failed chunk"
        value={execution.stopRunOnChunkFailure ? 'stop the whole run' : 'carry on with the rest'}
      />

      <Section title="WHAT IS KEPT" />
      <Field label="Audit level" value={audit.level} />
      <Field
        label="Rejected payloads"
        value={audit.captureRejectedPayloads ? 'kept, so they can be replayed' : 'not kept'}
      />
      {audit.level === 'INDEXED' && (
        <Field label="Index content" value={audit.indexPayloads ? 'yes' : 'identity only'} />
      )}
      <Field
        label="Retention"
        value={
          typeof audit.retention === 'number'
            ? `${Math.round(audit.retention / 86400)} days`
            : audit.retention
        }
      />
      {audit.redactedFields.length > 0 && (
        <Field
          label={`Redacted (${audit.redactionMode})`}
          value={audit.redactedFields.join(', ')}
        />
      )}
    </Paper>
  )
}

/**
 * The delivery setting as a sentence rather than a number.
 *
 * <p>{@code groupSize: 0} reads as "no grouping" to anyone who knows the model and as "zero records
 * per call" to anyone who does not. This panel is read by the second group — somebody asking what
 * a version running in production actually does.
 */
function describeDelivery(delivery: DeliveryPolicy | null | undefined): string {
  if (delivery?.splitScript) {
    return 'one call per group, decided by a script'
  }
  if (delivery?.groupSize === 1) {
    return 'one record per call'
  }
  if (delivery && delivery.groupSize > 1) {
    return `${delivery.groupSize} records per call`
  }
  return 'the whole batch in one call'
}

function Section({ title }: { title: string }) {
  return (
    <>
      <Divider sx={{ my: 1.5 }} />
      <Typography variant="caption" sx={{ fontWeight: 700, letterSpacing: '0.06em' }}>
        {title}
      </Typography>
    </>
  )
}

/** Label above value rather than beside it, so a long connection name is not truncated to fit. */
function Field({ label, value }: { label: string; value: string | number }) {
  return (
    <Box sx={{ mt: 1.25 }}>
      <Typography variant="caption" sx={{ display: 'block', color: muted }}>
        {label}
      </Typography>
      <Typography variant="body2" sx={{ ...tabular, wordBreak: 'break-word' }}>
        {value}
      </Typography>
    </Box>
  )
}
