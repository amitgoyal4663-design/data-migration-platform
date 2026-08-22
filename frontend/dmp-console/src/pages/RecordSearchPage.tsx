import { useMemo, useState } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import AlertTitle from '@mui/material/AlertTitle'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Collapse from '@mui/material/Collapse'
import InputAdornment from '@mui/material/InputAdornment'
import MenuItem from '@mui/material/MenuItem'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import SearchIcon from '@mui/icons-material/Search'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import CancelIcon from '@mui/icons-material/Cancel'
import FilterAltOffIcon from '@mui/icons-material/FilterAltOff'
import HourglassEmptyIcon from '@mui/icons-material/HourglassEmpty'
import ReportProblemIcon from '@mui/icons-material/ReportProblem'
import { PageHeader } from '@/components/PageHeader'
import { ErrorPanel } from '@/components/Feedback'
import { TimeRange, type TimeWindow } from '@/components/TimeRange'
import { useRecordLookup, usePipelines } from '@/api/hooks'
import type { RecordIndexEntry } from '@/api/types'
import { shortId } from '@/api/ids'

/**
 * "Did this record get transferred?" — the question a support desk is paid to answer.
 *
 * <p>They have an order number a customer quoted and a complaint. They do not have a pipeline id, a
 * MongoDB `_id`, or any idea which of nine pipelines touched it. The old screen required a pipeline
 * *and* an exact source key, which meant it could only be used by somebody who already knew the
 * answer.
 *
 * <p>So: one box, searched across every field of the record, filters optional. And the result is
 * written as a sentence rather than a row of columns, because the reply to the ticket is "yes, at
 * 23:08, in run 01a02567" — not a table.
 */

/** What each outcome actually means to the person answering the ticket. */
const OUTCOMES: Record<
  RecordIndexEntry['outcome'],
  { label: string; colour: string; icon: React.ReactElement; meaning: string }
> = {
  WRITTEN: {
    label: 'Transferred',
    colour: 'success.main',
    icon: <CheckCircleIcon />,
    meaning: 'the destination accepted it',
  },
  SENT: {
    label: 'Sent, awaiting verdict',
    colour: 'info.main',
    icon: <HourglassEmptyIcon />,
    meaning: 'the destination has it and has not said whether it kept it',
  },
  REJECTED: {
    label: 'Refused',
    colour: 'error.main',
    icon: <CancelIcon />,
    meaning: 'the destination refused it',
  },
  FILTERED: {
    label: 'Not sent, on purpose',
    colour: 'text.secondary',
    icon: <FilterAltOffIcon />,
    meaning: 'a transform dropped it deliberately — this is a success, not a loss',
  },
  TRANSFORM_FAILED: {
    label: 'Failed in a transform',
    colour: 'warning.main',
    icon: <ReportProblemIcon />,
    meaning: 'a script threw on this record, so it never reached the destination',
  },
  CALL_FAILED: {
    label: 'Sent in a call that failed',
    colour: 'error.main',
    icon: <ReportProblemIcon />,
    // Worded to stop the reading that costs a customer an apology they are not owed. The
    // destination did not refuse this record; it refused the request the record was in, and said
    // nothing about the record itself. Most destinations only ever answer at that grain.
    meaning:
      'the destination refused the whole request this record was part of, without saying anything '
      + 'about this record — a retry may still write it',
  },
}

export function RecordSearchPage() {
  const [term, setTerm] = useState('')
  const [submitted, setSubmitted] = useState('')
  const [pipelineId, setPipelineId] = useState('')
  const [outcome, setOutcome] = useState('')
  // Bounded by default. An unbounded search of a busy index answers the least useful version of
  // the question — every entry ever written — and is slower for the privilege.
  const [when, setWhen] = useState<TimeWindow>({
    after: new Date(Date.now() - 7 * 24 * 60 * 60_000).toISOString(),
  })

  const pipelines = usePipelines()
  const results = useRecordLookup({
    q: submitted || undefined,
    pipelineId: pipelineId || undefined,
    outcome: outcome || undefined,
    after: when.after,
    before: when.before,
  })

  const hits = results.data?.content ?? []

  return (
    <>
      <PageHeader
        title="Find a record"
        subtitle="Search by anything the record contains — an order number, an email, a reference"
        actions={<TimeRange onChange={setWhen} />}
      />

      <Paper sx={{ p: 2, mb: 2 }}>
        <Stack
          component="form"
          direction={{ xs: 'column', sm: 'row' }}
          spacing={1.5}
          onSubmit={(event) => {
            event.preventDefault()
            setSubmitted(term.trim())
          }}
        >
          <TextField
            value={term}
            onChange={(event) => setTerm(event.target.value)}
            placeholder="ORD-100123, customer@acme.io, any value in the record"
            size="small"
            autoFocus
            sx={{ flex: 1 }}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" />
                </InputAdornment>
              ),
            }}
          />

          {/* Both optional and both narrowing. The common case is a support engineer who knows
              neither, so neither may be required to get an answer. */}
          <TextField
            select
            size="small"
            label="Pipeline"
            value={pipelineId}
            onChange={(event) => setPipelineId(event.target.value)}
            sx={{ minWidth: 170 }}
          >
            <MenuItem value="">Any</MenuItem>
            {pipelines.data?.content.map((pipeline) => (
              <MenuItem key={pipeline.id} value={pipeline.id}>
                {pipeline.name}
              </MenuItem>
            ))}
          </TextField>

          <TextField
            select
            size="small"
            label="Outcome"
            value={outcome}
            onChange={(event) => setOutcome(event.target.value)}
            sx={{ minWidth: 190 }}
          >
            <MenuItem value="">Any</MenuItem>
            {Object.entries(OUTCOMES).map(([key, entry]) => (
              <MenuItem key={key} value={key}>
                {entry.label}
              </MenuItem>
            ))}
          </TextField>

          <Button type="submit" variant="contained" disabled={!term.trim()}>
            Search
          </Button>
        </Stack>
      </Paper>

      <ErrorPanel error={results.error} />

      {submitted && !results.isLoading && hits.length === 0 && (
        <NothingFound term={submitted} window={when} />
      )}

      {hits.length > 0 && (
        <Stack spacing={1.5}>
          <Typography variant="body2" color="text.secondary">
            {results.data?.totalElements?.toLocaleString()} entr
            {results.data?.totalElements === 1 ? 'y' : 'ies'} for “{submitted}”, newest first
          </Typography>
          {hits.map((hit) => (
            <Hit key={`${hit.chunkId}:${hit.seq}:${hit.ordinal}`} hit={hit} />
          ))}
        </Stack>
      )}
    </>
  )
}

/**
 * One entry, written as an answer.
 *
 * <p>The outcome leads, because it is the reply to the ticket. Everything needed to prove it — the
 * run, the chunk, the payload as it was handled — is one click away rather than in a column
 * somebody has to know to read.
 */
function Hit({ hit }: { hit: RecordIndexEntry }) {
  const [open, setOpen] = useState(false)
  const outcome = OUTCOMES[hit.outcome] ?? OUTCOMES.WRITTEN
  const when = useMemo(() => new Date(hit.occurredAt).toLocaleString(), [hit.occurredAt])

  return (
    <Paper variant="outlined" sx={{ p: 2 }}>
      <Stack direction="row" spacing={1.5} alignItems="flex-start">
        <Box sx={{ color: outcome.colour, display: 'flex', pt: 0.25 }}>{outcome.icon}</Box>

        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Stack direction="row" spacing={1} alignItems="baseline" flexWrap="wrap" useFlexGap>
            <Typography sx={{ fontWeight: 700, color: outcome.colour }}>
              {outcome.label}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              — {outcome.meaning}
            </Typography>
          </Stack>

          <Typography variant="body2" sx={{ mt: 0.5 }}>
            {when} ·{' '}
            <Box component={RouterLink} to={`/runs/${hit.runId}`} sx={{ color: 'primary.main' }}>
              run {shortId(hit.runId)}
            </Box>{' '}
            · chunk {shortId(hit.chunkId)} · record {hit.seq}
            {hit.ordinal > 0 && `.${hit.ordinal}`}
          </Typography>

          {hit.errorCode && (
            <Box sx={{ mt: 1 }}>
              <Chip size="small" color="error" variant="outlined" label={hit.errorCode} />
              {/* The sentence, not just the category. "TRANSFORM_FAILED" says a script broke;
                  "Cannot read property 'address' of undefined" says which line to open. */}
              {hit.errorMessage && (
                <Typography
                  variant="body2"
                  color="error.main"
                  sx={{ mt: 0.75, fontFamily: 'monospace', fontSize: 12.5 }}
                >
                  {hit.errorMessage}
                </Typography>
              )}
            </Box>
          )}

          <Stack direction="row" spacing={1} sx={{ mt: 1 }} alignItems="center">
            <Button size="small" onClick={() => setOpen((was) => !was)}>
              {open ? 'Hide record' : 'Show fetched and sent'}
            </Button>

            {/*
              Said on the collapsed card, not only inside it. Somebody scanning results reads
              "Transferred" and one payload and concludes we have no record of the fetch — which
              is the reading a support desk then repeats to a customer. The fact that the two ends
              are identical is the answer to their question, so it does not belong behind a click.
            */}
            {hit.payload && !hit.sourcePayload && reachedTheDestination(hit.outcome) && (
              <Typography variant="caption" color="text.disabled">
                fetched and sent are identical — no transform in this pipeline
              </Typography>
            )}
            {hit.recordKey && (
              <Typography variant="caption" color="text.disabled" sx={{ alignSelf: 'center' }}>
                source key {hit.recordKey}
              </Typography>
            )}
          </Stack>

          <Collapse in={open}>
            {hit.payload && hit.sourcePayload ? (
              // Both halves, because the question being asked is almost always about the
              // difference between them.
              <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.5} sx={{ mt: 1.5 }}>
                <Payload title="Fetched from the source" value={hit.sourcePayload} />
                <Payload title="Sent to the destination" value={hit.payload} />
              </Stack>
            ) : hit.payload ? (
              <Box sx={{ mt: 1.5 }}>
                {/*
                  One payload means the two ends are identical, and saying only "sent" leaves the
                  reader unable to tell that from "we did not keep what we fetched". A second copy
                  of an unchanged record would double the index to say nothing, so the label carries
                  the fact instead.
                */}
                <Payload
                  title={
                    reachedTheDestination(hit.outcome)
                      ? 'Fetched and sent — identical, no transform changed this record'
                      : 'Fetched from the source'
                  }
                  value={hit.payload}
                />
              </Box>
            ) : (
              // Absent by policy, not by accident — and the difference matters to somebody
              // deciding whether to tell a customer we have no record of their data.
              <Alert severity="info" sx={{ mt: 1.5 }}>
                This pipeline does not store record payloads. The entry is real; the contents were
                deliberately not kept.
              </Alert>
            )}
          </Collapse>
        </Box>
      </Stack>
    </Paper>
  )
}

/** Whether this outcome means the record actually left for the destination. */
function reachedTheDestination(outcome: RecordIndexEntry['outcome']): boolean {
  return (
    outcome === 'WRITTEN' ||
    outcome === 'SENT' ||
    outcome === 'REJECTED' ||
    outcome === 'CALL_FAILED'
  )
}

/**
 * One payload, labelled with which end of the pipeline it came from.
 *
 * <p>An unlabelled block of JSON is ambiguous exactly where it matters: a record that was
 * transformed looks like the source record to anybody who does not already know which one they are
 * being shown.
 */
function Payload({ title, value }: { title: string; value: Record<string, unknown> }) {
  return (
    <Box sx={{ flex: 1, minWidth: 0 }}>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
        {title}
      </Typography>
      <Box
        component="pre"
        sx={{
          p: 1.5,
          m: 0,
          bgcolor: 'action.hover',
          borderRadius: 1,
          fontSize: 12,
          overflow: 'auto',
          maxHeight: 320,
        }}
      >
        {JSON.stringify(value, null, 2)}
      </Box>
    </Box>
  )
}

/**
 * An empty result, said honestly.
 *
 * <p>The dangerous reading of "no results" is "we never received it", and support will give that
 * answer to a customer if the screen lets them. There are four reasons a record is not here and
 * only one of them means it was never handled.
 */
function NothingFound({ term, window }: { term: string; window: TimeWindow }) {
  return (
    <Alert severity="warning">
      <AlertTitle>Nothing indexed for “{term}”</AlertTitle>
      <Typography variant="body2" sx={{ mb: 1 }}>
        {window.after
          ? `Searched from ${new Date(window.after).toLocaleString()}${
              window.before ? ` to ${new Date(window.before).toLocaleString()}` : ' onwards'
            }. `
          : 'Searched the whole index. '}
        This is not proof the record was never migrated. It can mean any of:
      </Typography>
      <Box component="ul" sx={{ m: 0, pl: 2.5 }}>
        <li>
          <Typography variant="body2">the source never held it</Typography>
        </li>
        <li>
          <Typography variant="body2">
            it was handled outside the window above — widen it to “Any time”
          </Typography>
        </li>
        <li>
          <Typography variant="body2">
            the pipeline that moved it does not index records — audit level below INDEXED writes no
            entries at all
          </Typography>
        </li>
        <li>
          <Typography variant="body2">
            the entry is older than the pipeline's index retention
          </Typography>
        </li>
        <li>
          <Typography variant="body2">
            the value is spelled differently in the record than in the ticket
          </Typography>
        </li>
      </Box>
    </Alert>
  )
}
