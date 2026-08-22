import Box from '@mui/material/Box'
import LinearProgress from '@mui/material/LinearProgress'
import { useEffect, useState } from 'react'
import { useStatusBoard } from '@/api/hooks'
import type { BoardAttention, BoardLiveRun, StatusBoard } from '@/api/types'

/**
 * A screen for a wall, not for a desk.
 *
 * Everything here follows from one constraint: it is read from across a room by somebody walking
 * past who did not come to look at it. So there is no navigation, no filtering, no interaction of
 * any kind — nobody is standing at it with a mouse — and type is sized to be legible at four or
 * five metres rather than to fit more on.
 *
 * Dark on purpose, and regardless of the viewer's theme. A bright panel in an office is glare on
 * every screen facing it, and this is the one page whose viewer never chose to open it.
 *
 * Colour is never the only signal. Red, amber and green all carry a word as well, because roughly
 * one man in twelve cannot separate the first two, and across a room nobody can separate any of
 * them from a reflection.
 */
export function StatusBoardPage() {
  // Faster than the desk screen: this is meant to feel live, and a wall showing a failure from
  // four minutes ago has people asking whether it is stuck.
  const board = useStatusBoard(15_000)
  const [now, setNow] = useState(() => new Date())

  useEffect(() => {
    const tick = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(tick)
  }, [])

  // Its own background rather than the app's, and fixed rather than in the page flow, so the board
  // fills whatever it is cast to without the console's chrome around it.
  useEffect(() => {
    const previous = document.body.style.backgroundColor
    document.body.style.backgroundColor = '#07090d'
    return () => {
      document.body.style.backgroundColor = previous
    }
  }, [])

  const data = board.data
  const stale = board.isError || (board.dataUpdatedAt > 0 && Date.now() - board.dataUpdatedAt > 90_000)

  return (
    <Box
      sx={{
        position: 'fixed',
        inset: 0,
        bgcolor: '#07090d',
        color: '#e8edf5',
        display: 'flex',
        flexDirection: 'column',
        p: '1.6vw',
        gap: '1.2vw',
        fontFamily: '"Inter", system-ui, sans-serif',
        overflow: 'hidden',
      }}
    >
      <Header now={now} data={data} stale={stale} />

      {data && (
        <>
          <Verdict data={data} stale={stale} />
          <Box sx={{ display: 'flex', gap: '1.2vw', flex: 1, minHeight: 0 }}>
            <Attention items={data.attention} />
            <Live runs={data.live} />
          </Box>
          <Totals data={data} />
        </>
      )}
    </Box>
  )
}

function Header({ now, data, stale }: { now: Date; data?: StatusBoard; stale: boolean }) {
  return (
    <Box sx={{ display: 'flex', alignItems: 'baseline', gap: '1.5vw' }}>
      <Box sx={{ fontSize: '1.5vw', letterSpacing: '0.25em', color: '#5b6b85', fontWeight: 600 }}>
        DATA MIGRATION
      </Box>
      <Box sx={{ flex: 1 }} />
      {/*
        A board that has stopped updating looks exactly like a board where nothing is happening,
        which is the most dangerous state a wall display can be in. So it says so, loudly.
      */}
      {stale && (
        <Box sx={{ fontSize: '1.5vw', color: '#ffb020', fontWeight: 700 }}>
          NOT UPDATING — last {data ? new Date(data.generatedAt).toLocaleTimeString() : 'never'}
        </Box>
      )}
      <Box sx={{ fontSize: '2.4vw', fontVariantNumeric: 'tabular-nums', color: '#8a9ab5' }}>
        {now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
      </Box>
    </Box>
  )
}

const TONE = {
  CRITICAL: { bg: '#2a0d10', line: '#ff4d5e', text: '#ff8a95', word: 'FAILURES' },
  WARNING: { bg: '#2a1f08', line: '#ffb020', text: '#ffcc66', word: 'ANOMALIES' },
  INFO: { bg: '#0b2018', line: '#25c37a', text: '#5fe0a4', word: 'ALL CLEAR' },
} as const

function Verdict({ data, stale }: { data: StatusBoard; stale: boolean }) {
  const tone = TONE[data.verdict as keyof typeof TONE] ?? TONE.INFO
  const count = data.attention.length

  return (
    <Box
      sx={{
        bgcolor: tone.bg,
        borderLeft: `0.6vw solid ${tone.line}`,
        px: '2vw',
        py: '1.2vw',
        display: 'flex',
        alignItems: 'center',
        gap: '2vw',
        opacity: stale ? 0.45 : 1,
      }}
    >
      <Box sx={{ fontSize: '4.5vw', fontWeight: 800, color: tone.text, lineHeight: 1 }}>
        {count === 0 ? 'ALL CLEAR' : `${count} ${count === 1 ? 'ISSUE' : 'ISSUES'}`}
      </Box>
      <Box sx={{ flex: 1 }} />
      <Box sx={{ fontSize: '1.4vw', color: '#7d8ca6', textAlign: 'right', lineHeight: 1.5 }}>
        {data.today.running > 0 ? (
          <>
            <strong style={{ color: '#e8edf5', fontSize: '2vw' }}>{data.today.running}</strong>{' '}
            running now
          </>
        ) : (
          'nothing running'
        )}
      </Box>
    </Box>
  )
}

function Attention({ items }: { items: BoardAttention[] }) {
  return (
    <Panel title={`NEEDS ATTENTION — LAST 24 HOURS`} flex={1.4}>
      {items.length === 0 ? (
        <Box sx={{ fontSize: '1.8vw', color: '#3f6b55', py: '2vw' }}>
          Nothing has failed and nothing looks unusual.
        </Box>
      ) : (
        // Only what fits. A board that scrolls is a board nobody can read, and the count in the
        // headline already says how many there are in total.
        items.slice(0, 7).map((item, index) => {
          const critical = item.severity === 'CRITICAL'
          return (
            <Box
              key={index}
              sx={{
                display: 'flex',
                alignItems: 'baseline',
                gap: '1vw',
                py: '0.55vw',
                borderBottom: '1px solid #161c26',
              }}
            >
              <Box
                sx={{
                  fontSize: '0.85vw',
                  fontWeight: 800,
                  letterSpacing: '0.1em',
                  color: critical ? '#ff4d5e' : '#ffb020',
                  minWidth: '5.5vw',
                }}
              >
                {critical ? 'FAILED' : 'ANOMALY'}
              </Box>
              <Box sx={{ minWidth: 0, flex: 1 }}>
                <Box sx={{ fontSize: '1.5vw', fontWeight: 600, ...clip }}>{item.pipeline}</Box>
                <Box sx={{ fontSize: '1.05vw', color: '#8a9ab5', ...clip }}>
                  {item.headline} — {item.detail}
                </Box>
              </Box>
              <Box sx={{ fontSize: '1vw', color: '#5b6b85', whiteSpace: 'nowrap' }}>
                {ago(item.at)}
              </Box>
            </Box>
          )
        })
      )}
      {items.length > 7 && (
        <Box sx={{ fontSize: '1.1vw', color: '#5b6b85', pt: '0.6vw' }}>
          and {items.length - 7} more
        </Box>
      )}
    </Panel>
  )
}

function Live({ runs }: { runs: BoardLiveRun[] }) {
  return (
    <Panel title="RUNNING NOW" flex={1}>
      {runs.length === 0 ? (
        <Box sx={{ fontSize: '1.5vw', color: '#3d4757', py: '2vw' }}>Idle.</Box>
      ) : (
        runs.slice(0, 6).map((run) => (
          <Box key={run.runId} sx={{ py: '0.6vw' }}>
            <Box sx={{ display: 'flex', alignItems: 'baseline', gap: '0.8vw' }}>
              <Box sx={{ fontSize: '1.4vw', fontWeight: 600, flex: 1, ...clip }}>
                {run.pipeline}
              </Box>
              <Box
                sx={{
                  fontSize: '1.2vw',
                  fontVariantNumeric: 'tabular-nums',
                  color: '#25c37a',
                }}
              >
                {run.recordsWritten.toLocaleString()}
              </Box>
            </Box>
            <LinearProgress
              variant={run.progress === null ? 'indeterminate' : 'determinate'}
              value={run.progress === null ? undefined : run.progress * 100}
              sx={{
                mt: '0.35vw',
                height: '0.5vw',
                borderRadius: 0,
                bgcolor: '#161c26',
                '& .MuiLinearProgress-bar': { bgcolor: '#25c37a' },
              }}
            />
          </Box>
        ))
      )}
    </Panel>
  )
}

function Totals({ data }: { data: StatusBoard }) {
  const { today } = data
  return (
    <Box sx={{ display: 'flex', gap: '1.2vw' }}>
      <Stat label="MOVED TODAY" value={compact(today.recordsWritten)} tone="#25c37a" />
      <Stat label="READ" value={compact(today.recordsRead)} />
      <Stat
        label="NOT DELIVERED"
        value={compact(today.recordsFailed)}
        tone={today.recordsFailed > 0 ? '#ff4d5e' : undefined}
      />
      <Stat label="RUNS DONE" value={String(today.completed)} />
      <Stat
        label="RUNS FAILED"
        value={String(today.failed)}
        tone={today.failed > 0 ? '#ff4d5e' : undefined}
      />
    </Box>
  )
}

function Stat({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <Box sx={{ flex: 1, bgcolor: '#0d1119', px: '1.2vw', py: '0.9vw' }}>
      <Box sx={{ fontSize: '0.85vw', letterSpacing: '0.15em', color: '#5b6b85', fontWeight: 600 }}>
        {label}
      </Box>
      <Box
        sx={{
          fontSize: '3vw',
          fontWeight: 700,
          lineHeight: 1.1,
          fontVariantNumeric: 'tabular-nums',
          color: tone ?? '#e8edf5',
        }}
      >
        {value}
      </Box>
    </Box>
  )
}

function Panel({
  title,
  flex,
  children,
}: {
  title: string
  flex: number
  children: React.ReactNode
}) {
  return (
    <Box
      sx={{
        flex,
        minWidth: 0,
        bgcolor: '#0d1119',
        px: '1.4vw',
        py: '1vw',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
      }}
    >
      <Box
        sx={{
          fontSize: '0.95vw',
          letterSpacing: '0.18em',
          color: '#5b6b85',
          fontWeight: 700,
          mb: '0.8vw',
        }}
      >
        {title}
      </Box>
      <Box sx={{ flex: 1, minHeight: 0, overflow: 'hidden' }}>{children}</Box>
    </Box>
  )
}

const clip = { overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' } as const

/** Thousands and millions, because nine digits across a room is a smear. */
function compact(n: number) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`
  return String(n)
}

function ago(iso: string) {
  const minutes = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 60_000))
  if (minutes < 1) return 'now'
  if (minutes < 60) return `${minutes}m`
  return `${Math.floor(minutes / 60)}h`
}
