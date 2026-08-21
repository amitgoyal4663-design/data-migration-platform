import Alert from '@mui/material/Alert'
import AlertTitle from '@mui/material/AlertTitle'
import Box from '@mui/material/Box'
import CircularProgress from '@mui/material/CircularProgress'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import type { ReactNode } from 'react'
import { ApiError } from '@/api/client'
import { muted } from '@/theme'

/**
 * An API failure, rendered from the server's problem detail.
 *
 * Shows the backend's own message rather than a generic one. The API is written to explain what to
 * do — "Topic X does not exist, ask your platform team to create it" — and replacing that with
 * "Something went wrong" throws away the useful half.
 */
export function ErrorPanel({ error, title }: { error: unknown; title?: string }) {
  if (!error) return null

  const api = error instanceof ApiError ? error : null
  const message = api?.message ?? (error instanceof Error ? error.message : String(error))

  return (
    <Alert severity={api?.retryable ? 'warning' : 'error'} sx={{ my: 2 }}>
      <AlertTitle>{title ?? api?.problem.title ?? 'Something failed'}</AlertTitle>
      <Typography variant="body2">{message}</Typography>

      {api && Object.keys(api.fieldErrors).length > 0 && (
        <Box component="ul" sx={{ mt: 1, mb: 0, pl: 2.5 }}>
          {Object.entries(api.fieldErrors).map(([field, detail]) => (
            <li key={field}>
              <Typography variant="body2">
                <strong>{field}</strong>: {detail}
              </Typography>
            </li>
          ))}
        </Box>
      )}

      {api?.retryable && (
        <Typography variant="caption" sx={{ display: 'block', mt: 1 }}>
          This looks temporary — trying again may work.
        </Typography>
      )}
    </Alert>
  )
}

export function Loading({ label }: { label?: string }) {
  return (
    <Stack alignItems="center" justifyContent="center" spacing={2} sx={{ py: 8 }}>
      <CircularProgress size={28} />
      {label && (
        <Typography variant="body2" sx={{ color: muted }}>
          {label}
        </Typography>
      )}
    </Stack>
  )
}

/**
 * The empty state.
 *
 * Always offers the next action. An empty list with no way forward is a dead end, and it is the
 * first thing every new user sees.
 */
export function EmptyState({
  icon,
  title,
  description,
  action,
}: {
  icon?: ReactNode
  title: string
  description?: string
  action?: ReactNode
}) {
  return (
    <Paper sx={{ p: 6, textAlign: 'center' }}>
      <Stack alignItems="center" spacing={1.5}>
        {icon && <Box sx={{ color: muted, '& svg': { fontSize: 40 } }}>{icon}</Box>}
        <Typography variant="h3">{title}</Typography>
        {description && (
          <Typography variant="body2" sx={{ color: muted, maxWidth: 460 }}>
            {description}
          </Typography>
        )}
        {action && <Box sx={{ pt: 1 }}>{action}</Box>}
      </Stack>
    </Paper>
  )
}
