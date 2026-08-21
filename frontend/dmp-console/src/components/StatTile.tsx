import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import Tooltip from '@mui/material/Tooltip'
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined'
import { muted } from '@/theme'

/**
 * A single headline number.
 *
 * Deliberately not a chart. One value over time is a sparkline; one value right now is a number,
 * and drawing a bar of length one only makes it harder to read. The figure uses proportional
 * digits — tabular figures are for columns that must align, not for standalone values.
 */
export function StatTile({
  label,
  value,
  unit,
  hint,
  tone = 'default',
  emphasis = false,
}: {
  label: string
  value: string | number
  unit?: string
  hint?: string
  tone?: 'default' | 'good' | 'critical'
  emphasis?: boolean
}) {
  const toneColor =
    tone === 'good' ? 'success.main' : tone === 'critical' ? 'error.main' : 'text.primary'

  return (
    <Paper sx={{ p: 2.5, height: '100%' }}>
      <Stack direction="row" spacing={0.5} alignItems="center" sx={{ mb: 1 }}>
        <Typography
          variant="caption"
          sx={{ color: muted, textTransform: 'uppercase', letterSpacing: '0.06em', fontWeight: 600 }}
        >
          {label}
        </Typography>
        {hint && (
          <Tooltip title={hint}>
            <InfoOutlinedIcon sx={{ fontSize: 14, color: muted }} />
          </Tooltip>
        )}
      </Stack>

      <Stack direction="row" spacing={0.75} alignItems="baseline">
        <Typography
          sx={{
            fontSize: emphasis ? '2.25rem' : '1.75rem',
            fontWeight: 600,
            lineHeight: 1.1,
            letterSpacing: '-0.02em',
            color: toneColor,
          }}
        >
          {typeof value === 'number' ? value.toLocaleString() : value}
        </Typography>
        {unit && (
          <Typography variant="body2" sx={{ color: muted }}>
            {unit}
          </Typography>
        )}
      </Stack>
    </Paper>
  )
}
