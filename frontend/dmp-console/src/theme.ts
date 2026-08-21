import { createTheme, type Theme } from '@mui/material/styles'

/**
 * Status colours, fixed and never themed.
 *
 * These are reserved roles — they never double as "series 4". Validated against both surfaces;
 * `warning` sits below 3:1 on the light surface and its CVD separation from `good` falls in the
 * warn band (tritan ΔE 5.1 dark / 6.2 light), so **a status colour never carries meaning alone**.
 * Every state chip in this console pairs the colour with an icon and the state name in text, which
 * is the required mitigation for both.
 */
export const status = {
  good: '#0ca30c',
  warning: '#fab219',
  critical: '#d03b3b',
} as const

/** Informational, not a status: a run in flight is neither good nor bad yet. */
export const info = { light: '#2a78d6', dark: '#3987e5' } as const

/** Recessive ink for chrome, gridlines and inactive states. */
export const muted = '#898781'

const surface = {
  light: { page: '#f6f7f9', paper: '#ffffff' },
  dark: { page: '#0f1115', paper: '#12151b' },
} as const

export function buildTheme(mode: 'light' | 'dark'): Theme {
  const dark = mode === 'dark'

  return createTheme({
    palette: {
      mode,
      primary: { main: dark ? info.dark : info.light },
      success: { main: status.good },
      warning: { main: status.warning },
      error: { main: status.critical },
      background: {
        default: dark ? surface.dark.page : surface.light.page,
        paper: dark ? surface.dark.paper : surface.light.paper,
      },
      divider: dark ? 'rgba(255,255,255,0.10)' : 'rgba(11,11,11,0.10)',
      text: {
        primary: dark ? '#ffffff' : '#0b0b0b',
        secondary: dark ? '#c3c2b7' : '#52514e',
      },
    },

    typography: {
      // One system sans throughout, including large figures. No display face.
      fontFamily: 'system-ui, -apple-system, "Segoe UI", sans-serif',
      h1: { fontSize: '1.75rem', fontWeight: 600, letterSpacing: '-0.02em' },
      h2: { fontSize: '1.375rem', fontWeight: 600, letterSpacing: '-0.01em' },
      h3: { fontSize: '1.0625rem', fontWeight: 600 },
      body2: { fontSize: '0.875rem' },
      caption: { fontSize: '0.75rem', color: muted },
    },

    shape: { borderRadius: 10 },

    components: {
      MuiPaper: {
        // Hairline borders rather than shadows: a dense operational console reads better with
        // flat separation than with elevation, which stacks into visual noise at this density.
        styleOverrides: {
          root: {
            backgroundImage: 'none',
            border: `1px solid ${dark ? 'rgba(255,255,255,0.10)' : 'rgba(11,11,11,0.10)'}`,
          },
        },
        defaultProps: { elevation: 0 },
      },
      MuiButton: {
        styleOverrides: { root: { textTransform: 'none', fontWeight: 600 } },
        defaultProps: { disableElevation: true },
      },
      MuiChip: {
        styleOverrides: { root: { fontWeight: 600, fontSize: '0.75rem' } },
      },
      MuiTableCell: {
        styleOverrides: {
          head: { fontWeight: 600, color: muted, fontSize: '0.75rem', letterSpacing: '0.04em' },
        },
      },
      MuiTooltip: {
        defaultProps: { arrow: true },
        styleOverrides: {
          // MUI's default is a translucent grey. That is fine for a three-word hint floating over
          // whitespace and unreadable over a dense table, which is exactly where this console puts
          // its tooltips: the rows behind show straight through the text. Opaque, and inverted
          // against the page so it reads as an overlay rather than as another panel.
          tooltip: {
            backgroundColor: dark ? '#f2f3f5' : '#1c1f26',
            color: dark ? '#12151b' : '#f6f7f9',
            fontSize: '0.8125rem',
            fontWeight: 400,
            lineHeight: 1.55,
            padding: '10px 12px',
            borderRadius: 8,
            // Wide enough for a sentence of explanation to break naturally, narrow enough that the
            // eye does not have to travel back across the screen to find the next line.
            maxWidth: 420,
            boxShadow: dark
              ? '0 8px 24px rgba(0,0,0,0.45)'
              : '0 8px 24px rgba(11,11,11,0.22)',
          },
          arrow: {
            color: dark ? '#f2f3f5' : '#1c1f26',
          },
        },
      },
    },
  })
}

/**
 * Tabular figures, for numbers that must align down a column.
 *
 * Deliberately not applied to standalone figures such as stat-tile values, where proportional
 * digits read better.
 */
export const tabular = { fontVariantNumeric: 'tabular-nums' } as const
