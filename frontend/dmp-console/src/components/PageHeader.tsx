import Box from '@mui/material/Box'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import { useEffect, type ReactNode } from 'react'
import { usePageChrome } from '@/store'
import { muted } from '@/theme'

/**
 * A page's title, subtitle and actions.
 *
 * Breadcrumbs are not drawn here. They are published to the top bar, which spans the full width
 * and was otherwise carrying a single icon — so the trail costs no vertical space at all, and the
 * first thing below the bar is the page's own title rather than a line of navigation.
 */
export function PageHeader({
  title,
  subtitle,
  breadcrumbs,
  actions,
}: {
  title: string
  subtitle?: ReactNode
  breadcrumbs?: { label: string; to?: string }[]
  actions?: ReactNode
}) {
  const setBreadcrumbs = usePageChrome((state) => state.setBreadcrumbs)

  // Serialised as the dependency so an inline array literal — which every caller passes — does not
  // re-publish on each render.
  const trail = JSON.stringify(breadcrumbs ?? [])
  useEffect(() => {
    setBreadcrumbs(JSON.parse(trail))
    return () => setBreadcrumbs([])
  }, [trail, setBreadcrumbs])

  return (
    <Box sx={{ pt: 2, pb: 2 }}>
      <Stack
        direction={{ xs: 'column', sm: 'row' }}
        spacing={2}
        alignItems={{ xs: 'flex-start', sm: 'center' }}
        justifyContent="space-between"
      >
        <Box sx={{ minWidth: 0 }}>
          <Typography variant="h1" noWrap>
            {title}
          </Typography>
          {subtitle && (
            <Typography variant="body2" sx={{ color: muted, mt: 0.25 }}>
              {subtitle}
            </Typography>
          )}
        </Box>
        {actions && <Stack direction="row" spacing={1}>{actions}</Stack>}
      </Stack>
    </Box>
  )
}
