import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import { useEffect, useRef } from 'react'
import { muted } from '@/theme'

/**
 * Fetches the next page when it scrolls into view.
 *
 * <p>An observer rather than a button, because the natural gesture at the bottom of a list is to
 * keep scrolling. The count stays visible either way: knowing you are looking at four hundred of
 * ninety thousand entries is what tells you to narrow by chunk instead of scrolling for an hour.
 */
export function MoreOnScroll({
  hasMore,
  loading,
  onReach,
  shown,
  total,
  noun = 'stages',
}: {
  hasMore: boolean
  loading: boolean
  onReach: () => void
  shown: number
  total: number
  /** What is being counted, for the line that says how far through the list you are. */
  noun?: string
}) {
  const sentinel = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    const node = sentinel.current
    if (!node || !hasMore || loading) {
      return
    }
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          onReach()
        }
      },
      // Starts fetching before the reader arrives, so the list rarely stalls under them.
      { rootMargin: '400px' },
    )
    observer.observe(node)
    return () => observer.disconnect()
  }, [hasMore, loading, onReach])

  if (total === 0) {
    return null
  }

  return (
    <Stack ref={sentinel} alignItems="center" sx={{ py: 1 }}>
      <Typography variant="caption" sx={{ color: muted }}>
        {loading
          ? 'Loading more…'
          : hasMore
            ? `${shown.toLocaleString()} of ${total.toLocaleString()} ${noun} — scroll for more`
            : `${shown.toLocaleString()} ${noun}`}
      </Typography>
    </Stack>
  )
}
