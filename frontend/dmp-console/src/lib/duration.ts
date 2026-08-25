/**
 * A duration in the coarsest units that still distinguish one run from another.
 *
 * <p>Lived in the dashboard page, which three other screens then imported from — so a page nobody
 * needed could not be deleted without breaking the run list. Shared code belongs somewhere shared.
 */
export function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`
}
