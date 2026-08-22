import type { Run } from '@/api/types'

/**
 * A migration and the attempts behind it.
 *
 * The grouping itself is the server's: `GET /runs` returns one entry per migration with its
 * resumes and retries nested, so a page size counts migrations. This turns that into the shape the
 * tables draw, and adds up what the whole chain moved.
 *
 * <p>It was briefly done in the browser, from `retryOf`, and the flaw was structural rather than
 * cosmetic: grouping can only see the runs on the current page, so a chain spanning a page boundary
 * came apart, and "25 per page" drew however many groups those 25 rows happened to form.
 */

export interface RunChain {
  /** The run the migration started from. */
  root: Run
  /** Every later attempt, oldest first. Empty for the overwhelming majority of runs. */
  attempts: Run[]
  /** Records written across the whole chain. */
  totalWritten: number
  /** Records read across the whole chain, which is what exposes a source being re-read. */
  totalRead: number
  /** Where the migration stands now, which is its latest attempt. */
  latest: Run
}

export function toChains(runs: Run[]): RunChain[] {
  return runs.map((root) => {
    const attempts = root.attempts ?? []
    const all = [root, ...attempts]
    return {
      root,
      attempts,
      totalWritten: all.reduce((sum, run) => sum + run.metrics.recordsWritten, 0),
      totalRead: all.reduce((sum, run) => sum + run.metrics.recordsRead, 0),
      latest: all[all.length - 1] ?? root,
    }
  })
}

/**
 * Why an attempt exists, in the words somebody reading it six months later would want.
 *
 * <p>A chain can mix reasons: a run somebody stopped, a retry of the chunks that failed, a resume
 * of the ones that never started. Without this the timeline is a column of identical lines with
 * different ids.
 */
export function attemptReason(run: Run, previous?: Run): string {
  if (!run.retryOf) return 'first attempt'
  if (!previous) return 'resumed'
  if (previous.state === 'STOPPED') return 'resumed after stop'
  if (previous.state === 'FAILED') return 'retried after failure'
  return 'retried'
}
