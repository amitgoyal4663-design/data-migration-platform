import { BaseEdge, EdgeLabelRenderer, getBezierPath, type EdgeProps } from '@xyflow/react'
import { muted } from '@/theme'

/**
 * One of several parallel strands leaving the delivery step.
 *
 * Delivery turns one chunk into some number of requests, and a single arrow says the opposite. Two
 * or three strands say "more than one" at a glance, which is the fact people miss — that a batch
 * script runs once per group rather than once per chunk.
 *
 * <p><b>The count is on the label, not in the line count.</b> Drawing a strand per request is
 * legible at three and unreadable at a hundred, and a hundred is an ordinary number here. So the
 * strands mean "divided" and the label says by how much.
 */
export function FanEdge({
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerEnd,
  data,
}: EdgeProps) {
  const offset = (data?.offset as number) ?? 0
  const spread = 13

  // Spread across the column, because the flow runs down it. Offsetting these vertically — which
  // is what a left-to-right canvas wanted — would stack the strands on top of one another and draw
  // three identical lines.
  const [path, labelX, labelY] = getBezierPath({
    sourceX: sourceX + offset * spread,
    sourceY,
    targetX: targetX + offset * spread,
    targetY,
    sourcePosition,
    targetPosition,
  })

  const label = data?.label as string | undefined

  return (
    <>
      <BaseEdge
        path={path}
        markerEnd={offset === 0 ? markerEnd : undefined}
        style={{
          // The outer strands are quieter: they are there to say "several", and three equally
          // weighted lines read as three specific things rather than as one divided thing.
          strokeWidth: offset === 0 ? 1.5 : 1,
          opacity: offset === 0 ? 1 : 0.45,
        }}
      />
      {label && offset === 0 && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX + 78}px, ${labelY}px)`,
              fontSize: 11,
              fontWeight: 700,
              color: muted,
              background: 'var(--dmp-edge-label-bg, rgba(128,128,128,0.12))',
              padding: '2px 6px',
              borderRadius: 4,
              pointerEvents: 'none',
              whiteSpace: 'nowrap',
            }}
          >
            {label}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}
