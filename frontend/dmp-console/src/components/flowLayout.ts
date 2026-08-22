import type { Edge, Node } from '@xyflow/react'
import type { NodeType } from '@/api/types'

/**
 * Lays the graph out as one vertical column, in the order it executes.
 *
 * ```
 *   source
 *     ↓
 *   transform…
 *     ↓
 *   delivery
 *     ↓
 *   batch transform
 *     ↓
 *   sink
 * ```
 *
 * <p>Vertical rather than horizontal because a pipeline is read as a sequence of steps and a page
 * has far more room downwards than across. Going across, every node competed for width with every
 * other one and the type had to shrink to fit; going down, a node can be as wide as it needs and
 * still leave the column tidy.
 *
 * <p>Positions are not stored in a pipeline definition — they are recomputed every time the
 * designer opens — so this is not a tidy-up on top of remembered coordinates, it is the only thing
 * deciding where anything appears.
 *
 * <p><b>Every node gets its own slot in the column.</b> Putting nodes that share a stage side by
 * side reads as a fork, when in a pipeline of this shape they are almost always sequential. One
 * column, evenly spaced, says "this, then this, then this", which is what the diagram is for.
 *
 * <p><b>Slots come from what a node is, not from how far along it sits.</b> Deriving position from
 * path length was correct in principle and fragile in practice: the delivery step is inserted
 * afterwards and had to be squeezed into whatever gap the derivation left, so its position depended
 * on the layout having already run — and after a drag, or a hot reload that kept the canvas state,
 * it had not. Its slot is now reserved by construction.
 */

/**
 * Distance between steps, down the page.
 *
 * <p>Comfortably taller than the tallest node a slot can hold, including a delivery step with two
 * lines of detail on it. Nodes are sized by their content and the console cannot know what a label
 * will measure, so the spacing carries the margin rather than every node being trusted to stay
 * inside a tight one.
 */
const SLOT = 170

/** The column everything sits in. A single line of steps needs only one. */
const COLUMN_X = 0

/**
 * Which stage a node belongs to, and therefore where in the row it goes.
 *
 * <p>Everything acting on records one at a time shares the transform stage. A batch transform does
 * not: it runs *after* delivery has divided the chunk, on one group at a time, and making that
 * ordering visible is the whole reason the delivery step is drawn at all.
 */
const STAGE: Record<NodeType, number> = {
  SOURCE: 0,
  TRANSFORM: 1,
  FILTER: 1,
  MAPPER: 1,
  SPLITTER: 1,
  MERGER: 1,
  VALIDATION: 1,
  ERROR_HANDLER: 1,
  DELAY: 1,
  RETRY: 1,
  // 2 is the delivery step, which is derived rather than a real node.
  BATCH_TRANSFORM: 3,
  SINK: 4,
}

/**
 * The delivery step's stage, and the marker that puts it there.
 *
 * <p>It is laid out alongside the real nodes rather than positioned relative to one of them. Three
 * attempts placed it by measuring off a neighbour — halfway between two, one column back from the
 * anchor, into a reserved gap — and each depended on the rest of the row already being where the
 * arithmetic assumed. When it was not, the node landed on top of something. A single pass that
 * assigns every x in the same sort cannot disagree with itself.
 */
export const DELIVERY_STAGE = 2
export const DELIVERY_NODE_TYPE = '__delivery'

export function layout(nodes: Node[], edges: Edge[]): Node[] {
  if (nodes.length === 0) return nodes

  return executionOrder(nodes, edges).map((node, index) => ({
    ...node,
    position: { x: COLUMN_X, y: index * SLOT },
  }))
}

/**
 * The nodes in the order they run: by stage, and within the transform stage by chain position.
 *
 * <p>Longest path rather than shortest inside the chain, so a transform fed by both the source and
 * another transform is drawn below the one it depends on rather than above it — otherwise its
 * incoming edge runs back up the column.
 */
function executionOrder(nodes: Node[], edges: Edge[]): Node[] {
  const depth = chainDepth(nodes, edges)

  return [...nodes].sort((a, b) => {
    const byStage = stageOf(a) - stageOf(b)
    if (byStage !== 0) return byStage
    const byChain = (depth.get(a.id) ?? 0) - (depth.get(b.id) ?? 0)
    if (byChain !== 0) return byChain
    // Stable beyond that: whatever order they arrived in, so tidying never shuffles two nodes past
    // each other and makes the user re-find them.
    return nodes.indexOf(a) - nodes.indexOf(b)
  })
}

function stageOf(node: Node): number {
  const type = node.data?.nodeType as string | undefined
  if (type === DELIVERY_NODE_TYPE) return DELIVERY_STAGE
  return STAGE[(type as NodeType) ?? 'TRANSFORM'] ?? 1
}

/**
 * How far along the graph each node sits.
 *
 * <p>Bounded by the node count so a cycle — which the canvas permits drawing even though the engine
 * would refuse to run it — settles rather than looping for ever.
 */
function chainDepth(nodes: Node[], edges: Edge[]): Map<string, number> {
  const depth = new Map<string, number>(nodes.map((node) => [node.id, 0]))

  for (let pass = 0; pass < nodes.length; pass++) {
    let moved = false
    for (const edge of edges) {
      const from = depth.get(edge.source)
      const to = depth.get(edge.target)
      if (from === undefined || to === undefined) continue
      if (to < from + 1) {
        depth.set(edge.target, from + 1)
        moved = true
      }
    }
    if (!moved) break
  }

  return depth
}
