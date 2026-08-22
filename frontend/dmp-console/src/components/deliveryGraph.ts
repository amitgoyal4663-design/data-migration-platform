import type { Edge, Node } from '@xyflow/react'
import type { ConnectorSpec, PipelineVersion } from '@/api/types'
import { DELIVERY_NODE_TYPE, layout } from './flowLayout'

/**
 * Inserts the delivery step into the rendered graph, immediately before whatever writes.
 *
 * Delivery divides a chunk into the groups that become requests, and it happens *before* a batch
 * transform shapes each group. So it is drawn there — ahead of the batch transform when there is
 * one, ahead of the sink when there is not.
 *
 * Nothing here touches the saved definition. The node is derived from the delivery policy every
 * render and dropped before anything is written back, so a pipeline saved from this canvas is the
 * same pipeline it would have been without it.
 */

export const DELIVERY_NODE_ID = '__delivery'

export function withDeliveryNode(
  nodes: Node[],
  edges: Edge[],
  version?: PipelineVersion,
  sinkSpec?: ConnectorSpec,
): { nodes: Node[]; edges: Edge[] } {
  const anchor = writingNode(nodes, edges)
  if (!anchor) {
    return { nodes, edges }
  }

  const inbound = edges.filter((edge) => edge.target === anchor.id)
  if (inbound.length === 0) {
    // Nothing flows into it yet, so there is no division to describe. Drawing a delivery step
    // dangling off an unconnected sink would be describing a pipeline that does not exist.
    return { nodes, edges }
  }

  const description = describe(version, sinkSpec)

  const node: Node = {
    id: DELIVERY_NODE_ID,
    type: 'delivery',
    // Placed by the shared layout below, alongside everything else. Any value here is temporary.
    position: { x: 0, y: 0 },
    data: { ...description, nodeType: DELIVERY_NODE_TYPE, locked: !description.configurable },
    draggable: false,
    deletable: false,
    connectable: false,
    selectable: true,
  }

  const rerouted: Edge[] = edges.flatMap((edge) => {
    if (edge.target !== anchor.id) return [edge]
    return [
      { ...edge, id: `${edge.id}__to-delivery`, target: DELIVERY_NODE_ID },
      ...fanOut(edge, anchor.id, description),
    ]
  })

  // One pass over the whole row, delivery included, so no position is computed relative to another
  // node's — which is what put this on top of its neighbour three times over.
  return { nodes: layout([...nodes, node], rerouted), edges: rerouted }
}

/**
 * The strands from delivery to whatever writes: one when a chunk is one call, three when it is many.
 *
 * Three rather than the real number, always. A chunk divided into a hundred requests is an ordinary
 * configuration and a hundred arrows is a smear; three strands say "divided" and the label carries
 * the count. One strand is drawn when the chunk really does leave as a single call, because that is
 * the case where a single arrow is the truth rather than a simplification.
 */
function fanOut(edge: Edge, target: string, description: Description): Edge[] {
  const offsets = description.divided ? [-1, 0, 1] : [0]

  return offsets.map((offset) => ({
    id: `${edge.id}__from-delivery${offset}`,
    source: DELIVERY_NODE_ID,
    target,
    type: 'fan',
    markerEnd: edge.markerEnd,
    data: { offset, label: offset === 0 ? description.strandLabel : undefined },
  }))
}

/**
 * The node delivery sits in front of: the batch transform if the pipeline has one, else the sink.
 *
 * <p>A batch transform is where a group becomes a request body, so the division has to be shown
 * before it or the script's input makes no sense — it receives a group, never the whole chunk.
 */
export function deliveryAnchorId(nodes: Node[], edges: Edge[]): string | undefined {
  return writingNode(nodes, edges)?.id
}

function writingNode(nodes: Node[], edges: Edge[]): Node | undefined {
  const sink = nodes.find((node) => node.data?.nodeType === 'SINK')
  if (!sink) return undefined

  const batch = nodes.find(
    (node) =>
      node.data?.nodeType === 'BATCH_TRANSFORM' &&
      edges.some((edge) => edge.source === node.id && edge.target === sink.id),
  )
  return batch ?? sink
}

interface Description {
  headline: string
  detail?: string
  configurable: boolean
  /** Whether one chunk leaves as more than one call, and so should be drawn as several strands. */
  divided: boolean
  /** What the strands carry, written on the middle one. */
  strandLabel: string
}

/** The policy said in the destination's units, and whether it is a setting anyone can change. */
function describe(version?: PipelineVersion, sinkSpec?: ConnectorSpec): Description {
  if (sinkSpec?.callCost === 'PER_CHUNK') {
    // The chunk is one job here — created, uploaded, polled to completion — so there is nothing for
    // delivery to divide. Showing a group size would advertise a setting that cannot take effect.
    return {
      headline: 'One job per chunk',
      detail: 'this destination takes the whole chunk as one unit of work',
      configurable: false,
      divided: false,
      strandLabel: '1 job',
    }
  }

  const rows = version?.executionPolicy?.rowsPerChunk ?? 0
  const delivery = version?.deliveryPolicy

  if (delivery?.splitScript) {
    return {
      headline: 'Split by script',
      detail: 'one call per group the script produces',
      configurable: true,
      divided: true,
      strandLabel: 'one call per group',
    }
  }

  const groupSize = delivery?.groupSize ?? 0
  if (groupSize === 1) {
    return {
      headline: 'One record per call',
      detail: rows > 0 ? `${rows.toLocaleString()} calls per chunk` : undefined,
      configurable: true,
      divided: true,
      strandLabel: rows > 0 ? `× ${rows.toLocaleString()} calls` : 'one call per record',
    }
  }
  if (groupSize > 1) {
    const calls = rows > 0 ? Math.ceil(rows / groupSize) : 0
    return {
      headline: `${groupSize} records per call`,
      detail: calls > 0 ? `${calls.toLocaleString()} calls per chunk` : undefined,
      configurable: true,
      // A group size larger than the chunk divides nothing: the engine hands the whole chunk over
      // in one call, and drawing strands would advertise a division that does not happen.
      divided: calls !== 1,
      strandLabel: calls > 0 ? `× ${calls.toLocaleString()} calls` : `${groupSize} per call`,
    }
  }
  return {
    headline: 'Whole chunk in one call',
    detail: rows > 0 ? `${rows.toLocaleString()} records per call` : undefined,
    configurable: true,
    divided: false,
    strandLabel: '1 call',
  }
}
