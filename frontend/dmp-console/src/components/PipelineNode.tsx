import Box from '@mui/material/Box'
import Typography from '@mui/material/Typography'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import { muted, status } from '@/theme'
import type { NodeType } from '@/api/types'

/**
 * A node on the designer canvas.
 *
 * <p>Custom rather than React Flow's default node for two reasons. The connection handles need a
 * generous hit target, or people drag at the edge, hit nothing and conclude the canvas is broken.
 * And the flow runs top to bottom, so they belong on the top and bottom edges — a handle on the
 * side of a node in a vertical column invites a connection that would loop back on itself.
 */

/** Which handles a node type gets. A source has no input; a sink has no output. */
const SHAPE: Record<NodeType, { inbound: boolean; outbound: boolean; accent: string }> = {
  SOURCE: { inbound: false, outbound: true, accent: status.good },
  SINK: { inbound: true, outbound: false, accent: '#3987e5' },
  TRANSFORM: { inbound: true, outbound: true, accent: muted },
  BATCH_TRANSFORM: { inbound: true, outbound: true, accent: muted },
  FILTER: { inbound: true, outbound: true, accent: muted },
  MAPPER: { inbound: true, outbound: true, accent: muted },
  SPLITTER: { inbound: true, outbound: true, accent: muted },
  MERGER: { inbound: true, outbound: true, accent: muted },
  VALIDATION: { inbound: true, outbound: true, accent: status.warning },
  ERROR_HANDLER: { inbound: true, outbound: true, accent: status.critical },
  DELAY: { inbound: true, outbound: true, accent: muted },
  RETRY: { inbound: true, outbound: true, accent: muted },
}

const HANDLE_STYLE = {
  width: 12,
  height: 12,
  border: '2px solid var(--dmp-surface)',
  background: '#3987e5',
}

export function PipelineNode({ data, selected }: NodeProps) {
  const nodeType = (data.nodeType as NodeType) ?? 'TRANSFORM'
  const shape = SHAPE[nodeType]
  const connectorId = data.connectorInstanceId as string | null
  const needsConnector = nodeType === 'SOURCE' || nodeType === 'SINK'

  return (
    <Box
      sx={{
        '--dmp-surface': (theme) => theme.palette.background.paper,
        boxSizing: 'border-box',
        // Fixed rather than content-sized: in a column, nodes of differing widths read as a ragged
        // list rather than as one sequence. Width is free here in a way it never was across.
        width: 300,
        px: 2.5,
        py: 1.75,
        borderRadius: 2,
        bgcolor: 'background.paper',
        border: 2,
        borderColor: selected ? 'primary.main' : shape.accent,
        boxShadow: selected ? 3 : 0,
      }}
    >
      {shape.inbound && (
        <Handle type="target" position={Position.Top} style={HANDLE_STYLE} isConnectable />
      )}

      <Typography
        variant="caption"
        sx={{
          display: 'block',
          color: shape.accent,
          fontWeight: 700,
          letterSpacing: '0.08em',
          fontSize: 11,
        }}
      >
        {nodeType.replace('_', ' ')}
      </Typography>

      <Typography sx={{ fontSize: 16, fontWeight: 600, lineHeight: 1.35 }}>
        {(data.label as string) || nodeType}
      </Typography>

      {/* Surfaced on the node itself, not only in the inspector: an unconfigured source is the
          single most common reason a publish is refused, and finding out at publish time means
          hunting for which node it was. */}
      {needsConnector && !connectorId && (
        <Typography sx={{ fontSize: 12, color: status.critical, mt: 0.5 }}>
          No connection chosen
        </Typography>
      )}

      {shape.outbound && (
        <Handle type="source" position={Position.Bottom} style={HANDLE_STYLE} isConnectable />
      )}
    </Box>
  )
}
