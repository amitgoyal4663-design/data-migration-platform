import Box from '@mui/material/Box'
import Typography from '@mui/material/Typography'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import { muted, status } from '@/theme'
import type { NodeType } from '@/api/types'

/**
 * A node on the designer canvas.
 *
 * <p>Custom rather than React Flow's default node for one reason that matters: the connection
 * handles. The default node puts them top and bottom and renders them small, so on a left-to-right
 * flow people drag from the left or right edge, hit nothing, and conclude the canvas is broken.
 * Explicit left/right handles with a generous hit target make connecting a source to a sink the
 * obvious gesture rather than a discovered one.
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
        minWidth: 168,
        px: 2,
        py: 1.25,
        borderRadius: 2,
        bgcolor: 'background.paper',
        border: 2,
        borderColor: selected ? 'primary.main' : shape.accent,
        boxShadow: selected ? 3 : 0,
      }}
    >
      {shape.inbound && (
        <Handle type="target" position={Position.Left} style={HANDLE_STYLE} isConnectable />
      )}

      <Typography
        variant="caption"
        sx={{
          display: 'block',
          color: shape.accent,
          fontWeight: 700,
          letterSpacing: '0.06em',
          fontSize: 10,
        }}
      >
        {nodeType.replace('_', ' ')}
      </Typography>

      <Typography sx={{ fontSize: 13, fontWeight: 600, lineHeight: 1.3 }}>
        {(data.label as string) || nodeType}
      </Typography>

      {/* Surfaced on the node itself, not only in the inspector: an unconfigured source is the
          single most common reason a publish is refused, and finding out at publish time means
          hunting for which node it was. */}
      {needsConnector && !connectorId && (
        <Typography sx={{ fontSize: 10, color: status.critical, mt: 0.25 }}>
          No connection chosen
        </Typography>
      )}

      {shape.outbound && (
        <Handle type="source" position={Position.Right} style={HANDLE_STYLE} isConnectable />
      )}
    </Box>
  )
}
