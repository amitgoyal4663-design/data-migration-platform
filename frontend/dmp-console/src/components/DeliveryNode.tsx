import Box from '@mui/material/Box'
import Typography from '@mui/material/Typography'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import { muted } from '@/theme'

/**
 * Where a chunk becomes calls, drawn where it actually happens.
 *
 * Delivery is applied *before* a batch transform runs, not after: the chunk is divided into groups
 * first, and the script then shapes each group into the body of one request. The canvas used to
 * imply the opposite — transform, then sink — so a script that received a group of five was a
 * surprise to anyone who had read the diagram. Putting the division on the canvas ahead of the
 * script makes the order self-evident instead of documented.
 *
 * <p><b>Derived, not stored.</b> This node is inserted into the rendered graph from the version's
 * delivery policy; it is never part of the definition, never saved and never draggable. Making it a
 * real node would mean a node type users could place anywhere — including after a batch transform,
 * which is the one arrangement the engine cannot perform. The dashed border says "this is a view of
 * a setting" rather than "this is a thing you wired".
 */

const HANDLE_STYLE = {
  width: 12,
  height: 12,
  border: '2px solid var(--dmp-surface)',
  background: muted,
  opacity: 0.6,
}

export function DeliveryNode({ data, selected }: NodeProps) {
  const headline = data.headline as string
  const detail = data.detail as string | undefined
  const locked = Boolean(data.locked)

  return (
    <Box
      sx={{
        '--dmp-surface': (theme) => theme.palette.background.paper,
        boxSizing: 'border-box',
        // The same fixed width as a real node, so the column reads as one sequence rather than as
        // a list with something odd wedged into it.
        width: 300,
        px: 2.5,
        py: 1.75,
        borderRadius: 2,
        bgcolor: 'background.paper',
        border: 2,
        borderStyle: 'dashed',
        borderColor: selected ? 'primary.main' : muted,
        cursor: locked ? 'default' : 'pointer',
        opacity: 0.95,
      }}
    >
      <Handle type="target" position={Position.Top} style={HANDLE_STYLE} isConnectable={false} />

      <Typography
        variant="caption"
        sx={{
          display: 'block',
          color: muted,
          fontWeight: 700,
          letterSpacing: '0.08em',
          fontSize: 11,
        }}
      >
        DELIVERY
      </Typography>

      <Typography sx={{ fontSize: 16, fontWeight: 600, lineHeight: 1.35 }}>{headline}</Typography>

      {detail && (
        <Typography sx={{ fontSize: 12, color: muted, mt: 0.5, lineHeight: 1.4 }}>
          {detail}
        </Typography>
      )}

      <Handle type="source" position={Position.Bottom} style={HANDLE_STYLE} isConnectable={false} />
    </Box>
  )
}
