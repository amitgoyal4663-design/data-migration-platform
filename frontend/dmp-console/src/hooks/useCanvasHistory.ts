import { useCallback, useEffect, useRef, useState } from 'react'
import type { Edge, Node } from '@xyflow/react'

type Snapshot = { nodes: Node[]; edges: Edge[] }

const MAX_HISTORY = 50

/**
 * Undo, redo, copy and paste for the designer canvas.
 *
 * <p>Snapshots are taken at meaningful moments — adding a node, connecting, deleting, finishing a
 * drag — not on every change event. React Flow emits a change per pixel of movement, so recording
 * all of them would make one undo move a node by one pixel and fill the history with fifty of them.
 *
 * <p>Keyboard shortcuts fire only for events originating inside the canvas. Without that, typing a
 * node name and pressing ⌘Z would undo the whole graph instead of the last character, and ⌘V in
 * the script editor would paste a node instead of code.
 */
export function useCanvasHistory({
  nodes,
  edges,
  setNodes,
  setEdges,
  enabled,
}: {
  nodes: Node[]
  edges: Edge[]
  setNodes: (nodes: Node[]) => void
  setEdges: (edges: Edge[]) => void
  enabled: boolean
}) {
  const past = useRef<Snapshot[]>([])
  const future = useRef<Snapshot[]>([])
  const clipboard = useRef<Snapshot | null>(null)

  // Mirrors of the live graph, so the keyboard handler reads current values without being
  // re-created on every change — which would tear down and re-add the listener constantly.
  const live = useRef<Snapshot>({ nodes, edges })
  live.current = { nodes, edges }

  const [canUndo, setCanUndo] = useState(false)
  const [canRedo, setCanRedo] = useState(false)

  const sync = () => {
    setCanUndo(past.current.length > 0)
    setCanRedo(future.current.length > 0)
  }

  /** Records the state *before* a change. Call it just before mutating. */
  const commit = useCallback(() => {
    past.current = [
      ...past.current.slice(-(MAX_HISTORY - 1)),
      { nodes: live.current.nodes, edges: live.current.edges },
    ]
    // A new action invalidates the redo branch — the same rule every editor follows, because
    // redoing onto a diverged timeline produces a graph the user never saw.
    future.current = []
    sync()
  }, [])

  const undo = useCallback(() => {
    const previous = past.current.pop()
    if (!previous) return

    future.current = [...future.current, { nodes: live.current.nodes, edges: live.current.edges }]
    setNodes(previous.nodes)
    setEdges(previous.edges)
    sync()
  }, [setNodes, setEdges])

  const redo = useCallback(() => {
    const next = future.current.pop()
    if (!next) return

    past.current = [...past.current, { nodes: live.current.nodes, edges: live.current.edges }]
    setNodes(next.nodes)
    setEdges(next.edges)
    sync()
  }, [setNodes, setEdges])

  const copy = useCallback(() => {
    const selected = live.current.nodes.filter((node) => node.selected)
    if (selected.length === 0) return

    const ids = new Set(selected.map((node) => node.id))
    clipboard.current = {
      nodes: selected,
      // Only edges wholly inside the selection. An edge with one end outside would paste as a
      // dangling reference the validator would then reject.
      edges: live.current.edges.filter((edge) => ids.has(edge.source) && ids.has(edge.target)),
    }
  }, [])

  const paste = useCallback(() => {
    const copied = clipboard.current
    if (!copied || copied.nodes.length === 0) return

    commit()

    const suffix = Date.now().toString(36).slice(-4)
    const idMap = new Map(copied.nodes.map((node) => [node.id, `${node.id}-${suffix}`]))

    const pastedNodes: Node[] = copied.nodes.map((node) => ({
      ...node,
      id: idMap.get(node.id)!,
      // Offset so the copy is visibly a copy rather than sitting exactly on the original.
      position: { x: node.position.x + 40, y: node.position.y + 40 },
      selected: true,
    }))

    const pastedEdges: Edge[] = copied.edges.map((edge) => ({
      ...edge,
      id: `${edge.id}-${suffix}`,
      source: idMap.get(edge.source)!,
      target: idMap.get(edge.target)!,
    }))

    setNodes([
      ...live.current.nodes.map((node) => ({ ...node, selected: false })),
      ...pastedNodes,
    ])
    setEdges([...live.current.edges, ...pastedEdges])
  }, [commit, setNodes, setEdges])

  const duplicate = useCallback(() => {
    copy()
    paste()
  }, [copy, paste])

  useEffect(() => {
    if (!enabled) return

    const onKeyDown = (event: KeyboardEvent) => {
      // Canvas shortcuts belong to the canvas, so they only fire for events that came from it.
      //
      // This used to ask instead whether the event looked like it came from a text field, by
      // checking tagName and contentEditable. That is a guess, and it was wrong for the code
      // editor: MUI's drawer keeps focus on its own container, so a paste into the editor arrived
      // with a div as its target, failed the text-field check, and had preventDefault called on
      // it — making it impossible to paste a script. Asking where the event came from has one
      // answer; asking what it looks like has as many answers as there are widgets.
      const target = event.target as HTMLElement | null
      if (!target?.closest('.react-flow')) {
        return
      }

      const modifier = event.metaKey || event.ctrlKey
      if (!modifier) return

      switch (event.key.toLowerCase()) {
        case 'z':
          event.preventDefault()
          // ⇧⌘Z is the redo gesture on macOS; ⌃Y is handled below for Windows habits.
          if (event.shiftKey) redo()
          else undo()
          break
        case 'y':
          event.preventDefault()
          redo()
          break
        case 'c':
          copy()
          break
        case 'v':
          event.preventDefault()
          paste()
          break
        case 'd':
          event.preventDefault()
          duplicate()
          break
        default:
          break
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [enabled, undo, redo, copy, paste, duplicate])

  return { commit, undo, redo, copy, paste, duplicate, canUndo, canRedo }
}
