import Alert from '@mui/material/Alert'
import AlertTitle from '@mui/material/AlertTitle'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Drawer from '@mui/material/Drawer'
import MenuItem from '@mui/material/MenuItem'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import CheckIcon from '@mui/icons-material/CheckCircleOutline'
import PublishIcon from '@mui/icons-material/PublishOutlined'
import SaveIcon from '@mui/icons-material/SaveOutlined'
import TuneIcon from '@mui/icons-material/TuneOutlined'
import UndoIcon from '@mui/icons-material/UndoOutlined'
import RedoIcon from '@mui/icons-material/RedoOutlined'
import ContentCopyIcon from '@mui/icons-material/ContentCopyOutlined'
import IconButton from '@mui/material/IconButton'
import Tooltip from '@mui/material/Tooltip'
import {
  Background,
  Controls,
  MarkerType,
  ReactFlow,
  ReactFlowProvider,
  addEdge,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type Connection,
  type Edge,
  type Node,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { PipelineNode } from '@/components/PipelineNode'
import { ScriptEditor, STARTER_SCRIPTS } from '@/components/ScriptEditor'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  useConnectorCatalogue,
  useConnectorInstances,
  usePublishVersion,
  useSaveDefinition,
  useValidateVersion,
  useVersion,
} from '@/api/hooks'
import { PageHeader } from '@/components/PageHeader'
import { VersionDetails } from '@/components/VersionDetails'
import { useCanvasHistory } from '@/hooks/useCanvasHistory'
import { PolicyDialog } from '@/components/PolicyDialog'
import { FlowSummary } from '@/components/FlowSummary'
import { DeliveryNode } from '@/components/DeliveryNode'
import { DELIVERY_NODE_ID, withDeliveryNode } from '@/components/deliveryGraph'
import { FanEdge } from '@/components/FanEdge'
import { layout } from '@/components/flowLayout'
import { ErrorPanel, Loading } from '@/components/Feedback'
import type {
  NodeType,
  PipelineDefinition,
  TransformStage,
  ValidationResponse,
} from '@/api/types'
import { useThemeMode } from '@/store'

/**
 * What can be dropped on the canvas. Source and sink are the two that require a connection.
 *
 * <p>Only steps that do something. Filter, Mapper, Validation and Error handler were offered here
 * while they passed every record through untouched, which meant dragging one on produced a step
 * that silently did nothing — the worst kind of feature, because it looks like it works. A
 * Transform expresses all three in one line of JavaScript, and rejected records already reach the
 * dead-letter queue without a node saying so.
 *
 * <p>Their constants stay in {@code NodeType} so a pipeline already saved with one still loads.
 * They return here if they ever gain behaviour of their own.
 */
const PALETTE: { type: NodeType; label: string; hint: string }[] = [
  { type: 'SOURCE', label: 'Source', hint: 'Where data is read from' },
  { type: 'TRANSFORM', label: 'Transform', hint: 'JavaScript on each record — map, filter, split' },
  {
    type: 'BATCH_TRANSFORM',
    label: 'Batch transform',
    hint: 'JavaScript on the outgoing batch — shape the payload a sink sends',
  },
  { type: 'SINK', label: 'Sink', hint: 'Where data is written to' },
]

/**
 * Node types the executor runs at most one of, and why.
 *
 * <p>The canvas used to let a second one be added, the validator used to accept it, and the engine
 * refused it — after the version was published, after the run was created, on a worker. Blocking it
 * here is half the fix; the validator enforces the same rule so the API cannot be used to get
 * around a disabled button.
 *
 * <p>The text replaces the button's hint rather than hiding in a tooltip, because "why is this
 * greyed out" should not require hovering to answer.
 */
const ONE_PER_PIPELINE: Partial<Record<NodeType, string>> = {
  SOURCE: 'Already added — a pipeline reads from one source. Build a second pipeline instead.',
  SINK: 'Already added — a pipeline writes to one sink. Two destinations would need a checkpoint each.',
  BATCH_TRANSFORM: 'Already added — only one batch transform runs, so a second would never execute.',
}

/**
 * Registered outside the component so the object identity is stable. React Flow warns loudly and
 * re-mounts every node on each render if this is redefined inline.
 */
const NODE_TYPES = { pipelineNode: PipelineNode, delivery: DeliveryNode }
const EDGE_TYPES = { fan: FanEdge }

/**
 * How the canvas frames itself on open.
 *
 * <p>Zoom is capped at life size. {@code fitView} scales the graph to fill the pane, so a two-node
 * pipeline — which is most of them — was magnified until the nodes filled the screen and the canvas
 * looked broken. Zooming past 1 never helps read a diagram; it only makes a small pipeline look
 * like a mistake.
 */
const FIT_VIEW = { padding: 0.25, maxZoom: 1 }

export function DesignerPage() {
  return (
    <ReactFlowProvider>
      <Designer />
    </ReactFlowProvider>
  )
}

function Designer() {
  const { pipelineId = '', versionId = '' } = useParams()
  const navigate = useNavigate()
  const flow = useReactFlow()
  const mode = useThemeMode((state) => state.mode)

  const version = useVersion(pipelineId, versionId)
  const connectors = useConnectorInstances()
  const catalogue = useConnectorCatalogue()
  const save = useSaveDefinition(pipelineId, versionId)
  const validate = useValidateVersion(pipelineId, versionId)
  const publish = usePublishVersion(pipelineId)

  /**
   * The connector instance wired into a role, or undefined while either query is still loading.
   *
   * Read from the saved definition rather than from canvas state, so it does not flicker while a
   * node is being dragged — the summary describes what would run, not what is under the cursor.
   */
  const instanceFor = (role: 'SOURCE' | 'SINK') => {
    const id = version.data?.definition?.nodes?.find((n) => n.type === role)?.connectorInstanceId
    return id ? connectors.data?.content?.find((c) => c.id === id) : undefined
  }

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [validation, setValidation] = useState<ValidationResponse | null>(null)
  const [tuning, setTuning] = useState(false)

  const sinkSpec = catalogue.data?.find(
    (spec) => spec.type === instanceFor('SINK')?.connectorType,
  )

  /**
   * What is actually drawn: the saved graph with the delivery step inserted where it happens.
   *
   * Derived every render rather than held in state, so it can never be saved by accident and can
   * never drift from the policy it describes.
   */
  const drawn = useMemo(
    () => withDeliveryNode(nodes, edges, version.data, sinkSpec),
    [nodes, edges, version.data, sinkSpec],
  )

  // Loaded once. Re-syncing on every fetch would discard edits mid-drag whenever the query
  // refetched, which is the most infuriating possible bug in a canvas editor.
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => {
    if (!version.data || hydrated) return
    const { nodes: definitionNodes, edges: definitionEdges } = version.data.definition
    const flowEdges = definitionEdges.map(toFlowEdge)

    // Positions are not stored in a definition, so every viewing starts from nothing and the
    // layout is what decides whether this reads as a pipeline or as a scatter of boxes.
    const flowNodes = definitionNodes.map((node) => toFlowNode(node, ORIGIN))
    setNodes(layout(flowNodes, flowEdges))
    setEdges(flowEdges)
    setHydrated(true)
  }, [version.data, hydrated, setNodes, setEdges])

  const readOnly = version.data?.status === 'PUBLISHED'

  const history = useCanvasHistory({
    nodes,
    edges,
    setNodes,
    setEdges,
    enabled: !readOnly,
  })

  const definition: PipelineDefinition = useMemo(
    () => ({
      nodes: nodes.map((node) => ({
        id: node.id,
        type: node.data.nodeType as NodeType,
        name: (node.data.label as string) ?? '',
        connectorInstanceId: (node.data.connectorInstanceId as string | null) ?? null,
        config: (node.data.config as Record<string, unknown>) ?? {},
      })),
      edges: edges.map((edge) => ({
        id: edge.id,
        from: edge.source,
        to: edge.target,
        condition: (edge.label as string) || null,
      })),
    }),
    [nodes, edges],
  )

  const onConnect = useCallback(
    (connection: Connection) => {
      history.commit()
      setEdges((current) =>
        addEdge(
          {
            ...connection,
            id: `e-${connection.source}-${connection.target}`,
            markerEnd: { type: MarkerType.ArrowClosed },
          },
          current,
        ),
      )
    },
    [setEdges, history],
  )

  /**
   * Adds a node and wires it in, rather than leaving the user to draw an arrow the engine could
   * have inferred.
   *
   * The order transforms run in is only ever ambiguous between two per-record scripts — one
   * computing a field and one filtering on it give different answers depending on which is first,
   * and only the author knows which they meant. Everything else is already determined: a stage
   * runs where its type says it runs. So a new step is spliced in before the sink, which is what
   * someone adding one almost always wants, and the arrows stay editable for the case that
   * actually needs them.
   */
  /** Whether the canvas already holds one of a type the executor allows only once. */
  const alreadyPresent = (type: NodeType) =>
    Boolean(ONE_PER_PIPELINE[type]) && nodes.some((node) => node.data.nodeType === type)

  const addNode = (type: NodeType) => {
    // Guarded here as well as on the button. The button being disabled is a courtesy; this is the
    // rule, and it holds for any path that reaches this function.
    if (alreadyPresent(type)) {
      return
    }
    history.commit()
    // Suffixed with a counter that never reuses a number, even after deletions — a reused node id
    // would silently reconnect to edges that belonged to the deleted node.
    const id = `${type.toLowerCase()}-${Date.now().toString(36).slice(-4)}`

    setNodes((current) => [
      ...current,
      toFlowNode(
        {
          id,
          type,
          name: PALETTE.find((entry) => entry.type === type)?.label ?? type,
          connectorInstanceId: null,
          // A script step opens with working code rather than an empty editor. An empty one made
          // the first move "find the button that fills it in", which is a step that need not exist.
          config: starterConfig(type),
        },
        placeInView(current),
      ),
    ])

    setEdges((currentEdges) => spliceIntoChain(id, type, nodes, currentEdges))
  }

  /**
   * Connects a new node into the existing flow.
   *
   * A source with nothing after it, or a sink with nothing before it, gets joined to whatever is
   * already there. A processing step is inserted on the edge feeding the sink, so adding one to a
   * working pipeline leaves a working pipeline. When there is nothing to attach to, nothing
   * happens and the user wires it themselves.
   */
  function spliceIntoChain(id: string, type: NodeType, existing: Node[], currentEdges: Edge[]) {
    const idOf = (wanted: NodeType) => existing.find((n) => n.data.nodeType === wanted)?.id
    const link = (from: string, to: string): Edge => ({
      id: `e-${from}-${to}`,
      source: from,
      target: to,
      type: 'smoothstep',
      markerEnd: { type: MarkerType.ArrowClosed },
    })

    if (type === 'SOURCE') {
      const downstream = idOf('SINK') ?? existing.find((n) => n.data.nodeType !== 'SOURCE')?.id
      return downstream ? addEdge(link(id, downstream), currentEdges) : currentEdges
    }

    if (type === 'SINK') {
      const upstream = lastBeforeSink(existing, currentEdges) ?? idOf('SOURCE')
      return upstream ? addEdge(link(upstream, id), currentEdges) : currentEdges
    }

    const sinkId = idOf('SINK')
    if (!sinkId) {
      const sourceId = idOf('SOURCE')
      return sourceId ? addEdge(link(sourceId, id), currentEdges) : currentEdges
    }

    // Splice: whatever fed the sink now feeds this node, and this node feeds the sink.
    const feedingSink = currentEdges.find((edge) => edge.target === sinkId)
    const withoutOld = currentEdges.filter((edge) => edge.id !== feedingSink?.id)

    return feedingSink
      ? addEdge(link(id, sinkId), addEdge(link(feedingSink.source, id), withoutOld))
      : addEdge(link(id, sinkId), withoutOld)
  }

  /** The node currently feeding the sink, which a new step should be inserted after. */
  function lastBeforeSink(existing: Node[], currentEdges: Edge[]) {
    const sinkId = existing.find((n) => n.data.nodeType === 'SINK')?.id
    if (!sinkId) {
      return existing.filter((n) => n.data.nodeType !== 'SINK').at(-1)?.id
    }
    return currentEdges.find((edge) => edge.target === sinkId)?.source
  }

  /**
   * Where a newly added node goes: in front of the user.
   *
   * The previous version numbered nodes from a counter that lived outside the component and never
   * reset, so each node landed further right than the last and, after a few, off screen entirely —
   * with fitView only running on mount, the viewport never followed. Placing relative to the
   * current viewport means a node always appears where the user is looking, whatever they have
   * panned or zoomed to.
   */
  function placeInView(existing: Node[]) {
    const { x, y, zoom } = flow.getViewport()
    const pane = document.querySelector('.react-flow__viewport')?.parentElement
    const width = pane?.clientWidth ?? 900
    const height = pane?.clientHeight ?? 600

    const centre = {
      x: (width / 2 - x) / zoom - NODE_WIDTH / 2,
      y: (height / 2 - y) / zoom - NODE_HEIGHT / 2,
    }

    // Nudged down and right per existing node so a second one does not land exactly on the first
    // and look like nothing happened. Wraps so a long session does not march off the canvas again.
    const offset = (existing.length % 6) * 36
    return { x: centre.x + offset, y: centre.y + offset }
  }

  const doSave = () => save.mutate(definition, { onSuccess: () => setValidation(null) })

  const doValidate = () =>
    save.mutate(definition, {
      // Validate what is on screen, not what was last saved. Otherwise the report describes a
      // graph the user is no longer looking at.
      onSuccess: () => validate.mutate(undefined, { onSuccess: setValidation }),
    })

  if (version.isLoading) return <Loading />
  if (version.error) return <ErrorPanel error={version.error} />

  const selectedNode = nodes.find((node) => node.id === selected)

  return (
    <>
      <PageHeader
        breadcrumbs={[
          { label: 'Pipelines', to: '/pipelines' },
          { label: 'Pipeline', to: `/pipelines/${pipelineId}` },
          { label: `v${version.data?.versionNumber}` },
        ]}
        title={`Design v${version.data?.versionNumber}`}
        subtitle={
          readOnly
            ? 'This version is published and frozen. Create a new version to make changes.'
            : 'Drag from a node’s right edge to its neighbour’s left edge. ⌘Z undo · ⌘D duplicate · ⌫ delete'
        }
        actions={
          readOnly ? (
            // The same toolbar, not a different screen. Hiding it entirely was the old behaviour
            // and it took the settings with it — so a published version could be looked at as a
            // diagram and nothing else, with no way to see the chunk size, the delivery mode or
            // the scripts that version actually runs with. Editing is refused; looking is not.
            <Button startIcon={<TuneIcon />} onClick={() => setTuning(true)}>
              Settings
            </Button>
          ) : (
            <>
              <Tooltip title="Undo (⌘Z)">
                <span>
                  <IconButton size="small" onClick={history.undo} disabled={!history.canUndo}>
                    <UndoIcon fontSize="small" />
                  </IconButton>
                </span>
              </Tooltip>
              <Tooltip title="Redo (⇧⌘Z)">
                <span>
                  <IconButton size="small" onClick={history.redo} disabled={!history.canRedo}>
                    <RedoIcon fontSize="small" />
                  </IconButton>
                </span>
              </Tooltip>
              <Tooltip title="Duplicate selected (⌘D)">
                <span>
                  <IconButton size="small" onClick={history.duplicate}>
                    <ContentCopyIcon fontSize="small" />
                  </IconButton>
                </span>
              </Tooltip>
              <Button startIcon={<TuneIcon />} onClick={() => setTuning(true)}>
                Settings
              </Button>
              <Button startIcon={<CheckIcon />} onClick={doValidate} disabled={validate.isPending}>
                Validate
              </Button>
              <Button startIcon={<SaveIcon />} onClick={doSave} disabled={save.isPending}>
                Save
              </Button>
              <Button
                startIcon={<PublishIcon />}
                variant="contained"
                disabled={publish.isPending}
                onClick={() =>
                  save.mutate(definition, {
                    onSuccess: () =>
                      publish.mutate(version.data!.versionNumber, {
                        onSuccess: () => navigate(`/pipelines/${pipelineId}`),
                      }),
                  })
                }
              >
                Publish
              </Button>
            </>
          )
        }
      />

      <ErrorPanel error={save.error} />
      <ErrorPanel error={publish.error} />

      {validation && <ValidationReport report={validation} />}

      <Stack direction="row" spacing={2} sx={{ height: 620 }}>
        {readOnly && version.data && (
          <VersionDetails version={version.data} connectors={connectors.data?.content ?? []} />
        )}

        {!readOnly && (
          <Paper sx={{ width: 190, p: 1.5, flexShrink: 0, overflowY: 'auto' }}>
            <Typography variant="caption" sx={{ fontWeight: 700, letterSpacing: '0.06em' }}>
              ADD A STEP
            </Typography>
            <Stack spacing={0.5} sx={{ mt: 1 }}>
              {PALETTE.map((entry) => {
                const blocked = alreadyPresent(entry.type)
                return (
                  <Button
                    key={entry.type}
                    size="small"
                    disabled={blocked}
                    onClick={() => addNode(entry.type)}
                    sx={{ justifyContent: 'flex-start', textAlign: 'left' }}
                  >
                    <Box>
                      <Typography variant="body2" sx={{ fontWeight: 600 }}>
                        {entry.label}
                      </Typography>
                      <Typography variant="caption" sx={{ display: 'block' }}>
                        {blocked ? ONE_PER_PIPELINE[entry.type] : entry.hint}
                      </Typography>
                    </Box>
                  </Button>
                )
              })}
            </Stack>
          </Paper>
        )}

        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0 }}>
          {version.data && (
            <FlowSummary
              version={version.data}
              source={instanceFor('SOURCE')}
              sink={instanceFor('SINK')}
              sinkSpec={catalogue.data?.find((spec) => spec.type === instanceFor('SINK')?.connectorType)}
            />
          )}
          <Paper sx={{ flex: 1, overflow: 'hidden' }}>
          <ReactFlow
            nodes={drawn.nodes}
            edges={drawn.edges}
            onNodesChange={
              readOnly
                ? undefined
                : (changes) =>
                    onNodesChange(changes.filter((change) =>
                      !('id' in change) || change.id !== DELIVERY_NODE_ID))
            }
            onEdgesChange={readOnly ? undefined : onEdgesChange}
            onConnect={readOnly ? undefined : onConnect}
            onNodeClick={(_, node) => {
              if (node.id === DELIVERY_NODE_ID) {
                if (!node.data?.locked) setTuning(true)
                return
              }
              setSelected(node.id)
            }}
            // Snapshot on drag start, not on every position change: React Flow emits a change per
            // pixel, so recording each one makes undo move a node by one pixel.
            onNodeDragStart={readOnly ? undefined : history.commit}
            onSelectionDragStart={readOnly ? undefined : history.commit}
            // Positions are never stored in a definition and the row is recomputed every render,
            // so dragging could only ever be undone a frame later. Arrangement is the layout's job.
            nodesDraggable={false}
            deleteKeyCode={readOnly ? null : ['Backspace', 'Delete']}
            onBeforeDelete={readOnly ? undefined : async () => {
              history.commit()
              return true
            }}
            nodeTypes={NODE_TYPES}
            edgeTypes={EDGE_TYPES}
            colorMode={mode}
            // Without an explicit connection radius the drop target is only the handle itself,
            // which is a demanding gesture on a trackpad.
            connectionRadius={30}
            defaultEdgeOptions={{ markerEnd: { type: MarkerType.ArrowClosed }, animated: true }}
            fitView
            fitViewOptions={FIT_VIEW}
            // A column of steps is taller than it is wide, so a very low bound frames it as a
            // smear on a wide screen.
            minZoom={0.4}
            maxZoom={1.5}
            proOptions={{ hideAttribution: false }}
          >
            <Background gap={16} />
            <Controls />
          </ReactFlow>
          </Paper>
        </Box>
      </Stack>

      {tuning && version.data && (
        <PolicyDialog
          version={version.data}
          pipelineId={pipelineId}
          sinkConnectorType={instanceFor('SINK')?.connectorType}
          readOnly={readOnly}
          onClose={() => setTuning(false)}
        />
      )}

      {/*
        disableEnforceFocus because the code editor manages focus itself, through a hidden textarea
        it creates and moves around. MUI's focus trap pulls focus back to the drawer whenever that
        happens, which leaves the editor visibly focused but unable to receive typing or a paste.
        The trap exists for keyboard navigation of modal dialogs; this drawer is an inspector
        panel, and closing it with Escape still works.
      */}
      <Drawer
        anchor="right"
        open={Boolean(selectedNode)}
        onClose={() => setSelected(null)}
        disableEnforceFocus
      >
        {selectedNode && (
          <NodeInspector
            node={selectedNode}
            readOnly={readOnly}
            connectors={connectors.data?.content ?? []}
            source={instanceFor('SOURCE')}
            onChange={(changes) =>
              setNodes((current) =>
                current.map((node) =>
                  node.id === selectedNode.id
                    ? { ...node, data: { ...node.data, ...changes } }
                    : node,
                ),
              )
            }
            onDelete={() => {
              history.commit()
              setNodes((current) => current.filter((node) => node.id !== selectedNode.id))
              setEdges((current) =>
                current.filter(
                  (edge) => edge.source !== selectedNode.id && edge.target !== selectedNode.id,
                ),
              )
              setSelected(null)
            }}
          />
        )}
      </Drawer>
    </>
  )
}

function NodeInspector({
  node,
  connectors,
  source,
  onChange,
  onDelete,
  readOnly = false,
}: {
  node: Node
  connectors: import('@/api/types').ConnectorInstance[]
  /**
   * The pipeline's source, so a script can be tried against a record it will actually be given.
   *
   * Read from the saved definition, which means it appears once the source node has been chosen
   * and saved — the point at which "what do these records look like" becomes answerable.
   */
  source?: import('@/api/types').ConnectorInstance
  onChange: (changes: Record<string, unknown>) => void
  onDelete: () => void
  /**
   * Shown but not editable, for a published version.
   *
   * A frozen version could not be inspected at all — the drawer simply refused to open — so the
   * one thing that decides what a step does, its query or its script, was invisible on exactly the
   * version somebody was trying to understand because it is the one in production.
   */
  readOnly?: boolean
}) {
  const nodeType = node.data.nodeType as NodeType
  const needsConnector = nodeType === 'SOURCE' || nodeType === 'SINK'
  const isScripted = nodeType === 'TRANSFORM' || nodeType === 'BATCH_TRANSFORM'
  const stage: TransformStage = nodeType === 'BATCH_TRANSFORM' ? 'BATCH' : 'RECORD'

  const config = (node.data.config as Record<string, unknown>) ?? {}
  const script = (config.script as string) ?? ''

  // Only offer connections that can actually fill this role. A sink-only connection listed under a
  // source node is an error the user would only discover at publish.
  const usable = connectors.filter((connector) =>
    nodeType === 'SOURCE'
      ? connector.direction !== 'SINK'
      : nodeType === 'SINK'
        ? connector.direction !== 'SOURCE'
        : true,
  )

  return (
    <Box sx={{ width: isScripted ? 560 : 340, p: 3 }}>
      <Typography variant="h3" sx={{ mb: 0.5 }}>
        {nodeType}
      </Typography>
      <Typography variant="caption" sx={{ display: 'block', mb: 3 }}>
        {node.id}
      </Typography>

      <Stack spacing={2.5}>
        <TextField
          label="Name"
          value={(node.data.label as string) ?? ''}
          onChange={(event) => onChange({ label: event.target.value })}
          size="small"
          fullWidth
          disabled={readOnly}
        />

        {isScripted && (
          <ScriptEditor
            stage={stage}
            script={script}
            readOnly={readOnly}
            sourceInstanceId={source?.id}
            sourceName={source?.name}
            onChange={(next) => onChange({ config: { ...config, script: next } })}
          />
        )}

        {isScripted && !readOnly && !script.trim() && (
          <Button
            size="small"
            onClick={() => onChange({ config: { ...config, script: STARTER_SCRIPTS[stage] } })}
          >
            Restore the example
          </Button>
        )}

        {!needsConnector && !isScripted && (
          <Alert severity="info" sx={{ '& .MuiAlert-message': { fontSize: 13 } }}>
            {/*
              Said plainly rather than hidden. A step that silently does nothing is worse than one
              that admits it, because the user would otherwise spend an afternoon wondering why
              their filter had no effect. Use a Transform node until these gain behaviour.
            */}
            This step type has no behaviour yet — it passes records through unchanged. For now, use
            a <strong>Transform</strong> step and write the logic in JavaScript.
          </Alert>
        )}

        {needsConnector && (
          <TextField
            select
            label="Connection *"
            value={(node.data.connectorInstanceId as string) ?? ''}
            onChange={(event) => onChange({ connectorInstanceId: event.target.value })}
            size="small"
            fullWidth
            disabled={readOnly}
            helperText={
              usable.length === 0
                ? 'No suitable connection exists. Create one under Connectors first.'
                : 'Which configured connection this step uses'
            }
          >
            {usable.map((connector) => (
              <MenuItem key={connector.id} value={connector.id}>
                {connector.name} ({connector.connectorType})
              </MenuItem>
            ))}
          </TextField>
        )}

        {readOnly ? (
          <Alert severity="info" sx={{ '& .MuiAlert-message': { fontSize: 13 } }}>
            This version is published and frozen, so nothing here can be changed. Copy it to a new
            version from the pipeline page to edit it.
          </Alert>
        ) : (
          <Button color="error" onClick={onDelete}>
            Delete this step
          </Button>
        )}
      </Stack>
    </Box>
  )
}

/**
 * The validation report.
 *
 * Errors and warnings are shown separately because only errors block publishing. A warning that
 * looks like a failure teaches people to ignore both.
 */
function ValidationReport({ report }: { report: ValidationResponse }) {
  if (report.valid && report.warnings.length === 0) {
    return (
      <Alert severity="success" sx={{ mb: 2 }}>
        This pipeline is valid and ready to publish.
      </Alert>
    )
  }

  return (
    <Stack spacing={1} sx={{ mb: 2 }}>
      {report.errors.length > 0 && (
        <Alert severity="error">
          <AlertTitle>
            {report.errors.length} problem{report.errors.length === 1 ? '' : 's'} to fix before
            publishing
          </AlertTitle>
          <Box component="ul" sx={{ m: 0, pl: 2.5 }}>
            {report.errors.map((issue, index) => (
              <li key={`${issue.code}-${index}`}>
                <Typography variant="body2">
                  {issue.message}
                  {issue.nodeId && (
                    <Typography component="span" variant="caption" sx={{ ml: 0.5 }}>
                      ({issue.nodeId})
                    </Typography>
                  )}
                </Typography>
              </li>
            ))}
          </Box>
        </Alert>
      )}

      {report.warnings.length > 0 && (
        <Alert severity="warning">
          <AlertTitle>Worth a look — these do not block publishing</AlertTitle>
          <Box component="ul" sx={{ m: 0, pl: 2.5 }}>
            {report.warnings.map((issue, index) => (
              <li key={`${issue.code}-${index}`}>
                <Typography variant="body2">{issue.message}</Typography>
              </li>
            ))}
          </Box>
        </Alert>
      )}
    </Stack>
  )
}

const NODE_WIDTH = 220
const NODE_HEIGHT = 80

/** What a freshly dropped node carries. Script steps arrive with runnable code in them. */
function starterConfig(type: NodeType): Record<string, unknown> {
  if (type === 'TRANSFORM') {
    return { script: STARTER_SCRIPTS.RECORD }
  }
  if (type === 'BATCH_TRANSFORM') {
    return { script: STARTER_SCRIPTS.BATCH }
  }
  return {}
}

/**
 * Where a node from a saved pipeline goes.
 *
 * Positions are not persisted, so an opened pipeline is laid out fresh: sources on the left, sinks
 * on the right, everything else between, and each stacked by its index. Derived purely from the
 * node and its index — a counter held outside the component would keep climbing across every
 * render and hydration, which is exactly how nodes ended up off screen.
 */
/** Every node starts here and is moved by {@link layout}, which is what actually places them. */
const ORIGIN = { x: 0, y: 0 }

function toFlowNode(
  node: import('@/api/types').NodeDefinition,
  position: { x: number; y: number },
): Node {
  return {
    id: node.id,
    type: 'pipelineNode',
    position,
    data: {
      label: node.name,
      nodeType: node.type,
      connectorInstanceId: node.connectorInstanceId,
      config: node.config,
    },
  }
}

function toFlowEdge(edge: import('@/api/types').EdgeDefinition): Edge {
  return {
    id: edge.id,
    source: edge.from,
    target: edge.to,
    label: edge.condition ?? undefined,
    markerEnd: { type: MarkerType.ArrowClosed },
  }
}
