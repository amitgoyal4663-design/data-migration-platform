import { Navigate, createBrowserRouter } from 'react-router-dom'
import { AppShell } from './layout/AppShell'
import { PipelinesPage } from './pages/PipelinesPage'
import { PipelineDetailPage } from './pages/PipelineDetailPage'
import { DesignerPage } from './pages/DesignerPage'
import { ConnectorsPage } from './pages/ConnectorsPage'
import { OperationsPage } from './pages/OperationsPage'
import { RunsPage } from './pages/RunsPage'
import { RunDetailPage } from './pages/RunDetailPage'
import { SchedulesPage } from './pages/SchedulesPage'
import { NotifiersPage } from './pages/NotifiersPage'
import { RecordSearchPage } from './pages/RecordSearchPage'
import { AuditPage } from './pages/AuditPage'

export const router = createBrowserRouter([
  {
    path: '/',
    element: <AppShell />,
    children: [
      { index: true, element: <OperationsPage /> },
      { path: 'pipelines', element: <PipelinesPage /> },
      { path: 'pipelines/:pipelineId', element: <PipelineDetailPage /> },
      { path: 'pipelines/:pipelineId/versions/:versionId/design', element: <DesignerPage /> },
      { path: 'connectors', element: <ConnectorsPage /> },
      { path: 'schedules', element: <SchedulesPage /> },
      { path: 'notifications', element: <NotifiersPage /> },
      // Kept: a filtered operations view is a link somebody has already shared.
      { path: 'operations', element: <Navigate to="/" replace /> },
      { path: 'runs', element: <RunsPage /> },
      { path: 'runs/:runId', element: <RunDetailPage /> },
      { path: 'records', element: <RecordSearchPage /> },
      { path: 'audit', element: <AuditPage /> },
    ],
  },
])
