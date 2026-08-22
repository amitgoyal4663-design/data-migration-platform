import Link from '@mui/material/Link'
import Breadcrumbs from '@mui/material/Breadcrumbs'
import AppBar from '@mui/material/AppBar'
import Box from '@mui/material/Box'
import Drawer from '@mui/material/Drawer'
import IconButton from '@mui/material/IconButton'
import List from '@mui/material/List'
import ListItemButton from '@mui/material/ListItemButton'
import ListItemIcon from '@mui/material/ListItemIcon'
import ListItemText from '@mui/material/ListItemText'
import Stack from '@mui/material/Stack'
import Toolbar from '@mui/material/Toolbar'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import DashboardIcon from '@mui/icons-material/SpaceDashboardOutlined'
import AccountTreeIcon from '@mui/icons-material/AccountTreeOutlined'
import CableIcon from '@mui/icons-material/CableOutlined'
import HistoryIcon from '@mui/icons-material/HistoryOutlined'
import SearchIcon from '@mui/icons-material/ManageSearchOutlined'
import GavelIcon from '@mui/icons-material/GavelOutlined'
import NotificationsIcon from '@mui/icons-material/NotificationsOutlined'
import ScheduleIcon from '@mui/icons-material/ScheduleOutlined'
import DarkModeIcon from '@mui/icons-material/DarkModeOutlined'
import LightModeIcon from '@mui/icons-material/LightModeOutlined'
import MenuIcon from '@mui/icons-material/Menu'
import { useState } from 'react'
import { Link as RouterLink, NavLink, Outlet, useLocation } from 'react-router-dom'
import { usePageChrome, useThemeMode } from '@/store'
import { muted } from '@/theme'

const NAV = [
  { to: '/', label: 'Dashboard', Icon: DashboardIcon, exact: true },
  { to: '/pipelines', label: 'Pipelines', Icon: AccountTreeIcon },
  { to: '/connectors', label: 'Connectors', Icon: CableIcon },
  { to: '/schedules', label: 'Schedules', Icon: ScheduleIcon },
  { to: '/notifications', label: 'Notifications', Icon: NotificationsIcon },
  { to: '/runs', label: 'Runs', Icon: HistoryIcon },
  { to: '/records', label: 'Find a record', Icon: SearchIcon },
  { to: '/audit', label: 'Audit trail', Icon: GavelIcon },
]

const DRAWER_WIDTH = 232

export function AppShell() {
  const { mode, toggle } = useThemeMode()
  const breadcrumbs = usePageChrome((state) => state.breadcrumbs)
  const [mobileOpen, setMobileOpen] = useState(false)
  const location = useLocation()

  const nav = (
    <Box sx={{ px: 1.5, py: 2 }}>
      <Stack direction="row" spacing={1} alignItems="center" sx={{ px: 1.5, pb: 2.5 }}>
        <Box
          sx={{
            width: 26,
            height: 26,
            borderRadius: 1.5,
            bgcolor: 'primary.main',
            display: 'grid',
            placeItems: 'center',
            color: '#fff',
            fontWeight: 700,
            fontSize: 13,
          }}
        >
          D
        </Box>
        <Typography sx={{ fontWeight: 700, letterSpacing: '-0.01em' }}>Migration</Typography>
      </Stack>

      <List disablePadding>
        {NAV.map(({ to, label, Icon, exact }) => {
          const active = exact ? location.pathname === to : location.pathname.startsWith(to)
          return (
            <ListItemButton
              key={to}
              component={NavLink}
              to={to}
              onClick={() => setMobileOpen(false)}
              selected={active}
              sx={{
                borderRadius: 2,
                mb: 0.5,
                '&.Mui-selected': { bgcolor: 'action.selected' },
              }}
            >
              <ListItemIcon sx={{ minWidth: 34, color: active ? 'primary.main' : muted }}>
                <Icon sx={{ fontSize: 20 }} />
              </ListItemIcon>
              <ListItemText
                primary={label}
                primaryTypographyProps={{ fontSize: 14, fontWeight: active ? 600 : 500 }}
              />
            </ListItemButton>
          )
        })}
      </List>
    </Box>
  )

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      <AppBar
        position="fixed"
        color="inherit"
        sx={{
          zIndex: (t) => t.zIndex.drawer + 1,
          borderBottom: 1,
          borderColor: 'divider',
          bgcolor: 'background.paper',
        }}
        elevation={0}
      >
        <Toolbar variant="dense" sx={{ minHeight: 52 }}>
          <IconButton
            edge="start"
            onClick={() => setMobileOpen((open) => !open)}
            sx={{ mr: 1, display: { md: 'none' } }}
          >
            <MenuIcon />
          </IconButton>

          {/*
            The trail lives here rather than on the page. The bar already spans the full width and
            was holding a spacer; the page was spending a line of its own on the same two words.
          */}
          <Breadcrumbs sx={{ flex: 1, fontSize: 13, minWidth: 0 }}>
            {breadcrumbs.map((crumb) =>
              crumb.to ? (
                <Link
                  key={crumb.label}
                  component={RouterLink}
                  to={crumb.to}
                  underline="hover"
                  sx={{ color: muted, fontSize: 13 }}
                >
                  {crumb.label}
                </Link>
              ) : (
                <Typography key={crumb.label} noWrap sx={{ fontSize: 13, color: 'text.primary' }}>
                  {crumb.label}
                </Typography>
              ),
            )}
          </Breadcrumbs>

          <Tooltip title={mode === 'dark' ? 'Switch to light' : 'Switch to dark'}>
            <IconButton onClick={toggle} size="small">
              {mode === 'dark' ? <LightModeIcon fontSize="small" /> : <DarkModeIcon fontSize="small" />}
            </IconButton>
          </Tooltip>
        </Toolbar>
      </AppBar>

      <Box component="nav" sx={{ width: { md: DRAWER_WIDTH }, flexShrink: { md: 0 } }}>
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={() => setMobileOpen(false)}
          ModalProps={{ keepMounted: true }}
          sx={{
            display: { xs: 'block', md: 'none' },
            '& .MuiDrawer-paper': { width: DRAWER_WIDTH, boxSizing: 'border-box' },
          }}
        >
          {nav}
        </Drawer>

        <Drawer
          variant="permanent"
          open
          sx={{
            display: { xs: 'none', md: 'block' },
            '& .MuiDrawer-paper': {
              width: DRAWER_WIDTH,
              boxSizing: 'border-box',
              borderRight: 1,
              borderColor: 'divider',
            },
          }}
        >
          <Toolbar variant="dense" sx={{ minHeight: 52 }} />
          {nav}
        </Drawer>
      </Box>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          minWidth: 0,
          bgcolor: 'background.default',
          px: { xs: 2, md: 4 },
          pb: 6,
        }}
      >
        <Toolbar variant="dense" sx={{ minHeight: 52 }} />
        <Outlet />
      </Box>
    </Box>
  )
}
