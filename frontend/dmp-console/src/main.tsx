import CssBaseline from '@mui/material/CssBaseline'
import { ThemeProvider } from '@mui/material/styles'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { StrictMode, useMemo } from 'react'
import { createRoot } from 'react-dom/client'
import { RouterProvider } from 'react-router-dom'
import { router } from './routes'
import { useThemeMode } from './store'
import { buildTheme } from './theme'
import { ApiError } from './api/client'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // A console is read constantly; refetching on every window focus produces a flicker of
      // spinners that makes it feel unstable. Live views set their own interval instead.
      refetchOnWindowFocus: false,
      staleTime: 5_000,
      retry: (failureCount, error) => {
        // The backend says whether a failure is worth retrying. Retrying a validation error four
        // times only delays the message the user needs to read.
        if (error instanceof ApiError) {
          return error.retryable && failureCount < 2
        }
        return failureCount < 2
      },
    },
  },
})

function Root() {
  const mode = useThemeMode((state) => state.mode)
  const theme = useMemo(() => buildTheme(mode), [mode])

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </ThemeProvider>
  )
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Root />
  </StrictMode>,
)
