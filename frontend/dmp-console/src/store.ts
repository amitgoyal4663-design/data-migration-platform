import { create } from 'zustand'
import { persist } from 'zustand/middleware'

/**
 * Theme preference.
 *
 * Persisted so a choice survives a reload, and initialised from the operating system so the first
 * visit already matches the rest of the user's environment. Dark mode here is a selected palette
 * validated against the dark surface, not an inverted light one.
 */
type ThemeState = {
  mode: 'light' | 'dark'
  toggle: () => void
  set: (mode: 'light' | 'dark') => void
}

const systemPrefersDark =
  typeof window !== 'undefined' && window.matchMedia?.('(prefers-color-scheme: dark)').matches

export const useThemeMode = create<ThemeState>()(
  persist(
    (set) => ({
      mode: systemPrefersDark ? 'dark' : 'light',
      toggle: () => set((state) => ({ mode: state.mode === 'dark' ? 'light' : 'dark' })),
      set: (mode) => set({ mode }),
    }),
    { name: 'dmp-theme' },
  ),
)

/**
 * Breadcrumbs, published by the page and rendered in the top bar.
 *
 * The bar spans the full width and held one icon, with a flexible spacer occupying everything
 * between — while directly beneath it every page spent a line of its own on a breadcrumb. Putting
 * the trail where the empty strip already was gives the page that line back and gives the bar
 * something to do.
 *
 * Kept out of React context deliberately: a context provider wrapping the whole shell would
 * re-render every page on each navigation, to move two words.
 */
type PageChromeState = {
  breadcrumbs: { label: string; to?: string }[]
  setBreadcrumbs: (breadcrumbs: { label: string; to?: string }[]) => void
}

export const usePageChrome = create<PageChromeState>((set) => ({
  breadcrumbs: [],
  setBreadcrumbs: (breadcrumbs) => set({ breadcrumbs }),
}))
