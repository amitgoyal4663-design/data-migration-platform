import type { ProblemDetail } from './types'

/**
 * A failed API call, carrying the server's problem detail.
 *
 * The backend returns a stable `code` and a `retryable` flag on every error, so the UI can decide
 * how to react without parsing prose. That is the point of RFC 7807 here: error handling that does
 * not break when someone improves a message.
 */
export class ApiError extends Error {
  constructor(
    readonly problem: ProblemDetail,
    readonly status: number,
  ) {
    super(problem.detail || problem.title || `Request failed with ${status}`)
    this.name = 'ApiError'
  }

  get code(): string {
    return this.problem.code
  }

  get retryable(): boolean {
    return this.problem.retryable
  }

  /** Field-level messages from bean validation, for showing errors next to inputs. */
  get fieldErrors(): Record<string, string> {
    return this.problem.fieldErrors ?? {}
  }
}

/**
 * Tenant sent on every request.
 *
 * A development posture, and only that: a header the caller chooses is not an authorisation
 * decision. When company SSO lands, this is replaced by a token and nothing else in the console
 * changes — which is why it lives in one place rather than at each call site.
 */
const TENANT = 'default'

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      'X-Tenant-Id': TENANT,
      'X-Actor': 'console',
      ...init?.headers,
    },
  })

  if (response.status === 204) {
    return undefined as T
  }

  const text = await response.text()
  const body = text ? JSON.parse(text) : undefined

  if (!response.ok) {
    throw new ApiError(
      body ?? {
        type: 'about:blank',
        title: 'Request failed',
        status: response.status,
        detail: response.statusText,
        code: 'UNKNOWN',
        retryable: false,
      },
      response.status,
    )
  }

  return body as T
}

export const api = {
  get: <T>(path: string) => request<T>(path),

  post: <T>(path: string, body?: unknown, headers?: Record<string, string>) =>
    request<T>(path, {
      method: 'POST',
      body: body === undefined ? undefined : JSON.stringify(body),
      headers,
    }),

  put: <T>(path: string, body: unknown) =>
    request<T>(path, { method: 'PUT', body: JSON.stringify(body) }),

  delete: (path: string) => request<void>(path, { method: 'DELETE' }),

  /**
   * Fetches a file rather than JSON, and returns null when there is none.
   *
   * <p>Lives here so the tenant header stays in one place, like every other call. It does not go
   * through `request` because that parses JSON and throws on a non-2xx — and for a file the
   * interesting non-2xx is 404, which means "the destination no longer holds it" and is an
   * ordinary answer rather than an error to surface as one.
   */
  download: async (path: string): Promise<Blob | null> => {
    const response = await fetch(path, {
      headers: { 'X-Tenant-Id': TENANT, 'X-Actor': 'console' },
    })
    return response.ok ? response.blob() : null
  },
}

/** Builds a query string, omitting empty values so the URL stays readable. */
export function query(params: Record<string, string | number | boolean | undefined | null>): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      search.set(key, String(value))
    }
  }
  const rendered = search.toString()
  return rendered ? `?${rendered}` : ''
}
