import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import Settings from './Settings'

function json(value: unknown, status = 200) {
  return new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
}

describe('Settings stream probe switch', () => {
  beforeEach(() => { vi.restoreAllMocks() })

  it('turns the probe on for the chosen duration and shows when it expires', async () => {
    let probe = { enabled: false, entries: 0, turns: 0 } as { enabled: boolean; until?: number; entries: number; turns: number }
    let posted: unknown
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/admin/settings')) return json({ config: { host: '0.0.0.0', port: 18610, site: 'https://muse.ai' }, version: '2.1.0', restart_required_for: 'env' })
      if (url.endsWith('/admin/apikey')) return json({ configured: true, api_key_masked: 'm2a_…', base_url: 'http://x' })
      if (url.endsWith('/admin/debug/stream-probe') && init?.method === 'POST') {
        posted = JSON.parse(String(init.body))
        probe = { enabled: true, until: 1_900_000_000, entries: 0, turns: 0 }
        return json(probe)
      }
      if (url.endsWith('/admin/debug/stream-probe')) return json(probe)
      return json({})
    })
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={client}><Settings /></QueryClientProvider>)

    expect(await screen.findByText(/· 已关闭/)).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('探测时长'), { target: { value: '60' } })
    fireEvent.click(screen.getByRole('button', { name: '开启' }))
    await waitFor(() => expect(posted).toEqual({ enabled: true, minutes: 60 }))
    expect(await screen.findByText(/已开启，.*自动关闭/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /下载探测记录/ })).toBeDisabled()
  })
})
