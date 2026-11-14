import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import Playground from './Playground'

function json(value: unknown, status = 200) {
  return new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
}

function renderPlayground() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><Playground /></QueryClientProvider>)
}

describe('Playground image task', () => {
  beforeEach(() => { vi.restoreAllMocks(); vi.useFakeTimers({ shouldAdvanceTime: true }) })
  afterEach(() => { vi.useRealTimers() })

  it('sends uploaded reference images with the prompt and shows the finished image', async () => {
    let posted: { prompt?: string; images?: string[] } | undefined
    let polls = 0
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/models')) return json({ data: [{ id: 'muse-image', status: 'verified' }] })
      if (url.endsWith('/v1/images/tasks') && init?.method === 'POST') {
        posted = JSON.parse(String(init.body))
        return json({ id: 'task_1', task_id: 'task_1', object: 'image.task', status: 'queued', progress: 0 }, 202)
      }
      if (url.endsWith('/v1/images/tasks/task_1')) {
        polls++
        if (polls === 1) return json({ id: 'task_1', task_id: 'task_1', status: 'processing', progress: 40 })
        return json({ task: { id: 'task_1', status: 'completed' }, id: 'task_1', task_id: 'task_1', status: 'completed', progress: 100, data: [{ url: '/v1/media/out.png' }] })
      }
      return json({})
    })

    renderPlayground()
    fireEvent.click(screen.getByRole('tab', { name: /图片任务/ }))
    const prompt = screen.getByRole('textbox')
    fireEvent.change(prompt, { target: { value: '把第 1 张图改成水彩风格' } })
    const reference = new File([new Uint8Array([137, 80, 78, 71])], 'ref.png', { type: 'image/png' })
    fireEvent.change(screen.getByTestId('reference-input'), { target: { files: [reference] } })
    expect(await screen.findByAltText('参考图 1：ref.png')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /发送/ }))
    await waitFor(() => expect(posted).toBeDefined())
    expect(posted?.prompt).toBe('把第 1 张图改成水彩风格')
    expect(posted?.images).toHaveLength(1)
    expect(posted?.images?.[0]).toMatch(/^data:image\/png;base64,/)

    await act(async () => { await vi.advanceTimersByTimeAsync(2100) })
    expect(await screen.findByText('生成中 40%')).toBeInTheDocument()
    await act(async () => { await vi.advanceTimersByTimeAsync(2100) })
    const image = await screen.findByAltText('生成的图片')
    expect(image).toHaveAttribute('src', '/v1/media/out.png')
    expect(screen.getByText('已完成')).toBeInTheDocument()
  })

  it('rejects non-image files without sending them', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async () => json({ data: [] }))
    renderPlayground()
    fireEvent.click(screen.getByRole('tab', { name: /视频任务/ }))
    fireEvent.change(screen.getByTestId('reference-input'), { target: { files: [new File(['x'], 'notes.txt', { type: 'text/plain' })] } })
    expect(await screen.findByRole('alert')).toHaveTextContent('不是 PNG')
    expect(screen.queryByAltText(/参考图 1/)).not.toBeInTheDocument()
  })
})
