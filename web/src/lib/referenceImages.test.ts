import { describe, expect, it } from 'vitest'
import { REF_MAX_IMAGE_BYTES, checkReferenceFiles, readTaskSnapshot, redactDataUrls, type ReferenceImage } from './referenceImages'

const file = (name: string, size: number, type = 'image/png') => new File([new Uint8Array(size)], name, { type })
const existing = (count: number): ReferenceImage[] => Array.from({ length: count }, (_, index) => ({ id: `r${index}`, name: `r${index}.png`, size: 10, type: 'image/png', dataUrl: 'data:image/png;base64,AA==' }))

describe('reference image limits', () => {
  it('accepts images until a limit is hit and explains the first rejection', () => {
    expect(checkReferenceFiles([], [file('a.png', 10), file('b.jpg', 10, 'image/jpeg')])).toEqual({ accepted: expect.any(Array) })
    expect(checkReferenceFiles([], [file('a.txt', 10, 'text/plain')]).error).toContain('不是 PNG')
    expect(checkReferenceFiles(existing(3), [file('a.png', 10), file('b.png', 10)])).toMatchObject({ accepted: [expect.any(File)], error: '参考图最多 4 张' })
    expect(checkReferenceFiles([], [file('big.png', REF_MAX_IMAGE_BYTES + 1)]).error).toContain('单张')
  })

  it('redacts inline images for cURL but keeps other fields', () => {
    const body = { prompt: 'x', images: ['data:image/png;base64,' + 'A'.repeat(4096), 'https://example.com/a.png'] }
    expect(redactDataUrls(body)).toEqual({ prompt: 'x', images: ['data:image/png;base64,<已省略 3 KB>', 'https://example.com/a.png'] })
  })
})

describe('task snapshots', () => {
  it('reads queued, finished image and finished video responses', () => {
    expect(readTaskSnapshot({ id: 't1', task_id: 't1', status: 'queued', progress: 0 })).toMatchObject({ id: 't1', status: 'queued', terminal: false, ok: false })
    expect(readTaskSnapshot({ task: { id: 't1', status: 'completed', url: '/v1/media/old.png' }, id: 't1', status: 'completed', progress: 100, data: [{ url: '/v1/media/a.png' }] }))
      .toMatchObject({ status: 'completed', terminal: true, ok: true, url: '/v1/media/a.png', progress: 100 })
    expect(readTaskSnapshot({ id: 'v1', task_id: 'v1', status: 'completed', url: '/v1/media/v.mp4' })).toMatchObject({ ok: true, url: '/v1/media/v.mp4' })
    expect(readTaskSnapshot({ id: 'v2', status: 'failed', error: '上游拒绝生成媒体' })).toMatchObject({ terminal: true, ok: false, error: '上游拒绝生成媒体' })
    expect(readTaskSnapshot({ error: { message: 'bad' } })).toBeUndefined()
  })
})
