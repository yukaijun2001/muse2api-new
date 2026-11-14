// Reference images for the image/video playground. Limits mirror
// internal/refimage so oversize uploads fail in the browser instead of after
// a 30 MB request round trip.
export const REF_MAX_IMAGES = 4
export const REF_MAX_IMAGE_BYTES = 10 * 1024 * 1024
export const REF_MAX_TOTAL_BYTES = 24 * 1024 * 1024
export const REF_ACCEPT = ['image/png', 'image/jpeg', 'image/webp', 'image/gif']

export interface ReferenceImage { id: string; name: string; size: number; type: string; dataUrl: string }

export function formatBytes(bytes: number) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`
}

/** Returns the files that fit the limits plus a message for the first one that did not. */
export function checkReferenceFiles(current: ReferenceImage[], files: File[]): { accepted: File[]; error?: string } {
  const accepted: File[] = []
  let total = current.reduce((sum, image) => sum + image.size, 0)
  for (const file of files) {
    if (!REF_ACCEPT.includes(file.type)) return { accepted, error: `「${file.name}」不是 PNG、JPEG、WebP 或 GIF 图片` }
    if (current.length + accepted.length >= REF_MAX_IMAGES) return { accepted, error: `参考图最多 ${REF_MAX_IMAGES} 张` }
    if (file.size > REF_MAX_IMAGE_BYTES) return { accepted, error: `「${file.name}」超过单张 ${formatBytes(REF_MAX_IMAGE_BYTES)} 上限` }
    if (total + file.size > REF_MAX_TOTAL_BYTES) return { accepted, error: `参考图总大小超过 ${formatBytes(REF_MAX_TOTAL_BYTES)}` }
    total += file.size
    accepted.push(file)
  }
  return { accepted }
}

export function readAsDataURL(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => resolve(String(reader.result))
    reader.onerror = () => reject(reader.error ?? new Error(`无法读取「${file.name}」`))
    reader.readAsDataURL(file)
  })
}

/** Image files carried by a paste or drop, in order. */
export function imageFilesFrom(data: DataTransfer | null): File[] {
  if (!data) return []
  return Array.from(data.files).filter(file => file.type.startsWith('image/'))
}

/**
 * Replaces inline data URLs with a size note so a copied cURL stays readable.
 * The note is not valid base64: the command must be completed by hand.
 */
export function redactDataUrls(value: unknown): unknown {
  if (typeof value === 'string') {
    const match = /^data:([^;,]+);base64,(.*)$/s.exec(value)
    if (!match) return value
    return `data:${match[1]};base64,<已省略 ${formatBytes(Math.floor(match[2].length * 3 / 4))}>`
  }
  if (Array.isArray(value)) return value.map(redactDataUrls)
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, redactDataUrls(item)]))
  return value
}

export interface TaskSnapshot { id: string; status: string; progress?: number; url?: string; error?: string; terminal: boolean; ok: boolean }

const terminalStatuses = new Set(['completed', 'succeeded', 'failed', 'cancelled', 'unknown'])

/**
 * Normalizes an image/video task response. Image task polls wrap a finished
 * task as { task, data: [{ url }] }; every other state (and video tasks)
 * return the task object itself.
 */
export function readTaskSnapshot(body: unknown): TaskSnapshot | undefined {
  if (!body || typeof body !== 'object') return undefined
  const record = body as Record<string, unknown>
  const task = (record.task && typeof record.task === 'object' ? record.task : record) as Record<string, unknown>
  const id = String(record.task_id ?? record.id ?? task.task_id ?? task.id ?? '')
  const status = String(record.status ?? task.status ?? '')
  if (!id || !status) return undefined
  const data = Array.isArray(record.data) ? record.data[0] as Record<string, unknown> | undefined : undefined
  const url = [data?.url, task.url, record.url].find((value): value is string => typeof value === 'string' && value !== '')
  const progress = typeof (record.progress ?? task.progress) === 'number' ? Number(record.progress ?? task.progress) : undefined
  const error = typeof task.error === 'string' && task.error ? task.error : undefined
  const ok = status === 'completed' || status === 'succeeded'
  return { id, status, progress, url, error, terminal: terminalStatuses.has(status), ok }
}

export const taskStatusText: Record<string, string> = {
  queued: '排队中', processing: '生成中', running: '生成中', completed: '已完成', succeeded: '已完成', failed: '失败', cancelled: '已取消', unknown: '结果未知',
}
