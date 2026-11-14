import { useEffect, useRef, useState, type ClipboardEvent } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Check, ChevronRight, Clipboard, Download, KeyRound, Play, Square, Zap } from 'lucide-react'
import { api, apiRaw } from '../lib/api'
import { Button, Field, InlineError, PageHeader, Panel, Select } from '../components/UI'
import { ReferenceImagePicker } from '../components/ReferenceImages'
import { useReferenceImages } from '../lib/useReferenceImages'
import { imageFilesFrom, readTaskSnapshot, redactDataUrls, taskStatusText, type TaskSnapshot } from '../lib/referenceImages'

interface Model { id: string; status: string; description?: string }
type Endpoint = 'chat' | 'responses' | 'image' | 'video'
const endpoints: Record<Endpoint, { path: string; label: string; hint: string; prompt: string }> = {
  chat: { path: '/v1/chat/completions', label: 'Chat Completions', hint: '对话与文本生成', prompt: '请用一句话介绍 Muse2API。' },
  responses: { path: '/v1/responses', label: 'Responses API', hint: 'OpenAI Responses 兼容', prompt: '请用一句话介绍 Muse2API。' },
  image: { path: '/v1/images/tasks', label: '图片任务', hint: '异步图片生成，可附参考图', prompt: '一座海边灯塔，黄昏，电影感光线' },
  video: { path: '/v1/videos', label: '视频任务', hint: '单片段视频生成，可附参考图', prompt: '镜头缓慢推进，海浪拍打礁石' },
}
const isTaskEndpoint = (value: Endpoint) => value === 'image' || value === 'video'
const pollPath = (value: Endpoint, id: string) => value === 'image' ? `/v1/images/tasks/${encodeURIComponent(id)}` : `/v1/videos/${encodeURIComponent(id)}`
const POLL_INTERVAL_MS = 2000
// Longer than the server's video timeout (10 min) so the final state is seen.
const POLL_DEADLINE_MS = 15 * 60_000

function sleep(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const timer = window.setTimeout(resolve, ms)
    signal.addEventListener('abort', () => { window.clearTimeout(timer); reject(new DOMException('aborted', 'AbortError')) }, { once: true })
  })
}

function pretty(text: string) {
  try { return JSON.stringify(JSON.parse(text), null, 2) } catch { return text }
}

export default function Playground() {
  const [endpoint, setEndpoint] = useState<Endpoint>('chat')
  const [model, setModel] = useState('muse-spark')
  const [prompt, setPrompt] = useState(endpoints.chat.prompt)
  const [stream, setStream] = useState(true)
  const [sending, setSending] = useState(false)
  const [controller, setController] = useState<AbortController>()
  const [status, setStatus] = useState<number>()
  const [elapsed, setElapsed] = useState<number>()
  const [requestID, setRequestID] = useState('')
  const [output, setOutput] = useState('')
  const [error, setError] = useState('')
  const [copied, setCopied] = useState(false)
  const [responseCopied, setResponseCopied] = useState(false)
  const [task, setTask] = useState<TaskSnapshot>()
  const [taskKind, setTaskKind] = useState<'image' | 'video'>()
  const references = useReferenceImages()
  const outputRef = useRef<HTMLPreElement>(null)
  const { data } = useQuery({ queryKey: ['models'], queryFn: ({ signal }) => api<{ data: Model[] }>('/v1/models', { signal }) })
  const withReferences = isTaskEndpoint(endpoint)

  useEffect(() => {
    if (!sending || !outputRef.current) return
    outputRef.current.scrollTop = outputRef.current.scrollHeight
  }, [output, sending])

  const changeEndpoint = (value: Endpoint) => {
    setEndpoint(value)
    setModel(value === 'image' ? 'muse-image' : value === 'video' ? 'muse-video' : 'muse-spark')
    // Swap the sample prompt only if the user has not written their own.
    if (prompt === endpoints[endpoint].prompt) setPrompt(endpoints[value].prompt)
    setOutput(''); setStatus(undefined); setError(''); setRequestID(''); setTask(undefined)
  }
  const body = () => {
    if (endpoint === 'responses') return { model, input: prompt, stream }
    if (endpoint === 'chat') return { model, messages: [{ role: 'user', content: prompt }], stream }
    const images = references.images.map(image => image.dataUrl)
    return images.length > 0 ? { model, prompt, images } : { model, prompt }
  }
  const curl = `curl ${location.origin}${endpoints[endpoint].path} \\\n  -H "Authorization: Bearer $MUSE2API_KEY" \\\n  -H "Content-Type: application/json" \\\n  -d '${JSON.stringify(redactDataUrls(body()))}'`

  const pollTask = async (kind: 'image' | 'video', id: string, signal: AbortSignal) => {
    const deadline = Date.now() + POLL_DEADLINE_MS
    while (Date.now() < deadline) {
      await sleep(POLL_INTERVAL_MS, signal)
      const response = await apiRaw(pollPath(kind, id), { signal })
      const text = await response.text()
      setOutput(pretty(text))
      if (!response.ok) { setError(`查询任务失败（HTTP ${response.status}）`); return }
      let snapshot: TaskSnapshot | undefined
      try { snapshot = readTaskSnapshot(JSON.parse(text)) } catch { snapshot = undefined }
      if (!snapshot) { setError('任务响应无法识别'); return }
      setTask(snapshot)
      if (snapshot.terminal) {
        if (!snapshot.ok) setError(snapshot.error ? `任务${taskStatusText[snapshot.status] ?? snapshot.status}：${snapshot.error}` : `任务${taskStatusText[snapshot.status] ?? snapshot.status}`)
        return
      }
    }
    setError('等待超时，任务仍可能在后台运行，可到任务中心查看')
  }

  const send = async () => {
    if (!prompt.trim() || sending) return
    const abort = new AbortController()
    const current = endpoint
    setController(abort); setSending(true); setOutput(''); setError(''); setStatus(undefined); setRequestID(''); setTask(undefined)
    setTaskKind(current === 'image' || current === 'video' ? current : undefined)
    const started = performance.now()
    let polling = false
    try {
      const response = await apiRaw(endpoints[current].path, { method: 'POST', body: JSON.stringify(body()), signal: abort.signal })
      setStatus(response.status)
      setRequestID(response.headers.get('X-Request-ID') ?? '')
      if (stream && (current === 'chat' || current === 'responses') && response.ok && response.body) {
        const reader = response.body.getReader()
        const decoder = new TextDecoder()
        let text = ''
        while (true) { const { done, value } = await reader.read(); if (done) break; text += decoder.decode(value, { stream: true }); setOutput(text) }
      } else if ((current === 'image' || current === 'video') && response.ok) {
        const text = await response.text()
        setOutput(pretty(text))
        let snapshot: TaskSnapshot | undefined
        try { snapshot = readTaskSnapshot(JSON.parse(text)) } catch { snapshot = undefined }
        if (!snapshot) { setError('未拿到任务 ID，无法查询结果'); return }
        setTask(snapshot)
        if (!snapshot.terminal) { polling = true; await pollTask(current, snapshot.id, abort.signal) }
      } else {
        setOutput(pretty(await response.text()))
      }
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') setError(polling ? '已停止等待；任务仍在后台运行，可到任务中心查看' : '请求已取消')
      else setError(err instanceof Error ? err.message : '请求失败')
    } finally {
      setElapsed(Math.round(performance.now() - started))
      setSending(false)
      setController(undefined)
    }
  }

  const pasteImages = (event: ClipboardEvent<HTMLTextAreaElement>) => {
    if (!withReferences) return
    const files = imageFilesFrom(event.clipboardData)
    if (files.length === 0) return
    event.preventDefault()
    void references.add(files)
  }
  const copyCurl = async () => {
    await navigator.clipboard.writeText(curl)
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1500)
  }
  const copyResponse = async () => {
    if (!output) return
    await navigator.clipboard.writeText(output)
    setResponseCopied(true)
    window.setTimeout(() => setResponseCopied(false), 1500)
  }

  const statusLabel = status === undefined ? (sending ? '请求中' : '待发送') : `${status}`
  const statusClass = status === undefined ? '' : status >= 400 ? 'bad' : 'ok'
  const waitingTask = sending && task && !task.terminal
  const resultURL = task?.ok ? task.url : undefined

  return <div className="page-stack">
    <PageHeader title="API 调试台" description="直接向本地服务发起请求，确认鉴权、模型与流式响应。" kicker="工具" actions={<span className="session-chip"><KeyRound size={12} />管理员会话</span>} />
    <div className="playground-grid">
      <Panel className="request-panel">
        <div className="section-head"><div><h2>请求</h2><p>{endpoints[endpoint].hint}</p></div><span className="secondary-cell mono">{endpoints[endpoint].path}</span></div>
        <div className="endpoint-switch" role="tablist" aria-label="接口类型">
          {(Object.keys(endpoints) as Endpoint[]).map(key => <button key={key} role="tab" aria-selected={endpoint === key} className={`endpoint-tab ${endpoint === key ? 'active' : ''}`} onClick={() => changeEndpoint(key)}><strong>{endpoints[key].label}</strong><span>POST</span></button>)}
        </div>
        <div className="playground-form">
          <Field label="模型">
            <Select value={model} onChange={event => setModel(event.target.value)}>
              {(data?.data ?? []).filter(item => endpoint === 'image' ? item.id.includes('image') : endpoint === 'video' ? item.id.includes('video') : item.id.includes('spark')).map(item => <option key={item.id} value={item.id}>{item.id} · {item.status}</option>)}
              {!data && <option value={model}>{model}</option>}
            </Select>
          </Field>
          <Field label={withReferences ? '提示词' : '用户输入'} hint="⌘ / Ctrl + Enter 发送">
            <textarea className="input textarea prompt-input" rows={withReferences ? 5 : 8} value={prompt} onChange={event => setPrompt(event.target.value)} onPaste={pasteImages} onKeyDown={event => { if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') { event.preventDefault(); void send() } }} spellCheck={false} />
          </Field>
          {withReferences && <ReferenceImagePicker images={references.images} error={references.error} onAdd={files => void references.add(files)} onRemove={references.remove} disabled={sending} />}
          {endpoint === 'video' && <p className="field-hint">官网单片段约 10 秒、720p；duration 不控制上游；比例只能靠提示词；长视频请分段生成，本项目不做拼接。</p>}
          {(endpoint === 'chat' || endpoint === 'responses') && <label className="switch-row"><strong>SSE 流式响应</strong><input type="checkbox" checked={stream} onChange={event => setStream(event.target.checked)} /></label>}
          <div className="request-actions">
            <Button onClick={send} disabled={sending || !prompt.trim()}><Play size={15} />{waitingTask ? '生成中…' : sending ? '请求中…' : '发送'}</Button>
            {sending && <Button variant="secondary" onClick={() => controller?.abort()}><Square size={14} />{waitingTask ? '停止等待' : '取消'}</Button>}
            <Button variant="ghost" onClick={() => void copyCurl()} title={references.images.length > 0 && withReferences ? 'cURL 中的参考图正文已省略，需要自行补全 base64' : undefined}>{copied ? <Check size={15} /> : <Clipboard size={15} />}{copied ? '已复制' : '复制 cURL'}</Button>
          </div>
          <div className="endpoint-preview"><span>POST</span><code>{endpoints[endpoint].path}</code><ChevronRight size={12} /></div>
        </div>
      </Panel>

      <section className="console-panel" aria-label="响应">
        <div className="console-head">
          <div className="console-lights" aria-hidden="true"><i /><i /><i /></div>
          <strong>响应</strong>
          <span className="spacer" />
          {sending && !task && <span className="console-status"><Zap size={10} />{withReferences ? '提交中' : '流式接收'}</span>}
          {task && <span className={`console-status ${task.terminal ? (task.ok ? 'ok' : 'bad') : ''}`}>{taskStatusText[task.status] ?? task.status}{!task.terminal && typeof task.progress === 'number' && task.progress > 0 ? ` ${task.progress}%` : ''}</span>}
          <span className={`console-status ${statusClass}`}>{statusLabel}</span>
        </div>
        <div className="console-meta">
          <div><span>状态</span><strong className={status && status >= 400 ? 'text-danger' : status ? 'text-success' : ''}>{status ?? '—'}</strong></div>
          <div><span>耗时</span><strong>{elapsed !== undefined ? `${elapsed} ms` : sending && task ? '等待中' : '—'}</strong></div>
          <div><span>{task ? '任务 ID' : '请求 ID'}</span><strong className="mono" title={(task?.id ?? requestID) || undefined}>{task?.id ?? (requestID || '—')}</strong></div>
        </div>
        {error && <div className="console-error"><InlineError>{error}</InlineError></div>}
        {waitingTask && <div className="console-progress" role="progressbar" aria-label="任务进度" aria-valuemin={0} aria-valuemax={100} aria-valuenow={task.progress ?? 0}><span style={{ width: `${Math.max(4, task.progress ?? 0)}%` }} /></div>}
        {resultURL && <div className="console-media">
          {taskKind === 'video' ? <video src={resultURL} controls playsInline aria-label="生成的视频" /> : <img src={resultURL} alt="生成的图片" />}
          <a className="button button-secondary button-sm" href={resultURL} download target="_blank" rel="noreferrer"><Download size={13} />下载</a>
        </div>}
        <pre ref={outputRef} className={`console-output ${!output ? 'is-empty' : ''} ${error && !task ? 'is-error' : ''}`} aria-live="polite">{output || (sending ? '等待响应…' : '响应将显示在这里')}</pre>
        <div className="console-foot"><span>{output ? `${output.length} 字符` : '—'}</span><Button size="sm" variant="ghost" disabled={!output} onClick={() => void copyResponse()}><Clipboard size={13} />{responseCopied ? '已复制' : '复制响应'}</Button></div>
      </section>
    </div>
  </div>
}
