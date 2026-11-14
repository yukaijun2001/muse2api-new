import { useRef, useState, type DragEvent } from 'react'
import { ImagePlus, X } from 'lucide-react'
import { REF_ACCEPT, REF_MAX_IMAGES, REF_MAX_TOTAL_BYTES, formatBytes, imageFilesFrom, type ReferenceImage } from '../lib/referenceImages'

export function ReferenceImagePicker({ images, error, onAdd, onRemove, disabled }: { images: ReferenceImage[]; error: string; onAdd: (files: File[]) => void; onRemove: (id: string) => void; disabled?: boolean }) {
  const input = useRef<HTMLInputElement>(null)
  const [dragging, setDragging] = useState(false)
  const total = images.reduce((sum, image) => sum + image.size, 0)
  const full = images.length >= REF_MAX_IMAGES
  const drop = (event: DragEvent<HTMLElement>) => {
    event.preventDefault()
    setDragging(false)
    if (!disabled) onAdd(imageFilesFrom(event.dataTransfer))
  }

  return <div className="reference-picker">
    <div className="reference-picker-head">
      <span className="field-label">参考图（可选）</span>
      <span className="field-hint">{images.length}/{REF_MAX_IMAGES} 张 · {formatBytes(total)} / {formatBytes(REF_MAX_TOTAL_BYTES)}</span>
    </div>
    <div
      className={`reference-grid ${dragging ? 'dragging' : ''}`}
      onDragOver={event => { event.preventDefault(); if (!disabled) setDragging(true) }}
      onDragLeave={() => setDragging(false)}
      onDrop={drop}
    >
      {images.map((image, index) => <figure key={image.id} className="reference-thumb">
        <img src={image.dataUrl} alt={`参考图 ${index + 1}：${image.name}`} />
        <span className="reference-index">{index + 1}</span>
        <figcaption title={image.name}>{formatBytes(image.size)}</figcaption>
        <button type="button" className="reference-remove" onClick={() => onRemove(image.id)} disabled={disabled} aria-label={`移除参考图 ${index + 1}`}><X size={12} /></button>
      </figure>)}
      {!full && <button type="button" className="reference-add" onClick={() => input.current?.click()} disabled={disabled}>
        <ImagePlus size={18} />
        <span>{images.length === 0 ? '点击、拖入或在提示词里粘贴图片' : '继续添加'}</span>
      </button>}
    </div>
    <input
      ref={input}
      type="file"
      accept={REF_ACCEPT.join(',')}
      multiple
      hidden
      data-testid="reference-input"
      onChange={event => { onAdd(Array.from(event.target.files ?? [])); event.target.value = '' }}
    />
    {error ? <span className="field-error" role="alert">{error}</span> : <span className="field-hint">按顺序随提示词发送；提示词里可用“第 1 张图”指代。PNG / JPEG / WebP / GIF，单张 ≤ 10 MB。</span>}
  </div>
}
