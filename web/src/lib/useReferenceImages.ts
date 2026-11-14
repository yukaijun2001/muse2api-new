import { useCallback, useRef, useState } from 'react'
import { REF_MAX_IMAGES, checkReferenceFiles, readAsDataURL, type ReferenceImage } from './referenceImages'

let nextId = 0

export function useReferenceImages() {
  const [images, setImages] = useState<ReferenceImage[]>([])
  const [error, setError] = useState('')
  const imagesRef = useRef(images)
  imagesRef.current = images

  const add = useCallback(async (files: File[]) => {
    if (files.length === 0) return
    const { accepted, error: limitError } = checkReferenceFiles(imagesRef.current, files)
    setError(limitError ?? '')
    try {
      const read = await Promise.all(accepted.map(async file => ({ id: `ref-${++nextId}`, name: file.name || 'pasted-image', size: file.size, type: file.type, dataUrl: await readAsDataURL(file) })))
      // Two quick drops can both pass the check above; cap the merged list.
      setImages(current => [...current, ...read].slice(0, REF_MAX_IMAGES))
    } catch (err) {
      setError(err instanceof Error ? err.message : '图片读取失败')
    }
  }, [])
  const remove = useCallback((id: string) => { setImages(current => current.filter(image => image.id !== id)); setError('') }, [])
  const clear = useCallback(() => { setImages([]); setError('') }, [])
  return { images, error, add, remove, clear }
}
