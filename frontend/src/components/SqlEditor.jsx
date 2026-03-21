import { useEffect, useRef, useCallback } from "react"
import { EditorView, basicSetup } from "codemirror"
import { sql, PostgreSQL } from "@codemirror/lang-sql"
import { lintGutter, setDiagnostics } from "@codemirror/lint"
import { oneDark } from "@codemirror/theme-one-dark"

const VALIDATE_DEBOUNCE_MS = 600

const theme = EditorView.theme({
  ".cm-scroller": { fontFamily: "monospace", fontSize: "13px" },
  ".cm-content": { minHeight: "120px" },
})

export default function SqlEditor({ value, onChange, fileStore = "tables", viewRef: externalRef }) {
  const containerRef = useRef(null)
  const internalRef = useRef(null)
  const viewRef = externalRef ?? internalRef
  const debounceRef = useRef(null)

  const validate = useCallback(async (view, sqlText) => {
    if (!sqlText.trim()) {
      view.dispatch(setDiagnostics(view.state, []))
      return
    }
    try {
      const res = await fetch("http://localhost:8000/validate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql: sqlText, file_store: fileStore }),
      })
      const data = await res.json()
      view.dispatch(setDiagnostics(view.state, data.valid ? [] : [{
        from: 0,
        to: view.state.doc.length,
        severity: "error",
        message: data.error,
      }]))
    } catch {
    }
  }, [fileStore])

  useEffect(() => {
    if (!containerRef.current) return
    viewRef.current = new EditorView({
      doc: value ?? "",
      extensions: [
        basicSetup,
        oneDark,
        sql({ dialect: PostgreSQL }),
        lintGutter(),
        theme,
        EditorView.updateListener.of((update) => {
          if (!update.docChanged) return
          const text = update.state.doc.toString()
          onChange?.(text)
          clearTimeout(debounceRef.current)
          debounceRef.current = setTimeout(() => validate(update.view, text), VALIDATE_DEBOUNCE_MS)
        }),
      ],
      parent: containerRef.current,
    })
    return () => {
      viewRef.current?.destroy()
      clearTimeout(debounceRef.current)
    }
  }, [])

  return <div ref={containerRef} />
}