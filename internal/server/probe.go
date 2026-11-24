package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"muse2api/internal/apierr"
	"muse2api/internal/model"
	"muse2api/internal/muse/protocol/hatch"
)

// streamProber is implemented by drivers that can record chat.subscribe
// traffic for debugging streaming.
type streamProber interface {
	StreamProbe() *hatch.ProbeRecorder
}

func (s *Server) probeRecorder(w http.ResponseWriter, r *http.Request) *hatch.ProbeRecorder {
	prober, ok := s.driver.(streamProber)
	if !ok || prober.StreamProbe() == nil {
		apierr.Write(w, r, apierr.New(http.StatusNotFound, "capability_unavailable", "当前驱动不支持流式探测"))
		return nil
	}
	return prober.StreamProbe()
}

func (s *Server) streamProbeStatus(w http.ResponseWriter, r *http.Request) {
	if recorder := s.probeRecorder(w, r); recorder != nil {
		writeJSON(w, 200, recorder.Status())
	}
}

func (s *Server) setStreamProbe(w http.ResponseWriter, r *http.Request) {
	recorder := s.probeRecorder(w, r)
	if recorder == nil {
		return
	}
	var in struct {
		Enabled bool `json:"enabled"`
		Minutes int  `json:"minutes"`
	}
	if err := decodeJSON(w, r, &in); err != nil {
		apierr.Write(w, r, err)
		return
	}
	if in.Enabled {
		ttl := time.Duration(in.Minutes) * time.Minute
		if in.Minutes < 0 || ttl > hatch.ProbeMaxTTL {
			apierr.Write(w, r, apierr.BadRequest(fmt.Sprintf("探测时长须为 1..%d 分钟", int(hatch.ProbeMaxTTL.Minutes()))))
			return
		}
		recorder.Enable(ttl)
		s.store.AddEvent(model.Event{Level: "warning", Type: "debug.stream_probe", Message: "流式探测已开启（记录含对话文本，仅保存在内存）", RequestID: requestIDFrom(r.Context())})
	} else {
		recorder.Disable()
		s.store.AddEvent(model.Event{Level: "info", Type: "debug.stream_probe", Message: "流式探测已关闭", RequestID: requestIDFrom(r.Context())})
	}
	writeJSON(w, 200, recorder.Status())
}

// streamProbeEntries downloads the records as JSON Lines.
func (s *Server) streamProbeEntries(w http.ResponseWriter, r *http.Request) {
	recorder := s.probeRecorder(w, r)
	if recorder == nil {
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="stream-probe-%s.jsonl"`, time.Now().Format("20060102-150405")))
	w.Header().Set("Cache-Control", "no-store")
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	for _, entry := range recorder.Entries() {
		_ = encoder.Encode(entry)
	}
}

func (s *Server) clearStreamProbe(w http.ResponseWriter, r *http.Request) {
	if recorder := s.probeRecorder(w, r); recorder != nil {
		recorder.Clear()
		writeJSON(w, 200, recorder.Status())
	}
}
