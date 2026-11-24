package hatch

import (
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"time"
)

// The stream probe records what chat.subscribe delivers and which path
// (push or history polling) delivered each piece of a reply, so streaming
// regressions can be diagnosed against the live wire format.
//
// It is switched on from the admin settings page, expires on its own and
// keeps its records in memory only: they contain message text, so they are
// never written to the service log and vanish on restart.
const (
	probeMaxEntries  = 3000
	probeRecordBytes = 1200
	ProbeDefaultTTL  = 30 * time.Minute
	ProbeMaxTTL      = 2 * time.Hour
)

// ProbeEntry is one line of the downloadable probe log.
type ProbeEntry struct {
	Time   string         `json:"time"`
	Turn   int64          `json:"turn"`
	MS     int64          `json:"ms"`
	Event  string         `json:"event"`
	Fields map[string]any `json:"fields,omitempty"`
}

// ProbeStatus summarizes the recorder for the admin UI.
type ProbeStatus struct {
	Enabled bool  `json:"enabled"`
	Until   int64 `json:"until,omitempty"`
	Entries int   `json:"entries"`
	Turns   int64 `json:"turns"`
}

// ProbeRecorder is the shared, runtime-switchable probe sink.
type ProbeRecorder struct {
	mu      sync.Mutex
	until   time.Time
	entries []ProbeEntry
	turns   int64
}

func NewProbeRecorder() *ProbeRecorder { return &ProbeRecorder{} }

// Enable starts recording for ttl (capped) and clears earlier records.
func (r *ProbeRecorder) Enable(ttl time.Duration) {
	if ttl <= 0 {
		ttl = ProbeDefaultTTL
	}
	if ttl > ProbeMaxTTL {
		ttl = ProbeMaxTTL
	}
	r.mu.Lock()
	r.until = time.Now().Add(ttl)
	r.entries = nil
	r.turns = 0
	r.mu.Unlock()
}

// Disable stops recording; collected records stay downloadable.
func (r *ProbeRecorder) Disable() {
	r.mu.Lock()
	r.until = time.Time{}
	r.mu.Unlock()
}

// Clear drops collected records.
func (r *ProbeRecorder) Clear() {
	r.mu.Lock()
	r.entries = nil
	r.turns = 0
	r.mu.Unlock()
}

func (r *ProbeRecorder) active() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.until.IsZero() && time.Now().Before(r.until)
}

func (r *ProbeRecorder) Status() ProbeStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	status := ProbeStatus{Entries: len(r.entries), Turns: r.turns}
	if !r.until.IsZero() && time.Now().Before(r.until) {
		status.Enabled = true
		status.Until = r.until.Unix()
	}
	return status
}

// Entries returns a copy of the collected records.
func (r *ProbeRecorder) Entries() []ProbeEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ProbeEntry(nil), r.entries...)
}

func (r *ProbeRecorder) newTurn() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.turns++
	return r.turns
}

func (r *ProbeRecorder) add(entry ProbeEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) >= probeMaxEntries {
		return
	}
	r.entries = append(r.entries, entry)
}

// SetStreamProbe attaches the shared recorder to this client.
func (c *Client) SetStreamProbe(recorder *ProbeRecorder) { c.probe = recorder }

type streamProbe struct {
	recorder *ProbeRecorder
	turn     int64
	started  time.Time
}

func (p *streamProbe) log(event string, fields map[string]any) {
	if p == nil {
		return
	}
	p.recorder.add(ProbeEntry{Time: time.Now().Format(time.RFC3339Nano), Turn: p.turn, MS: time.Since(p.started).Milliseconds(), Event: event, Fields: fields})
}

// startStreamProbe opens a probe turn. Subscription records themselves are
// logged by the connection's push feed while the turn is attached.
func (c *Client) startStreamProbe(baseline int64, push bool) *streamProbe {
	if !c.probe.active() {
		return nil
	}
	p := &streamProbe{recorder: c.probe, turn: c.probe.newTurn(), started: time.Now()}
	p.log("turn", map[string]any{"node_id": c.nodeID, "baseline_seq": baseline, "push": push})
	return p
}

func (p *streamProbe) turnID() int64 {
	if p == nil {
		return 0
	}
	return p.turn
}

// emitted records which path delivered text, for timing comparison.
func (p *streamProbe) emitted(source string, deltaLen, totalLen int, finished bool) {
	p.log(source+"_delta", map[string]any{"delta_chars": deltaLen, "total_chars": totalLen, "finished": finished})
}

// probeValue masks secrets and signed URL queries; oversized values are
// replaced by a truncated JSON string.
func probeValue(value any) any {
	redacted := redactProbe(value, "")
	raw, err := json.Marshal(redacted)
	if err != nil {
		return "<unencodable>"
	}
	if len(raw) > probeRecordBytes {
		return truncate(raw, probeRecordBytes)
	}
	return redacted
}

func probeBody(body []byte) any {
	var decoded any
	if json.Unmarshal(body, &decoded) == nil {
		return probeValue(decoded)
	}
	return truncate(body, 400)
}

func redactProbe(value any, key string) any {
	lower := strings.ToLower(key)
	for _, secret := range []string{"token", "secret", "cookie", "authorization", "password", "signature", "data_base64"} {
		if strings.Contains(lower, secret) {
			return "***"
		}
	}
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = redactProbe(item, k)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = redactProbe(item, key)
		}
		return out
	case string:
		if strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://") {
			if u, err := url.Parse(v); err == nil && u.RawQuery != "" {
				u.RawQuery = "REDACTED"
				return u.String()
			}
		}
		return v
	}
	return value
}
