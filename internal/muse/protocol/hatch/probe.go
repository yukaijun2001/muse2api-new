package hatch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// The stream probe records what chat.subscribe delivers during a real turn,
// so push streaming can be rebuilt against the current wire format instead of
// guessed. It only observes: replies still come from chat.history polling.
//
// It is switched on from the admin settings page, expires on its own and
// keeps its records in memory only: they contain message text, so they are
// never written to the service log and vanish on restart.
const (
	probeMaxTurnRecords = 400
	probeMaxEntries     = 3000
	probeRecordBytes    = 1200
	probeGrace          = 3 * time.Second
	ProbeDefaultTTL     = 30 * time.Minute
	ProbeMaxTTL         = 2 * time.Hour
)

// probeCapabilities is what the web client declared when chat.subscribe was
// last known to work.
var probeCapabilities = []string{"chat_cancel", "delta_stream", "custom_reactions", "custom_reactions_facebook_thumbs_up_v1"}

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
	done     chan struct{}
	once     sync.Once
}

func (p *streamProbe) log(event string, fields map[string]any) {
	p.recorder.add(ProbeEntry{Time: time.Now().Format(time.RFC3339Nano), Turn: p.turn, MS: time.Since(p.started).Milliseconds(), Event: event, Fields: fields})
}

// startStreamProbe registers the node the way the web client does and opens
// chat.subscribe before the turn is sent. It never fails the turn.
func (c *Client) startStreamProbe(ctx context.Context, conn *Conn, baseline int64) *streamProbe {
	if !c.probe.active() {
		return nil
	}
	p := &streamProbe{recorder: c.probe, turn: c.probe.newTurn(), started: time.Now(), done: make(chan struct{})}
	p.log("turn", map[string]any{"node_id": c.nodeID, "baseline_seq": baseline})
	p.request(ctx, conn, "/api/nodes/register", map[string]any{
		"node_id":      c.nodeID,
		"display_name": "Web",
		"platform":     "web",
		"commands_v2":  map[string]any{"ping": map[string]any{"description": "Connection liveness check"}},
	})
	p.request(ctx, conn, "/client/register-capabilities", map[string]any{
		"client_id":    c.nodeID,
		"platform":     "web",
		"display_name": "Muse Web",
		"version":      "0.0.0",
		"capabilities": map[string]any{
			"data_sources":       map[string]any{},
			"device_commands":    map[string]any{},
			"hatch_app_commands": map[string]any{},
			"rendering": map[string]any{
				"supported_presentations":        []string{"text_with_button", "generic_list"},
				"supported_inline_presentations": []string{"option"},
				"supported_text_entities":        []string{},
			},
		},
	})
	params := map[string]any{"after_stream_seq": 0, "after_chat_event_seq": baseline, "capabilities": probeCapabilities}
	sub, err := conn.OpenStream(ctx, "/chat/subscribe", params)
	if err != nil {
		p.log("subscribe_failed", map[string]any{"error": err.Error()})
		return p
	}
	p.log("subscribed", map[string]any{"params": params})
	// Outlive the HTTP request a little so trailing events are captured.
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Minute)
	go func() {
		defer cancel()
		p.read(readCtx, sub)
	}()
	return p
}

func (p *streamProbe) request(ctx context.Context, conn *Conn, path string, body map[string]any) {
	started := time.Now()
	result, err := conn.Request(ctx, http.MethodPost, path, body, 10*time.Second)
	fields := map[string]any{"path": path, "rpc_ms": time.Since(started).Milliseconds()}
	if err != nil {
		fields["error"] = err.Error()
	} else {
		fields["status"] = result.Status
		fields["body"] = probeBody(result.Body)
	}
	p.log("register", fields)
}

func (p *streamProbe) read(ctx context.Context, sub *StreamHandle) {
	defer sub.Close()
	count := 0
	done := p.done
	var grace <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			p.log("stopped", map[string]any{"records": count, "reason": "context done"})
			return
		case <-done:
			// Keep listening briefly: trailing events often land after the
			// poller has seen the completed message.
			done = nil
			grace = time.After(probeGrace)
		case <-grace:
			p.log("stopped", map[string]any{"records": count, "reason": "turn finished"})
			return
		case record, ok := <-sub.Events():
			if !ok {
				fields := map[string]any{"records": count}
				if err := sub.Err(); err != nil {
					fields["error"] = err.Error()
				}
				p.log("stream_closed", fields)
				return
			}
			count++
			p.log("record", map[string]any{"n": count, "record": probeValue(record)})
			if count >= probeMaxTurnRecords {
				p.log("stopped", map[string]any{"records": count, "reason": "record cap"})
				return
			}
		}
	}
}

// polled records when the history poller emitted text, for timing comparison.
func (p *streamProbe) polled(deltaLen, totalLen int, finished bool) {
	if p == nil {
		return
	}
	p.log("poll_delta", map[string]any{"delta_chars": deltaLen, "total_chars": totalLen, "finished": finished})
}

func (p *streamProbe) finish() {
	if p == nil {
		return
	}
	p.once.Do(func() { close(p.done) })
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
