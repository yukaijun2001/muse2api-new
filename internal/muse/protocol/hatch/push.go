package hatch

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Push streaming. The web client keeps one chat.subscribe stream open per
// gateway connection and renders delta.* events as they arrive. chat.history
// only exposes an assistant message once it is complete, so polling alone
// delivers a whole reply in one piece, seconds after the model finished.
//
// Wire format (captured with the stream probe, 2026-11):
//
//	{"type":"res","status":"ok","payload":{"subscribed":true,...}}      ack
//	{"event":"message.user","seq":70,"ts_ms":...,"payload":{...}}      replayed history
//	{"event":"delta.message_start","seq":259,"payload":{"message_id":"assistant-msg-…",
//	    "message_seq":247,"reply_to_message_id":"","chat_context":{"device_id":"<node_id>"}}}
//	{"event":"delta.text_append","payload":{"message_id":"assistant-msg-…","seq":2,"text":"我是Muse，你"}}
//	{"event":"delta.message_done","payload":{"message_id":"assistant-msg-…","status":"completed",
//	    "transcript":{"messages":[{"content":[{"type":"text","text":"<full reply>"}]}]}}}
//
// device_id is the node_id sent with chat.stream, which is how a turn finds
// its own reply among everything else on the account.
var pushCapabilities = []string{"chat_cancel", "delta_stream", "custom_reactions", "custom_reactions_facebook_thumbs_up_v1"}

const pushListenerBuffer = 1024

// pushFeed fans the connection's subscription out to the current turn.
type pushFeed struct {
	mu       sync.Mutex
	listener *pushListener
	closed   bool
	probe    *ProbeRecorder
	turn     int64
}

type pushListener struct {
	ch       chan map[string]any
	overflow atomic.Bool
}

// startPush opens chat.subscribe on a fresh connection. A failure only
// disables push for this connection; turns fall back to polling.
func (c *Client) startPush(ctx context.Context, conn *Conn) *pushFeed {
	// Without a cursor the VM replays recent history first, and large old
	// messages delayed live events by ~11 s in the capture. Start after the
	// newest event: chat and stream events share one sequence.
	var latest int64
	if result, err := conn.Request(ctx, http.MethodGet, "/chat/history", map[string]any{"limit": 1}, 10*time.Second); err == nil {
		var payload map[string]any
		if result.JSON(&payload) == nil {
			latest = maxEventSeq(historyEvents(payload))
		}
	}
	params := map[string]any{"after_stream_seq": latest, "after_chat_event_seq": latest, "capabilities": pushCapabilities}
	sub, err := conn.OpenStream(ctx, "/chat/subscribe", params)
	feed := &pushFeed{probe: c.probe}
	if err != nil {
		feed.closed = true
		feed.log("subscribe_failed", map[string]any{"error": err.Error()})
		return feed
	}
	feed.log("subscribed", map[string]any{"params": params})
	go feed.dispatch(sub)
	return feed
}

func (f *pushFeed) dispatch(sub *StreamHandle) {
	defer sub.Close()
	for record := range sub.Events() {
		f.mu.Lock()
		listener, probe, turn := f.listener, f.probe, f.turn
		f.mu.Unlock()
		if probe.active() {
			probe.add(ProbeEntry{Time: time.Now().Format(time.RFC3339Nano), Turn: turn, Event: "record", Fields: map[string]any{"record": probeValue(record)}})
		}
		if listener == nil {
			continue
		}
		select {
		case listener.ch <- record:
		default:
			// Never block the connection's read loop; this turn falls back
			// to history polling for the rest of its text.
			listener.overflow.Store(true)
		}
	}
	fields := map[string]any{}
	if err := sub.Err(); err != nil {
		fields["error"] = err.Error()
	}
	f.log("stream_closed", fields)
	f.mu.Lock()
	f.closed = true
	if f.listener != nil {
		close(f.listener.ch)
		f.listener = nil
	}
	f.mu.Unlock()
}

// attach routes live records to one turn until detach is called. It returns
// nil when the subscription is gone.
func (f *pushFeed) attach(turn int64) (*pushListener, func()) {
	if f == nil {
		return nil, func() {}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, func() {}
	}
	listener := &pushListener{ch: make(chan map[string]any, pushListenerBuffer)}
	f.listener, f.turn = listener, turn
	return listener, func() {
		f.mu.Lock()
		if f.listener == listener {
			f.listener, f.turn = nil, 0
		}
		f.mu.Unlock()
	}
}

func (f *pushFeed) log(event string, fields map[string]any) {
	if f == nil || !f.probe.active() {
		return
	}
	f.probe.add(ProbeEntry{Time: time.Now().Format(time.RFC3339Nano), Event: event, Fields: fields})
}

// pushTurn assembles one turn's reply from subscription records.
type pushTurn struct {
	nodeID    string
	baseline  int64
	messageID string
	text      strings.Builder
	final     string
	done      bool
}

// apply consumes one record and returns newly appended text.
func (t *pushTurn) apply(record map[string]any) string {
	payload, _ := record["payload"].(map[string]any)
	if payload == nil || t.done {
		return ""
	}
	switch asString(record["event"]) {
	case "delta.message_start":
		if t.messageID != "" || asString(payload["reply_to_message_id"]) != "" {
			return ""
		}
		if seq, ok := payload["message_seq"].(float64); ok && int64(seq) <= t.baseline {
			return "" // replayed from before this turn
		}
		chatContext, _ := payload["chat_context"].(map[string]any)
		if device := asString(chatContext["device_id"]); device != "" && device != t.nodeID {
			return "" // another client's turn on the same account
		}
		t.messageID = asString(payload["message_id"])
	case "delta.text_append":
		if t.messageID == "" || asString(payload["message_id"]) != t.messageID {
			return ""
		}
		text := asString(payload["text"])
		t.text.WriteString(text)
		return text
	case "delta.message_done":
		if t.messageID == "" || asString(payload["message_id"]) != t.messageID {
			return ""
		}
		if !strings.EqualFold(asString(payload["status"]), "completed") {
			// Failed or cancelled: let history polling report the outcome.
			t.messageID = ""
			t.text.Reset()
			return ""
		}
		t.done = true
		t.final = transcriptText(payload)
	}
	return ""
}

// transcriptText is the full reply carried by delta.message_done.
func transcriptText(payload map[string]any) string {
	transcript, _ := payload["transcript"].(map[string]any)
	messages, _ := transcript["messages"].([]any)
	var b strings.Builder
	for _, raw := range messages {
		message, _ := raw.(map[string]any)
		content, _ := message["content"].([]any)
		for _, rawPart := range content {
			part, _ := rawPart.(map[string]any)
			if asString(part["type"]) == "text" {
				b.WriteString(asString(part["text"]))
			}
		}
	}
	return b.String()
}
