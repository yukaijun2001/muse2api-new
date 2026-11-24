package hatch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"muse2api/internal/muse/protocol"
)

// Fake gateway observations. A previous test's gateway goroutine can still be
// running, so every access goes through gatewayMu.
var gatewayMu sync.Mutex

// gatewayImageItems holds the image attachments of the last chat.stream turn
// the fake gateway received. Tests in this package do not run in parallel.
var gatewayImageItems []map[string]any

// gatewayHistoryLimits records the limit of every chat.history request.
var gatewayHistoryLimits []string

// gatewayPaths records every RPC path the fake gateway received.
var gatewayPaths []string

// gatewayRejectSubscribe makes the fake gateway refuse chat.subscribe, which
// leaves turns on history polling alone.
var gatewayRejectSubscribe bool

// gatewayPushReply makes the fake gateway stream the reply to "hi" over
// chat.subscribe the way the live VM does: start, two appends, done.
var gatewayPushReply bool

func setGateway(change func()) {
	gatewayMu.Lock()
	defer gatewayMu.Unlock()
	change()
}

func gatewayNodeID(body []byte) (string, bool) {
	var request map[string]any
	if json.Unmarshal(body, &request) != nil {
		return "", false
	}
	node, ok := request["node_id"].(string)
	return node, ok
}

func startChatGateway(t *testing.T, silent bool) (*httptest.Server, string) {
	t.Helper()
	var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	turnSent := false
	chatPrompt := ""
	historyPolls := 0
	subscribeStream := int64(0)
	handler := func(ws *websocket.Conn) {
		defer ws.Close()
		responder := newNoiseXXResponder()
		_, m1, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if _, err := responder.ReadMessage1(m1); err != nil {
			return
		}
		m2, _ := responder.WriteMessage2(nil)
		_ = ws.WriteMessage(websocket.BinaryMessage, m2)
		_, m3, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if _, err := responder.ReadMessage3(m3); err != nil {
			return
		}
		tx, rx := responder.Split()
		reasm := newReassembler()
		for {
			_, raw, err := ws.ReadMessage()
			if err != nil {
				return
			}
			plain, err := rx.DecryptWithAD(nil, raw)
			if err != nil {
				return
			}
			payload, complete, err := reasm.feed(plain)
			if err != nil || !complete {
				continue
			}
			fields, _ := parseFields(payload)
			var frameBytes []byte
			for _, field := range fields {
				if field.num == 2 && field.wire == 2 {
					frameBytes = field.bytes
				}
			}
			frame, err := decodeServiceFrame(frameBytes)
			if err != nil || frame.Kind != "request" {
				continue
			}
			_, path, body := parseTestRequest(frame.Request)
			basePath := strings.SplitN(path, "?", 2)[0]
			gatewayMu.Lock()
			gatewayPaths = append(gatewayPaths, basePath)
			rejectSubscribe, pushReply := gatewayRejectSubscribe, gatewayPushReply
			gatewayMu.Unlock()
			switch basePath {
			case "/chat/subscribe":
				if rejectSubscribe {
					sendTestResponse(ws, tx, frame.StreamID, 404, []byte("missing"), true)
					continue
				}
				subscribeStream = frame.StreamID
				// Ack, then a replayed reply of an earlier turn that must not
				// leak into this one, plus a record carrying secrets for the
				// probe redaction check.
				sendTestResponse(ws, tx, frame.StreamID, 200, []byte(`{"type":"res","status":"ok","payload":{"subscribed":true},"auth_token":"probe-secret"}`+"\n"), false)
				sendTestBody(ws, tx, frame.StreamID, []byte(`{"event":"delta.message_start","seq":1,"payload":{"message_id":"old","message_seq":0,"reply_to_message_id":"","chat_context":{"device_id":"someone-else"},"url":"https://cdn.example/x.png?sig=probe-secret"}}`+"\n"), false)
				continue
			case "/chat/stream":
				var request map[string]any
				if err := json.Unmarshal(body, &request); err != nil {
					t.Errorf("decode chat.stream body: %v", err)
				} else {
					if _, ok := request["message"]; ok {
						t.Error("chat.stream body unexpectedly contains legacy message field")
					}
					if _, ok := request["session_id"]; ok {
						t.Error("main chat request unexpectedly contains session_id")
					}
					if _, ok := request["timezone"]; ok {
						t.Error("chat.stream body unexpectedly contains timezone")
					}
					if caps, ok := request["capabilities"].(map[string]any); !ok || len(caps) != 0 {
						t.Errorf("chat.stream capabilities = %#v, want empty object", request["capabilities"])
					}
					if items, ok := request["items"].([]any); ok && len(items) > 0 {
						if item, ok := items[len(items)-1].(map[string]any); ok {
							chatPrompt, _ = item["text"].(string)
						}
						gatewayMu.Lock()
						gatewayImageItems = gatewayImageItems[:0]
						for _, raw := range items[:len(items)-1] {
							if item, ok := raw.(map[string]any); ok && item["type"] == "image" {
								gatewayImageItems = append(gatewayImageItems, item)
							}
						}
						gatewayMu.Unlock()
					}
				}
				turnSent = true
				if pushReply && subscribeStream != 0 && chatPrompt == "hi" {
					node, _ := gatewayNodeID(body)
					records := []string{
						`{"event":"delta.message_start","seq":10,"payload":{"message_id":"other","message_seq":5,"reply_to_message_id":"","chat_context":{"device_id":"another-browser"}}}`,
						`{"event":"delta.message_start","seq":11,"payload":{"message_id":"m1","message_seq":5,"reply_to_message_id":"","chat_context":{"device_id":"` + node + `"}}}`,
						`{"event":"delta.text_append","seq":12,"payload":{"message_id":"other","text":"WRONG"}}`,
						`{"event":"delta.text_append","seq":13,"payload":{"message_id":"m1","seq":2,"text":"hel"}}`,
						`{"event":"delta.text_append","seq":14,"payload":{"message_id":"m1","seq":3,"text":"lo"}}`,
						`{"event":"delta.message_done","seq":15,"payload":{"message_id":"m1","status":"completed","transcript":{"messages":[{"content":[{"type":"text","text":"hello!"}]}]}}}`,
					}
					for _, record := range records {
						sendTestBody(ws, tx, subscribeStream, []byte(record+"\n"), false)
					}
				}
				continue
			case "/chat/history":
				if parsed, err := url.Parse(path); err == nil {
					gatewayMu.Lock()
					gatewayHistoryLimits = append(gatewayHistoryLimits, parsed.Query().Get("limit"))
					gatewayMu.Unlock()
				}
				history := `{"chat_events":[]}`
				if turnSent && (chatPrompt == "draw" || !silent) {
					historyPolls++
					assistant := `{"seq":2,"event_name":"message.assistant","payload":{"status":"completed","display_text":"hello"}}`
					if chatPrompt == "hi" && historyPolls == 1 {
						assistant = `{"seq":2,"event_name":"message.assistant","payload":{"status":"running","display_text":"hel"}}`
					} else if chatPrompt == "draw" {
						assistant = `{"seq":2,"event_name":"message.assistant","payload":{"status":"completed","kind":"image","path":"sandbox://gen.png","display_text":"done"}}`
					} else if chatPrompt == "hi" {
						assistant = `{"seq":2,"event_name":"message.assistant","payload":{"status":"completed","display_text":"hello"}}`
					}
					history = `{"chat_events":[{"seq":1,"event_name":"message.user","text":"` + chatPrompt + `"},` + assistant + `]}`
				}
				sendTestResponse(ws, tx, frame.StreamID, 200, []byte(history), true)
			default:
				if strings.HasPrefix(path, "/media/raw/") || strings.HasPrefix(path, "/fs/raw/") {
					sendTestResponse(ws, tx, frame.StreamID, 200, nil, false)
					for _, part := range []string{"abc", "def"} {
						sendTestBody(ws, tx, frame.StreamID, []byte(part), part == "def")
					}
					continue
				}
				sendTestResponse(ws, tx, frame.StreamID, 404, []byte("missing"), true)
			}
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/{$}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, gatewayTestPage(t, "RUNNING", "assigned"))
	})
	mux.HandleFunc("/v1/noise", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") == "" || r.Header.Get("User-Agent") != chromeUserAgent {
			t.Errorf("gateway handshake Origin=%q User-Agent=%q", r.Header.Get("Origin"), r.Header.Get("User-Agent"))
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		handler(ws)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/noise"
	return server, wsURL
}

func TestChatStreamIntegration(t *testing.T) {
	server, wsURL := startChatGateway(t, false)
	client := NewClientWithWSURL(mustURL(t, server.URL), server.Client(), wsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events, err := client.ChatStream(ctx, map[string]string{"hatch_sess": "s"}, "hi", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	text := ""
	done := false
	for delta := range events {
		if delta.Err != nil {
			t.Fatalf("stream error: %v", delta.Err)
		}
		text += delta.Text
		done = done || delta.Done
	}
	if text != "hello" || !done {
		t.Fatalf("text=%q done=%v", text, done)
	}
}

func TestChatStreamPollsShortTailQuickly(t *testing.T) {
	server, wsURL := startChatGateway(t, false)
	client := NewClientWithWSURL(mustURL(t, server.URL), server.Client(), wsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	setGateway(func() { gatewayHistoryLimits = nil; gatewayRejectSubscribe = true })
	defer setGateway(func() { gatewayRejectSubscribe = false })
	started := time.Now()
	events, err := client.ChatStream(ctx, map[string]string{"hatch_sess": "s"}, "hi", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var chunks []string
	for delta := range events {
		if delta.Err != nil {
			t.Fatalf("stream error: %v", delta.Err)
		}
		if delta.Text != "" {
			chunks = append(chunks, delta.Text)
		}
	}
	elapsed := time.Since(started)
	// The fake turn needs two polls (running "hel", then completed "hello").
	// The old 500 ms ticker took over a second; the tight loop needs ~0.4 s.
	if elapsed > 800*time.Millisecond {
		t.Fatalf("stream took %s", elapsed)
	}
	if strings.Join(chunks, "|") != "hel|lo" {
		t.Fatalf("chunks = %q", chunks)
	}
	// Connect reads the newest seq for the subscription cursor, the turn
	// reads its baseline, then the stream polls a short tail.
	gatewayMu.Lock()
	limits := append([]string(nil), gatewayHistoryLimits...)
	gatewayMu.Unlock()
	if len(limits) < 4 || limits[0] != "1" || limits[1] != "1" {
		t.Fatalf("history limits = %v", limits)
	}
	for _, limit := range limits[2:] {
		if limit != "24" {
			t.Fatalf("stream poll limit = %s, want short tail 24 (all: %v)", limit, limits)
		}
	}
}

func TestStreamPollerWidensWhenReplyMayBeOutOfWindow(t *testing.T) {
	poller := newStreamPoller(10)
	tail := make([]map[string]any, streamTailLimit)
	for i := range tail {
		tail[i] = map[string]any{"seq": float64(40 + i), "event_name": "tool.call"}
	}
	poller.observe(tail, nil)
	if poller.limit != streamWideLimit {
		t.Fatalf("limit = %d, want widened %d", poller.limit, streamWideLimit)
	}
	short := newStreamPoller(10)
	short.observe(tail[:5], nil)
	if short.limit != streamTailLimit {
		t.Fatalf("a short page must not widen the window: %d", short.limit)
	}
}

func TestChatStreamUsesPushDeltasAndProbeRecordsThem(t *testing.T) {
	// History stays silent for "hi": only the subscription can deliver.
	server, wsURL := startChatGateway(t, true)
	setGateway(func() { gatewayPushReply = true })
	defer setGateway(func() { gatewayPushReply = false })
	recorder := NewProbeRecorder()
	recorder.Enable(time.Minute)
	client := NewClientWithWSURL(mustURL(t, server.URL), server.Client(), wsURL)
	client.SetStreamProbe(recorder)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	events, err := client.ChatStream(ctx, map[string]string{"hatch_sess": "s"}, "hi", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var chunks []string
	done := false
	for delta := range events {
		if delta.Err != nil {
			t.Fatalf("stream error: %v", delta.Err)
		}
		if delta.Text != "" {
			chunks = append(chunks, delta.Text)
		}
		done = done || delta.Done
	}
	// Appends arrive one by one; message_done's transcript fills the tail.
	// The replayed old turn and another browser's reply must not leak in.
	if strings.Join(chunks, "|") != "hel|lo|!" || !done {
		t.Fatalf("chunks = %q done=%v", chunks, done)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("push reply took %s; it must not wait for a history poll", elapsed)
	}
	raw, _ := json.Marshal(recorder.Entries())
	out := string(raw)
	for _, want := range []string{`"event":"subscribed"`, `"event":"record"`, `"event":"turn"`, `"event":"push_delta"`, "?REDACTED", `"after_stream_seq"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("probe entries missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "probe-secret") {
		t.Fatalf("probe entries leaked a secret:\n%s", out)
	}
}

func TestReplyTextNeverRepeatsSentText(t *testing.T) {
	reply := &replyText{}
	if got := reply.append("\n你好"); got != "\n你好" {
		t.Fatalf("append = %q", got)
	}
	// History trims whitespace; the snapshot still continues the sent text.
	if got := reply.extend("你好，世界"); got != "，世界" {
		t.Fatalf("extend trimmed snapshot = %q", got)
	}
	if got := reply.extend("完全不同的文本"); got != "" {
		t.Fatalf("a diverging snapshot must not resend text: %q", got)
	}
	if reply.text != "\n你好，世界" {
		t.Fatalf("sent text = %q", reply.text)
	}
}

func TestPushTurnIgnoresFailedMessage(t *testing.T) {
	turn := &pushTurn{nodeID: "n", baseline: 3}
	turn.apply(map[string]any{"event": "delta.message_start", "payload": map[string]any{"message_id": "m", "message_seq": float64(4), "chat_context": map[string]any{"device_id": "n"}}})
	turn.apply(map[string]any{"event": "delta.text_append", "payload": map[string]any{"message_id": "m", "text": "part"}})
	turn.apply(map[string]any{"event": "delta.message_done", "payload": map[string]any{"message_id": "m", "status": "failed"}})
	if turn.done || turn.messageID != "" {
		t.Fatalf("a failed message must hand the turn back to polling: %+v", turn)
	}
}

func TestProbeRecorderExpires(t *testing.T) {
	recorder := NewProbeRecorder()
	recorder.Enable(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if recorder.active() || recorder.Status().Enabled {
		t.Fatal("probe must switch itself off after its ttl")
	}
}

func TestChatStreamTimeoutIsAnError(t *testing.T) {
	server, wsURL := startChatGateway(t, true)
	client := NewClientWithWSURL(mustURL(t, server.URL), server.Client(), wsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events, err := client.ChatStream(ctx, map[string]string{"hatch_sess": "s"}, "hi", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var streamErr error
	for delta := range events {
		if delta.Err != nil {
			streamErr = delta.Err
		}
	}
	if !errors.Is(streamErr, protocol.ErrUpstreamTimeout) {
		t.Fatalf("timeout stream error = %v", streamErr)
	}
}

func TestChatWithItemsHistoryAndMedia(t *testing.T) {
	server, wsURL := startChatGateway(t, true)
	client := NewClientWithWSURL(mustURL(t, server.URL), server.Client(), wsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	text, media, err := client.ChatWithItems(ctx, map[string]string{"hatch_sess": "s"}, "draw", nil, true, 4*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if text != "done" || len(media) != 1 || media[0].Path != "gen.png" {
		t.Fatalf("text=%q media=%+v", text, media)
	}
}

func TestChatWithItemsSendsReferenceImagesBeforePrompt(t *testing.T) {
	server, wsURL := startChatGateway(t, true)
	client := NewClientWithWSURL(mustURL(t, server.URL), server.Client(), wsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	items, err := referenceItems([]string{
		"data:image/png;base64,iVBORw0KGgo=",
		"data:image/jpeg;base64,/9j/4AAQ",
	}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.ChatWithItems(ctx, map[string]string{"hatch_sess": "s"}, "draw", items, true, 4*time.Second); err != nil {
		t.Fatal(err)
	}
	gatewayMu.Lock()
	images := append([]map[string]any(nil), gatewayImageItems...)
	gatewayMu.Unlock()
	if len(images) != 2 {
		t.Fatalf("gateway image items = %d, want 2", len(images))
	}
	first, second := images[0], images[1]
	if first["mime_type"] != "image/png" || first["data_base64"] != "iVBORw0KGgo=" || first["filename"] != "reference.png" {
		t.Fatalf("first item = %#v", first)
	}
	if second["mime_type"] != "image/jpeg" || second["filename"] != "reference.jpg" {
		t.Fatalf("second item = %#v", second)
	}
}

func TestDownloadToFileStreamsGatewayBody(t *testing.T) {
	server, wsURL := startChatGateway(t, true)
	client := NewClientWithWSURL(mustURL(t, server.URL), server.Client(), wsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ref := MediaRef{Kind: "image", Path: "sandbox://x.png"}
	path, _, err := client.DownloadToFile(ctx, map[string]string{"hatch_sess": "s"}, ref, t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "abcdef" {
		t.Fatalf("downloaded %q err=%v", data, err)
	}
	_ = os.Remove(path)
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
