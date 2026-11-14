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
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"muse2api/internal/muse/protocol"
)

// gatewayImageItems holds the image attachments of the last chat.stream turn
// the fake gateway received. Tests in this package do not run in parallel.
var gatewayImageItems []map[string]any

func startChatGateway(t *testing.T, silent bool) (*httptest.Server, string) {
	t.Helper()
	var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	turnSent := false
	chatPrompt := ""
	historyPolls := 0
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
			switch basePath {
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
						gatewayImageItems = gatewayImageItems[:0]
						for _, raw := range items[:len(items)-1] {
							if item, ok := raw.(map[string]any); ok && item["type"] == "image" {
								gatewayImageItems = append(gatewayImageItems, item)
							}
						}
					}
				}
				turnSent = true
				continue
			case "/chat/history":
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
	if len(gatewayImageItems) != 2 {
		t.Fatalf("gateway image items = %d, want 2", len(gatewayImageItems))
	}
	first, second := gatewayImageItems[0], gatewayImageItems[1]
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
