package hatch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"muse2api/internal/muse/protocol"
	"muse2api/internal/refimage"
)

const (
	textSettle      = 2 * time.Second
	mediaSettle     = 4 * time.Second
	noMediaSettle   = 15 * time.Second
	pollInterval    = 500 * time.Millisecond
	chromeUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36"
)

// MediaRef is one generated artifact exposed by the upstream turn.
type MediaRef struct {
	Kind  string // image | video
	Path  string // normalized sandbox path (no sandbox:// prefix)
	URL   string // optional signed URL
	Mime  string
	Label string
}

func (m MediaRef) identity() string {
	return m.Kind + "|" + m.Path + "|" + m.URL
}

var sandboxPattern = regexp.MustCompile(`sandbox://[^\s)"']+`)

// refusalMarkers is a compact subset of the upstream refusal phrases; the goal
// is early failure instead of waiting out the whole media budget.
var refusalMarkers = []string{
	"image tool is currently unavailable",
	"video tool is currently unavailable",
	"tool is currently unavailable",
	"no image was created",
	"no video was created",
	"could not generate",
	"couldn't generate",
	"cannot generate",
	"can't generate",
	"unable to generate",
	"failed to generate",
	"generation failed",
	"content policy",
	"safety guidelines",
	"against our policies",
	"not available in your",
	"not available for your account",
	"out of credits",
	"limit reached",
	"没生成成",
	"没生成成功",
	"未生成成功",
	"生成失败",
	"无法生成",
}

// Client owns one gateway connection plus chat session state. Turns are
// serialized because the upstream VM handles one conversation at a time, and
// every turn opens its own thread so concurrent API requests cannot interleave.
type Client struct {
	mu         sync.Mutex
	chatMu     sync.Mutex
	conn       *Conn
	session    *Session
	accountID  string
	site       *url.URL
	wsURL      string
	dialer     *websocket.Dialer
	httpClient *http.Client
	nodeID     string
	// proxy is the account egress proxy this client's transport was built for.
	proxy string

	// quotaActionID caches the Next.js server-action id that returns the
	// subscription snapshot. The id is build-specific, so it is re-discovered
	// whenever the upstream deployment changes. quotaGate serializes refreshes
	// per account but remains cancellable when the caller gives up.
	quotaGate     chan struct{}
	quotaMu       sync.Mutex
	quotaActionID string
	quotaActionAt time.Time

	// connKey fingerprints the session credentials the live connection was
	// built with. A different account (or re-imported cookies) must not reuse
	// the previous account's VM connection.
	connKey     string
	invalidated bool
}

// NewClient builds a protocol client for one muse.ai site.
func NewClient(site *url.URL, httpClient *http.Client) *Client {
	return NewClientWithWSURL(site, httpClient, "")
}

// NewClientWithWSURL is the test/staging seam for a gateway that is not the
// production hatch.metaaivm.com host.
func NewClientWithWSURL(site *url.URL, httpClient *http.Client, wsURL string) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 25 * time.Second}
	}
	return &Client{site: site, wsURL: wsURL, httpClient: httpClient, nodeID: randomRequestID(), quotaGate: make(chan struct{}, 1)}
}

// SetWSDialer configures the WebSocket proxy/TLS dialer for this client.
func (c *Client) SetWSDialer(dialer *websocket.Dialer) { c.dialer = dialer }

func (c *Client) connect(ctx context.Context, credentials map[string]string) (*Conn, error) {
	key := credentialFingerprint(credentials)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.invalidated {
		return nil, errors.New("account connection invalidated")
	}
	if c.conn != nil && !c.conn.IsClosed() && c.connKey == key {
		return c.conn, nil
	}
	if c.conn != nil {
		// Different credentials: the old connection belongs to another
		// account or a session that was replaced. Drop it before dialing.
		_ = c.conn.Close()
		c.conn = nil
		c.session = nil
		c.connKey = ""
	}
	session, err := Bootstrap(ctx, SessionOptions{
		BaseURL: c.site,
		Cookies: credentials,
		Client:  c.httpClient,
		WSURL:   c.wsURL,
	})
	if err != nil {
		return nil, err
	}
	// Bootstrap can rotate hatch_sess/device cookies. Fingerprint the post-
	// bootstrap set so the next turn does not reconnect on every request.
	key = credentialFingerprint(session.Cookies)
	header := gatewayHeader(c.site)
	conn, err := DialWithHeader(ctx, session.WSURL, c.dialer, header)
	var dialErr *GatewayDialError
	if err != nil && errors.As(err, &dialErr) &&
		(dialErr.StatusCode == http.StatusUnauthorized || dialErr.StatusCode == http.StatusForbidden) {
		if session.FallbackWSURL == "" {
			err = fmt.Errorf("%w [homepage offered a single gateway token]", err)
		} else {
			// The early bootstrap token can be refused while the authoritative
			// server-data token is accepted; try it once before giving up.
			conn, err = DialWithHeader(ctx, session.FallbackWSURL, c.dialer, header)
			if err != nil {
				err = fmt.Errorf("authoritative token: %w; early bootstrap token: %v", err, dialErr)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	c.conn = conn
	c.session = session
	c.connKey = key
	return conn, nil
}

func (c *Client) forgetConnection() {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.session = nil
	c.connKey = ""
	c.mu.Unlock()
}

// invalidate permanently retires this client. An in-flight turn must not
// reconnect with a stale credential snapshot after the account was deleted or
// its cookies were replaced; the driver creates a fresh client on next use.
func (c *Client) invalidate() {
	c.mu.Lock()
	c.invalidated = true
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.session = nil
	c.connKey = ""
	c.mu.Unlock()
}

// ChatItem is one attachment or text entry in the outgoing chat turn, matching
// the web client's items[] shape.
type ChatItem struct {
	Type       string `json:"type"`
	Text       string `json:"text,omitempty"`
	MimeType   string `json:"mime_type,omitempty"`
	DataBase64 string `json:"data_base64,omitempty"`
	Filename   string `json:"filename,omitempty"`
}

// Chat sends one text-only prompt.
func (c *Client) Chat(ctx context.Context, credentials map[string]string, prompt string, wantMedia bool, timeout time.Duration) (string, []MediaRef, error) {
	return c.ChatWithItems(ctx, credentials, prompt, nil, wantMedia, timeout)
}

// ChatWithItems sends a prompt with optional inline image attachments
// (chat.stream items[]) and waits for the assistant reply plus media refs.
func (c *Client) ChatWithItems(ctx context.Context, credentials map[string]string, prompt string, mediaItems []ChatItem, wantMedia bool, timeout time.Duration) (string, []MediaRef, error) {
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	c.chatMu.Lock()
	defer c.chatMu.Unlock()
	deadline := time.Now().Add(timeout)
	conn, err := c.connect(ctx, credentials)
	if err != nil {
		return "", nil, err
	}

	items := make([]any, 0, len(mediaItems)+1)
	for _, item := range mediaItems {
		items = append(items, item)
	}
	items = append(items, ChatItem{Type: "text", Text: prompt})
	body := map[string]any{
		"items":        items,
		"node_id":      c.nodeID,
		"capabilities": map[string]any{},
	}
	baselineResult, err := conn.Request(ctx, http.MethodGet, "/chat/history", map[string]any{"limit": 1}, 20*time.Second)
	if err != nil {
		return "", nil, fmt.Errorf("发送前读取对话历史: %w", err)
	}
	var baselinePayload map[string]any
	if err := baselineResult.JSON(&baselinePayload); err != nil {
		return "", nil, fmt.Errorf("解析发送前对话历史: %w", err)
	}
	baseline := maxEventSeq(historyEvents(baselinePayload))
	if err := conn.Send(ctx, http.MethodPost, "/chat/stream", body); err != nil {
		return "", nil, err
	}

	var bestText string
	var bestMedia []MediaRef
	var lastText string
	var textStableAt time.Time
	mediaSignature := ""
	var mediaStableAt time.Time
	for {
		if err := ctx.Err(); err != nil {
			return bestText, bestMedia, err
		}
		if time.Now().After(deadline) {
			return bestText, bestMedia, fmt.Errorf("上游对话超时（最后回复：%s）", truncate([]byte(bestText), 160))
		}
		conn = c.currentConn()
		if conn == nil {
			if conn, err = c.connect(ctx, credentials); err != nil {
				sleepCtx(ctx, pollInterval)
				continue
			}
		}
		result, err := conn.Request(ctx, http.MethodGet, "/chat/history", map[string]any{"limit": 120}, 20*time.Second)
		if err != nil {
			if conn.IsClosed() {
				c.forgetConnection()
			}
			sleepCtx(ctx, 800*time.Millisecond)
			continue
		}
		var payload map[string]any
		if err := result.JSON(&payload); err != nil {
			sleepCtx(ctx, pollInterval)
			continue
		}
		events := historyEvents(payload)
		after := assistantReplies(events, baseline)
		currentText := latestAssistantText(after)
		var media []MediaRef
		if wantMedia {
			media = extractMedia(after)
		}
		if currentText != "" {
			bestText = currentText
		}
		if len(media) > 0 {
			bestMedia = media
		}

		// Refusal: the model answered with text and will not produce media.
		if len(media) == 0 && currentText != "" && isRefusal(currentText) {
			return bestText, nil, &MediaRefusalError{Message: truncate([]byte(currentText), 240)}
		}

		now := time.Now()
		if currentText != "" {
			if currentText == lastText {
				if textStableAt.IsZero() {
					textStableAt = now
				}
			} else {
				lastText = currentText
				textStableAt = now
			}
		}

		if wantMedia {
			if len(media) > 0 {
				sig := mediaSignatureOf(media)
				if sig != mediaSignature {
					mediaSignature = sig
					mediaStableAt = now
				}
				mediaStable := !mediaStableAt.IsZero() && now.Sub(mediaStableAt) >= mediaSettle
				textStable := !textStableAt.IsZero() && now.Sub(textStableAt) >= textSettle
				if mediaStable || textStable {
					return bestText, media, nil
				}
			} else {
				mediaSignature = ""
				mediaStableAt = time.Time{}
				// A previously observed media reference can scroll out of the
				// bounded history window. Do not turn a real artifact into a
				// refusal just because the current page is empty.
				if len(bestMedia) > 0 {
					return bestText, bestMedia, nil
				}
				if !textStableAt.IsZero() && now.Sub(textStableAt) >= noMediaSettle {
					return bestText, nil, &MediaRefusalError{Message: truncate([]byte(bestText), 240)}
				}
			}
		} else if assistantIsFinished(after) {
			return bestText, nil, nil
		}

		sleepCtx(ctx, pollInterval)
	}
}

func (c *Client) currentConn() *Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// MediaRefusalError means the upstream turn produced text but no media.
type MediaRefusalError struct {
	Message string
}

func (e *MediaRefusalError) Error() string {
	return "模型未生成媒体，仅返回文本：" + e.Message
}

// Unwrap keeps refusal handling on the protocol error contract instead of
// degrading to a generic 502 in the HTTP layer.
func (e *MediaRefusalError) Unwrap() error { return protocol.ErrModelRefusal }

// DownloadToFile is the bounded, streaming media path. It writes successful
// gateway bodies directly to a temporary file and never materializes a large
// video in process memory.
func (c *Client) DownloadToFile(ctx context.Context, credentials map[string]string, ref MediaRef, dir string, maxBytes int64) (string, string, error) {
	if maxBytes <= 0 {
		maxBytes = 512 << 20
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", err
	}
	conn, err := c.connect(ctx, credentials)
	if err != nil {
		return "", "", err
	}
	if ref.Path != "" {
		clean, cleanErr := safeSandboxPath(ref.Path)
		if cleanErr != nil {
			return "", "", cleanErr
		}
		for _, rpcPath := range []string{"/media/raw/" + clean, "/fs/raw/" + clean} {
			for attempt := 0; attempt < 2; attempt++ {
				file, createErr := os.CreateTemp(dir, ".download-*")
				if createErr != nil {
					return "", "", createErr
				}
				writer := &limitedWriter{file: file, max: maxBytes}
				result, requestErr := conn.RequestTo(ctx, http.MethodGet, rpcPath, nil, 300*time.Second, func(p []byte) error { _, writeErr := writer.Write(p); return writeErr })
				if requestErr == nil && result != nil && result.Status >= 200 && result.Status < 300 && writer.written > 0 {
					if err := file.Sync(); err != nil {
						_ = file.Close()
						_ = os.Remove(file.Name())
						return "", "", err
					}
					_ = file.Close()
					return file.Name(), headerValue(result.Headers, "content-type"), nil
				}
				name := file.Name()
				_ = file.Close()
				_ = os.Remove(name)
				if writer.err != nil {
					return "", "", writer.err
				}
				var statusErr *RpcStatusError
				if errors.As(requestErr, &statusErr) {
					if statusErr.Status == http.StatusUnauthorized || statusErr.Status == http.StatusForbidden {
						return "", "", &AuthError{Status: statusErr.Status, Body: truncate(statusErr.Body, 180)}
					}
					if statusErr.Status == http.StatusNotFound {
						break
					}
				}
				if conn.IsClosed() {
					c.forgetConnection()
					if conn, err = c.connect(ctx, credentials); err != nil {
						return "", "", err
					}
					continue
				}
				break
			}
		}
	}
	if strings.HasPrefix(ref.URL, "http://") || strings.HasPrefix(ref.URL, "https://") {
		return c.downloadURLToFile(ctx, credentials, ref.URL, dir, maxBytes)
	}
	return "", "", errors.New("媒体缺少可下载的路径或 URL")
}

func (c *Client) downloadURLToFile(ctx context.Context, credentials map[string]string, rawURL, dir string, maxBytes int64) (string, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", "", err
	}
	request.Header.Set("User-Agent", chromeUserAgent)
	if len(credentials) > 0 {
		request.Header.Set("Cookie", cookieHeader(credentials))
	}
	response, err := refimage.NewPublicHTTPClient(30 * time.Minute).Do(request)
	if err != nil {
		return "", "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", "", fmt.Errorf("媒体下载失败：HTTP %d", response.StatusCode)
	}
	file, err := os.CreateTemp(dir, ".download-*")
	if err != nil {
		return "", "", err
	}
	name := file.Name()
	written, copyErr := io.Copy(file, io.LimitReader(response.Body, maxBytes+1))
	if copyErr == nil && written > maxBytes {
		copyErr = fmt.Errorf("媒体超过大小限制 %d: %w", maxBytes, errResponseTooLarge)
	}
	if copyErr == nil {
		copyErr = file.Sync()
	}
	if closeErr := file.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		_ = os.Remove(name)
		return "", "", copyErr
	}
	return name, response.Header.Get("Content-Type"), nil
}

type limitedWriter struct {
	file    *os.File
	max     int64
	written int64
	err     error
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.written+int64(len(p)) > w.max {
		w.err = fmt.Errorf("媒体超过大小限制 %d: %w", w.max, errResponseTooLarge)
		return 0, w.err
	}
	n, err := w.file.Write(p)
	w.written += int64(n)
	if err != nil {
		w.err = err
	}
	return n, err
}

func safeSandboxPath(value string) (string, error) {
	raw := normalizeSandboxPath(value)
	for _, segment := range strings.Split(raw, "/") {
		if segment == ".." {
			return "", fmt.Errorf("非法媒体路径: %w", errProtocol)
		}
	}
	cleaned := path.Clean("/" + raw)
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "" || cleaned == "." || strings.HasPrefix(cleaned, "../") || cleaned == ".." {
		return "", fmt.Errorf("非法媒体路径: %w", errProtocol)
	}
	return cleaned, nil
}

func headerValue(headers []Header, key string) string {
	for _, header := range headers {
		if strings.EqualFold(header.Key, key) {
			return header.Value
		}
	}
	return ""
}

func historyEvents(payload map[string]any) []map[string]any {
	node := payload
	if result, ok := payload["result"].(map[string]any); ok {
		node = result
	}
	for _, key := range []string{"chat_events", "events", "history"} {
		list, ok := node[key].([]any)
		if !ok {
			continue
		}
		out := make([]map[string]any, 0, len(list))
		for _, item := range list {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

func eventName(event map[string]any) string {
	if name := asString(event["event_name"]); name != "" {
		return name
	}
	return asString(event["event"])
}

func eventText(event map[string]any) string {
	for _, key := range []string{"display_text", "text"} {
		if text := asString(event[key]); text != "" {
			return text
		}
	}
	if payload, ok := event["payload"].(map[string]any); ok {
		for _, key := range []string{"display_text", "text", "content", "delta"} {
			if text := asString(payload[key]); text != "" {
				return text
			}
		}
	}
	return ""
}

func maxEventSeq(events []map[string]any) int64 {
	var max int64
	for _, event := range events {
		if seq, ok := event["seq"].(float64); ok && int64(seq) > max {
			max = int64(seq)
		}
	}
	return max
}

func assistantReplies(events []map[string]any, baseline int64) []map[string]any {
	replies := make([]map[string]any, 0, len(events))
	for _, event := range events {
		if eventName(event) != "message.assistant" {
			continue
		}
		seq, _ := event["seq"].(float64)
		if int64(seq) <= baseline {
			continue
		}
		if asString(event["reply_to_message_id"]) != "" {
			continue
		}
		if payload, ok := event["payload"].(map[string]any); ok && asString(payload["reply_to_message_id"]) != "" {
			continue
		}
		if eventText(event) == "" {
			continue
		}
		replies = append(replies, event)
	}
	return replies
}

func assistantIsFinished(events []map[string]any) bool {
	if len(events) == 0 {
		return false
	}
	payload, ok := events[len(events)-1]["payload"].(map[string]any)
	if !ok {
		return false
	}
	switch strings.ToLower(asString(payload["status"])) {
	case "completed", "complete", "done", "finished":
		return true
	default:
		return false
	}
}

func latestAssistantText(events []map[string]any) string {
	best := ""
	for _, event := range events {
		if eventName(event) != "message.assistant" {
			continue
		}
		if asString(event["reply_to_message_id"]) != "" {
			continue
		}
		if payload, ok := event["payload"].(map[string]any); ok {
			if asString(payload["reply_to_message_id"]) != "" {
				continue
			}
		}
		if text := strings.TrimSpace(eventText(event)); text != "" {
			best = text
		}
	}
	return best
}

func mediaSignatureOf(media []MediaRef) string {
	parts := make([]string, 0, len(media))
	for _, ref := range media {
		parts = append(parts, ref.identity())
	}
	// The extraction path should already be ordered, but sorting here keeps
	// the settle detector stable even if an upstream event reorders resources.
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

func isRefusal(text string) bool {
	lower := strings.ToLower(text)
	for _, marker := range refusalMarkers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

var sandboxSuffixes = []struct {
	ext  string
	kind string
}{
	{".mp4", "video"}, {".webm", "video"}, {".mov", "video"}, {".m4v", "video"},
	{".png", "image"}, {".jpg", "image"}, {".jpeg", "image"}, {".webp", "image"}, {".gif", "image"},
}

func extractMedia(events []map[string]any) []MediaRef {
	found := map[string]MediaRef{}
	order := make([]string, 0, 4)
	add := func(ref MediaRef) {
		if ref.Kind == "" || (ref.Path == "" && ref.URL == "") {
			return
		}
		key := ref.identity()
		if _, exists := found[key]; exists {
			return
		}
		found[key] = ref
		order = append(order, key)
	}
	var walk func(node any, kindHint string)
	walk = func(node any, kindHint string) {
		switch value := node.(type) {
		case map[string]any:
			if ref, ok := mediaRefFromNode(value, kindHint); ok {
				add(ref)
			}
			kind := kindHint
			if k := asString(value["kind"]); k == "image" || k == "video" {
				kind = k
			}
			// Map iteration order is random in Go. Walk keys in a stable
			// order so the media slice is deterministic across polls.
			keys := make([]string, 0, len(value))
			for key := range value {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				switch key {
				case "path", "image_path", "video_path", "url", "variants", "mime",
					"mime_type", "mimeType", "label", "byte_len":
					continue
				}
				walk(value[key], kind)
			}
		case []any:
			for _, item := range value {
				walk(item, kindHint)
			}
		case string:
			for _, match := range sandboxPattern.FindAllString(value, -1) {
				walk(map[string]any{"path": match}, kindHint)
			}
		}
	}
	for _, event := range events {
		kindHint := ""
		if payload, ok := event["payload"].(map[string]any); ok {
			if k := asString(payload["kind"]); k == "image" || k == "video" {
				kindHint = k
			}
			walk(payload, kindHint)
		}
		if resources, ok := event["resources"]; ok && resources != nil {
			walk(resources, kindHint)
		}
	}
	out := make([]MediaRef, 0, len(order))
	for _, key := range order {
		out = append(out, found[key])
	}
	return out
}

func mediaRefFromNode(node map[string]any, kindHint string) (MediaRef, bool) {
	path := asString(node["path"])
	if path == "" {
		path = asString(node["image_path"])
	}
	if path == "" {
		path = asString(node["video_path"])
	}
	url := ""
	if variants, ok := node["variants"].(map[string]any); ok {
		url = asString(variants["original"])
	} else if variants, ok := node["variants"].([]any); ok {
		for _, item := range variants {
			if variant, ok := item.(map[string]any); ok {
				if candidate := asString(variant["original"]); candidate != "" {
					url = candidate
					break
				}
			}
		}
	}
	if url == "" {
		url = asString(node["url"])
	}
	mime := asString(node["mime"])
	if mime == "" {
		mime = asString(node["mime_type"])
	}
	if mime == "" {
		mime = asString(node["mimeType"])
	}
	kind := asString(node["kind"])
	if kind != "image" && kind != "video" {
		kind = kindHint
	}
	if kind != "image" && kind != "video" {
		if strings.HasPrefix(mime, "video/") {
			kind = "video"
		} else if strings.HasPrefix(mime, "image/") {
			kind = "image"
		}
	}
	if kind != "image" && kind != "video" {
		lower := strings.ToLower(path)
		if lower == "" {
			lower = strings.ToLower(url)
		}
		for _, suffix := range sandboxSuffixes {
			if strings.HasSuffix(strings.SplitN(lower, "?", 2)[0], suffix.ext) {
				kind = suffix.kind
				break
			}
		}
	}
	if (kind != "image" && kind != "video") || (path == "" && url == "") {
		return MediaRef{}, false
	}
	ref := MediaRef{Kind: kind, Mime: mime, Label: asString(node["label"])}
	if path != "" {
		ref.Path = normalizeSandboxPath(path)
	}
	if url != "" {
		ref.URL = url
	}
	return ref, true
}

func normalizeSandboxPath(value string) string {
	text := strings.TrimSpace(value)
	text = strings.TrimPrefix(text, "sandbox://")
	text = strings.ReplaceAll(text, "\\", "/")
	for strings.Contains(text, "//") {
		text = strings.ReplaceAll(text, "//", "/")
	}
	return strings.TrimLeft(text, "/")
}

// ChatDelta is one incremental assistant text fragment.
type ChatDelta struct {
	Text string
	Done bool
	Err  error
}

// ChatStream sends a prompt on the account's main chat and polls chat.history
// for the evolving assistant text. The turn is serialized because each account
// has one main conversation.
func (c *Client) ChatStream(ctx context.Context, credentials map[string]string, prompt string, timeout time.Duration) (<-chan ChatDelta, error) {
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	c.chatMu.Lock()
	conn, err := c.connect(ctx, credentials)
	if err != nil {
		c.chatMu.Unlock()
		return nil, err
	}
	baselineResult, err := conn.Request(ctx, http.MethodGet, "/chat/history", map[string]any{"limit": 1}, 20*time.Second)
	if err != nil {
		c.chatMu.Unlock()
		return nil, fmt.Errorf("发送前读取对话历史: %w", err)
	}
	var baselinePayload map[string]any
	if err := baselineResult.JSON(&baselinePayload); err != nil {
		c.chatMu.Unlock()
		return nil, fmt.Errorf("解析发送前对话历史: %w", err)
	}
	baseline := maxEventSeq(historyEvents(baselinePayload))
	body := map[string]any{
		"items":        []any{ChatItem{Type: "text", Text: prompt}},
		"node_id":      c.nodeID,
		"capabilities": map[string]any{},
	}
	if err := conn.Send(ctx, http.MethodPost, "/chat/stream", body); err != nil {
		c.chatMu.Unlock()
		return nil, err
	}

	out := make(chan ChatDelta, 64)
	ticker := time.NewTicker(pollInterval)
	var emitted string
	var lastText string
	var stableAt time.Time
	go func() {
		defer close(out)
		defer c.chatMu.Unlock()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				select {
				case out <- ChatDelta{Err: fmt.Errorf("上游对话流超时: %w", protocol.ErrUpstreamTimeout)}:
				case <-ctx.Done():
				}
				return
			case <-ticker.C:
				conn := c.currentConn()
				if conn == nil {
					continue
				}
				result, err := conn.Request(ctx, http.MethodGet, "/chat/history", map[string]any{"limit": 120}, 20*time.Second)
				if err != nil {
					if conn.IsClosed() {
						c.forgetConnection()
					}
					continue
				}
				var payload map[string]any
				if err := result.JSON(&payload); err != nil {
					continue
				}
				replies := assistantReplies(historyEvents(payload), baseline)
				text := latestAssistantText(replies)
				if text == "" {
					continue
				}
				now := time.Now()
				if text != lastText {
					lastText = text
					stableAt = now
				} else if stableAt.IsZero() {
					stableAt = now
				}
				delta := text
				if strings.HasPrefix(text, emitted) {
					delta = strings.TrimPrefix(text, emitted)
				}
				if delta != "" {
					select {
					case out <- ChatDelta{Text: delta}:
						emitted = text
					case <-ctx.Done():
						return
					}
				}
				if assistantIsFinished(replies) {
					select {
					case out <- ChatDelta{Done: true}:
					case <-ctx.Done():
					}
					return
				}
			}
		}
	}()
	return out, nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
