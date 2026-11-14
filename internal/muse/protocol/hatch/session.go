package hatch

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"muse2api/internal/muse/protocol"
)

// Gateway constants come from the web bundle (protocol.json).
const (
	DefaultLBHost     = "hatch.metaaivm.com"
	DefaultWSPath     = "/v1/noise"
	DefaultAppID      = "hatch-web"
	DefaultAppVersion = "1"

	gatewayPagePath    = "/"
	gatewayPageMaxSize = 8 << 20
	gatewayScanWindow  = 8 << 10
)

// Session is a bootstrapped gateway admission (VM + auth token).
type Session struct {
	VMID      string
	Endpoint  string
	AuthToken string
	Notary    string
	WSURL     string
	// FallbackWSURL carries the authoritative server-data token when it
	// differs from the early bootstrap token; it is tried once if the gateway
	// refuses the first admission.
	FallbackWSURL string
	Cookies       map[string]string
	CookieExpiry  map[string]int64
}

// SessionOptions carries the HTTP bootstrap inputs.
type SessionOptions struct {
	BaseURL      *url.URL // e.g. https://muse.ai
	Client       *http.Client
	Cookies      map[string]string
	CookieExpiry map[string]int64
	LBHost       string
	WSPath       string
	AppID        string
	// WSURL overrides the constructed wss:// URL. It is used by integration
	// tests and staging environments; empty keeps production behavior.
	WSURL string
}

// Bootstrap resolves the account VM and its gateway admission tokens, wakes
// the VM when needed and builds the wss URL.
//
// muse.ai no longer hands out VM admission through /api/session and
// /api/hatch/token (the former now answers status=unavailable). The signed-in
// homepage embeds the gateway bootstrap (vm, auth token, noise notary token)
// in its server-rendered HTML, and the web client connects with exactly that.
func Bootstrap(ctx context.Context, opts SessionOptions) (*Session, error) {
	if opts.BaseURL == nil {
		return nil, errors.New("missing base url")
	}
	if opts.Client == nil {
		opts.Client = &http.Client{Timeout: 25 * time.Second}
	}
	if opts.Cookies == nil {
		opts.Cookies = map[string]string{}
	}
	if opts.CookieExpiry == nil {
		opts.CookieExpiry = map[string]int64{}
	}
	lbHost := opts.LBHost
	if lbHost == "" {
		lbHost = DefaultLBHost
	}
	wsPath := opts.WSPath
	if wsPath == "" {
		wsPath = DefaultWSPath
	}
	appID := opts.AppID
	if appID == "" {
		appID = DefaultAppID
	}

	init, err := opts.loadGatewayInit(ctx)
	if err != nil {
		return nil, err
	}

	if !strings.EqualFold(init.VMState, "RUNNING") {
		// The web client requests a wake on mount whenever the VM is not
		// known to be running. An unknown state is not fatal: try the gateway.
		if err := opts.wakeVM(ctx, init.VMID); err != nil && init.VMState != "" {
			return nil, err
		}
		deadline := time.Now().Add(45 * time.Second)
		for init.VMState != "" && !strings.EqualFold(init.VMState, "RUNNING") {
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("VM %s did not become RUNNING (state=%s)", init.VMID, init.VMState)
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(1500 * time.Millisecond):
			}
			// Re-read the page: it carries the live state and fresh tokens.
			if next, err := opts.loadGatewayInit(ctx); err == nil {
				init = next
			}
		}
		// Let the VM's noise listener bind after a wake.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1200 * time.Millisecond):
		}
	}

	wsURL := opts.WSURL
	fallbackWSURL := ""
	if wsURL == "" {
		wsURL = gatewayWSURL(lbHost, wsPath, appID, init.VMID, init.AuthToken, init.NotaryToken)
		if init.AltAuthToken != "" && init.AltAuthToken != init.AuthToken {
			fallbackWSURL = gatewayWSURL(lbHost, wsPath, appID, init.VMID, init.AltAuthToken, init.AltNotaryToken)
		}
	}
	return &Session{VMID: init.VMID, Endpoint: init.GatewayURL, AuthToken: init.AuthToken, Notary: init.NotaryToken, WSURL: wsURL, FallbackWSURL: fallbackWSURL, Cookies: opts.Cookies, CookieExpiry: opts.CookieExpiry}, nil
}

// gatewayWSURL builds the admission URL with the web client's parameter order.
func gatewayWSURL(lbHost, wsPath, appID, vmID, authToken, notaryToken string) string {
	params := []string{
		"vm_id=" + url.QueryEscape(vmID),
		"auth_token=" + url.QueryEscape(authToken),
	}
	if notaryToken != "" {
		params = append(params, "notary_token="+url.QueryEscape(notaryToken))
	}
	params = append(params,
		"app_id="+url.QueryEscape(appID),
		"app_version="+DefaultAppVersion,
		"request_id="+randomUUID(),
	)
	return "wss://" + lbHost + wsPath + "?" + strings.Join(params, "&")
}

// gatewayInit is the gateway bootstrap the signed-in homepage renders.
type gatewayInit struct {
	VMID        string
	GatewayURL  string
	AuthToken   string
	NotaryToken string
	VMState     string
	Assignment  string
	Issue       any

	// AltAuthToken/AltNotaryToken come from the authoritative server data.
	AltAuthToken   string
	AltNotaryToken string
}

func (o *SessionOptions) loadGatewayInit(ctx context.Context) (*gatewayInit, error) {
	page, matchedPath, err := o.fetchPage(ctx, gatewayPagePath)
	if err != nil {
		return nil, err
	}
	init := parseGatewayInit(page)
	if init == nil {
		if strings.Contains(matchedPath, "logged-out") || looksLikeLoginPage([]byte(page)) {
			return nil, fmt.Errorf("muse.ai served the signed-out page: %w", protocol.ErrSessionExpired)
		}
		return nil, errors.New("muse.ai homepage has no gateway bootstrap (__HATCH_GATEWAY_INIT__)")
	}
	if init.Assignment != "" && !strings.EqualFold(init.Assignment, "assigned") {
		return nil, fmt.Errorf("muse.ai did not assign a VM (assignment=%s issue=%v)", init.Assignment, init.Issue)
	}
	if init.VMID == "" || init.AuthToken == "" {
		return nil, fmt.Errorf("muse.ai gateway bootstrap is missing vm or auth token (assignment=%s issue=%v)", init.Assignment, init.Issue)
	}
	return init, nil
}

var flightChunkRe = regexp.MustCompile(`self\.__next_f\.push\(\[1,("(?:[^"\\]|\\.)*")\]\)`)

// parseGatewayInit collects every gateway bootstrap object on the page. The
// HTML carries an early window.__HATCH_GATEWAY_INIT__ script (the tokens the
// web client actually connects with) plus React Flight copies of the server
// data, the authoritative one of which holds the live VM state.
func parseGatewayInit(page string) *gatewayInit {
	texts := []string{page}
	for _, match := range flightChunkRe.FindAllStringSubmatch(page, -1) {
		var chunk string
		if json.Unmarshal([]byte(match[1]), &chunk) == nil {
			texts = append(texts, chunk)
		}
	}
	var objects []map[string]any
	for _, text := range texts {
		objects = append(objects, gatewayObjects(text)...)
	}
	if len(objects) == 0 {
		return nil
	}

	rank := func(obj map[string]any) int {
		switch {
		case obj["version"] != nil:
			return 0
		case asString(obj["gatewayBootstrapSource"]) == "authoritative":
			return 1
		default:
			return 2
		}
	}
	best := objects[0]
	for _, obj := range objects[1:] {
		if rank(obj) < rank(best) {
			best = obj
		}
	}
	init := &gatewayInit{
		VMID:        firstNonEmpty(asString(best["vmId"]), asString(best["vmName"])),
		GatewayURL:  asString(best["gatewayUrl"]),
		AuthToken:   strings.TrimSpace(asString(best["authToken"])),
		NotaryToken: strings.TrimSpace(asString(best["noiseNotaryToken"])),
	}
	stateRank := 3
	for _, obj := range objects {
		if init.VMID == "" {
			init.VMID = firstNonEmpty(asString(obj["vmId"]), asString(obj["vmName"]))
		}
		if assignment := asString(obj["vmAssignment"]); assignment != "" && (init.Assignment == "" || rank(obj) == 1) {
			init.Assignment = assignment
			init.Issue = obj["vmResolutionIssue"]
		}
		if state := asString(obj["vmState"]); state != "" && rank(obj) < stateRank {
			init.VMState = state
			stateRank = rank(obj)
		}
		if rank(obj) == 1 && init.AltAuthToken == "" {
			init.AltAuthToken = strings.TrimSpace(asString(obj["authToken"]))
			init.AltNotaryToken = strings.TrimSpace(asString(obj["noiseNotaryToken"]))
		}
	}
	return init
}

// gatewayObjects decodes the JSON objects that directly own an authToken.
// Starting from each key it walks back to the nearest brace whose object
// decodes and contains the key at its top level.
func gatewayObjects(text string) []map[string]any {
	var out []map[string]any
	const key = `"authToken":"`
	for offset := 0; ; {
		idx := strings.Index(text[offset:], key)
		if idx < 0 {
			return out
		}
		pos := offset + idx
		offset = pos + len(key)
		low := pos - gatewayScanWindow
		if low < 0 {
			low = 0
		}
		for start := pos; ; {
			start = strings.LastIndexByte(text[low:start], '{')
			if start < 0 {
				break
			}
			start += low
			var obj map[string]any
			if json.NewDecoder(strings.NewReader(text[start:])).Decode(&obj) == nil {
				if _, ok := obj["authToken"].(string); ok {
					out = append(out, obj)
					break
				}
			}
		}
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// fetchPage loads a document the way a browser navigation does and returns
// the HTML plus the route Next.js matched (signed-out pages rewrite to
// /oidc/logged-out).
func (o *SessionOptions) fetchPage(ctx context.Context, path string) (string, string, error) {
	endpoint := strings.TrimRight(o.BaseURL.String(), "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Cookie", cookieHeader(o.Cookies))
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("User-Agent", chromeUserAgent)

	resp, err := o.Client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	o.absorbCookies(resp)
	body, err := io.ReadAll(io.LimitReader(resp.Body, gatewayPageMaxSize))
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return "", "", &AuthError{Status: resp.StatusCode, Body: truncate(body, 180)}
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("GET %s: upstream HTTP %d %s", path, resp.StatusCode, truncate(body, 180))
	}
	return string(body), resp.Header.Get("X-Matched-Path"), nil
}

// absorbCookies writes rotated session cookies back into the options.
func (o *SessionOptions) absorbCookies(resp *http.Response) {
	if o.Cookies == nil {
		o.Cookies = map[string]string{}
	}
	if o.CookieExpiry == nil {
		o.CookieExpiry = map[string]int64{}
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "" || cookie.Value == "" {
			continue
		}
		o.Cookies[cookie.Name] = cookie.Value
		switch {
		case cookie.MaxAge > 0:
			o.CookieExpiry[cookie.Name] = time.Now().Add(time.Duration(cookie.MaxAge) * time.Second).Unix()
		case cookie.MaxAge < 0:
			delete(o.CookieExpiry, cookie.Name)
		case !cookie.Expires.IsZero():
			o.CookieExpiry[cookie.Name] = cookie.Expires.Unix()
		}
	}
}

func (o *SessionOptions) requestJSON(ctx context.Context, method, path string, body any, accessToken string) (map[string]any, error) {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(buf)
	}
	endpoint := strings.TrimRight(o.BaseURL.String(), "/") + path
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	// Application headers copied from the web client's captured requests; the
	// reference implementation relies on the same set.
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9,en-GB;q=0.8")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", cookieHeader(o.Cookies))
	origin := o.BaseURL.Scheme + "://" + o.BaseURL.Host
	req.Header.Set("Origin", origin)
	req.Header.Set("Referer", origin+"/")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
	req.Header.Set("Priority", "u=4")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", chromeUserAgent)

	resp, err := o.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	o.absorbCookies(resp)
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, &AuthError{Status: resp.StatusCode, Body: truncate(payload, 180)}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s %s: upstream HTTP %d %s", method, path, resp.StatusCode, truncate(payload, 180))
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("%s %s: invalid JSON: %w", method, path, err)
	}
	return out, nil
}

// wakeVM mirrors the web client's wake request: cookie-authenticated, no
// bearer token.
func (o *SessionOptions) wakeVM(ctx context.Context, vmID string) error {
	_, err := o.requestJSON(ctx, http.MethodPost, "/api/hatch/vm/wake", map[string]any{
		"vm_id":              vmID,
		"retry_count":        0,
		"connect_attempt_id": randomUUID(),
		"wake_trigger":       "mount",
	}, "")
	return err
}

func randomUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func cookieHeader(cookies map[string]string) string {
	keys := make([]string, 0, len(cookies))
	for key, value := range cookies {
		if value != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+cookies[key])
	}
	return strings.Join(parts, "; ")
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

// AuthError marks an upstream 401/403 so account code can classify it as an
// expired session.
type AuthError struct {
	Status int
	Body   string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("upstream auth error %d: %s", e.Status, e.Body)
}

// Unwrap lets account/session code classify 401/403 responses with
// errors.Is(err, protocol.ErrSessionExpired). Without this, a dead cookie was
// only recorded as "check unconfirmed" and the account stayed schedulable.
func (e *AuthError) Unwrap() error { return protocol.ErrSessionExpired }

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
