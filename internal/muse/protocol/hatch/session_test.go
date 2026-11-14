package hatch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"

	"muse2api/internal/muse/protocol"
)

// gatewayTestPage mirrors the signed-in homepage: a React Flight copy of the
// server data (authoritative, with the live VM state) and the early
// __HATCH_GATEWAY_INIT__ script the web client connects with.
func gatewayTestPage(t *testing.T, vmState, assignment string) string {
	t.Helper()
	authoritative, err := json.Marshal(map[string]any{
		"backendAuthStatus":      "authenticated",
		"gatewayBootstrapSource": "authoritative",
		"gatewayUrl":             "wss://vm1.metaaivm.com/",
		"vmName":                 "vm1",
		"vmId":                   "vm1",
		"authToken":              "authoritative-token",
		"noiseNotaryToken":       "authoritative-notary",
		"vmAssignment":           assignment,
		"vmResolutionIssue":      nil,
		"vmState":                vmState,
		"vmStateSource":          "live",
	})
	if err != nil {
		t.Fatal(err)
	}
	flight, err := json.Marshal("fb:" + string(authoritative) + "\n")
	if err != nil {
		t.Fatal(err)
	}
	early := `{"version":1,"gatewayUrl":"wss://vm1.metaaivm.com/","vmName":"vm1","authToken":"hatch-token","noiseNotaryToken":"notary-token","credentialSession":{"ownerId":"1","sessionBindingId":"b"}}`
	return `<!DOCTYPE html><html><head><script>self.__next_f.push([1,` + string(flight) + `])</script>` +
		`<script id="hatch-early-gateway-bootstrap">window.__HATCH_GATEWAY_INIT__=` + early + `;window.__HATCH_GATEWAY_ON_INIT__?.();</script></head><body></body></html>`
}

func TestBootstrapReadsGatewayInitFromHomepage(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		if r.Header.Get("Cookie") != "hatch_sess=session" {
			t.Errorf("cookie = %q", r.Header.Get("Cookie"))
		}
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "hatch_vml", Value: "rotated"})
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, gatewayTestPage(t, "RUNNING", "assigned"))
	}))
	defer server.Close()

	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	session, err := Bootstrap(context.Background(), SessionOptions{
		BaseURL: base,
		Client:  server.Client(),
		Cookies: map[string]string{"hatch_sess": "session"},
	})
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if session.VMID != "vm1" || session.AuthToken != "hatch-token" || session.Notary != "notary-token" || session.Endpoint != "wss://vm1.metaaivm.com/" {
		t.Fatalf("unexpected session: %+v", session)
	}
	if session.Cookies["hatch_vml"] != "rotated" {
		t.Fatalf("rotated cookie was not kept: %v", session.Cookies)
	}
	if want := []string{"GET /"}; !reflect.DeepEqual(paths, want) {
		t.Fatalf("request sequence = %v, want %v", paths, want)
	}
	wsURL, err := url.Parse(session.WSURL)
	if err != nil {
		t.Fatal(err)
	}
	if wsURL.Scheme != "wss" || wsURL.Host != DefaultLBHost || wsURL.Path != DefaultWSPath {
		t.Fatalf("ws url = %s", session.WSURL)
	}
	var keys []string
	for _, part := range strings.Split(wsURL.RawQuery, "&") {
		keys = append(keys, strings.SplitN(part, "=", 2)[0])
	}
	if want := []string{"vm_id", "auth_token", "notary_token", "app_id", "app_version", "request_id"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("ws params = %v, want %v", keys, want)
	}
	query := wsURL.Query()
	if query.Get("auth_token") != "hatch-token" || query.Get("app_version") != "1" || len(query.Get("request_id")) != 36 {
		t.Fatalf("ws query = %v", query)
	}
}

func TestBootstrapWakesStoppedVMUntilRunning(t *testing.T) {
	var mu sync.Mutex
	pages := 0
	woken := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/":
			pages++
			state := "STOPPED"
			if woken {
				state = "RUNNING"
			}
			_, _ = io.WriteString(w, gatewayTestPage(t, state, "assigned"))
		case "/api/hatch/vm/wake":
			if r.Method != http.MethodPost {
				t.Errorf("wake method = %s", r.Method)
			}
			if got := r.Header.Get("Authorization"); got != "" {
				t.Errorf("wake Authorization = %q, want none", got)
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode wake body: %v", err)
			}
			if body["vm_id"] != "vm1" || body["wake_trigger"] != "mount" || len(asString(body["connect_attempt_id"])) != 36 {
				t.Errorf("wake body = %#v", body)
			}
			woken = true
			_, _ = io.WriteString(w, `{"status":"wake_requested"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	session, err := Bootstrap(context.Background(), SessionOptions{
		BaseURL: base,
		Client:  server.Client(),
		Cookies: map[string]string{"hatch_sess": "session"},
		WSURL:   "ws://example.invalid/v1/noise",
	})
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !woken || pages != 2 || session.VMID != "vm1" {
		t.Fatalf("woken=%v pages=%d session=%+v", woken, pages, session)
	}
}

func TestBootstrapClassifiesSignedOutAndUnassignedPages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		page    func(t *testing.T) string
		matched string
		check   func(t *testing.T, err error)
	}{
		{
			name:    "signed out",
			page:    func(*testing.T) string { return `<html><body>Log in to Muse</body></html>` },
			matched: "/oidc/logged-out",
			check: func(t *testing.T, err error) {
				if !errors.Is(err, protocol.ErrSessionExpired) {
					t.Fatalf("err = %v, want session expired", err)
				}
			},
		},
		{
			name: "unassigned",
			page: func(t *testing.T) string { return gatewayTestPage(t, "", "unassigned") },
			check: func(t *testing.T, err error) {
				if err == nil || !strings.Contains(err.Error(), "assignment=unassigned") {
					t.Fatalf("err = %v, want assignment detail", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.matched != "" {
					w.Header().Set("X-Matched-Path", tc.matched)
				}
				_, _ = io.WriteString(w, tc.page(t))
			}))
			defer server.Close()
			base, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Bootstrap(context.Background(), SessionOptions{BaseURL: base, Client: server.Client(), Cookies: map[string]string{"hatch_sess": "s"}})
			tc.check(t, err)
		})
	}
}
