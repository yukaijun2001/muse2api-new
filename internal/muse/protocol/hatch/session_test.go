package hatch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

func TestBootstrapUsesMuseAccessTokenForHatchAdmission(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		if r.Header.Get("Cookie") != "hatch_sess=session" {
			t.Errorf("cookie = %q", r.Header.Get("Cookie"))
		}
		switch r.URL.Path {
		case "/api/auth/check":
			if r.Method != http.MethodPost {
				t.Errorf("auth check method = %s", r.Method)
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("auth check Content-Type = %q", got)
			}
			if body, _ := io.ReadAll(r.Body); len(body) != 0 {
				t.Errorf("auth check body = %q", body)
			}
			_, _ = io.WriteString(w, `{"access_token":"access-token"}`)
		case "/api/session":
			_, _ = io.WriteString(w, `{"vm_id":"vm1","endpoint_url":"https://vm1","vm_state":"RUNNING"}`)
		case "/api/hatch/token":
			if got := r.Header.Get("Authorization"); got != "Bearer access-token" {
				t.Errorf("hatch token Authorization = %q", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode hatch body: %v", err)
			}
			if body["vmAddress"] != "wss://vm1.metaaivm.com/" || body["vmName"] != "vm1" {
				t.Errorf("hatch token body = %#v", body)
			}
			_, _ = io.WriteString(w, `{"token":"hatch-token"}`)
		case "/api/hatch/noise-notary-token":
			if got := r.Header.Get("Authorization"); got != "Bearer access-token" {
				t.Errorf("notary Authorization = %q", got)
			}
			_, _ = io.WriteString(w, `{"notaryToken":"notary-token"}`)
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
	if session.AccessToken != "access-token" || session.AuthToken != "hatch-token" || session.Notary != "notary-token" {
		t.Fatalf("unexpected session: %+v", session)
	}
	want := []string{
		"POST /api/auth/check",
		"GET /api/session",
		"POST /api/hatch/token",
		"POST /api/hatch/noise-notary-token",
	}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("request sequence = %v, want %v", paths, want)
	}
}
