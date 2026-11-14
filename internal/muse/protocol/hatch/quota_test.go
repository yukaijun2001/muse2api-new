package hatch

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"muse2api/internal/muse/protocol"
)

const quotaFlightSample = "0:{\"a\":\"$@1\",\"q\":\"\",\"i\":false}\n" +
	"1:{\"success\":true,\"subscription\":{\"tier\":{\"tierId\":\"hatch_free\",\"name\":\"Muse 免费版\",\"tierCode\":\"HATCH_FREE\",\"isPaid\":false,\"rank\":0},\"usage\":{\"state\":\"METERED\",\"percentUsed\":16,\"resetsAt\":1793467985,\"quotaStatus\":\"SUFFICIENT\"},\"statusSubtitle\":\"每周限额将在 10月31日重置\",\"usageRowLabel\":\"免费版\",\"usageRowValueLabel\":\"已使用 16%\",\"topupBalance\":40000000,\"topupTotal\":40000000,\"topupRowLabel\":\"额外的使用额度\",\"topupRowValueLabel\":\"已使用 0%（剩余 10 亿个词元）\",\"topupEligible\":true,\"entryPointCtaLabel\":\"升级\"}}\n"

func TestParseFlightSubscription(t *testing.T) {
	subscription, err := parseFlightSubscription([]byte(quotaFlightSample))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	quota, err := quotaFromSubscription(subscription)
	if err != nil {
		t.Fatalf("map: %v", err)
	}
	if !quota.Found || quota.Plan != "Muse 免费版" {
		t.Fatalf("unexpected plan: %+v", *quota)
	}
	if quota.WeeklyUsedPct != 16 {
		t.Fatalf("weekly used = %v", quota.WeeklyUsedPct)
	}
	if quota.WeeklyReset != "每周限额将在 10月31日重置" {
		t.Fatalf("reset = %q", quota.WeeklyReset)
	}
	if quota.ExtraUsedPct == nil || *quota.ExtraUsedPct != 0 {
		t.Fatalf("extra used = %v", quota.ExtraUsedPct)
	}
	if quota.ExtraLeft != "已使用 0%（剩余 10 亿个词元）" {
		t.Fatalf("extra left = %q", quota.ExtraLeft)
	}
}

func TestParseFlightSubscriptionFailure(t *testing.T) {
	body := "0:{\"a\":\"$@1\"}\n1:{\"success\":false,\"message\":\"unauthenticated\"}\n"
	if _, err := parseFlightSubscription([]byte(body)); err == nil || !strings.Contains(err.Error(), "unauthenticated") {
		t.Fatalf("expected upstream failure, got %v", err)
	}
	html := "<!DOCTYPE html><html><body>Sign in to continue</body></html>"
	if _, err := parseFlightSubscription([]byte(html)); err == nil {
		t.Fatal("expected error for html body")
	}
}

func TestQuotaFromSubscriptionPartial(t *testing.T) {
	subscription := map[string]any{
		"tier":         map[string]any{"tierId": "hatch_plus"},
		"topupTotal":   float64(0),
		"topupBalance": float64(0),
	}
	quota, err := quotaFromSubscription(subscription)
	if err != nil {
		t.Fatalf("map: %v", err)
	}
	if quota.Plan != "hatch_plus" || quota.ExtraUsedPct != nil {
		t.Fatalf("unexpected quota: %+v", *quota)
	}
}

func TestActionIDFromChunk(t *testing.T) {
	chunk := `let y=(0,g.createServerReference)("409b972cbb33d3e372f4ca9d3c8332677217064f62",g.callServer,void 0,g.findSourceMapURL,"fetchSubscriptionAction")`
	if id := actionIDFromChunk(chunk); id != "409b972cbb33d3e372f4ca9d3c8332677217064f62" {
		t.Fatalf("action id = %q", id)
	}
	if id := actionIDFromChunk("no action here"); id != "" {
		t.Fatalf("unexpected id %q", id)
	}
}

func TestQueryQuotaRediscoversActionAfter409FromRootChunks(t *testing.T) {
	const actionID = "0123456789abcdef0123456789abcdef01234567"
	var postCount atomic.Int32
	var fetchedSettings atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			postCount.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read action body: %v", err)
			}
			if r.URL.Path != "/" {
				http.Error(w, "Server Action unavailable.", http.StatusConflict)
				return
			}
			if got := r.Header.Get("Content-Type"); got != "text/plain;charset=UTF-8" {
				t.Errorf("Content-Type = %q", got)
			}
			if got := r.Header.Get("Next-Action"); got == actionID {
				if string(body) != `[{"includeAgreement":true}]` {
					t.Errorf("action body = %q", body)
				}
				w.Header().Set("Content-Type", "text/x-component")
				_, _ = io.WriteString(w, quotaFlightSample)
				return
			}
			http.Error(w, "Server Action unavailable.", http.StatusConflict)
			return
		}

		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/":
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, `<html><script src="/_next/static/chunks/app.js"></script><script src="/_next/static/chunks/loader.js"></script></html>`)
		case "/_next/static/chunks/app.js":
			_, _ = io.WriteString(w, `e.A(434304).then(({preloadHatchSettingsData:e})=>e({}))`)
		case "/_next/static/chunks/loader.js":
			_, _ = io.WriteString(w, `},56115,s=>{s.v(t=>Promise.all(["static/chunks/unrelated.js"]))},434304,s=>{s.v(t=>Promise.all(["static/chunks/settings~account.js"].map(t=>s.l(t))))}`)
		case "/_next/static/chunks/unrelated.js":
			t.Error("unrelated chunk was fetched")
		case "/_next/static/chunks/settings~account.js":
			fetchedSettings.Add(1)
			_, _ = io.WriteString(w, `createServerReference("`+actionID+`",g.callServer,void 0,g.findSourceMapURL,"fetchSubscriptionAction")`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	site, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	client := NewClientWithWSURL(site, server.Client(), "")
	quota, err := client.QueryQuota(context.Background(), protocol.AccountCredentials{
		ID:      "account-1",
		Cookies: map[string]string{"hatch_sess": "test-session"},
	})
	if err != nil {
		t.Fatalf("QueryQuota: %v", err)
	}
	if quota.Plan != "Muse 免费版" || quota.WeeklyUsedPct != 16 {
		t.Fatalf("unexpected quota: %+v", quota)
	}
	if got := postCount.Load(); got != 2 {
		t.Fatalf("action POST count = %d, want stale action plus rediscovered action", got)
	}
	if got := fetchedSettings.Load(); got != 1 {
		t.Fatalf("settings chunks fetched = %d, want 1", got)
	}
}

func TestQueryQuotaRediscoversActionFromImmutableChunks(t *testing.T) {
	const actionID = "0123456789abcdef0123456789abcdef01234567"
	for _, tc := range []struct {
		name   string
		app    string
		loader string
	}{
		{
			name:   "settings module",
			app:    `e.A(434304).then(({preloadHatchSettingsData:e})=>e({}))`,
			loader: `},56115,s=>{s.v(t=>Promise.all(["static/immutable/chunks/unrelated.js"]))},434304,s=>{s.v(t=>Promise.all(["static/immutable/chunks/settings~account.js"].map(t=>s.l(t))))}`,
		},
		{
			name:   "renamed settings module",
			app:    `e.A(434304).then(({preloadAccountPanel:e})=>e({}))`,
			loader: `},434304,s=>{s.v(t=>Promise.all(["static/immutable/chunks/settings~account.js"].map(t=>s.l(t))))}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fetchedSettings atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					if r.Header.Get("Next-Action") == actionID {
						w.Header().Set("Content-Type", "text/x-component")
						_, _ = io.WriteString(w, quotaFlightSample)
						return
					}
					http.Error(w, "Server Action unavailable.", http.StatusConflict)
					return
				}
				switch r.URL.Path {
				case "/":
					w.Header().Set("Content-Type", "text/html")
					_, _ = io.WriteString(w, `<html data-dpl-id="dpl_abc"><script src="/_next/static/immutable/chunks/app.js" async=""></script><script>self.__next_f.push([1,"2:I[1,[\"/_next/static/immutable/chunks/loader.js\"]]"])</script></html>`)
				case "/_next/static/immutable/chunks/app.js":
					_, _ = io.WriteString(w, tc.app)
				case "/_next/static/immutable/chunks/loader.js":
					_, _ = io.WriteString(w, tc.loader)
				case "/_next/static/immutable/chunks/unrelated.js":
					t.Error("unrelated chunk was fetched")
				case "/_next/static/immutable/chunks/settings~account.js":
					fetchedSettings.Add(1)
					_, _ = io.WriteString(w, `createServerReference)("`+actionID+`",g.callServer,void 0,g.findSourceMapURL,"fetchSubscriptionAction")`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			site, err := url.Parse(server.URL)
			if err != nil {
				t.Fatalf("parse server URL: %v", err)
			}
			client := NewClientWithWSURL(site, server.Client(), "")
			quota, err := client.QueryQuota(context.Background(), protocol.AccountCredentials{
				ID:      "account-1",
				Cookies: map[string]string{"hatch_sess": "test-session"},
			})
			if err != nil {
				t.Fatalf("QueryQuota: %v", err)
			}
			if quota.Plan != "Muse 免费版" {
				t.Fatalf("unexpected quota: %+v", quota)
			}
			if got := fetchedSettings.Load(); got != 1 {
				t.Fatalf("settings chunks fetched = %d, want 1", got)
			}
		})
	}
}

func TestQueryQuotaDoesNotRediscoverAfterRateLimitOrServerError(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var getCount atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					getCount.Add(1)
				}
				http.Error(w, http.StatusText(status), status)
			}))
			defer server.Close()

			site, err := url.Parse(server.URL)
			if err != nil {
				t.Fatalf("parse server URL: %v", err)
			}
			client := NewClientWithWSURL(site, server.Client(), "")
			_, err = client.QueryQuota(context.Background(), protocol.AccountCredentials{
				ID:      "account-1",
				Cookies: map[string]string{"hatch_sess": "test-session"},
			})
			if err == nil {
				t.Fatal("QueryQuota unexpectedly succeeded")
			}
			if got := getCount.Load(); got != 0 {
				t.Fatalf("homepage discovery GET count = %d, want 0", got)
			}
		})
	}
}
