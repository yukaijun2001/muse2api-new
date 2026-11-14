package hatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"muse2api/internal/model"
	"muse2api/internal/muse/protocol"
)

// Usage and subscription state are exposed through a Next.js server action,
// not a REST endpoint. The action id is compiled into a lazily loaded chunk,
// so it changes with every deployment: the fallback below is tried first and a
// chunk scan rediscovers the id when the deployment rotates it.
const (
	quotaActionName     = "fetchSubscriptionAction"
	quotaFallbackAction = "409b972cbb33d3e372f4ca9d3c8332677217064f62"
	quotaPagePath       = "/"
	quotaActionTTL      = 12 * time.Hour
	quotaScanWorkers    = 12
	quotaPageChunkLimit = 96
	quotaSettingsLimit  = 32
	quotaLazyChunkLimit = 400
	quotaChunkMaxBytes  = 2 << 20
	quotaBodyMaxBytes   = 4 << 20
)

// Chunk paths are matched with their directory prefix kept: older deployments
// serve them from static/chunks/, newer ones from static/immutable/chunks/.
var (
	chunkURLRe       = regexp.MustCompile(`(/_next/static/(?:immutable/)?chunks/[A-Za-z0-9_~./-]+\.js[^"'\s\\]*)`)
	chunkNameRe      = regexp.MustCompile(`(static/(?:immutable/)?chunks/[A-Za-z0-9_~./-]+\.js)`)
	settingsModuleRe = regexp.MustCompile(`\.A\((\d+)\)\.then\(\(\{[^}]*Settings`)
	actionRefRe      = regexp.MustCompile(`"([0-9a-f]{40,})",[^)]{0,400}"fetchSubscriptionAction"`)
	dplRe            = regexp.MustCompile(`dpl_[A-Za-z0-9]+`)
	loginMarkers     = []string{"sign in", "log in", "login", "登录", "signin"}
)

// QueryQuota reads the current subscription snapshot for one account. It uses
// the same HTTP session (cookies) as the gateway bootstrap, but never wakes or
// contacts the VM.
func (c *Client) QueryQuota(ctx context.Context, creds protocol.AccountCredentials) (*model.Quota, error) {
	if c.site == nil {
		return nil, errors.New("额度查询缺少上游地址")
	}
	if len(creds.Cookies) == 0 {
		return nil, protocol.ErrSessionExpired
	}
	// The deadline covers queueing as well as the request itself, so an admin
	// refresh cannot wait out a slow background scan before its own work starts.
	ctx, cancel := context.WithTimeout(ctx, 50*time.Second)
	defer cancel()
	// A cold action-id cache can trigger a multi-chunk scan. Serialize it per
	// account instead of launching the same scan from auto-check and admin.
	select {
	case c.quotaGate <- struct{}{}:
		defer func() { <-c.quotaGate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	actionID := c.cachedQuotaAction()
	subscription, err := c.callQuotaAction(ctx, creds.Cookies, actionID)
	if err == nil {
		c.rememberQuotaAction(actionID)
		return quotaFromSubscription(subscription)
	}
	if errors.Is(err, protocol.ErrSessionExpired) {
		return nil, err
	}
	if !quotaActionNeedsRediscovery(err) {
		return nil, err
	}

	// The cached/fallback id was rejected (new deployment). Rebuild it from
	// the chunks the homepage loads and retry once.
	discovered, discoverErr := c.discoverQuotaAction(ctx, creds.Cookies)
	if discoverErr != nil {
		return nil, fmt.Errorf("额度接口定位失败：%v（首次原因：%v）", discoverErr, err)
	}
	subscription, err = c.callQuotaAction(ctx, creds.Cookies, discovered)
	if err != nil {
		return nil, err
	}
	c.rememberQuotaAction(discovered)
	return quotaFromSubscription(subscription)
}

func quotaActionNeedsRediscovery(err error) bool {
	var actionErr *quotaActionHTTPError
	if !errors.As(err, &actionErr) {
		return false
	}
	return actionErr.status >= 400 && actionErr.status < 500 && actionErr.status != http.StatusTooManyRequests
}

type quotaActionHTTPError struct {
	status int
	body   string
}

func (e *quotaActionHTTPError) Error() string {
	return fmt.Sprintf("额度接口 HTTP %d: %s", e.status, e.body)
}

func (c *Client) cachedQuotaAction() string {
	c.quotaMu.Lock()
	defer c.quotaMu.Unlock()
	if c.quotaActionID != "" && time.Since(c.quotaActionAt) < quotaActionTTL {
		return c.quotaActionID
	}
	return quotaFallbackAction
}

func (c *Client) rememberQuotaAction(id string) {
	if id == "" {
		return
	}
	c.quotaMu.Lock()
	c.quotaActionID = id
	c.quotaActionAt = time.Now()
	c.quotaMu.Unlock()
}

// callQuotaAction invokes fetchSubscriptionAction and parses the Flight reply.
func (c *Client) callQuotaAction(ctx context.Context, cookies map[string]string, actionID string) (map[string]any, error) {
	if actionID == "" {
		return nil, errors.New("空的额度接口标识")
	}
	base := strings.TrimRight(c.site.String(), "/")
	endpoint := base + quotaPagePath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(`[{"includeAgreement":true}]`))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/x-component")
	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	req.Header.Set("Next-Action", actionID)
	req.Header.Set("Cookie", cookieHeader(cookies))
	req.Header.Set("Origin", c.site.Scheme+"://"+c.site.Host)
	req.Header.Set("Referer", endpoint)
	req.Header.Set("User-Agent", chromeUserAgent)
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Dest", "empty")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, quotaBodyMaxBytes))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, protocol.ErrSessionExpired
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &quotaActionHTTPError{status: resp.StatusCode, body: truncate(body, 160)}
	}
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/x-component") {
		if looksLikeLoginPage(body) {
			return nil, protocol.ErrSessionExpired
		}
		return nil, fmt.Errorf("额度接口返回了 %s，接口标识可能已过期", contentType)
	}
	return parseFlightSubscription(body)
}

// parseFlightSubscription extracts the subscription object from a React
// Flight stream: rows are "<id>:<json>", one per line.
func parseFlightSubscription(body []byte) (map[string]any, error) {
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		idx := bytes.IndexByte(line, ':')
		if idx <= 0 || idx == len(line)-1 {
			continue
		}
		var payload map[string]any
		if json.Unmarshal(line[idx+1:], &payload) != nil {
			continue
		}
		if success, ok := payload["success"].(bool); ok && !success {
			message := asString(payload["message"])
			if message == "" {
				message = "上游返回 success=false"
			}
			return nil, errors.New(message)
		}
		if subscription, ok := payload["subscription"].(map[string]any); ok {
			return subscription, nil
		}
	}
	return nil, errors.New("额度响应中没有 subscription 数据")
}

func quotaFromSubscription(subscription map[string]any) (*model.Quota, error) {
	tier, _ := subscription["tier"].(map[string]any)
	usage, _ := subscription["usage"].(map[string]any)
	quota := &model.Quota{Found: true, CheckedAt: time.Now().Unix()}
	quota.Plan = asString(tier["name"])
	if quota.Plan == "" {
		quota.Plan = asString(tier["tierId"])
	}
	quota.WeeklyReset = asString(subscription["statusSubtitle"])
	quota.WeeklyUsedPct = asFloat(usage["percentUsed"])
	if total := asFloat(subscription["topupTotal"]); total > 0 {
		balance := asFloat(subscription["topupBalance"])
		used := (total - balance) / total * 100
		used = clampPercent(used)
		quota.ExtraUsedPct = &used
	}
	quota.ExtraLeft = asString(subscription["topupRowValueLabel"])
	if quota.ExtraLeft != "" {
		quota.ExtraExpires = "从不过期"
	}
	if quota.Plan == "" && quota.WeeklyReset == "" && quota.ExtraLeft == "" {
		return nil, errors.New("额度响应缺少可识别字段")
	}
	return quota, nil
}

func clampPercent(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func asFloat(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	case string:
		var f float64
		if _, err := fmt.Sscanf(strings.TrimSpace(v), "%f", &f); err == nil {
			return f
		}
	}
	return 0
}

func looksLikeLoginPage(body []byte) bool {
	head := body
	if len(head) > 8192 {
		head = head[:8192]
	}
	lower := strings.ToLower(string(head))
	for _, marker := range loginMarkers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// discoverQuotaAction follows the homepage's lazy module table to the settings
// chunks instead of fetching unrelated chunks.
func (c *Client) discoverQuotaAction(ctx context.Context, cookies map[string]string) (string, error) {
	base := strings.TrimRight(c.site.String(), "/")
	html, err := c.fetchText(ctx, cookies, base+quotaPagePath, quotaBodyMaxBytes)
	if err != nil {
		return "", err
	}
	dpl := dplRe.FindString(html)

	firstScreen := make(map[string]struct{})
	for _, match := range chunkURLRe.FindAllStringSubmatch(html, -1) {
		url := match[1]
		if strings.HasPrefix(url, "/") {
			url = base + url
		}
		firstScreen[url] = struct{}{}
	}
	if len(firstScreen) == 0 {
		if looksLikeLoginPage([]byte(html)) {
			return "", protocol.ErrSessionExpired
		}
		return "", errors.New("首页没有引用任何可识别的 Next.js 脚本")
	}
	urls := make([]string, 0, len(firstScreen))
	for url := range firstScreen {
		urls = append(urls, url)
	}
	sort.Strings(urls)
	if len(urls) > quotaPageChunkLimit {
		urls = urls[:quotaPageChunkLimit]
	}
	firstScreenText := c.fetchChunks(ctx, cookies, urls)
	if id := actionIDFromChunk(firstScreenText); id != "" {
		return id, nil
	}

	settingsURLs, settingsErr := settingsChunkURLs(firstScreenText, base, dpl)
	fetched := make(map[string]struct{}, len(urls)+len(settingsURLs))
	for _, url := range urls {
		fetched[url] = struct{}{}
	}
	if settingsErr == nil {
		if id := actionIDFromChunk(c.fetchChunks(ctx, cookies, settingsURLs)); id != "" {
			return id, nil
		}
		settingsErr = errors.New("设置脚本中没有找到额度接口标识")
		for _, url := range settingsURLs {
			fetched[url] = struct{}{}
		}
	}

	// The settings module is located by name, which a deployment can rename.
	// Fall back to every lazily loaded chunk the homepage can reach.
	lazyURLs := chunkURLsFromNames(chunkNameRe.FindAllStringSubmatch(firstScreenText, -1), base, dpl, fetched)
	if len(lazyURLs) > quotaLazyChunkLimit {
		lazyURLs = lazyURLs[:quotaLazyChunkLimit]
	}
	if len(lazyURLs) > 0 {
		if id := actionIDFromChunk(c.fetchChunks(ctx, cookies, lazyURLs)); id != "" {
			return id, nil
		}
	}
	return "", settingsErr
}

// settingsChunkURLs resolves the chunks behind the settings module's lazy
// loader entry in the homepage's module table.
func settingsChunkURLs(firstScreenText, base, dpl string) ([]string, error) {
	module := settingsModuleRe.FindStringSubmatch(firstScreenText)
	if len(module) < 2 {
		return nil, errors.New("首页应用中没有找到设置模块")
	}
	loaderRe, err := regexp.Compile(`(?:^|[,{\[])` + regexp.QuoteMeta(module[1]) + `,\w+=>\{\w+\.v\(\w+=>Promise\.all\(\[([^\]]*)`)
	if err != nil {
		return nil, err
	}
	loader := loaderRe.FindStringSubmatch(firstScreenText)
	if len(loader) < 2 {
		return nil, errors.New("首页应用中没有找到设置模块加载表")
	}
	settingsURLs := chunkURLsFromNames(chunkNameRe.FindAllStringSubmatch(loader[1], -1), base, dpl, nil)
	if len(settingsURLs) == 0 {
		return nil, errors.New("设置模块加载表没有引用脚本")
	}
	if len(settingsURLs) > quotaSettingsLimit {
		settingsURLs = settingsURLs[:quotaSettingsLimit]
	}
	return settingsURLs, nil
}

func chunkURLsFromNames(matches [][]string, base, dpl string, skip map[string]struct{}) []string {
	seen := map[string]struct{}{}
	urls := make([]string, 0, len(matches))
	for _, match := range matches {
		url := base + "/_next/" + match[1]
		if dpl != "" {
			url += "?dpl=" + dpl
		}
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		if _, ok := skip[url]; ok {
			continue
		}
		if _, ok := skip[base+"/_next/"+match[1]]; ok {
			continue
		}
		urls = append(urls, url)
	}
	sort.Strings(urls)
	return urls
}

func (c *Client) fetchChunks(ctx context.Context, cookies map[string]string, urls []string) string {
	sem := make(chan struct{}, quotaScanWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var texts strings.Builder

	for _, url := range urls {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			defer func() { <-sem }()
			text, err := c.fetchText(ctx, cookies, url, quotaChunkMaxBytes)
			if err != nil {
				return
			}
			mu.Lock()
			texts.WriteString(text)
			texts.WriteByte('\n')
			mu.Unlock()
		}(url)
	}
	wg.Wait()
	return texts.String()
}

func actionIDFromChunk(text string) string {
	match := actionRefRe.FindStringSubmatch(text)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func (c *Client) fetchText(ctx context.Context, cookies map[string]string, rawURL string, limit int64) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Cookie", cookieHeader(cookies))
	req.Header.Set("User-Agent", chromeUserAgent)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return "", err
	}
	return string(data), nil
}
