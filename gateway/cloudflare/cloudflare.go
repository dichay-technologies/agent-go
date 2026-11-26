package cloudflare

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	cfopenai "proj.dichay.tech/agent-go/gateway/cloudflare/openai"
)

const (
	defaultModel               = "@cf/meta/llama-3.3-70b-instruct-fp8-fast"
	defaultTimeout             = 60 * time.Second
	defaultBatchPollInterval   = 200 * time.Millisecond
	defaultMaxBatchConcurrency = 16
)

// Dialect adapts normalized gateway.Request / gateway.Response to a specific
// upstream provider wire format behind Cloudflare.
type Dialect interface {
	Name() string
	BuildRequest(model string, req gateway.Request) (pathSuffix string, headers map[string]string, body []byte, err error)
	ParseResponse(model string, raw []byte) (gateway.Response, error)
}

// BatchDialect is an optional extension of Dialect for native asynchronous batching
// (e.g. Cloudflare Workers AI ?queueRequest=true).
type BatchDialect interface {
	Dialect
	BuildBatchRequest(model string, reqs []gateway.Request) (pathSuffix string, headers map[string]string, body []byte, err error)
	ParseBatchSubmit(raw []byte) (requestID string, err error)
	BatchPollPath(model, requestID string) string
	ParseBatchPoll(model string, count int, raw []byte) (done bool, resps []gateway.Response, errs []error, err error)
}

// Config configures the Cloudflare provider.
type Config struct {
	// Name overrides the registered provider name (defaults to "cloudflare").
	Name string
	// AccountID is your Cloudflare account ID (required unless BaseURL is set).
	AccountID string
	// APIToken is the bearer token (Cloudflare API Token or upstream key).
	APIToken string
	// GatewayID, when set, routes through Cloudflare AI Gateway:
	// https://gateway.ai.cloudflare.com/v1/{AccountID}/{GatewayID}/{UpstreamProvider}
	GatewayID string
	// UpstreamProvider specifies the AI Gateway provider segment (e.g. "workers-ai",
	// "openai", "anthropic", "google-ai-studio", "compat"). Defaults to "workers-ai".
	UpstreamProvider string
	// AIGatewayToken is an optional cf-aig-authorization token if AI Gateway auth is enabled.
	AIGatewayToken string
	// BaseURL overrides the computed base URL (useful for testing or custom proxies).
	BaseURL string
	// DefaultModel is used when Request.Model is empty.
	DefaultModel string
	// Dialect selects the request/response wire format. Defaults to openai.Dialect{}.
	Dialect Dialect
	// HTTPClient overrides the default http.Client.
	HTTPClient *http.Client

	// CacheTTL sets cf-aig-cache-ttl (in seconds) for Cloudflare AI Gateway response caching.
	CacheTTL time.Duration
	// EnableSessionAffinity sets x-session-affinity header from ProjectID + SystemPrefix
	// to improve Workers AI prefix cache hit rates.
	EnableSessionAffinity bool

	// EnableAsyncBatch uses BatchDialect (when implemented by Dialect) in BatchComplete.
	// When false or unsupported, BatchComplete fans out concurrently up to MaxBatchConcurrency.
	EnableAsyncBatch bool
	// BatchPollInterval controls how often async batch status is polled (defaults to 200ms).
	BatchPollInterval time.Duration
	// MaxBatchConcurrency caps concurrent HTTP calls in fan-out BatchComplete (defaults to 16).
	MaxBatchConcurrency int
}

// Provider implements gateway.Provider and gateway.BatchProvider against
// Cloudflare Workers AI or Cloudflare AI Gateway.
type Provider struct {
	name       string
	cfg        Config
	baseURL    string
	dialect    Dialect
	httpClient *http.Client
}

// New creates a new Cloudflare Provider.
func New(cfg Config) (*Provider, error) {
	if cfg.BaseURL == "" && cfg.AccountID == "" {
		return nil, fmt.Errorf("cloudflare: AccountID or BaseURL is required")
	}
	if cfg.Name == "" {
		cfg.Name = "cloudflare"
	}
	if cfg.DefaultModel == "" {
		cfg.DefaultModel = defaultModel
	}
	if cfg.Dialect == nil {
		cfg.Dialect = cfopenai.Dialect{}
	}
	if cfg.BatchPollInterval <= 0 {
		cfg.BatchPollInterval = defaultBatchPollInterval
	}
	if cfg.MaxBatchConcurrency <= 0 {
		cfg.MaxBatchConcurrency = defaultMaxBatchConcurrency
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}

	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		if cfg.GatewayID != "" {
			upstream := cfg.UpstreamProvider
			if upstream == "" {
				upstream = "workers-ai"
			}
			baseURL = fmt.Sprintf(
				"https://gateway.ai.cloudflare.com/v1/%s/%s/%s",
				cfg.AccountID, cfg.GatewayID, strings.Trim(upstream, "/"),
			)
		} else {
			baseURL = fmt.Sprintf(
				"https://api.cloudflare.com/client/v4/accounts/%s/ai",
				cfg.AccountID,
			)
		}
	}

	return &Provider{
		name:       cfg.Name,
		cfg:        cfg,
		baseURL:    baseURL,
		dialect:    cfg.Dialect,
		httpClient: client,
	}, nil
}

// Name returns the configured provider name.
func (p *Provider) Name() string {
	return p.name
}

// Complete sends a normalized Request using the configured Dialect.
func (p *Provider) Complete(ctx context.Context, req gateway.Request) (gateway.Response, error) {
	model := req.Model
	if model == "" {
		model = p.cfg.DefaultModel
	}

	suffix, extraHeaders, body, err := p.dialect.BuildRequest(model, req)
	if err != nil {
		return gateway.Response{}, fmt.Errorf("cloudflare (%s): build request: %w", p.dialect.Name(), err)
	}

	headers := p.cacheHeaders(req, extraHeaders)
	respBody, err := p.doHTTP(ctx, http.MethodPost, suffix, headers, body)
	if err != nil {
		return gateway.Response{}, err
	}

	resp, err := p.dialect.ParseResponse(model, respBody)
	if err != nil {
		return gateway.Response{}, fmt.Errorf("cloudflare (%s): %w", p.dialect.Name(), err)
	}
	return resp, nil
}

// Stream implements gateway.StreamProvider over SSE when using the OpenAI-compatible dialect,
// and gracefully falls back to a single chunk from Complete for non-streaming dialects.
func (p *Provider) Stream(ctx context.Context, req gateway.Request) iter.Seq2[gateway.StreamChunk, error] {
	return func(yield func(gateway.StreamChunk, error) bool) {
		if p.dialect.Name() != "openai" {
			resp, err := p.Complete(ctx, req)
			if err != nil {
				yield(gateway.StreamChunk{}, err)
				return
			}
			u := resp.Usage
			yield(gateway.StreamChunk{
				Text:      resp.Text,
				ToolCalls: resp.ToolCalls,
				Usage:     &u,
				Done:      true,
			}, nil)
			return
		}

		model := req.Model
		if model == "" {
			model = p.cfg.DefaultModel
		}
		suffix, extraHeaders, body, err := p.dialect.BuildRequest(model, req)
		if err != nil {
			yield(gateway.StreamChunk{}, err)
			return
		}
		var bodyMap map[string]any
		if err := json.Unmarshal(body, &bodyMap); err != nil {
			yield(gateway.StreamChunk{}, err)
			return
		}
		bodyMap["stream"] = true
		streamBody, _ := json.Marshal(bodyMap)

		endpoint := p.baseURL
		if suffix != "" {
			endpoint += "/" + strings.TrimLeft(suffix, "/")
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(streamBody))
		if err != nil {
			yield(gateway.StreamChunk{}, err)
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "text/event-stream")
		if p.cfg.APIToken != "" {
			httpReq.Header.Set("Authorization", "Bearer "+p.cfg.APIToken)
		}
		if p.cfg.AIGatewayToken != "" {
			httpReq.Header.Set("cf-aig-authorization", "Bearer "+p.cfg.AIGatewayToken)
		}
		for k, v := range p.cacheHeaders(req, extraHeaders) {
			httpReq.Header.Set(k, v)
		}

		httpResp, err := p.httpClient.Do(httpReq)
		if err != nil {
			yield(gateway.StreamChunk{}, err)
			return
		}
		defer func() { _ = httpResp.Body.Close() }()

		if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
			errBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20))
			yield(gateway.StreamChunk{}, gateway.ClassifyStatusCode(p.name, httpResp.StatusCode, strings.TrimSpace(string(errBody))))
			return
		}

		for ev, sseErr := range gateway.ReadSSE(httpResp.Body) {
			if sseErr != nil {
				yield(gateway.StreamChunk{}, sseErr)
				return
			}
			data := strings.TrimSpace(ev.Data)
			if data == "" || data == "[DONE]" {
				if data == "[DONE]" {
					break
				}
				continue
			}
			var frame struct {
				Choices []struct {
					Delta struct {
						Content string `json:"content"`
					} `json:"delta"`
				} `json:"choices"`
			}
			if json.Unmarshal([]byte(data), &frame) == nil && len(frame.Choices) > 0 && frame.Choices[0].Delta.Content != "" {
				if !yield(gateway.StreamChunk{Text: frame.Choices[0].Delta.Content}, nil) {
					return
				}
			}
		}
		yield(gateway.StreamChunk{Done: true}, nil)
	}
}

// BatchComplete executes a batch of requests. If EnableAsyncBatch is true and the
// Dialect implements BatchDialect, it submits a native async batch and polls until done.
// Otherwise, it fans out concurrently over the HTTP connection pool.
func (p *Provider) BatchComplete(ctx context.Context, reqs []gateway.Request) ([]gateway.Response, []error) {
	if len(reqs) == 0 {
		return nil, nil
	}

	if p.cfg.EnableAsyncBatch {
		if bd, ok := p.dialect.(BatchDialect); ok {
			return p.batchAsync(ctx, bd, reqs)
		}
	}

	resps := make([]gateway.Response, len(reqs))
	errs := make([]error, len(reqs))
	sem := make(chan struct{}, p.cfg.MaxBatchConcurrency)
	var wg sync.WaitGroup

	for i, req := range reqs {
		wg.Add(1)
		go func(idx int, r gateway.Request) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errs[idx] = ctx.Err()
				return
			}
			resps[idx], errs[idx] = p.Complete(ctx, r)
		}(i, req)
	}
	wg.Wait()
	return resps, errs
}

func (p *Provider) batchAsync(ctx context.Context, bd BatchDialect, reqs []gateway.Request) ([]gateway.Response, []error) {
	model := reqs[0].Model
	if model == "" {
		model = p.cfg.DefaultModel
	}

	suffix, extraHeaders, body, err := bd.BuildBatchRequest(model, reqs)
	if err != nil {
		return failAll(len(reqs), fmt.Errorf("cloudflare (%s): build batch: %w", bd.Name(), err))
	}

	rawSubmit, err := p.doHTTP(ctx, http.MethodPost, suffix, extraHeaders, body)
	if err != nil {
		return failAll(len(reqs), err)
	}

	reqID, err := bd.ParseBatchSubmit(rawSubmit)
	if err != nil {
		return failAll(len(reqs), fmt.Errorf("cloudflare (%s): %w", bd.Name(), err))
	}

	pollPath := bd.BatchPollPath(model, reqID)
	ticker := time.NewTicker(p.cfg.BatchPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return failAll(len(reqs), ctx.Err())
		case <-ticker.C:
			rawPoll, err := p.doHTTP(ctx, http.MethodGet, pollPath, extraHeaders, nil)
			if err != nil {
				return failAll(len(reqs), err)
			}
			done, resps, errs, err := bd.ParseBatchPoll(model, len(reqs), rawPoll)
			if err != nil {
				return failAll(len(reqs), err)
			}
			if done {
				return resps, errs
			}
		}
	}
}

func (p *Provider) doHTTP(ctx context.Context, method, suffix string, extraHeaders map[string]string, body []byte) ([]byte, error) {
	endpoint := p.baseURL
	if suffix != "" {
		endpoint += "/" + strings.TrimLeft(suffix, "/")
	}

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("cloudflare (%s): create request: %w", p.dialect.Name(), err)
	}
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	if p.cfg.APIToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.cfg.APIToken)
	}
	if p.cfg.AIGatewayToken != "" {
		httpReq.Header.Set("cf-aig-authorization", "Bearer "+p.cfg.AIGatewayToken)
	}
	for k, v := range extraHeaders {
		httpReq.Header.Set(k, v)
	}

	httpResp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("cloudflare (%s): request failed: %w", p.dialect.Name(), err)
	}
	defer func() { _ = httpResp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(httpResp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("cloudflare (%s): read response: %w", p.dialect.Name(), err)
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, gateway.ClassifyStatusCode(p.name, httpResp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return respBody, nil
}

func (p *Provider) cacheHeaders(req gateway.Request, base map[string]string) map[string]string {
	out := make(map[string]string, len(base)+3)
	for k, v := range base {
		out[k] = v
	}
	if req.BypassCache {
		out["cf-aig-skip-cache"] = "true"
	} else if p.cfg.CacheTTL > 0 {
		secs := int(p.cfg.CacheTTL.Seconds())
		if secs > 0 {
			out["cf-aig-cache-ttl"] = strconv.Itoa(secs)
		}
	}
	if p.cfg.EnableSessionAffinity && (req.ProjectID != "" || req.SystemPrefix != "") {
		sum := sha256.Sum256([]byte(req.ProjectID + "\x00" + req.SystemPrefix))
		out["x-session-affinity"] = hex.EncodeToString(sum[:12])
	}
	return out
}

func failAll(n int, err error) ([]gateway.Response, []error) {
	resps := make([]gateway.Response, n)
	errs := make([]error, n)
	for i := range errs {
		errs[i] = err
	}
	return resps, errs
}
