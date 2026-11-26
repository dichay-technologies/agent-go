package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	cfant "proj.dichay.tech/agent-go/gateway/cloudflare/anthropic"
)

const (
	DefaultBaseURL     = "https://api.anthropic.com"
	DefaultModel       = "claude-sonnet-4-20250514"
	DefaultVersion     = "2023-06-01"
	defaultTimeout     = 60 * time.Second
	defaultMaxRetries  = 3
	defaultConcurrency = 16
	defaultPoll        = 500 * time.Millisecond
)

// APIError represents a non-2xx HTTP response from the Anthropic API.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("anthropic: HTTP %d: %s", e.Status, e.Body)
}

// Unwrap exposes typed gateway.ProviderError categories (ErrRateLimit, ErrRetryable, ErrAuth, ErrContextLength).
func (e *APIError) Unwrap() error {
	return gateway.ClassifyStatusCode("anthropic", e.Status, e.Body)
}

// Retryable reports whether the error (429, 529, 5xx) should be retried.
func (e *APIError) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status == 529 || e.Status >= 500
}

// Config configures the standalone Anthropic provider.
type Config struct {
	Name                string
	APIKey              string // defaults to ANTHROPIC_API_KEY
	BaseURL             string // defaults to DefaultBaseURL
	DefaultModel        string // defaults to DefaultModel
	Version             string // defaults to DefaultVersion
	EnablePromptCache   bool
	HTTPClient          *http.Client
	MaxRetries          int
	MaxBatchConcurrency int
	EnableNativeBatch   bool // uses /v1/messages/batches when true
	BatchPollInterval   time.Duration
}

// Provider implements gateway.Provider and gateway.BatchProvider for Anthropic.
type Provider struct {
	name       string
	cfg        Config
	baseURL    string
	dialect    cfant.Dialect
	httpClient *http.Client
}

// New creates a standalone Anthropic Provider.
func New(cfg Config) (*Provider, error) {
	if cfg.APIKey == "" {
		cfg.APIKey = os.Getenv("ANTHROPIC_API_KEY")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("anthropic: APIKey is required")
	}
	if cfg.Name == "" {
		cfg.Name = "anthropic"
	}
	if cfg.DefaultModel == "" {
		cfg.DefaultModel = DefaultModel
	}
	if cfg.Version == "" {
		cfg.Version = DefaultVersion
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.MaxBatchConcurrency <= 0 {
		cfg.MaxBatchConcurrency = defaultConcurrency
	}
	if cfg.BatchPollInterval <= 0 {
		cfg.BatchPollInterval = defaultPoll
	}
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}

	return &Provider{
		name:    cfg.Name,
		cfg:     cfg,
		baseURL: baseURL,
		dialect: cfant.Dialect{
			APIKey:            cfg.APIKey,
			Version:           cfg.Version,
			EnablePromptCache: cfg.EnablePromptCache,
		},
		httpClient: client,
	}, nil
}

func (p *Provider) Name() string { return p.name }

func (p *Provider) Complete(ctx context.Context, req gateway.Request) (gateway.Response, error) {
	model := req.Model
	if model == "" {
		model = p.cfg.DefaultModel
	}
	suffix, headers, body, err := p.dialect.BuildRequest(model, req)
	if err != nil {
		return gateway.Response{}, fmt.Errorf("anthropic: build request: %w", err)
	}
	raw, err := p.doWithRetry(ctx, http.MethodPost, p.baseURL+"/"+strings.TrimLeft(suffix, "/"), headers, body)
	if err != nil {
		return gateway.Response{}, err
	}
	return p.dialect.ParseResponse(model, raw)
}

// Stream implements gateway.StreamProvider over Anthropic Messages SSE (stream=true).
func (p *Provider) Stream(ctx context.Context, req gateway.Request) iter.Seq2[gateway.StreamChunk, error] {
	return func(yield func(gateway.StreamChunk, error) bool) {
		model := req.Model
		if model == "" {
			model = p.cfg.DefaultModel
		}
		suffix, headers, body, err := p.dialect.BuildRequest(model, req)
		if err != nil {
			yield(gateway.StreamChunk{}, fmt.Errorf("anthropic: build stream request: %w", err))
			return
		}
		var bodyMap map[string]any
		if err := json.Unmarshal(body, &bodyMap); err != nil {
			yield(gateway.StreamChunk{}, err)
			return
		}
		bodyMap["stream"] = true
		streamBody, _ := json.Marshal(bodyMap)

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/"+strings.TrimLeft(suffix, "/"), bytes.NewReader(streamBody))
		if err != nil {
			yield(gateway.StreamChunk{}, fmt.Errorf("anthropic: create stream request: %w", err))
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "text/event-stream")
		for k, v := range headers {
			httpReq.Header.Set(k, v)
		}

		res, err := p.httpClient.Do(httpReq)
		if err != nil {
			yield(gateway.StreamChunk{}, fmt.Errorf("anthropic: stream request failed: %w", err))
			return
		}
		defer func() { _ = res.Body.Close() }()

		if res.StatusCode < 200 || res.StatusCode >= 300 {
			errBody, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
			yield(gateway.StreamChunk{}, &APIError{Status: res.StatusCode, Body: strings.TrimSpace(string(errBody))})
			return
		}

		type antAccTool struct {
			id   string
			name string
			args strings.Builder
		}
		toolsByIdx := make(map[int]*antAccTool)
		maxIdx := -1
		var usage gateway.Usage
		var hasUsage bool

		for ev, sseErr := range gateway.ReadSSE(res.Body) {
			if sseErr != nil {
				yield(gateway.StreamChunk{}, sseErr)
				return
			}
			data := strings.TrimSpace(ev.Data)
			if data == "" {
				continue
			}
			var frame struct {
				Type    string `json:"type"`
				Index   int    `json:"index"`
				Message *struct {
					Usage struct {
						InputTokens          int64 `json:"input_tokens"`
						CacheReadInputTokens int64 `json:"cache_read_input_tokens"`
					} `json:"usage"`
				} `json:"message"`
				ContentBlock *struct {
					Type string `json:"type"`
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"content_block"`
				Delta *struct {
					Type        string `json:"type"`
					Text        string `json:"text"`
					PartialJSON string `json:"partial_json"`
				} `json:"delta"`
				Usage *struct {
					OutputTokens int64 `json:"output_tokens"`
				} `json:"usage"`
			}
			if json.Unmarshal([]byte(data), &frame) != nil {
				continue
			}
			switch frame.Type {
			case "message_start":
				if frame.Message != nil {
					usage.InputTokens = frame.Message.Usage.InputTokens
					usage.CachedTokens = frame.Message.Usage.CacheReadInputTokens
					hasUsage = true
				}
			case "content_block_start":
				if frame.ContentBlock != nil && frame.ContentBlock.Type == "tool_use" {
					toolsByIdx[frame.Index] = &antAccTool{
						id:   frame.ContentBlock.ID,
						name: frame.ContentBlock.Name,
					}
					if frame.Index > maxIdx {
						maxIdx = frame.Index
					}
				}
			case "content_block_delta":
				if frame.Delta != nil {
					if frame.Delta.Type == "text_delta" && frame.Delta.Text != "" {
						if !yield(gateway.StreamChunk{Text: frame.Delta.Text}, nil) {
							return
						}
					} else if frame.Delta.Type == "input_json_delta" {
						if t, ok := toolsByIdx[frame.Index]; ok {
							t.args.WriteString(frame.Delta.PartialJSON)
						}
					}
				}
			case "message_delta":
				if frame.Usage != nil {
					usage.OutputTokens = frame.Usage.OutputTokens
					hasUsage = true
				}
			}
		}

		var calls []gateway.ToolCall
		for idx := 0; idx <= maxIdx; idx++ {
			if t, ok := toolsByIdx[idx]; ok {
				rawArgs := t.args.String()
				if rawArgs == "" {
					rawArgs = "{}"
				}
				calls = append(calls, gateway.ToolCall{
					ID:        t.id,
					Name:      t.name,
					Arguments: json.RawMessage(rawArgs),
				})
			}
		}
		var uPtr *gateway.Usage
		if hasUsage {
			uCopy := usage
			uPtr = &uCopy
		}
		yield(gateway.StreamChunk{
			ToolCalls: calls,
			Usage:     uPtr,
			Done:      true,
		}, nil)
	}
}

func (p *Provider) BatchComplete(ctx context.Context, reqs []gateway.Request) ([]gateway.Response, []error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if p.cfg.EnableNativeBatch {
		return p.batchNative(ctx, reqs)
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

func (p *Provider) batchNative(ctx context.Context, reqs []gateway.Request) ([]gateway.Response, []error) {
	type batchReq struct {
		CustomID string          `json:"custom_id"`
		Params   json.RawMessage `json:"params"`
	}
	items := make([]batchReq, len(reqs))
	for i, r := range reqs {
		model := r.Model
		if model == "" {
			model = p.cfg.DefaultModel
		}
		_, _, body, err := p.dialect.BuildRequest(model, r)
		if err != nil {
			return failAll(len(reqs), err)
		}
		items[i] = batchReq{
			CustomID: fmt.Sprintf("req_%d", i),
			Params:   body,
		}
	}

	submitBody, _ := json.Marshal(struct {
		Requests []batchReq `json:"requests"`
	}{Requests: items})

	headers := map[string]string{
		"x-api-key":         p.cfg.APIKey,
		"anthropic-version": p.cfg.Version,
	}
	rawSubmit, err := p.doWithRetry(ctx, http.MethodPost, p.baseURL+"/v1/messages/batches", headers, submitBody)
	if err != nil {
		return failAll(len(reqs), err)
	}

	var batch struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rawSubmit, &batch); err != nil || batch.ID == "" {
		return failAll(len(reqs), fmt.Errorf("anthropic: decode batch id: %w", err))
	}

	ticker := time.NewTicker(p.cfg.BatchPollInterval)
	defer ticker.Stop()
	var resultsURL string

pollLoop:
	for {
		select {
		case <-ctx.Done():
			return failAll(len(reqs), ctx.Err())
		case <-ticker.C:
			rawPoll, err := p.doWithRetry(ctx, http.MethodGet, p.baseURL+"/v1/messages/batches/"+batch.ID, headers, nil)
			if err != nil {
				return failAll(len(reqs), err)
			}
			var st struct {
				ProcessingStatus string `json:"processing_status"`
				ResultsURL       string `json:"results_url"`
			}
			if err := json.Unmarshal(rawPoll, &st); err != nil {
				return failAll(len(reqs), fmt.Errorf("anthropic: decode batch status: %w", err))
			}
			if st.ProcessingStatus == "ended" {
				resultsURL = st.ResultsURL
				break pollLoop
			}
		}
	}

	if !strings.HasPrefix(resultsURL, "http") {
		resultsURL = p.baseURL + "/" + strings.TrimLeft(resultsURL, "/")
	}
	rawResults, err := p.doWithRetry(ctx, http.MethodGet, resultsURL, headers, nil)
	if err != nil {
		return failAll(len(reqs), err)
	}

	resps := make([]gateway.Response, len(reqs))
	errs := make([]error, len(reqs))
	for i := range errs {
		errs[i] = fmt.Errorf("anthropic: batch missing response %d", i)
	}

	scanner := bufio.NewScanner(bytes.NewReader(rawResults))
	scanner.Buffer(make([]byte, 64*1024), 16<<20)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var item struct {
			CustomID string `json:"custom_id"`
			Result   struct {
				Type    string          `json:"type"`
				Message json.RawMessage `json:"message"`
				Error   *struct {
					Message string `json:"message"`
				} `json:"error"`
			} `json:"result"`
		}
		if err := json.Unmarshal(line, &item); err != nil {
			continue
		}
		var idx int
		if _, scanErr := fmt.Sscanf(item.CustomID, "req_%d", &idx); scanErr != nil || idx < 0 || idx >= len(reqs) {
			continue
		}
		if item.Result.Type == "errored" && item.Result.Error != nil {
			errs[idx] = fmt.Errorf("anthropic: batch item error: %s", item.Result.Error.Message)
			continue
		}
		model := reqs[idx].Model
		if model == "" {
			model = p.cfg.DefaultModel
		}
		resps[idx], errs[idx] = p.dialect.ParseResponse(model, item.Result.Message)
	}
	return resps, errs
}

func (p *Provider) doWithRetry(ctx context.Context, method, endpoint string, headers map[string]string, body []byte) ([]byte, error) {
	backoff := 500 * time.Millisecond
	maxRetries := p.cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}

	for attempt := 0; ; attempt++ {
		raw, err := p.doOnce(ctx, method, endpoint, headers, body)
		var apiErr *APIError
		if err == nil || !errors.As(err, &apiErr) || !apiErr.Retryable() || attempt >= maxRetries {
			return raw, err
		}
		wait := backoff<<attempt + time.Duration(rand.Int64N(int64(backoff)))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *Provider) doOnce(ctx context.Context, method, endpoint string, headers map[string]string, body []byte) ([]byte, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("anthropic: create request: %w", err)
	}
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		httpReq.Header.Set(k, v)
	}

	res, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("anthropic: request failed: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("anthropic: read body: %w", err)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, &APIError{Status: res.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	return data, nil
}

func failAll(n int, err error) ([]gateway.Response, []error) {
	resps := make([]gateway.Response, n)
	errs := make([]error, n)
	for i := range errs {
		errs[i] = err
	}
	return resps, errs
}

