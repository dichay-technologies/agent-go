package jev

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	cfjev "proj.dichay.tech/agent-go/gateway/cloudflare/jev"
)

const (
	DefaultBaseURL     = "https://api.typesafe.ai"
	DefaultModel       = "jev-latest"
	defaultTimeout     = 30 * time.Second
	defaultMaxRetries  = 3
	defaultConcurrency = 16
)

// APIError represents a non-2xx response from the Jev API.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("jev: HTTP %d: %s", e.Status, e.Body)
}

// Retryable reports whether the error (429 / 529) can be retried.
func (e *APIError) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status == 529
}

// Config configures the standalone TypeSafe System One (Jev) provider.
type Config struct {
	// Name overrides the registered provider name (defaults to "jev").
	Name string
	// APIKey is the TypeSafe API key. If empty, reads TYPESAFE_API_KEY from env.
	APIKey string
	// BaseURL overrides DefaultBaseURL.
	BaseURL string
	// DefaultModel overrides DefaultModel ("jev-latest").
	DefaultModel string
	// HTTPClient overrides the default http.Client.
	HTTPClient *http.Client
	// MaxRetries controls retries on 429/529 responses (defaults to 3; <0 disables).
	MaxRetries int
	// MaxBatchConcurrency caps concurrent HTTP calls in BatchComplete (defaults to 16).
	MaxBatchConcurrency int
}

// Provider implements gateway.Provider and gateway.BatchProvider for TypeSafe System One (Jev).
type Provider struct {
	name       string
	cfg        Config
	baseURL    string
	dialect    cfjev.Dialect
	httpClient *http.Client
}

// New creates a standalone Jev Provider.
func New(cfg Config) (*Provider, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("jev: APIKey is required")
	}
	if cfg.Name == "" {
		cfg.Name = "jev"
	}
	if cfg.DefaultModel == "" {
		cfg.DefaultModel = DefaultModel
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.MaxBatchConcurrency <= 0 {
		cfg.MaxBatchConcurrency = defaultConcurrency
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
		name:       cfg.Name,
		cfg:        cfg,
		baseURL:    baseURL,
		httpClient: client,
	}, nil
}

func (p *Provider) Name() string { return p.name }

func (p *Provider) Complete(ctx context.Context, req gateway.Request) (gateway.Response, error) {
	model := req.Model
	if model == "" {
		model = p.cfg.DefaultModel
	}

	suffix, _, body, err := p.dialect.BuildRequest(model, req)
	if err != nil {
		return gateway.Response{}, fmt.Errorf("jev: build request: %w", err)
	}

	raw, err := p.doWithRetry(ctx, p.baseURL+"/"+strings.TrimLeft(suffix, "/"), body)
	if err != nil {
		return gateway.Response{}, err
	}
	return p.dialect.ParseResponse(model, raw)
}

func (p *Provider) BatchComplete(ctx context.Context, reqs []gateway.Request) ([]gateway.Response, []error) {
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

func (p *Provider) doWithRetry(ctx context.Context, endpoint string, body []byte) ([]byte, error) {
	backoff := 500 * time.Millisecond
	maxRetries := p.cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}

	for attempt := 0; ; attempt++ {
		raw, err := p.doOnce(ctx, endpoint, body)
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

func (p *Provider) doOnce(ctx context.Context, endpoint string, body []byte) ([]byte, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("jev: create request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")

	res, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("jev: request failed: %w", err)
	}
	defer res.Body.Close()

	data, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("jev: read body: %w", err)
	}
	if res.StatusCode/100 != 2 {
		return nil, &APIError{Status: res.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	return data, nil
}
