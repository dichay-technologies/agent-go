package middleware

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"proj.dichay.tech/agent-go/gateway"
)

// RateLimit returns a Middleware that limits requests using a token bucket
// with rate rps (requests per second) and burst capacity.
func RateLimit(rps float64, burst int) gateway.Middleware {
	if burst <= 0 {
		burst = 1
	}
	var mu sync.Mutex
	tokens := float64(burst)
	last := time.Now()

	return func(next gateway.CompleteFunc) gateway.CompleteFunc {
		return func(ctx context.Context, req gateway.Request) (gateway.Response, error) {
			if rps <= 0 {
				return next(ctx, req)
			}
			for {
				mu.Lock()
				now := time.Now()
				tokens += now.Sub(last).Seconds() * rps
				if tokens > float64(burst) {
					tokens = float64(burst)
				}
				last = now
				if tokens >= 1 {
					tokens -= 1
					mu.Unlock()
					return next(ctx, req)
				}
				wait := time.Duration((1 - tokens) / rps * float64(time.Second))
				mu.Unlock()

				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return gateway.Response{}, ctx.Err()
				case <-timer.C:
				}
			}
		}
	}
}

// Budget returns a Middleware that enforces a maximum total token budget per key.
// If keyFn is nil, it groups by req.ProjectID.
func Budget(maxTokens int64, keyFn func(gateway.Request) string) gateway.Middleware {
	var mu sync.Mutex
	used := make(map[string]int64)

	return func(next gateway.CompleteFunc) gateway.CompleteFunc {
		return func(ctx context.Context, req gateway.Request) (gateway.Response, error) {
			key := req.ProjectID
			if keyFn != nil {
				key = keyFn(req)
			}
			mu.Lock()
			cur := used[key]
			mu.Unlock()
			if maxTokens > 0 && cur >= maxTokens {
				return gateway.Response{}, fmt.Errorf("gateway: token budget %d exceeded for %q", maxTokens, key)
			}

			resp, err := next(ctx, req)
			if err == nil {
				mu.Lock()
				used[key] += resp.Usage.InputTokens + resp.Usage.OutputTokens
				mu.Unlock()
			}
			return resp, err
		}
	}
}

// Retry returns a Middleware that retries requests when errors match gateway.ErrRetryable
// using exponential backoff starting at baseDelay.
func Retry(maxAttempts int, baseDelay time.Duration) gateway.Middleware {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if baseDelay <= 0 {
		baseDelay = 50 * time.Millisecond
	}

	return func(next gateway.CompleteFunc) gateway.CompleteFunc {
		return func(ctx context.Context, req gateway.Request) (gateway.Response, error) {
			var lastErr error
			delay := baseDelay
			for attempt := 0; attempt < maxAttempts; attempt++ {
				resp, err := next(ctx, req)
				if err == nil {
					return resp, nil
				}
				lastErr = err
				if !errors.Is(err, gateway.ErrRetryable) || attempt == maxAttempts-1 {
					return gateway.Response{}, err
				}
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return gateway.Response{}, ctx.Err()
				case <-timer.C:
				}
				delay *= 2
			}
			return gateway.Response{}, lastErr
		}
	}
}

// Fallback returns a Middleware that tries each RouteTarget in order if the primary
// call fails with an error (excluding context cancellation).
func Fallback(targets ...gateway.RouteTarget) gateway.Middleware {
	return func(next gateway.CompleteFunc) gateway.CompleteFunc {
		return func(ctx context.Context, req gateway.Request) (gateway.Response, error) {
			resp, err := next(ctx, req)
			if err == nil || ctx.Err() != nil {
				return resp, err
			}
			for _, t := range targets {
				alt := req
				alt.Provider = t.Provider
				if t.Model != "" {
					alt.Model = t.Model
				}
				if resp, err = next(ctx, alt); err == nil {
					return resp, nil
				}
				if ctx.Err() != nil {
					return gateway.Response{}, ctx.Err()
				}
			}
			return gateway.Response{}, err
		}
	}
}

