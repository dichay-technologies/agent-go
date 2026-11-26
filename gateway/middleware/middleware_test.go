package middleware_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/middleware"
	"proj.dichay.tech/agent-go/gateway/mock"
)

func TestRetryBudgetFallbackAndRateLimit(t *testing.T) {
	var attempts atomic.Int32
	gw := gateway.New(gateway.Config{})
	gw.Register("primary", mock.New("primary", func(_ context.Context, _ gateway.Request) (gateway.Response, error) {
		if attempts.Add(1) < 2 {
			return gateway.Response{}, gateway.ClassifyStatusCode("primary", 429, "rate limit")
		}
		return gateway.Response{Text: "ok", Usage: gateway.Usage{InputTokens: 6, OutputTokens: 4}}, nil
	}))
	gw.Register("backup", mock.New("backup", func(_ context.Context, _ gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: "from-backup", Usage: gateway.Usage{InputTokens: 2}}, nil
	}))

	gw.Use(
		middleware.RateLimit(1000, 5),
		middleware.Budget(10, nil),
		middleware.Retry(3, time.Millisecond),
	)

	resp, err := gw.Complete(context.Background(), gateway.Request{ProjectID: "p1", Provider: "primary", Model: "m1"})
	if err != nil || resp.Text != "ok" || attempts.Load() != 2 {
		t.Fatalf("expected retry success, got resp=%+v err=%v attempts=%d", resp, err, attempts.Load())
	}

	// Second call exceeds Budget(10).
	if _, err := gw.Complete(context.Background(), gateway.Request{ProjectID: "p1", Provider: "primary", Model: "m1"}); err == nil {
		t.Fatalf("expected budget exceeded error")
	}

	// Fallback test on a fresh gateway.
	fgw := gateway.New(gateway.Config{})
	fgw.Register("bad", mock.New("bad", func(_ context.Context, _ gateway.Request) (gateway.Response, error) {
		return gateway.Response{}, errors.New("down")
	}))
	fgw.Register("good", mock.New("good", func(_ context.Context, _ gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: "recovered"}, nil
	}))
	fgw.Use(middleware.Fallback(gateway.RouteTarget{Provider: "good", Model: "m2"}))
	fresp, err := fgw.Complete(context.Background(), gateway.Request{Provider: "bad", Model: "m1"})
	if err != nil || fresp.Text != "recovered" {
		t.Fatalf("expected fallback success, got %+v err=%v", fresp, err)
	}
}

