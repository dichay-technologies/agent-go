package jev_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/jev"
)

func TestJevProviderRetryAndBatch(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer ts-secret" {
			t.Fatalf("missing or wrong auth header: %q", r.Header.Get("Authorization"))
		}
		if r.URL.Path != "/v1/systemone" {
			t.Fatalf("unexpected path: %q", r.URL.Path)
		}
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`rate limited`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model": "jev-latest",
			"answers": {
				"action": {"type": "choice", "choice": "run", "confidence": 0.99}
			},
			"usage": {"input_tokens": 11, "output_tokens": 2}
		}`))
	}))
	defer srv.Close()

	p, err := jev.New(jev.Config{
		APIKey:     "ts-secret",
		BaseURL:    srv.URL,
		MaxRetries: 2,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	gw := gateway.New(gateway.Config{})
	gw.Register("jev", p)

	resp, err := gw.Complete(context.Background(), gateway.Request{
		ProjectID: "proj-jev",
		Messages:  []gateway.Message{{Role: gateway.MsgUser, Content: "go?"}},
		ResponseSchema: &gateway.JSONSchema{
			Type: "object",
			Properties: map[string]*gateway.JSONSchema{
				"action": {Type: "string", Enum: []any{"run", "skip"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	if resp.Text != `{"action":"run"}` || resp.Usage.InputTokens != 11 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts (1 retry), got %d", attempts.Load())
	}

	resps, errs := p.BatchComplete(context.Background(), []gateway.Request{
		{Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "a"}}},
		{Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "b"}}},
	})
	if len(errs) != 2 || errs[0] != nil || errs[1] != nil {
		t.Fatalf("unexpected batch errs: %+v", errs)
	}
	if resps[0].Text != `{"action":"run"}` || resps[1].Text != `{"action":"run"}` {
		t.Fatalf("unexpected batch responses: %+v", resps)
	}
}

