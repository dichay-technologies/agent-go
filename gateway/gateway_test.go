package gateway_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/mock"
)

func TestJSONSchemaValidate(t *testing.T) {
	noExtra := false
	schema := &gateway.JSONSchema{
		Type: "object",
		Required: []string{
			"action",
			"count",
		},
		AdditionalProperties: &noExtra,
		Properties: map[string]*gateway.JSONSchema{
			"action": {Type: "string", Enum: []any{"run", "skip"}},
			"count":  {Type: "integer"},
			"tags":   {Type: "array", Items: &gateway.JSONSchema{Type: "string"}},
		},
	}

	if err := schema.Validate(json.RawMessage(`{"action":"run","count":3,"tags":["a","b"]}`)); err != nil {
		t.Fatalf("expected valid payload, got error: %v", err)
	}
	if err := schema.Validate(json.RawMessage(`{"action":"run"}`)); err == nil {
		t.Fatalf("expected error for missing required field")
	}
	if err := schema.Validate(json.RawMessage(`{"action":"invalid","count":1}`)); err == nil {
		t.Fatalf("expected error for enum mismatch")
	}
	if err := schema.Validate(json.RawMessage(`{"action":"run","count":1.5}`)); err == nil {
		t.Fatalf("expected error for float where integer expected")
	}
	if err := schema.Validate(json.RawMessage(`{"action":"run","count":1,"extra":true}`)); err == nil {
		t.Fatalf("expected error for unexpected property")
	}
}

func TestGatewayRoutingAndUsage(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	m := mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		return gateway.Response{
			Text:  "ok:" + req.Model,
			Model: req.Model,
			Usage: gateway.Usage{InputTokens: 10, OutputTokens: 5},
		}, nil
	})
	gw.Register("mock", m)

	resp, err := gw.Complete(context.Background(), gateway.Request{
		ProjectID: "proj-a",
		Model:     "mock/fast-1",
		Messages:  []gateway.Message{{Role: gateway.MsgUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Text != "ok:fast-1" {
		t.Fatalf("unexpected response text: %q", resp.Text)
	}

	u := gw.UsageByProject("proj-a")
	if u.InputTokens != 10 || u.OutputTokens != 5 {
		t.Fatalf("unexpected usage: %+v", u)
	}
}

func TestGatewayCacheAndSingleflight(t *testing.T) {
	gw := gateway.New(gateway.Config{EnableCache: true, CacheTTL: time.Minute})
	var calls atomic.Int64
	m := mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		calls.Add(1)
		time.Sleep(30 * time.Millisecond)
		return gateway.Response{
			Text:  "computed",
			Usage: gateway.Usage{InputTokens: 20, OutputTokens: 10},
		}, nil
	})
	gw.Register("mock", m)

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			proj := "proj-1"
			if idx%2 == 1 {
				proj = "proj-2"
			}
			resp, err := gw.Complete(context.Background(), gateway.Request{
				ProjectID: proj,
				Model:     "m1",
				Messages:  []gateway.Message{{Role: gateway.MsgUser, Content: "same"}},
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if resp.Text != "computed" {
				t.Errorf("unexpected text: %q", resp.Text)
			}
		}(i)
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 singleflight provider call, got %d", got)
	}

	resp, err := gw.Complete(context.Background(), gateway.Request{
		ProjectID: "proj-3",
		Model:     "m1",
		Messages:  []gateway.Message{{Role: gateway.MsgUser, Content: "same"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Cached {
		t.Fatalf("expected cached response")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected cache hit with no extra provider call, got %d", got)
	}
}

func TestGatewayCrossProjectCoalescing(t *testing.T) {
	gw := gateway.New(gateway.Config{
		CoalesceWindow: 40 * time.Millisecond,
		MaxBatchSize:   10,
	})
	m := mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		return gateway.Response{
			Text:  "reply:" + req.Messages[0].Content,
			Usage: gateway.Usage{InputTokens: 5, OutputTokens: 2},
		}, nil
	})
	gw.Register("mock", m)

	var wg sync.WaitGroup
	resps := make([]gateway.Response, 2)
	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		resps[0], errs[0] = gw.Complete(context.Background(), gateway.Request{
			ProjectID: "alpha",
			Coalesce:  true,
			Model:     "decider-1",
			Messages:  []gateway.Message{{Role: gateway.MsgUser, Content: "q1"}},
		})
	}()
	go func() {
		defer wg.Done()
		resps[1], errs[1] = gw.Complete(context.Background(), gateway.Request{
			ProjectID: "beta",
			Coalesce:  true,
			Model:     "decider-1",
			Messages:  []gateway.Message{{Role: gateway.MsgUser, Content: "q2"}},
		})
	}()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
	if resps[0].Text != "reply:q1" || resps[1].Text != "reply:q2" {
		t.Fatalf("unexpected coalesced responses: %+v", resps)
	}
	batches := m.BatchCalls()
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("expected 1 batch of 2 requests, got %d batches", len(batches))
	}
	if gw.UsageByProject("alpha").InputTokens != 5 || gw.UsageByProject("beta").InputTokens != 5 {
		t.Fatalf("unexpected per-project usage: %+v", gw.AllUsage())
	}
}

func TestGatewaySchemaValidationError(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, _ gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: `{"ok": "not-a-bool"}`}, nil
	}))

	_, err := gw.Complete(context.Background(), gateway.Request{
		ProjectID: "proj",
		ResponseSchema: &gateway.JSONSchema{
			Type:       "object",
			Required:   []string{"ok"},
			Properties: map[string]*gateway.JSONSchema{"ok": {Type: "boolean"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "schema validation failed") {
		t.Fatalf("expected schema validation error, got: %v", err)
	}
}

func TestGatewayRouterAndSupportedModels(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	gw.Register("openai", mock.New("openai", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: "oai:" + req.Model}, nil
	}))
	gw.Register("gemini", mock.New("gemini", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: "gem:" + req.Model}, nil
	}))
	gw.AddRoute("fast", "gemini", "gemini-2.5-flash")

	r1, err := gw.Complete(context.Background(), gateway.Request{Model: "fast"})
	if err != nil || r1.Text != "gem:gemini-2.5-flash" {
		t.Fatalf("alias route failed: %+v, err=%v", r1, err)
	}
	r2, err := gw.Complete(context.Background(), gateway.Request{Model: "gpt-4o-mini"})
	if err != nil || r2.Text != "oai:gpt-4o-mini" {
		t.Fatalf("prefix route failed: %+v, err=%v", r2, err)
	}
	if len(gateway.SupportedModels()) == 0 {
		t.Fatalf("expected non-empty SupportedModels")
	}
	if _, ok := gateway.FindModel("gemini-2.5-flash-native-audio-preview-12-2025"); !ok {
		t.Fatalf("expected native audio gemini model in SupportedModels")
	}
	if len(gateway.ModelsByCapability(gateway.CapVoice)) < 3 {
		t.Fatalf("expected multiple voice-capable models")
	}
}

func TestGatewayMiddleware(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, _ gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: "ok"}, nil
	}))

	var trace []string
	gw.Use(func(next gateway.CompleteFunc) gateway.CompleteFunc {
		return func(ctx context.Context, req gateway.Request) (gateway.Response, error) {
			trace = append(trace, "mw1-in")
			resp, err := next(ctx, req)
			trace = append(trace, "mw1-out")
			return resp, err
		}
	})
	gw.Use(func(next gateway.CompleteFunc) gateway.CompleteFunc {
		return func(ctx context.Context, req gateway.Request) (gateway.Response, error) {
			trace = append(trace, "mw2")
			return next(ctx, req)
		}
	})

	resp, err := gw.Complete(context.Background(), gateway.Request{Model: "m1"})
	if err != nil || resp.Text != "ok" {
		t.Fatalf("Complete failed: %+v, err=%v", resp, err)
	}
	if strings.Join(trace, ",") != "mw1-in,mw2,mw1-out" {
		t.Fatalf("unexpected middleware trace: %v", trace)
	}
}

func TestGatewayClassifierRouter(t *testing.T) {
	jevClassifier := mock.New("jev", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		msg := req.Messages[0].Content
		if strings.Contains(msg, "code") {
			return gateway.Response{Text: `{"route":"coding"}`}, nil
		}
		return gateway.Response{Text: `{"route":"fast"}`}, nil
	})

	router := &gateway.ClassifierRouter{
		Classifier:      jevClassifier,
		ClassifierModel: "jev-latest",
		DefaultRoute:    "fast",
		Routes: map[string]gateway.RouteTarget{
			"fast":   {Provider: "gemini", Model: "gemini-2.5-flash", Description: "simple questions"},
			"coding": {Provider: "openai", Model: "gpt-4.1", Description: "complex code tasks"},
		},
	}

	gw := gateway.New(gateway.Config{Router: router})
	gw.Register("gemini", mock.New("gemini", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: "gem:" + req.Model}, nil
	}))
	gw.Register("openai", mock.New("openai", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: "oai:" + req.Model}, nil
	}))

	// 1. Dynamic classification via Jev when Model == "auto" (or "")
	r1, err := gw.Complete(context.Background(), gateway.Request{
		Model:    "auto",
		Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "write Go code"}},
	})
	if err != nil || r1.Text != "oai:gpt-4.1" {
		t.Fatalf("expected coding route to openai/gpt-4.1, got: %+v, err=%v", r1, err)
	}

	// 2. Explicit model bypasses classifier and uses TableRouter fallback
	r2, err := gw.Complete(context.Background(), gateway.Request{
		Model:    "gemini-4-argon",
		Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "write Go code"}},
	})
	if err != nil || r2.Text != "gem:gemini-4-argon" {
		t.Fatalf("expected explicit model bypass, got: %+v, err=%v", r2, err)
	}
}
