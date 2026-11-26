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

func TestJSONSchemaEnumObjectNoPanic(t *testing.T) {
	schema := &gateway.JSONSchema{
		Type: "object",
		Enum: []any{
			map[string]any{"k": "v"},
			[]any{"a", "b"},
		},
	}
	if err := schema.Validate(json.RawMessage(`{"k":"v"}`)); err != nil {
		t.Fatalf("expected valid object enum match, got: %v", err)
	}
	if err := schema.Validate(json.RawMessage(`{"k":"other"}`)); err == nil {
		t.Fatalf("expected enum mismatch error")
	}
}

func TestGatewaySingleflightLeaderCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var provCanceled atomic.Bool

	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(ctx context.Context, req gateway.Request) (gateway.Response, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return gateway.Response{Text: "ok", Usage: gateway.Usage{InputTokens: 10}}, nil
		case <-ctx.Done():
			provCanceled.Store(true)
			return gateway.Response{}, ctx.Err()
		}
	}))

	req := gateway.Request{
		Model:    "m1",
		Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "hello"}},
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErrCh := make(chan error, 1)
	go func() {
		_, err := gw.Complete(leaderCtx, req)
		leaderErrCh <- err
	}()

	<-started

	followerCh := make(chan gateway.Response, 1)
	followerErrCh := make(chan error, 1)
	go func() {
		r, err := gw.Complete(context.Background(), req)
		followerCh <- r
		followerErrCh <- err
	}()

	// Give follower time to join singleflight.
	time.Sleep(20 * time.Millisecond)
	cancelLeader()

	if err := <-leaderErrCh; err != context.Canceled {
		t.Fatalf("expected leader context.Canceled, got: %v", err)
	}
	close(release)

	if err := <-followerErrCh; err != nil {
		t.Fatalf("follower should succeed when leader cancels, got: %v", err)
	}
	if resp := <-followerCh; resp.Text != "ok" {
		t.Fatalf("unexpected follower response: %+v", resp)
	}
	if provCanceled.Load() {
		t.Fatalf("provider context should not be canceled while follower is waiting")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 provider call, got %d", calls.Load())
	}
}

func TestGatewayCacheBoundedEviction(t *testing.T) {
	var calls atomic.Int32
	gw := gateway.New(gateway.Config{
		EnableCache:     true,
		MaxCacheEntries: 2,
	})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		calls.Add(1)
		return gateway.Response{Text: req.Messages[0].Content}, nil
	}))

	call := func(msg string) gateway.Response {
		r, err := gw.Complete(context.Background(), gateway.Request{
			Model:    "m1",
			Messages: []gateway.Message{{Role: gateway.MsgUser, Content: msg}},
		})
		if err != nil {
			t.Fatalf("Complete(%q): %v", msg, err)
		}
		return r
	}

	call("k1")
	call("k2")
	call("k3") // evicts k1
	if got := calls.Load(); got != 3 {
		t.Fatalf("expected 3 provider calls, got %d", got)
	}
	if r := call("k2"); !r.Cached {
		t.Fatalf("expected k2 to remain cached")
	}
	if r := call("k1"); r.Cached {
		t.Fatalf("expected k1 to be evicted")
	}
}

func TestClassifierRouterDeterministicOrder(t *testing.T) {
	var seenPrompts []string
	var mu sync.Mutex
	jevClassifier := mock.New("jev", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		mu.Lock()
		seenPrompts = append(seenPrompts, req.Messages[0].Content)
		mu.Unlock()
		return gateway.Response{Text: `{"route":"alpha"}`}, nil
	})

	router := &gateway.ClassifierRouter{
		Classifier: jevClassifier,
		Routes: map[string]gateway.RouteTarget{
			"zeta":  {Provider: "p", Model: "m-z", Description: "z"},
			"alpha": {Provider: "p", Model: "m-a", Description: "a"},
			"mid":   {Provider: "p", Model: "m-m", Description: "m"},
		},
	}
	gw := gateway.New(gateway.Config{Router: router})
	gw.Register("p", mock.New("p", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: req.Model}, nil
	}))

	for i := 0; i < 10; i++ {
		if _, err := gw.Complete(context.Background(), gateway.Request{
			Model:    "auto",
			Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "hi"}},
		}); err != nil {
			t.Fatalf("Complete failed: %v", err)
		}
	}

	for i := 1; i < len(seenPrompts); i++ {
		if seenPrompts[i] != seenPrompts[0] {
			t.Fatalf("non-deterministic classifier prompt:\n%s\nvs\n%s", seenPrompts[0], seenPrompts[i])
		}
	}
}

func TestSchemaForStruct(t *testing.T) {
	type item struct {
		Name  string   `json:"name" desc:"item name"`
		Count int      `json:"count"`
		Note  string   `json:"note,omitempty"`
		Tags  []string `json:"tags"`
	}
	s := gateway.SchemaFor[item]()
	if s.Type != "object" || s.Properties["name"].Description != "item name" || s.Properties["count"].Type != "integer" {
		t.Fatalf("unexpected generated schema: %+v", s)
	}
	if err := s.Validate(json.RawMessage(`{"name":"a","count":1,"tags":["x"]}`)); err != nil {
		t.Fatalf("expected valid payload, got: %v", err)
	}
	if err := s.Validate(json.RawMessage(`{"name":"a"}`)); err == nil {
		t.Fatalf("expected missing required field error")
	}
}

func TestGatewayUsageSinkMetaAndStream(t *testing.T) {
	var sinkCalls atomic.Int32
	var lastMeta string
	gw := gateway.New(gateway.Config{
		UsageSink: gateway.UsageSinkFunc(func(_ context.Context, req gateway.Request, u gateway.Usage) {
			sinkCalls.Add(1)
			lastMeta = req.Meta["tenant"]
		}),
		UsageKey: func(req gateway.Request) string {
			return req.Meta["tenant"]
		},
	})
	gw.Register("mock", mock.New("mock", func(_ context.Context, _ gateway.Request) (gateway.Response, error) {
		return gateway.Response{
			Text:  "streamed",
			Usage: gateway.Usage{InputTokens: 3, OutputTokens: 7},
		}, nil
	}))

	var gotText string
	for ch, err := range gw.Stream(context.Background(), gateway.Request{
		Model: "m1",
		Meta:  map[string]string{"tenant": "acme"},
	}) {
		if err != nil {
			t.Fatalf("Stream failed: %v", err)
		}
		gotText += ch.Text
	}
	if gotText != "streamed" || sinkCalls.Load() != 1 || lastMeta != "acme" {
		t.Fatalf("unexpected stream/usage: text=%q calls=%d meta=%q", gotText, sinkCalls.Load(), lastMeta)
	}
	if u := gw.UsageByProject("acme"); u.InputTokens != 3 || u.OutputTokens != 7 {
		t.Fatalf("unexpected UsageByProject for custom key: %+v", u)
	}
}

func TestGatewayTypedErrors(t *testing.T) {
	err429 := gateway.ClassifyStatusCode("openai", 429, "rate limit")
	if !isErr(err429, gateway.ErrRateLimit) || !isErr(err429, gateway.ErrRetryable) {
		t.Fatalf("expected ErrRateLimit and ErrRetryable, got %v", err429)
	}
	err401 := gateway.ClassifyStatusCode("openai", 401, "bad key")
	if !isErr(err401, gateway.ErrAuth) {
		t.Fatalf("expected ErrAuth, got %v", err401)
	}
	errCtx := gateway.ClassifyStatusCode("openai", 400, "maximum context length exceeded")
	if !isErr(errCtx, gateway.ErrContextLength) {
		t.Fatalf("expected ErrContextLength, got %v", errCtx)
	}

	fc, err := gateway.NewFileCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileCache: %v", err)
	}
	fc.Set(context.Background(), "k1", gateway.Response{Text: "persisted", Usage: gateway.Usage{InputTokens: 9}}, time.Minute)
	got, ok := fc.Get(context.Background(), "k1")
	if !ok || got.Text != "persisted" || got.Usage.InputTokens != 9 {
		t.Fatalf("unexpected FileCache Get: ok=%v resp=%+v", ok, got)
	}
}

func isErr(err, target error) bool {
	return err != nil && (err == target || strings.Contains(err.Error(), target.Error()) || unwrapContains(err, target))
}

func unwrapContains(err, target error) bool {
	type multiUnwrap interface{ Unwrap() []error }
	if mu, ok := err.(multiUnwrap); ok {
		for _, e := range mu.Unwrap() {
			if e == target {
				return true
			}
		}
	}
	return false
}
