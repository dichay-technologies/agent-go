package otel_test

import (
	"context"
	"testing"

	agentgo "proj.dichay.tech/agent-go"
	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/mock"
	"proj.dichay.tech/agent-go/otel"
)

type recSpan struct {
	ended bool
	attrs map[string]any
}

func (s *recSpan) SetAttribute(k string, v any) { s.attrs[k] = v }
func (s *recSpan) RecordError(_ error)          {}
func (s *recSpan) End()                         { s.ended = true }

type recTracer struct {
	spans []*recSpan
}

func (t *recTracer) Start(ctx context.Context, _ string) (context.Context, otel.Span) {
	s := &recSpan{attrs: make(map[string]any)}
	t.spans = append(t.spans, s)
	return ctx, s
}

func TestOTelGatewayMiddleware(t *testing.T) {
	tr := &recTracer{}
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, _ gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: "ok", Usage: gateway.Usage{InputTokens: 4}}, nil
	}))
	gw.Use(otel.GatewayMiddleware(tr))

	if _, err := gw.Complete(context.Background(), gateway.Request{Model: "m1"}); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	if len(tr.spans) != 1 || !tr.spans[0].ended || tr.spans[0].attrs["llm.input_tokens"] != int64(4) {
		t.Fatalf("unexpected span state: %+v", tr.spans)
	}
}

func TestOTelToolMiddlewarePreservesContextCapabilities(t *testing.T) {
	tr := &recTracer{}
	mw := otel.ToolMiddleware(tr)

	bank := agentgo.NewMemoryBank()
	bank.Set("k", "v")
	bus := agentgo.NewBus()
	var spawned bool

	baseCtx := agentgo.NewContext(context.Background(), agentgo.ContextConfig{
		ProjectID: "p1",
		Bank:      bank,
		Bus:       bus,
		Children:  map[string]agentgo.Agent{"sub": nil},
		SpawnNamedFn: func(_ context.Context, _ string, _ agentgo.Agent) (*agentgo.Future, error) {
			spawned = true
			return nil, nil
		},
	})

	inv := mw(agentgo.ToolInvokerFunc(func(ctx agentgo.Context, _ agentgo.ToolCallRequest) (agentgo.ToolCallResult, error) {
		if ctx.Bank() != bank || ctx.Bus() != bus {
			t.Fatalf("expected Bank and Bus preserved across otel.ToolMiddleware")
		}
		if _, ok := ctx.Child("sub"); !ok {
			t.Fatalf("expected Children map preserved across otel.ToolMiddleware")
		}
		_, _ = ctx.Spawn(&dummyAgent{})
		return agentgo.ToolCallResult{Selected: true, ToolName: "check"}, nil
	}))

	res, err := inv.Invoke(baseCtx, agentgo.ToolCallRequest{Model: "m1", Attempt: 1})
	if err != nil || !res.Selected || !spawned {
		t.Fatalf("unexpected ToolMiddleware result: res=%+v spawned=%v err=%v", res, spawned, err)
	}
	if len(tr.spans) != 1 || tr.spans[0].attrs["agent.tool_name"] != "check" {
		t.Fatalf("unexpected tool span: %+v", tr.spans[0])
	}
}

type dummyAgent struct{}

func (*dummyAgent) Run(_ context.Context) (any, error) { return "ok", nil }
func (*dummyAgent) Parent() agentgo.Agent              { return nil }

