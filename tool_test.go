package agentgo

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/mock"
)

type stubTool struct {
	name     string
	desc     string
	schema   *gateway.JSONSchema
	needsGen bool
	execFn   func(ctx Context, args json.RawMessage) (any, error)
}

func (s stubTool) Name() string               { return s.name }
func (s stubTool) Description() string        { return s.desc }
func (s stubTool) Schema() *gateway.JSONSchema { return s.schema }
func (s stubTool) NeedsGeneration() bool      { return s.needsGen }
func (s stubTool) Execute(ctx Context, args json.RawMessage) (any, error) {
	return s.execFn(ctx, args)
}

func TestDefaultToolInvokerDirectAndGenerated(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	m := mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		if req.Model == "arg-model" {
			return gateway.Response{Text: `{"sql":"SELECT 1"}`}, nil
		}
		if req.Messages[len(req.Messages)-1].Content == "direct" {
			return gateway.Response{
				ToolCalls: []gateway.ToolCall{{
					ID:        "1",
					Name:      "ping",
					Arguments: json.RawMessage(`{"host":"1.1.1.1"}`),
				}},
			}, nil
		}
		return gateway.Response{
			ToolCalls: []gateway.ToolCall{{
				ID:   "2",
				Name: "query",
			}},
		}, nil
	})
	gw.Register("mock", m)

	invoker := DefaultToolInvoker{}
	pingTool := stubTool{
		name:     "ping",
		desc:     "ping host",
		needsGen: false,
		schema: &gateway.JSONSchema{
			Type:       "object",
			Required:   []string{"host"},
			Properties: map[string]*gateway.JSONSchema{"host": {Type: "string"}},
		},
		execFn: func(_ Context, args json.RawMessage) (any, error) {
			return "pong:" + string(args), nil
		},
	}
	queryTool := stubTool{
		name:     "query",
		desc:     "run sql",
		needsGen: true,
		schema: &gateway.JSONSchema{
			Type:       "object",
			Required:   []string{"sql"},
			Properties: map[string]*gateway.JSONSchema{"sql": {Type: "string"}},
		},
		execFn: func(_ Context, args json.RawMessage) (any, error) {
			return "rows:" + string(args), nil
		},
	}

	ctx := NewContext(context.Background(), ContextConfig{
		ProjectID: "p1",
		Gateway:   gw,
	})

	// 1. Direct execution (NeedsGeneration == false)
	res1, err := invoker.Invoke(ctx, ToolCallRequest{
		ProjectID: "p1",
		Model:     "dec-model",
		Messages:  []gateway.Message{{Role: gateway.MsgUser, Content: "direct"}},
		Tools:     []Tool{pingTool, queryTool},
	})
	if err != nil {
		t.Fatalf("direct invoke failed: %v", err)
	}
	if !res1.Selected || res1.ToolName != "ping" || res1.Output != `pong:{"host":"1.1.1.1"}` {
		t.Fatalf("unexpected direct result: %+v", res1)
	}

	// 2. Micro-generated execution (NeedsGeneration == true)
	res2, err := invoker.Invoke(ctx, ToolCallRequest{
		ProjectID: "p1",
		Model:     "dec-model",
		ArgModel:  "arg-model",
		Messages:  []gateway.Message{{Role: gateway.MsgUser, Content: "generate"}},
		Tools:     []Tool{pingTool, queryTool},
	})
	if err != nil {
		t.Fatalf("generated invoke failed: %v", err)
	}
	if !res2.Selected || res2.ToolName != "query" || res2.Output != `rows:{"sql":"SELECT 1"}` {
		t.Fatalf("unexpected generated result: %+v", res2)
	}
}

func TestCustomToolInvokerFunc(t *testing.T) {
	custom := ToolInvokerFunc(func(ctx Context, req ToolCallRequest) (ToolCallResult, error) {
		if len(req.Tools) == 0 {
			return ToolCallResult{}, errors.New("no tools")
		}
		out, err := req.Tools[0].Execute(ctx, json.RawMessage(`{"x":1}`))
		return ToolCallResult{Selected: true, ToolName: req.Tools[0].Name(), Output: out}, err
	})

	ctx := NewContext(context.Background(), ContextConfig{})
	res, err := custom.Invoke(ctx, ToolCallRequest{
		Tools: []Tool{stubTool{
			name:   "custom",
			execFn: func(_ Context, _ json.RawMessage) (any, error) { return 42, nil },
		}},
	})
	if err != nil {
		t.Fatalf("custom invoke failed: %v", err)
	}
	if res.Output != 42 || res.ToolName != "custom" {
		t.Fatalf("unexpected custom result: %+v", res)
	}
}

type stubAgent struct {
	parent Agent
	fn     func(ctx context.Context) (any, error)
}

func (s *stubAgent) Run(ctx context.Context) (any, error) { return s.fn(ctx) }
func (s *stubAgent) Parent() Agent                       { return s.parent }

func TestUnifiedContextInTool(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: "ai:" + req.Messages[0].Content}, nil
	}))

	rootAgent := &stubAgent{fn: func(ctx context.Context) (any, error) {
		c, _ := ContextFrom(ctx)
		resp, err := c.Call(gateway.Request{
			Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "from-agent"}},
		})
		if err != nil {
			return nil, err
		}
		return "agent:" + resp.Text, nil
	}}
	childAgent := &stubAgent{parent: rootAgent, fn: rootAgent.fn}

	tool := stubTool{
		name: "smart_tool",
		execFn: func(ctx Context, _ json.RawMessage) (any, error) {
			if ctx.AI() == nil || ctx.Agent() == nil {
				return nil, errors.New("missing AI or Agent in context")
			}
			directResp, err := ctx.Call(gateway.Request{
				Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "from-tool"}},
			})
			if err != nil {
				return nil, err
			}
			selfOut, err := ctx.Run(ctx.Agent())
			if err != nil {
				return nil, err
			}
			child, ok := ctx.Child("helper")
			if !ok || child.Parent() != ctx.Agent() {
				return nil, errors.New("expected child agent to have rootAgent as parent")
			}
			fut, err := ctx.Spawn(child)
			if err != nil {
				return nil, err
			}
			subOut, err := fut.Await(ctx)
			if err != nil {
				return nil, err
			}
			return directResp.Text + "|" + selfOut.(string) + "|" + subOut.(string), nil
		},
	}

	ctx := NewContext(context.Background(), ContextConfig{
		ProjectID:    "proj-ctx",
		DefaultModel: "m-default",
		Gateway:      gw,
		Agent:        rootAgent,
		Children:     map[string]Agent{"helper": childAgent},
	})

	out, err := tool.Execute(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("tool execute failed: %v", err)
	}
	if out != "ai:from-tool|agent:ai:from-agent|agent:ai:from-agent" {
		t.Fatalf("unexpected output: %v", out)
	}
}

func TestNewTypedTool(t *testing.T) {
	type sumArgs struct {
		A int `json:"a"`
		B int `json:"b"`
	}
	addTool := NewTypedTool("add", "add two numbers", &gateway.JSONSchema{
		Type:     "object",
		Required: []string{"a", "b"},
		Properties: map[string]*gateway.JSONSchema{
			"a": {Type: "integer"},
			"b": {Type: "integer"},
		},
	}, false, func(_ Context, in sumArgs) (any, error) {
		return in.A + in.B, nil
	})

	ctx := NewContext(context.Background(), ContextConfig{})
	out, err := addTool.Execute(ctx, json.RawMessage(`{"a":7,"b":5}`))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if out != 12 {
		t.Fatalf("expected 12, got %v", out)
	}
}
