package agentgo

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/mock"
)

type appState struct {
	StepsCompleted int
	LastOutput     string
	Failed         bool
}

func TestAgentFullLifecycle(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		if len(req.Tools) > 0 {
			return gateway.Response{
				ToolCalls: []gateway.ToolCall{{
					ID:        "call_1",
					Name:      "echo",
					Arguments: json.RawMessage(`{"msg":"hello"}`),
				}},
			}, nil
		}
		return gateway.Response{Text: "analyzed"}, nil
	}))

	echoTool := stubTool{
		name: "echo",
		desc: "echo message",
		schema: &gateway.JSONSchema{
			Type:       "object",
			Required:   []string{"msg"},
			Properties: map[string]*gateway.JSONSchema{"msg": {Type: "string"}},
		},
		execFn: func(_ Context, args json.RawMessage) (any, error) {
			return "echo:" + string(args), nil
		},
	}

	ag := &AI[appState]{
		ID:           "root-1",
		ProjectID:    "proj-1",
		Gateway:      gw,
		Model:        "mock/m1",
		Task:         "say hello",
		Prompt:       "Run tool for: {{.Task}}",
		SystemPrefix: "You are helpful.",
		Memory:       NewLocalState(appState{}),
		Tools:        []Tool{echoTool},
		Data:         []DataSource{StaticData("doc", "some context")},
		Hooks: Hooks[appState]{
			OnStep: func(_ Context, st *LocalState[appState], _ string, res ToolCallResult) {
				st.Update(func(s *appState) {
					s.StepsCompleted++
					s.LastOutput = res.ToolName
				})
			},
		},
	}

	out, err := ag.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if out != `echo:{"msg":"hello"}` {
		t.Fatalf("unexpected output: %v", out)
	}
	if ag.State.Status != StatusDone {
		t.Fatalf("expected StatusDone, got %s", ag.State.Status)
	}
	st := ag.Memory.Get()
	if st.StepsCompleted != 1 || st.LastOutput != "echo" {
		t.Fatalf("unexpected memory state: %+v", st)
	}
}

func TestAgentGuardrailEscalationAndFail(t *testing.T) {
	t.Run("escalates after 3 attempts and recovers", func(t *testing.T) {
		gw := gateway.New(gateway.Config{})
		gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
			if req.Model == "esc-model" {
				return gateway.Response{Text: "recovered"}, nil
			}
			return gateway.Response{
				ToolCalls: []gateway.ToolCall{{ID: "1", Name: "flaky", Arguments: json.RawMessage(`{}`)}},
			}, nil
		}))

		var escalated bool
		ag := &AI[appState]{
			ID:            "g-1",
			Gateway:       gw,
			Model:         "main-model",
			EscalateModel: "esc-model",
			SkipPlanning:  true,
			Task:          "do flaky",
			Tools: []Tool{stubTool{
				name: "flaky",
				execFn: func(_ Context, _ json.RawMessage) (any, error) {
					return nil, errors.New("boom")
				},
			}},
			Hooks: Hooks[appState]{
				OnEscalate: func(_ Context, _ *LocalState[appState], history []AttemptLog) error {
					if len(history) != 3 {
						return errors.New("expected 3 attempts in history")
					}
					escalated = true
					return nil
				},
			},
		}

		out, err := ag.Run(context.Background())
		if err != nil {
			t.Fatalf("expected recovery, got err: %v", err)
		}
		if !escalated || out != "recovered" {
			t.Fatalf("expected escalated recovery, escalated=%v out=%v", escalated, out)
		}
	})

	t.Run("fails and triggers OnFail when escalation fails", func(t *testing.T) {
		gw := gateway.New(gateway.Config{})
		gw.Register("mock", mock.New("mock", func(_ context.Context, _ gateway.Request) (gateway.Response, error) {
			return gateway.Response{}, errors.New("provider down")
		}))

		ag := &AI[appState]{
			ID:           "g-2",
			Gateway:      gw,
			Model:        "main-model",
			SkipPlanning: true,
			Task:         "fail",
			Memory:       NewLocalState(appState{}),
			Hooks: Hooks[appState]{
				OnFail: func(_ Context, st *LocalState[appState], _ error) {
					st.Update(func(s *appState) { s.Failed = true })
				},
			},
		}

		_, err := ag.Run(context.Background())
		if err == nil || !strings.Contains(err.Error(), "escalation failed") {
			t.Fatalf("expected escalation failed error, got: %v", err)
		}
		if ag.State.Status != StatusFailed || !ag.Memory.Get().Failed {
			t.Fatalf("expected StatusFailed and OnFail hook, status=%s mem=%+v", ag.State.Status, ag.Memory.Get())
		}
	})
}

func TestAgentSpawnAsyncSemaphoreAndDepth(t *testing.T) {
	var active, maxObserved atomic.Int32

	root := &AI[appState]{
		ID:                "root",
		MaxActiveChildren: 4,
		MaxSpawnDepth:     2,
		SkipPlanning:      true,
		Memory:            NewLocalState(appState{}),
	}

	ctx := NewContext(context.Background(), ContextConfig{Agent: root})
	var futs []*Future
	for i := range 12 {
		child := &AI[appState]{
			ID:           "child",
			ParentAgent:  root,
			SkipPlanning: true,
			Task:         "work",
			State:        AgentState{SpawnDepth: root.State.SpawnDepth + 1},
			Tools: []Tool{stubTool{
				name: "noop",
			}},
		}
		// Use ToolInvokerFunc to measure concurrent active goroutines inside child.Run.
		child.ToolInvoker = ToolInvokerFunc(func(_ Context, _ ToolCallRequest) (ToolCallResult, error) {
			cur := active.Add(1)
			for {
				prev := maxObserved.Load()
				if cur <= prev || maxObserved.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
			return ToolCallResult{Selected: false, Text: "done"}, nil
		})

		f, err := root.SpawnAsync(ctx, "c"+string(rune('0'+i)), child)
		if err != nil {
			t.Fatalf("SpawnAsync %d failed: %v", i, err)
		}
		if child.Parent() != root {
			t.Fatalf("expected child.Parent() == root")
		}
		futs = append(futs, f)
	}

	if err := root.WaitDeps(ctx); err != nil {
		t.Fatalf("WaitDeps failed: %v", err)
	}
	if maxObserved.Load() > 4 {
		t.Fatalf("semaphore breached: maxObserved=%d > 4", maxObserved.Load())
	}

	// Fork-bomb depth guard check.
	deep := &AI[appState]{
		ID:            "deep",
		MaxSpawnDepth: 2,
		State:         AgentState{SpawnDepth: 2},
	}
	if _, err := deep.SpawnAsync(ctx, "too-deep", root); err == nil || !strings.Contains(err.Error(), "max spawn depth") {
		t.Fatalf("expected max spawn depth error, got: %v", err)
	}
}

func TestAgentMultiStepPlanDAG(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		msg := req.Messages[len(req.Messages)-1].Content
		switch {
		case strings.HasPrefix(msg, "Create an execution plan"):
			return gateway.Response{
				Text: `{"steps":[{"id":"s1","task":"fetch"},{"id":"s2","task":"process","depends_on":["s1"]}]}`,
			}, nil
		case strings.HasPrefix(msg, "fetch"):
			return gateway.Response{Text: "raw-data"}, nil
		case strings.HasPrefix(msg, "process") && strings.Contains(msg, "s1: raw-data"):
			return gateway.Response{Text: "final-processed"}, nil
		default:
			return gateway.Response{Text: "ok"}, nil
		}
	}))

	ag := &AI[appState]{
		ID:        "dag-agent",
		Gateway:   gw,
		Model:     "m1",
		PlanModel: "m-plan",
		Task:      "build report",
	}

	out, err := ag.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if out != "final-processed" {
		t.Fatalf("expected final-processed, got: %v", out)
	}
	if len(ag.State.Plan) != 2 || ag.State.Status != StatusDone {
		t.Fatalf("unexpected plan/status: plan=%+v status=%s", ag.State.Plan, ag.State.Status)
	}
}

func TestNewOptionsForkAndPipelineOverride(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		return gateway.Response{Text: req.Model + ":" + req.Messages[len(req.Messages)-1].Content}, nil
	}))

	root := New[appState](
		WithID[appState]("root"),
		WithProject[appState]("proj-lib"),
		WithGateway[appState](gw),
		WithModel[appState]("base-m"),
		WithStageModel[appState](StatusExecuting, "exec-m"),
		WithTask[appState]("custom-task"),
		WithPipeline(Pipeline[appState]{
			Analyze: func(_ Context, _ *AI[appState]) error { return nil },
			Plan: func(_ Context, a *AI[appState]) error {
				a.State.Plan = []PlanStep{{ID: "p1", Task: "custom-step"}}
				return nil
			},
		}),
	)

	out, err := root.Run(context.Background())
	if err != nil {
		t.Fatalf("root.Run failed: %v", err)
	}
	if out != "exec-m:custom-step" {
		t.Fatalf("unexpected output: %v", out)
	}

	child := Fork[appState, appState](root, WithID[appState]("child"), WithSkipPlanning[appState](true), WithTask[appState]("sub"))
	if child.Parent() != root || child.State.SpawnDepth != 1 || child.Gateway != gw || child.ProjectID != "proj-lib" {
		t.Fatalf("Fork did not inherit expected fields: parent=%v depth=%d gw=%v proj=%q",
			child.Parent(), child.State.SpawnDepth, child.Gateway, child.ProjectID)
	}
	cout, err := child.Run(context.Background())
	if err != nil {
		t.Fatalf("child.Run failed: %v", err)
	}
	if cout != "base-m:sub" {
		t.Fatalf("unexpected child output: %v", cout)
	}
}

