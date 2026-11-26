package agentgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		if !ag.Memory.Get().Failed {
			t.Fatalf("expected OnFail hook, mem=%+v", ag.Memory.Get())
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

	sess := newRunSession(root, RunState{}, nil)
	ctx := NewContext(context.WithValue(context.Background(), runSessionKey{}, sess), ContextConfig{Agent: root, RunStateFn: sess.snapshot})
	var futs []*Future
	for i := range 12 {
		child := &AI[appState]{
			ID:             "child",
			ParentAgent:    root,
			SkipPlanning:   true,
			Task:           "work",
			BaseSpawnDepth: 1,
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
		ID:             "deep",
		MaxSpawnDepth:  2,
		BaseSpawnDepth: 2,
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

	var lastPlan []PlanStep
	ag := &AI[appState]{
		ID:         "dag-agent",
		Gateway:    gw,
		Model:      "m1",
		PlanModel:  "m-plan",
		Task:       "build report",
		EnablePlan: true,
		Hooks: Hooks[appState]{
			OnStep: func(ctx Context, _ *LocalState[appState], _ string, _ ToolCallResult) {
				lastPlan = ctx.RunState().Plan
			},
		},
	}

	out, err := ag.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if out != "final-processed" {
		t.Fatalf("expected final-processed, got: %v", out)
	}
	if len(lastPlan) != 2 {
		t.Fatalf("unexpected plan: %+v", lastPlan)
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
			Plan: func(ctx Context, a *AI[appState]) error {
				SetPlan(ctx, a, []PlanStep{{ID: "p1", Task: "custom-step"}})
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
	if child.Parent() != root || child.BaseSpawnDepth != 1 || child.Gateway != gw || child.ProjectID != "proj-lib" {
		t.Fatalf("Fork did not inherit expected fields: parent=%v depth=%d gw=%v proj=%q",
			child.Parent(), child.BaseSpawnDepth, child.Gateway, child.ProjectID)
	}
	cout, err := child.Run(context.Background())
	if err != nil {
		t.Fatalf("child.Run failed: %v", err)
	}
	if cout != "base-m:sub" {
		t.Fatalf("unexpected child output: %v", cout)
	}
}

func TestForkInheritsToolsAndActiveFuturesCleanup(t *testing.T) {
	echoTool := NewTypedTool(
		"echo",
		"echoes input",
		&gateway.JSONSchema{Type: "object"},
		false,
		func(_ Context, in map[string]any) (any, error) { return in, nil },
	)
	altTool := NewTypedTool(
		"alt",
		"alt tool",
		&gateway.JSONSchema{Type: "object"},
		false,
		func(_ Context, in map[string]any) (any, error) { return in, nil },
	)
	inv := ToolInvokerFunc(func(_ Context, req ToolCallRequest) (ToolCallResult, error) {
		return ToolCallResult{Selected: false, Text: fmt.Sprintf("tools=%d arg=%s", len(req.Tools), req.ArgModel)}, nil
	})

	root := New[appState](
		WithID[appState]("root"),
		WithTools[appState](echoTool),
		WithToolInvoker[appState](inv),
	)
	root.ArgModel = "arg-m"

	inherited := Fork[appState, appState](root, WithID[appState]("inherited"), WithSkipPlanning[appState](true))
	if len(inherited.Tools) != 1 || inherited.Tools[0].Name() != "echo" {
		t.Fatalf("expected inherited Tools=[echo], got %v", inherited.Tools)
	}
	if inherited.ToolInvoker == nil || inherited.ArgModel != "arg-m" {
		t.Fatalf("expected inherited ToolInvoker and ArgModel=arg-m, got inv=%v arg=%q", inherited.ToolInvoker, inherited.ArgModel)
	}

	overridden := Fork[appState, appState](root, WithID[appState]("overridden"), WithTools[appState](altTool))
	if len(overridden.Tools) != 1 || overridden.Tools[0].Name() != "alt" {
		t.Fatalf("expected overridden Tools=[alt], got %v", overridden.Tools)
	}

	sess := newRunSession(root, RunState{}, nil)
	ctx := NewContext(context.WithValue(context.Background(), runSessionKey{}, sess), ContextConfig{Agent: root, RunStateFn: sess.snapshot})
	fut, err := root.SpawnAsync(ctx, "c1", inherited)
	if err != nil {
		t.Fatalf("SpawnAsync failed: %v", err)
	}
	out, err := fut.Await(ctx)
	if err != nil {
		t.Fatalf("Await failed: %v", err)
	}
	if out != "tools=1 arg=arg-m" {
		t.Fatalf("unexpected child output: %v", out)
	}
	if err := root.WaitDeps(ctx); err != nil {
		t.Fatalf("WaitDeps failed: %v", err)
	}
	sess.mu.Lock()
	remaining := len(sess.activeFutures)
	status := sess.state.Status
	sess.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected activeFutures to be empty after child completion, got %d", remaining)
	}
	if status == StatusWaitingDeps {
		t.Fatalf("expected WaitDeps not to transition to StatusWaitingDeps when no futures are active")
	}
}

type stubDataSource struct {
	name    string
	content string
}

func (s stubDataSource) Name() string                  { return s.name }
func (s stubDataSource) Content(_ Context) (string, error) { return s.content, nil }

func TestGatherContextPassedToExecute(t *testing.T) {
	var seenSys string
	var seenMsgs []gateway.Message
	inv := ToolInvokerFunc(func(_ Context, req ToolCallRequest) (ToolCallResult, error) {
		seenSys = req.SystemPrefix
		seenMsgs = req.Messages
		return ToolCallResult{Selected: false, Text: "ok"}, nil
	})
	dummyTool := NewTypedTool("noop", "noop", &gateway.JSONSchema{Type: "object"}, false, func(_ Context, _ map[string]any) (any, error) {
		return nil, nil
	})

	ag := New[appState](
		WithID[appState]("ctx-exec"),
		WithSystemPrefix[appState]("sys-base"),
		WithData[appState](stubDataSource{name: "chat_history", content: "user: prev\nassistant: answer"}),
		WithTools[appState](dummyTool),
		WithToolInvoker[appState](inv),
		WithTask[appState]("follow up"),
	)

	out, err := ag.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if out != "ok" {
		t.Fatalf("unexpected output: %v", out)
	}
	if seenSys != "sys-base" {
		t.Fatalf("expected SystemPrefix to remain stable as %q, got: %q", "sys-base", seenSys)
	}
	if len(seenMsgs) < 2 || !strings.Contains(seenMsgs[0].Content, "user: prev\nassistant: answer") {
		t.Fatalf("expected gathered context in separate user message, got: %+v", seenMsgs)
	}
}


type panicAgent struct{}

func (panicAgent) Run(_ context.Context) (any, error) {
	panic("boom")
}
func (panicAgent) Parent() Agent { return nil }

func TestFutureAndSpawnAsyncRecoverPanic(t *testing.T) {
	fut := NewFuture(context.Background(), "panic-fut", panicAgent{})
	if _, err := fut.Await(context.Background()); err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected panic error from NewFuture, got: %v", err)
	}

	parent := New[appState](WithID[appState]("parent"))
	ctx := NewContext(context.Background(), ContextConfig{Agent: parent})
	sfut, err := parent.SpawnAsync(ctx, "panic-child", panicAgent{})
	if err != nil {
		t.Fatalf("SpawnAsync failed: %v", err)
	}
	if _, err := sfut.Await(context.Background()); err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected panic error from SpawnAsync, got: %v", err)
	}
}

func TestSpawnAsyncUniqueUnnamedKeys(t *testing.T) {
	parent := New[appState](WithID[appState]("parent-seq"), WithLimits[appState](10, 5, 1))
	sess := newRunSession(parent, RunState{}, nil)
	ctx := NewContext(context.WithValue(context.Background(), runSessionKey{}, sess), ContextConfig{Agent: parent, RunStateFn: sess.snapshot})

	gate := make(chan struct{})
	slowChild := &stubAgent{fn: func(c context.Context) (any, error) {
		<-gate
		return "done", nil
	}}
	fastChild := &stubAgent{fn: func(c context.Context) (any, error) {
		return "fast", nil
	}}

	f1, err := parent.SpawnAsync(ctx, "", slowChild)
	if err != nil {
		t.Fatalf("spawn 1: %v", err)
	}
	f2, err := parent.SpawnAsync(ctx, "", fastChild)
	if err != nil {
		t.Fatalf("spawn 2: %v", err)
	}
	if _, err := f2.Await(context.Background()); err != nil {
		t.Fatalf("await f2: %v", err)
	}
	// Spawn a 3rd unnamed child while f1 is still active and f2 was deleted.
	f3, err := parent.SpawnAsync(ctx, "", slowChild)
	if err != nil {
		t.Fatalf("spawn 3: %v", err)
	}

	sess.mu.Lock()
	activeCount := len(sess.activeFutures)
	sess.mu.Unlock()
	if activeCount != 2 {
		t.Fatalf("expected 2 active futures without key collision, got %d", activeCount)
	}

	close(gate)
	if err := parent.WaitDeps(ctx); err != nil {
		t.Fatalf("WaitDeps: %v", err)
	}
	if _, err := f1.Await(context.Background()); err != nil {
		t.Fatalf("await f1: %v", err)
	}
	if _, err := f3.Await(context.Background()); err != nil {
		t.Fatalf("await f3: %v", err)
	}
}

func TestWaitDepsCanceledContext(t *testing.T) {
	parent := New[appState](WithID[appState]("parent-cancel"))
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx := NewContext(cctx, ContextConfig{Agent: parent})
	if err := parent.WaitDeps(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestAgentConcurrentReuseAndRunFrom(t *testing.T) {
	var calls atomic.Int32
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		calls.Add(1)
		msg := req.Messages[len(req.Messages)-1].Content
		return gateway.Response{Text: "resp:" + msg}, nil
	}))

	ag := FromConfig(Config[appState]{
		ID:      "reusable",
		Gateway: gw,
		Model:   "m1",
		Task:    "hello",
	})

	// Run twice concurrently on the same immutable AI[S].
	errCh := make(chan error, 2)
	for range 2 {
		go func() {
			out, err := ag.Run(context.Background())
			if err == nil && out != "resp:hello" {
				err = fmt.Errorf("unexpected output: %v", out)
			}
			errCh <- err
		}()
	}
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent Run failed: %v", err)
		}
	}

	// Resume from a partially completed RunState: s1 already done, only s2 should run.
	calls.Store(0)
	resumed, err := ag.RunFrom(context.Background(), RunState{
		Plan: []PlanStep{
			{ID: "s1", Task: "step-one"},
			{ID: "s2", Task: "step-two", DependsOn: []string{"s1"}},
		},
		StepResults: map[string]any{"s1": "cached-s1"},
	})
	if err != nil {
		t.Fatalf("RunFrom failed: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected only 1 model call on resume (s2), got %d", calls.Load())
	}
	if !strings.Contains(fmt.Sprint(resumed), "s1: cached-s1") {
		t.Fatalf("expected resumed s2 to receive s1 output, got: %v", resumed)
	}
}

func TestDAGParallelSchedulerAndFailurePolicy(t *testing.T) {
	var active, maxConc atomic.Int32
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		cur := active.Add(1)
		for {
			prev := maxConc.Load()
			if cur <= prev || maxConc.CompareAndSwap(prev, cur) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		active.Add(-1)
		msg := req.Messages[len(req.Messages)-1].Content
		if strings.HasPrefix(msg, "flaky") {
			return gateway.Response{}, errors.New("boom")
		}
		return gateway.Response{Text: "ok:" + msg}, nil
	}))

	ag := FromConfig(Config[appState]{
		ID:            "dag-par",
		Gateway:       gw,
		Model:         "m1",
		MaxAttempts:   1,
		Scheduler:     DAGParallelScheduler{MaxConcurrency: 2},
		FailurePolicy: FallbackValue("fallback-val"),
		Pipeline: Pipeline[appState]{
			Plan: func(ctx Context, a *AI[appState]) error {
				SetPlan(ctx, a, []PlanStep{
					{ID: "a", Task: "task-a"},
					{ID: "b", Task: "task-b"},
					{ID: "c", Task: "flaky"},
				})
				return nil
			},
		},
	})

	out, err := ag.Run(context.Background())
	if err != nil {
		t.Fatalf("DAGParallel Run failed: %v", err)
	}
	if maxConc.Load() < 2 {
		t.Fatalf("expected parallel execution (>=2 concurrent), got maxConc=%d", maxConc.Load())
	}
	if out == nil {
		t.Fatalf("expected non-nil output")
	}
}

func TestLoopToolInvokerFuncMiddlewareAndRunAs(t *testing.T) {
	type addIn struct {
		A int `json:"a" desc:"first"`
		B int `json:"b" desc:"second"`
	}
	type calcOut struct {
		Sum int `json:"sum"`
	}

	var turn atomic.Int32
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		if turn.Add(1) == 1 {
			return gateway.Response{
				ToolCalls: []gateway.ToolCall{{
					ID:        "tc1",
					Name:      "add",
					Arguments: json.RawMessage(`{"a":2,"b":3}`),
				}},
			}, nil
		}
		// Second turn sees MsgTool result and returns JSON matching calcOut.
		last := req.Messages[len(req.Messages)-1]
		if last.Role != gateway.MsgTool || last.Content != "5" {
			return gateway.Response{}, fmt.Errorf("expected MsgTool=5, got %+v", last)
		}
		return gateway.Response{Text: `{"sum":5}`}, nil
	}))

	var mwCalled bool
	mw := func(next ToolInvoker) ToolInvoker {
		return ToolInvokerFunc(func(ctx Context, req ToolCallRequest) (ToolCallResult, error) {
			mwCalled = true
			return next.Invoke(ctx, req)
		})
	}

	addTool := Func("add", "adds two numbers", func(_ Context, in addIn) (int, error) {
		return in.A + in.B, nil
	})

	ag := FromConfig(Config[appState]{
		ID:              "react-calc",
		Gateway:         gw,
		Model:           "m1",
		Task:            "add 2 and 3",
		Tools:           []Tool{addTool},
		ToolInvoker:     LoopToolInvoker{MaxIterations: 5},
		ToolMiddlewares: []ToolMiddleware{mw},
	})

	res, err := RunAs[calcOut](context.Background(), ag)
	if err != nil {
		t.Fatalf("RunAs failed: %v", err)
	}
	if !mwCalled {
		t.Fatalf("expected ToolMiddleware to be called")
	}
	if res.Sum != 5 {
		t.Fatalf("expected Sum=5, got %+v", res)
	}
	if !strings.Contains(Untrusted("hi"), "<untrusted_data>") {
		t.Fatalf("Untrusted helper did not wrap text")
	}
}
