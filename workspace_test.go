package agentgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/mock"
)

func TestInterAgentBusAndMemoryTransfer(t *testing.T) {
	bus := NewBus()
	coordBank := NewMemoryBank()
	coordBank.Set("fps", 60, "video")
	coordBank.SetPinned("resolution", "1080x1920", true, "video")

	workerBank := NewMemoryBank()
	workerBank.SetPinned("locked_key", "keep-me", true)

	bus.Register("coordinator", coordBank)
	bus.Register("audio-worker", workerBank)
	bus.Register("html-worker", NewMemoryBank())

	var seenBroadcast atomic.Int32
	unsub := bus.Subscribe("*", func(env Envelope) {
		if env.To == "*" {
			seenBroadcast.Add(1)
		}
	})
	defer unsub()

	// 1. Coordinator sends message + shared memory to audio-worker.
	_, err := bus.Send(Envelope{
		From:    "coordinator",
		To:      "audio-worker",
		Kind:    "task",
		Content: "Sync beats at target FPS",
		Memory:  coordBank.Export("fps", "locked_key"),
	})
	if err != nil {
		t.Fatalf("bus.Send failed: %v", err)
	}

	// Verify memory was imported into audio-worker, while pinned key stayed protected.
	fpsVal, ok := workerBank.Value("fps")
	if !ok || fpsVal != 60 {
		t.Fatalf("expected transferred fps=60 in workerBank, got %v (ok=%v)", fpsVal, ok)
	}
	lockedVal, _ := workerBank.Value("locked_key")
	if lockedVal != "keep-me" {
		t.Fatalf("expected pinned key not to be overwritten, got %v", lockedVal)
	}

	msg, err := bus.Receive(context.Background(), "audio-worker")
	if err != nil || msg.Content != "Sync beats at target FPS" {
		t.Fatalf("unexpected received message: %+v, err=%v", msg, err)
	}

	// 2. Broadcast reaches both workers (excluding sender).
	if _, err := bus.Send(Envelope{From: "coordinator", To: "*", Content: "render starting"}); err != nil {
		t.Fatalf("broadcast failed: %v", err)
	}
	if seenBroadcast.Load() != 1 {
		t.Fatalf("expected wildcard subscriber callback once, got %d", seenBroadcast.Load())
	}
	if _, ok := bus.TryReceive("audio-worker"); !ok {
		t.Fatalf("expected audio-worker to receive broadcast")
	}
	if _, ok := bus.TryReceive("html-worker"); !ok {
		t.Fatalf("expected html-worker to receive broadcast")
	}
	if _, ok := bus.TryReceive("coordinator"); ok {
		t.Fatalf("sender should not receive its own broadcast")
	}
}

func TestAgentSelfManagedWorkspaceChatAndAgentsTalking(t *testing.T) {
	var turn atomic.Int32
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		n := turn.Add(1)
		switch n {
		case 1:
			// Turn 1: Agent saves a fact, adds a task, and pins nothing yet.
			return gateway.Response{
				ToolCalls: []gateway.ToolCall{
					{ID: "c1", Name: "memory_set", Arguments: json.RawMessage(`{"key":"voice_id","value":"Kore","tags":["tts"]}`)},
					{ID: "c2", Name: "task_add", Arguments: json.RawMessage(`{"id":"t1","title":"Render intro"}`)},
				},
			}, nil
		case 2:
			return gateway.Response{Text: "Saved voice Kore and added task t1."}, nil
		case 3:
			// Turn 2: Verify Workspace facts & tasks + Tell event + prior chat turn are all in context!
			var combined strings.Builder
			for _, m := range req.Messages {
				combined.WriteString(m.Content + "\n")
			}
			all := combined.String()
			if !strings.Contains(all, "voice_id") || !strings.Contains(all, "Render intro") {
				return gateway.Response{}, fmt.Errorf("missing workspace memory/tasks in context: %s", all)
			}
			if !strings.Contains(all, "Scene 2 locked by user") {
				return gateway.Response{}, fmt.Errorf("missing Tell event in context: %s", all)
			}
			if !strings.Contains(all, "Saved voice Kore") {
				return gateway.Response{}, fmt.Errorf("missing prior chat history in context: %s", all)
			}
			// Agent updates task status and compacts chat!
			return gateway.Response{
				ToolCalls: []gateway.ToolCall{
					{ID: "c3", Name: "task_update", Arguments: json.RawMessage(`{"id":"t1","status":"done","notes":"rendered"}`)},
					{ID: "c4", Name: "chat_manage", Arguments: json.RawMessage(`{"summary":"Configured voice Kore","keep_last":1}`)},
				},
			}, nil
		default:
			return gateway.Response{Text: "Intro done and chat compacted!"}, nil
		}
	}))

	coord := New[appState](
		WithID[appState]("coord"),
		WithProject[appState]("video-proj"),
		WithGateway[appState](gw),
		WithModel[appState]("m1"),
		WithSystemPrefix[appState]("Coordinator system prefix"),
		WithWorkspaceTools[appState](),
		WithToolInvoker[appState](LoopToolInvoker{MaxIterations: 5}),
	)

	// Host pre-populates a pinned chat rule that must survive compaction.
	ruleID := coord.EnsureBank().AppendMessage(gateway.Message{Role: gateway.MsgUser, Content: "RULE: Never touch locked scenes"}, true)

	reply1, err := coord.Chat(context.Background(), "Use voice Kore and plan intro")
	if err != nil {
		t.Fatalf("Chat turn 1 failed: %v", err)
	}
	if !strings.Contains(reply1, "Saved voice Kore") {
		t.Fatalf("unexpected reply1: %q", reply1)
	}

	// External host/UI sends an asynchronous Tell instruction before Turn 2.
	if _, err := coord.Tell("editor-ui", "Scene 2 locked by user"); err != nil {
		t.Fatalf("Tell failed: %v", err)
	}

	reply2, err := coord.Chat(context.Background(), "Finish intro and clean up chat")
	if err != nil {
		t.Fatalf("Chat turn 2 failed: %v", err)
	}
	if reply2 != "Intro done and chat compacted!" {
		t.Fatalf("unexpected reply2: %q", reply2)
	}

	// Verify task t1 is done, voice_id is stored, and pinned chat message survived compaction.
	tasks := coord.EnsureBank().Tasks(TaskDone)
	if len(tasks) != 1 || tasks[0].ID != "t1" {
		t.Fatalf("expected task t1 to be done, got %+v", tasks)
	}
	entries := coord.EnsureBank().ChatEntries()
	foundPinned := false
	foundSummary := false
	for _, ce := range entries {
		if ce.ID == ruleID && ce.Pinned {
			foundPinned = true
		}
		if ce.Summary && strings.Contains(ce.Message.Content, "Configured voice Kore") {
			foundSummary = true
		}
	}
	if !foundPinned || !foundSummary {
		t.Fatalf("expected pinned message + summary in chat entries, got %+v", entries)
	}

	// Forked subagent automatically shares Bus and can message "parent" + transfer memory via BusTools.
	sub := Fork[appState, appState](coord,
		WithID[appState]("lua-sub"),
		WithSkipPlanning[appState](true),
	)
	sub.EnsureBank().Set("generated_shader", "kage_ transition_v1")

	subCtx := NewContext(context.Background(), ContextConfig{
		Gateway: gw,
		Agent:   sub,
		Bus:     sub.Bus,
		Bank:    sub.Bank,
	})
	var sendTool Tool
	for _, tl := range sub.Tools {
		if tl.Name() == "agent_send" {
			sendTool = tl
			break
		}
	}
	if sendTool == nil {
		t.Fatalf("expected forked subagent to inherit agent_send tool")
	}
	_, err = sendTool.Execute(subCtx, json.RawMessage(`{
		"to": "parent",
		"kind": "artifact",
		"content": "Shader ready",
		"share_memory_keys": ["generated_shader"]
	}`))
	if err != nil {
		t.Fatalf("subagent agent_send to parent failed: %v", err)
	}

	shaderVal, ok := coord.EnsureBank().Value("generated_shader")
	if !ok || shaderVal != "kage_ transition_v1" {
		t.Fatalf("expected parent bank to receive subagent's shared memory, got %v (ok=%v)", shaderVal, ok)
	}
	inbox := coord.EnsureBus().Drain("coord")
	if len(inbox) != 1 || inbox[0].From != "lua-sub" || inbox[0].Content != "Shader ready" {
		t.Fatalf("expected parent inbox to contain subagent message, got %+v", inbox)
	}
}

func TestDynamicPlanAndSubFuturePointToPointDependency(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		msg := req.Messages[len(req.Messages)-1].Content
		return gateway.Response{Text: "done:" + msg}, nil
	}))

	slowUnrelatedDone := make(chan struct{})
	defer close(slowUnrelatedDone)

	root := New[appState](
		WithID[appState]("coord-dag"),
		WithGateway[appState](gw),
		WithModel[appState]("m1"),
		WithScheduler[appState](DAGParallelScheduler{MaxConcurrency: 4}),
		WithPipeline(Pipeline[appState]{
			Plan: func(ctx Context, a *AI[appState]) error {
				// Spawn 2 background subagents:
				// - "tts_3" finishes quickly with audio path
				// - "broll_slow" takes much longer; step "assemble_3" must NOT block on "broll_slow"!
				ttsChild := &stubAgent{fn: func(_ context.Context) (any, error) {
					time.Sleep(15 * time.Millisecond)
					return "scene3.wav", nil
				}}
				slowChild := &stubAgent{fn: func(_ context.Context) (any, error) {
					select {
					case <-time.After(80 * time.Millisecond):
					case <-slowUnrelatedDone:
					}
					return "broll.mp4", nil
				}}
				if _, err := a.SpawnAsync(ctx, "tts_3", ttsChild); err != nil {
					return err
				}
				if _, err := a.SpawnAsync(ctx, "broll_slow", slowChild); err != nil {
					return err
				}

				SetPlan(ctx, a, []PlanStep{
					{ID: "prep", Task: "prepare"},
					{ID: "assemble_3", Task: "assemble scene 3", DependsOn: []string{"tts_3"}},
				})
				return nil
			},
		}),
		WithHooks(Hooks[appState]{
			OnStep: func(ctx Context, _ *LocalState[appState], stepID string, _ ToolCallResult) {
				if stepID == "prep" {
					// Dynamically inject a final step during execution!
					AppendPlanSteps(ctx, PlanStep{
						ID:        "finalize",
						Task:      "export",
						DependsOn: []string{"assemble_3"},
					})
				}
			},
		}),
	)

	out, err := root.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	outStr := fmt.Sprint(out)
	if !strings.Contains(outStr, "assemble_3:") || !strings.Contains(outStr, "scene3.wav") {
		t.Fatalf("expected finalize step to receive assemble_3 + tts_3 SubFuture output, got %q", outStr)
	}
}

func TestPermissionsNarrowingAllowSubagentsAndSchedulerFailure(t *testing.T) {
	parent := New[appState](
		WithID[appState]("parent"),
		WithPermissions[appState]("video.read", "audio.mix"),
	)

	// Child attempts to request "filesystem.write" (which parent lacks) + "video.read".
	child := Fork[appState, appState](parent,
		WithID[appState]("child"),
		WithPermissions[appState]("video.read", "filesystem.write"),
		WithAllowSubagents[appState](false),
	)
	if len(child.Permissions) != 1 || child.Permissions[0] != "video.read" {
		t.Fatalf("expected child permissions narrowed to [video.read], got %v", child.Permissions)
	}

	ctx := NewContext(context.Background(), ContextConfig{
		Agent:       child,
		Permissions: child.Permissions,
	})
	if !ctx.HasPermission("video.read") || ctx.HasPermission("filesystem.write") {
		t.Fatalf("unexpected HasPermission results for narrowed child")
	}

	// AllowSubagents=false blocks dynamic spawning.
	if _, err := child.SpawnAsync(ctx, "grandchild", parent); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("expected spawning disabled error, got %v", err)
	}

	// Scheduler must NOT swallow step-2 failure when step-1 succeeded under FailureFail.
	sched := SequentialScheduler{}
	_, _, serr := sched.Schedule(ctx, []PlanStep{
		{ID: "s1", Task: "ok"},
		{ID: "s2", Task: "fail", DependsOn: []string{"s1"}},
	}, nil, func(_ Context, step PlanStep, _ map[string]any) (any, error) {
		if step.ID == "s2" {
			return nil, errors.New("step 2 boom")
		}
		return "s1-ok", nil
	})
	if serr == nil || !strings.Contains(serr.Error(), "step 2 boom") {
		t.Fatalf("expected scheduler to return step-2 error even after step-1 succeeded, got %v", serr)
	}
}

func TestGatesPauseResumeSelectiveGatherPermissionMiddlewareAndFailureBacklog(t *testing.T) {
	var planCalls, execCalls atomic.Int32
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		msg := req.Messages[len(req.Messages)-1].Content
		switch {
		case strings.HasPrefix(msg, "Select only the data sources"):
			return gateway.Response{Text: `{"sources":["audio_timings"]}`}, nil
		case strings.HasPrefix(msg, "Create an execution plan"):
			planCalls.Add(1)
			return gateway.Response{Text: "```json\n{\"steps\":[{\"id\":\"s1\",\"task\":\"good\"},{\"id\":\"s2\",\"task\":\"bad\"}]}\n```"}, nil
		case strings.HasPrefix(msg, "bad") || strings.Contains(msg, "Step s2 failed"):
			return gateway.Response{}, errors.New("broken render")
		default:
			execCalls.Add(1)
			return gateway.Response{Text: "ok:" + msg}, nil
		}
	}))

	var droppedLoaded atomic.Bool
	ag := New[appState](
		WithID[appState]("gated-coord"),
		WithGateway[appState](gw),
		WithModel[appState]("m1"),
		WithEnablePlan[appState](true),
		WithSelectiveGather[appState](true),
		WithGates[appState]("plan"),
		WithTask[appState]("sync audio"),
		WithMaxAttemptsHelper[appState](1),
		WithFailurePolicy[appState](FallbackValue("placeholder-scene")),
		WithData[appState](
			StaticData("audio_timings", "beat=1.2s"),
			funcDataSource{name: "unused_heavy_table", fn: func() string {
				droppedLoaded.Store(true)
				return "huge"
			}},
		),
	)

	// 1. Run pauses right after "plan" gate.
	res1, err := ag.RunDetailed(context.Background())
	if !errors.Is(err, ErrGatePaused) || !res1.Paused || res1.Gate != "plan" {
		t.Fatalf("expected ErrGatePaused at plan gate, got res=%+v err=%v", res1, err)
	}
	if droppedLoaded.Load() {
		t.Fatalf("SelectiveGather should not have loaded unused_heavy_table")
	}
	if !strings.Contains(res1.State.ContextData, "beat=1.2s") || len(res1.State.Plan) != 2 {
		t.Fatalf("expected gathered audio_timings and 2 parsed fenced-JSON plan steps, got %+v", res1.State)
	}

	// 2. Serialize & deserialize RunState, then Resume: "gather" and "plan" must NOT re-run!
	snapJSON, err := json.Marshal(res1.State)
	if err != nil {
		t.Fatalf("marshal RunState: %v", err)
	}
	var restored RunState
	if err := json.Unmarshal(snapJSON, &restored); err != nil {
		t.Fatalf("unmarshal RunState: %v", err)
	}

	res2, err := ag.Resume(context.Background(), restored)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	if planCalls.Load() != 1 {
		t.Fatalf("expected plan stage not to re-run on Resume, planCalls=%d", planCalls.Load())
	}
	if res2.Output != "placeholder-scene" {
		t.Fatalf("expected FallbackValue output on s2 failure, got %v", res2.Output)
	}
	if len(res2.State.Failures) != 1 || res2.State.Failures[0].StepID != "s2" || res2.State.Failures[0].Action != FailureFallback {
		t.Fatalf("expected FailureRecord in RunState.Failures backlog, got %+v", res2.State.Failures)
	}

	// 3. Two-level PermissionMiddleware + ToolWithPermission check.
	mw := PermissionMiddleware(map[string]string{
		"safe_read":   "video.read",
		"danger_exec": "host.shell",
	})
	safeTool := NewTool(ToolDef{Name: "safe_read", Execute: func(_ Context, _ json.RawMessage) (any, error) { return "ok", nil }})
	dangerTool := NewTool(ToolDef{Name: "danger_exec", Execute: func(_ Context, _ json.RawMessage) (any, error) { return "pwn", nil }})

	restrictedCtx := NewContext(context.Background(), ContextConfig{Permissions: []string{"video.read"}})
	wrappedInv := mw(ToolInvokerFunc(func(_ Context, req ToolCallRequest) (ToolCallResult, error) {
		if len(req.Tools) != 1 || req.Tools[0].Name() != "safe_read" {
			return ToolCallResult{}, fmt.Errorf("expected only safe_read, got %d tools", len(req.Tools))
		}
		return ToolCallResult{Selected: true, ToolName: "safe_read"}, nil
	}))
	if _, err := wrappedInv.Invoke(restrictedCtx, ToolCallRequest{Tools: []Tool{safeTool, dangerTool}}); err != nil {
		t.Fatalf("PermissionMiddleware failed: %v", err)
	}
	guardedDanger := ToolWithPermission("host.shell", dangerTool)
	if _, err := guardedDanger.Execute(restrictedCtx, nil); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected ErrPermissionDenied on direct Execute, got %v", err)
	}
}

type mockStreamProv struct{}

func (mockStreamProv) Name() string { return "stream-mock" }
func (mockStreamProv) Complete(_ context.Context, _ gateway.Request) (gateway.Response, error) {
	return gateway.Response{Text: "fallback"}, nil
}
func (mockStreamProv) Stream(_ context.Context, _ gateway.Request) iter.Seq2[gateway.StreamChunk, error] {
	return func(yield func(gateway.StreamChunk, error) bool) {
		for _, tok := range []string{"Hello", ", ", "stream!"} {
			if !yield(gateway.StreamChunk{Text: tok}, nil) {
				return
			}
		}
		u := gateway.Usage{InputTokens: 4, OutputTokens: 3}
		yield(gateway.StreamChunk{Usage: &u, Done: true}, nil)
	}
}

func TestStreamChatAndProjectSnapshotPersistence(t *testing.T) {
	gw := gateway.New(gateway.Config{})
	gw.Register("stream-mock", mockStreamProv{})

	var hookChunks []string
	ag := New[appState](
		WithID[appState]("streamer"),
		WithGateway[appState](gw),
		WithModel[appState]("stream-mock/m1"),
		WithWorkspaceTools[appState](),
		WithToolInvoker[appState](LoopToolInvoker{MaxIterations: 3}),
		WithHooks(Hooks[appState]{
			OnStream: func(_ Context, chunk gateway.StreamChunk) {
				hookChunks = append(hookChunks, chunk.Text)
			},
		}),
	)
	ag.EnsureBank().Set("project", "demo-reel")

	var parts []string
	for delta, err := range ag.StreamChat(context.Background(), "Say hello") {
		if err != nil {
			t.Fatalf("StreamChat err: %v", err)
		}
		parts = append(parts, delta)
	}
	if strings.Join(parts, "") != "Hello, stream!" || len(parts) != 3 || len(hookChunks) != 3 {
		t.Fatalf("unexpected streamed chunks: parts=%v hook=%v", parts, hookChunks)
	}
	if msgs := ag.EnsureBank().ChatMessages(); len(msgs) != 2 || msgs[1].Content != "Hello, stream!" {
		t.Fatalf("expected streamed assistant reply saved to MemoryBank, got %+v", msgs)
	}

	// Test SaveSnapshot & LoadSnapshot on disk.
	snapPath := t.TempDir() + "/proj.snapshot.json"
	state := RunState{Status: StatusPaused, PausedAtGate: "plan", Plan: []PlanStep{{ID: "s1", Task: "cut"}}}
	if err := SaveSnapshot(snapPath, state, ag.EnsureBank()); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	restoredBank := NewMemoryBank()
	loaded, err := LoadSnapshot(snapPath, restoredBank)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if loaded.RunState.PausedAtGate != "plan" || len(loaded.RunState.Plan) != 1 {
		t.Fatalf("unexpected loaded RunState: %+v", loaded.RunState)
	}
	if v, ok := restoredBank.Value("project"); !ok || v != "demo-reel" {
		t.Fatalf("unexpected restored bank fact: %v (ok=%v)", v, ok)
	}
}

func WithMaxAttemptsHelper[S any](n int) Option[S] {
	return func(a *AI[S]) { a.MaxAttempts = n }
}

type funcDataSource struct {
	name string
	fn   func() string
}

func (f funcDataSource) Name() string                      { return f.name }
func (f funcDataSource) Content(_ Context) (string, error) { return f.fn(), nil }

