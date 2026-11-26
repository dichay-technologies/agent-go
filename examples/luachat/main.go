package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lua "github.com/yuin/gopher-lua"

	"proj.dichay.tech/agent-go"
	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/anthropic"
	"proj.dichay.tech/agent-go/gateway/gemini"
	"proj.dichay.tech/agent-go/gateway/jev"
	"proj.dichay.tech/agent-go/gateway/openai"
)

type ChatStats struct {
	ToolRuns    int
	SpawnedJobs int
}

type luaExecArgs struct {
	Code string `json:"code"`
}

type spawnWorkerArgs struct {
	ID         string   `json:"id,omitempty"`
	Task       string   `json:"task"`
	Tools      []string `json:"tools,omitempty"`
	MemoryKeys []string `json:"memory_keys,omitempty"`
	Background bool     `json:"background,omitempty"`
}

func main() {
	gw, chatModel, decisionModel := setupGateway()

	var stdinMu sync.Mutex
	scanner := bufio.NewScanner(os.Stdin)
	readLine := func() (string, bool) {
		stdinMu.Lock()
		defer stdinMu.Unlock()
		if !scanner.Scan() {
			return "", false
		}
		return scanner.Text(), true
	}

	var traceEnabled atomic.Bool
	traceEnabled.Store(true)

	// Gateway Middleware: logs every model call, latency, and token usage.
	gw.Use(func(next gateway.CompleteFunc) gateway.CompleteFunc {
		return func(ctx context.Context, req gateway.Request) (gateway.Response, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			if traceEnabled.Load() {
				if err != nil {
					fmt.Printf("  [ai:err] model=%s (%s): %v\n", req.Model, time.Since(start).Round(time.Millisecond), err)
				} else {
					fmt.Printf("  [ai] model=%s in=%d out=%d cached=%d (%s) calls=%d\n",
						resp.Model, resp.Usage.InputTokens, resp.Usage.OutputTokens, resp.Usage.CachedTokens,
						time.Since(start).Round(time.Millisecond), len(resp.ToolCalls))
				}
			}
			return resp, err
		}
	})

	// Tool 1: Full-access gopher-lua with interactive user permission prompt.
	luaExec := agentgo.NewTypedTool(
		"lua_exec",
		"Executes Lua 5.1 code on the host with full filesystem and shell access (os, io, io.open, io.popen, math, string, table). Use print(...) for stdout.",
		&gateway.JSONSchema{
			Type:     "object",
			Required: []string{"code"},
			Properties: map[string]*gateway.JSONSchema{
				"code": {Type: "string", Description: "Lua 5.1 source code to execute"},
			},
		},
		true,
		func(ctx agentgo.Context, in luaExecArgs) (any, error) {
			if !ctx.HasPermission("lua.exec") {
				return nil, fmt.Errorf("permission lua.exec denied")
			}
			fmt.Println("\n  ┌─ [PERMISSION] Agent wants to execute Lua code:")
			for _, line := range strings.Split(strings.TrimRight(in.Code, "\n"), "\n") {
				fmt.Printf("  │ %s\n", line)
			}
			fmt.Print("  └─ Allow execution? [y/N]: ")
			rawAns, ok := readLine()
			if !ok {
				return map[string]any{"denied": true, "reason": "stdin closed"}, nil
			}
			ans := strings.ToLower(strings.TrimSpace(rawAns))
			if ans != "y" && ans != "yes" && ans != "д" && ans != "да" {
				fmt.Println("  [lua] Denied by user.")
				return map[string]any{"denied": true, "reason": "user rejected execution"}, nil
			}

			res, err := runFullLua(ctx, in.Code, 10*time.Second)
			if err != nil {
				fmt.Printf("  [lua:err] %v\n", err)
				return nil, err
			}
			if stdout, _ := res["stdout"].(string); stdout != "" {
				fmt.Printf("  [lua:stdout]\n%s\n", stdout)
			}
			if ret, _ := res["return"].([]string); len(ret) > 0 {
				fmt.Printf("  [lua:return] %v\n", ret)
			}
			return res, nil
		},
	)

	// Tool 2: Spawn a child agent on the shared Bus, optionally passing selected memory keys.
	spawnWorker := agentgo.NewTypedTool(
		"spawn_worker",
		"Spawns a child AI agent on the shared message bus to handle a subtask. Supports passing memory_keys from your memory bank and running in background.",
		&gateway.JSONSchema{
			Type:     "object",
			Required: []string{"task"},
			Properties: map[string]*gateway.JSONSchema{
				"id":          {Type: "string", Description: "Optional worker ID on the bus"},
				"task":        {Type: "string", Description: "Subtask description for the child agent"},
				"tools":       {Type: "array", Description: "Optional subset of tool names to grant", Items: &gateway.JSONSchema{Type: "string"}},
				"memory_keys": {Type: "array", Description: "Optional memory keys to copy into the child's memory bank", Items: &gateway.JSONSchema{Type: "string"}},
				"background":  {Type: "boolean", Description: "If true, runs asynchronously and reports back via agent_send or WaitDeps"},
			},
		},
		true,
		func(ctx agentgo.Context, in spawnWorkerArgs) (any, error) {
			parent, _ := ctx.Agent().(*agentgo.AI[ChatStats])
			workerID := in.ID
			if workerID == "" {
				workerID = fmt.Sprintf("worker-%d", time.Now().UnixNano()%10000)
			}
			opts := []agentgo.Option[ChatStats]{
				agentgo.WithID[ChatStats](workerID),
				agentgo.WithSkipPlanning[ChatStats](true),
				agentgo.WithTask[ChatStats](in.Task),
			}
			if len(in.Tools) > 0 && parent != nil {
				allowed := make(map[string]bool, len(in.Tools))
				for _, name := range in.Tools {
					allowed[name] = true
				}
				var selected []agentgo.Tool
				for _, pt := range parent.Tools {
					if allowed[pt.Name()] {
						selected = append(selected, pt)
					}
				}
				opts = append(opts, agentgo.WithTools[ChatStats](selected...))
			}
			child := agentgo.Fork[ChatStats, ChatStats](parent, opts...)
			if len(in.MemoryKeys) > 0 && ctx.Bank() != nil {
				ctx.Bank().CopyTo(child.EnsureBank(), in.MemoryKeys...)
			}
			fmt.Printf("  [spawn] launching child %q (depth=%d, bg=%v) for: %s\n",
				child.ID, child.BaseSpawnDepth, in.Background, in.Task)

			fut, err := ctx.SpawnNamed(child.ID, child)
			if err != nil {
				return nil, err
			}
			if parent != nil {
				parent.Memory.Update(func(s *ChatStats) { s.SpawnedJobs++ })
			}
			if in.Background {
				return map[string]any{"worker": child.ID, "status": "running_in_background"}, nil
			}
			out, err := fut.Await(ctx)
			if err != nil {
				return nil, err
			}
			fmt.Printf("  [spawn] child %q finished.\n", child.ID)
			return map[string]any{"worker": child.ID, "output": out}, nil
		},
	)

	stats := agentgo.NewLocalState(ChatStats{})

	// Wrap default stages with FSM phase logging.
	tracedStage := func(name string, fn agentgo.StageFunc[ChatStats]) agentgo.StageFunc[ChatStats] {
		return func(ctx agentgo.Context, a *agentgo.AI[ChatStats]) error {
			if traceEnabled.Load() {
				fmt.Printf("  [fsm] -> %s (status=%s)\n", name, ctx.RunState().Status)
			}
			err := fn(ctx, a)
			if err == nil && name == "Plan" && traceEnabled.Load() {
				if plan := ctx.RunState().Plan; len(plan) > 0 {
					b, _ := json.Marshal(plan)
					fmt.Printf("  [plan] %s\n", string(b))
				}
			}
			return err
		}
	}

	// Adaptive Plan stage: decisionModel (Jev) decides whether multi-step planning is needed.
	adaptivePlan := func(ctx agentgo.Context, a *agentgo.AI[ChatStats]) error {
		taskText := ctx.RunState().Task
		if decisionModel != "" && taskText != "" {
			resp, err := ctx.Call(gateway.Request{
				Model: decisionModel,
				Messages: []gateway.Message{{
					Role:    gateway.MsgUser,
					Content: fmt.Sprintf("Does this task require a multi-step execution plan, or can it be answered/executed directly in one step?\nTask: %s", taskText),
				}},
				ResponseSchema: &gateway.JSONSchema{
					Type:     "object",
					Required: []string{"needs_plan"},
					Properties: map[string]*gateway.JSONSchema{
						"needs_plan": {Type: "string", Enum: []any{"yes", "no"}},
					},
				},
			})
			if err == nil {
				var dec struct {
					NeedsPlan string `json:"needs_plan"`
				}
				if json.Unmarshal([]byte(resp.Text), &dec) == nil && dec.NeedsPlan == "no" {
					if traceEnabled.Load() {
						fmt.Printf("  [plan] %s decided: direct execute\n", decisionModel)
					}
					return nil
				}
			}
		}
		return agentgo.DefaultPlan(ctx, a)
	}

	ag := agentgo.New[ChatStats](
		agentgo.WithID[ChatStats]("luachat-root"),
		agentgo.WithProject[ChatStats]("luachat"),
		agentgo.WithGateway[ChatStats](gw),
		agentgo.WithModel[ChatStats](chatModel),
		agentgo.WithStageModel[ChatStats](agentgo.StatusAnalyzing, decisionModel),
		agentgo.WithStageModel[ChatStats](agentgo.StatusPlanning, chatModel),
		agentgo.WithStageModel[ChatStats](agentgo.StatusExecuting, decisionModel),
		agentgo.WithStageModel[ChatStats](agentgo.StatusEscalated, chatModel),
		agentgo.WithPermissions[ChatStats]("lua.exec", "worker.spawn"),
		agentgo.WithSystemPrefix[ChatStats](
			"You are a helpful conversational assistant with self-managed memory (memory_set/get/delete), controllable chat context (chat_inspect/pin/manage/clear), a personal task list (task_add/update/list), inter-agent messaging (agent_send/recv/peers), lua_exec, and spawn_worker. Reply naturally in plain text and use tools only when helpful.",
		),
		agentgo.WithWorkspaceTools[ChatStats](),
		agentgo.WithTools[ChatStats](luaExec, spawnWorker),
		agentgo.WithToolInvoker[ChatStats](agentgo.LoopToolInvoker{
			MaxIterations:       8,
			ReplyModel:          chatModel,
			ContinueOnToolError: true,
		}),
		agentgo.WithPipeline(agentgo.Pipeline[ChatStats]{
			Analyze:       tracedStage("Analyze", agentgo.DefaultAnalyze[ChatStats]),
			GatherContext: tracedStage("GatherContext", agentgo.DefaultGatherContext[ChatStats]),
			Plan:          tracedStage("Plan", adaptivePlan),
			Execute:       tracedStage("Execute", agentgo.DefaultExecute[ChatStats]),
			WaitDeps:      tracedStage("WaitDeps", agentgo.DefaultWaitDeps[ChatStats]),
		}),
		agentgo.WithHooks(agentgo.Hooks[ChatStats]{
			OnStep: func(_ agentgo.Context, st *agentgo.LocalState[ChatStats], stepID string, res agentgo.ToolCallResult) {
				if res.ToolName != "" {
					st.Update(func(s *ChatStats) { s.ToolRuns++ })
				}
				if traceEnabled.Load() {
					fmt.Printf("  [hook:step] %s done (tool=%q)\n", stepID, res.ToolName)
				}
			},
			OnEscalate: func(_ agentgo.Context, _ *agentgo.LocalState[ChatStats], history []agentgo.AttemptLog) error {
				if traceEnabled.Load() {
					fmt.Printf("  [hook:escalate] guardrail triggered after %d attempts\n", len(history))
				}
				return nil
			},
			OnFail: func(_ agentgo.Context, _ *agentgo.LocalState[ChatStats], err error) {
				fmt.Printf("  [hook:fail] %v\n", err)
			},
		}),
	)
	ag.Memory = stats
	ag.ArgModel = chatModel

	// Log inter-agent Bus traffic when tracing is enabled.
	ag.EnsureBus().Subscribe("*", func(env agentgo.Envelope) {
		if traceEnabled.Load() {
			fmt.Printf("  [bus] %s -> %s (%s): %s\n", env.From, env.To, env.Kind, env.Content)
		}
	})

	fmt.Printf("Luachat ready (chat: %s, decision: %s) — MemoryBank, Bus & 4-phase FSM enabled.\n", chatModel, decisionModel)
	fmt.Println("Commands: /memory, /set <k>=<v>, /tasks, /chat, /compact, /tell <msg>, /peers, /usage, /trace, /exit")

	for {
		fmt.Print("\nYou> ")
		rawLine, ok := readLine()
		if !ok {
			break
		}
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		switch {
		case line == "/exit" || line == "/quit":
			fmt.Printf("\nTotal usage: %+v\n", gw.UsageByProject("luachat"))
			return
		case line == "/usage":
			fmt.Printf("Usage: %+v\n", gw.UsageByProject("luachat"))
			continue
		case line == "/trace":
			next := !traceEnabled.Load()
			traceEnabled.Store(next)
			fmt.Printf("Trace logging: %v\n", next)
			continue
		case line == "/memory":
			b, _ := json.MarshalIndent(ag.EnsureBank().Entries(), "", "  ")
			fmt.Printf("MemoryBank Facts:\n%s\nStats: %+v\n", string(b), ag.Memory.Get())
			continue
		case strings.HasPrefix(line, "/set "):
			kv := strings.SplitN(strings.TrimPrefix(line, "/set "), "=", 2)
			if len(kv) != 2 {
				fmt.Println("Usage: /set key=value")
				continue
			}
			ag.EnsureBank().Set(strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1]), "user")
			fmt.Printf("Saved %q to agent MemoryBank.\n", strings.TrimSpace(kv[0]))
			continue
		case line == "/tasks":
			b, _ := json.MarshalIndent(ag.EnsureBank().Tasks(), "", "  ")
			fmt.Printf("Agent Tasks:\n%s\n", string(b))
			continue
		case line == "/chat":
			for _, ce := range ag.EnsureBank().ChatEntries() {
				pin := ""
				if ce.Pinned {
					pin = " [pinned]"
				}
				fmt.Printf("  %s (%s)%s: %s\n", ce.ID, ce.Message.Role, pin, ce.Message.Content)
			}
			continue
		case line == "/compact":
			n := ag.EnsureBank().CompactChat("Earlier conversation compacted by user.", 4)
			fmt.Printf("Compacted %d older messages.\n", n)
			continue
		case strings.HasPrefix(line, "/tell "):
			msg := strings.TrimSpace(strings.TrimPrefix(line, "/tell "))
			_, _ = ag.Tell("user-cli", msg)
			fmt.Println("Queued event into agent Bus inbox (will be processed on next turn).")
			continue
		case line == "/peers":
			fmt.Printf("Bus peers: %v\n", ag.EnsureBus().Peers())
			continue
		}

		fmt.Print("AI> ")
		var streamed bool
		for delta, err := range ag.StreamChat(context.Background(), line) {
			if err != nil {
				fmt.Printf("\nError: %v\n", err)
				streamed = true
				break
			}
			fmt.Print(delta)
			streamed = true
		}
		if streamed {
			fmt.Println()
		}
	}
}

func setupGateway() (*gateway.AIGateway, string, string) {
	gw := gateway.New(gateway.Config{EnableCache: true})
	var chatModel, decisionModel string

	if key := os.Getenv("GEMINI_API_KEY"); key != "" {
		model := gemini.DefaultModel
		if m := os.Getenv("GEMINI_MODEL"); m != "" {
			model = m
		}
		p, err := gemini.New(gemini.Config{
			APIKey:       key,
			DefaultModel: model,
		})
		if err != nil {
			log.Fatal(err)
		}
		gw.Register("gemini", p)
		chatModel = model
	}
	if key := os.Getenv("OPENAI_API_KEY"); key != "" {
		model := openai.DefaultModel
		if m := os.Getenv("OPENAI_MODEL"); m != "" {
			model = m
		}
		p, err := openai.New(openai.Config{
			APIKey:       key,
			BaseURL:      os.Getenv("OPENAI_BASE_URL"),
			DefaultModel: model,
		})
		if err != nil {
			log.Fatal(err)
		}
		gw.Register("openai", p)
		if chatModel == "" {
			chatModel = model
		}
	}
	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		p, err := anthropic.New(anthropic.Config{APIKey: key})
		if err != nil {
			log.Fatal(err)
		}
		gw.Register("anthropic", p)
		if chatModel == "" {
			chatModel = anthropic.DefaultModel
		}
	}
	if key := os.Getenv("TYPESAFE_API_KEY"); key != "" {
		p, err := jev.New(jev.Config{APIKey: key})
		if err != nil {
			log.Fatal(err)
		}
		gw.Register("jev", p)
		decisionModel = jev.DefaultModel
	}

	if chatModel == "" {
		log.Fatal("Set GEMINI_API_KEY, OPENAI_API_KEY, or ANTHROPIC_API_KEY to run luachat")
	}
	if decisionModel == "" {
		decisionModel = chatModel
	}
	return gw, chatModel, decisionModel
}

// runFullLua runs Lua with ALL standard libraries open (os, io, debug, etc.).
func runFullLua(parent context.Context, code string, timeout time.Duration) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	L := lua.NewState()
	defer L.Close()
	L.SetContext(ctx)

	var stdout []string
	L.SetGlobal("print", L.NewFunction(func(L *lua.LState) int {
		top := L.GetTop()
		parts := make([]string, top)
		for i := 1; i <= top; i++ {
			parts[i-1] = L.Get(i).String()
		}
		stdout = append(stdout, strings.Join(parts, "\t"))
		return 0
	}))

	if err := L.DoString(code); err != nil {
		return nil, fmt.Errorf("lua error: %w", err)
	}

	var ret []string
	top := L.GetTop()
	for i := 1; i <= top; i++ {
		ret = append(ret, L.Get(i).String())
	}

	return map[string]any{
		"stdout": strings.Join(stdout, "\n"),
		"return": ret,
	}, nil
}
