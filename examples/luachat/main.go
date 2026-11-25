package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
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

type ChatState struct {
	History     []gateway.Message
	ToolRuns    int
	SpawnedJobs int
}

type luaExecArgs struct {
	Code string `json:"code"`
}

type spawnWorkerArgs struct {
	Task string `json:"task"`
}

func main() {
	gw, chatModel, decisionModel := setupGateway()
	scanner := bufio.NewScanner(os.Stdin)
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
		"Executes Lua 5.1 code with full standard libraries (os, io, math, string, table). Use print(...) for stdout.",
		&gateway.JSONSchema{
			Type:     "object",
			Required: []string{"code"},
			Properties: map[string]*gateway.JSONSchema{
				"code": {Type: "string", Description: "Lua 5.1 source code to execute"},
			},
		},
		true,
		func(ctx agentgo.Context, in luaExecArgs) (any, error) {
			fmt.Println("\n  ┌─ [PERMISSION] Agent wants to execute Lua code:")
			for _, line := range strings.Split(strings.TrimRight(in.Code, "\n"), "\n") {
				fmt.Printf("  │ %s\n", line)
			}
			fmt.Print("  └─ Allow execution? [y/N]: ")
			if !scanner.Scan() {
				return map[string]any{"denied": true, "reason": "stdin closed"}, nil
			}
			ans := strings.ToLower(strings.TrimSpace(scanner.Text()))
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

	// Tool 2: Spawn a child agent using ctx.Spawn(agentgo.Fork(...)).
	spawnWorker := agentgo.NewTypedTool(
		"spawn_worker",
		"Spawns an independent child AI agent to handle a subtask and returns its result.",
		&gateway.JSONSchema{
			Type:     "object",
			Required: []string{"task"},
			Properties: map[string]*gateway.JSONSchema{
				"task": {Type: "string", Description: "Subtask description for the child agent"},
			},
		},
		true,
		func(ctx agentgo.Context, in spawnWorkerArgs) (any, error) {
			parent, _ := ctx.Agent().(*agentgo.AI[ChatState])
			child := agentgo.Fork[ChatState, ChatState](parent,
				agentgo.WithID[ChatState](fmt.Sprintf("worker-%d", time.Now().UnixNano()%10000)),
				agentgo.WithSkipPlanning[ChatState](true),
				agentgo.WithTask[ChatState](in.Task),
			)
			fmt.Printf("  [spawn] launching child %q (depth=%d, parent=%s) for: %s\n",
				child.ID, child.State.SpawnDepth, parent.ID, in.Task)

			fut, err := ctx.Spawn(child)
			if err != nil {
				return nil, err
			}
			out, err := fut.Await(ctx)
			if err != nil {
				return nil, err
			}
			parent.Memory.Update(func(s *ChatState) { s.SpawnedJobs++ })
			fmt.Printf("  [spawn] child %q finished.\n", child.ID)
			return map[string]any{"worker": child.ID, "output": out}, nil
		},
	)

	state := agentgo.NewLocalState(ChatState{})

	chatInvoker := agentgo.ToolInvokerFunc(func(ctx agentgo.Context, req agentgo.ToolCallRequest) (agentgo.ToolCallResult, error) {
		specs := make([]gateway.ToolSpec, len(req.Tools))
		toolMap := make(map[string]agentgo.Tool, len(req.Tools))
		for i, t := range req.Tools {
			toolMap[t.Name()] = t
			specs[i] = gateway.ToolSpec{
				Name:        t.Name(),
				Description: t.Description(),
				Parameters:  t.Schema(),
			}
		}

		msgs := append([]gateway.Message(nil), req.Messages...)
		for turn := 0; turn < 5; turn++ {
			selectModel := req.Model
			if turn == 0 && decisionModel != "" {
				selectModel = decisionModel
			}
			if traceEnabled.Load() {
				fmt.Printf("  [step] turn=%d deciding with model=%s (tools=%d)\n", turn+1, selectModel, len(specs))
			}

			resp, err := ctx.Call(gateway.Request{
				Model:        selectModel,
				SystemPrefix: req.SystemPrefix,
				Messages:     msgs,
				Tools:        specs,
			})
			if err != nil {
				return agentgo.ToolCallResult{}, err
			}
			if len(resp.ToolCalls) == 0 {
				if selectModel == decisionModel && chatModel != "" && selectModel != chatModel {
					if traceEnabled.Load() {
						fmt.Printf("  [step] no tool needed -> generating reply with %s\n", chatModel)
					}
					chatResp, err := ctx.Call(gateway.Request{
						Model:        chatModel,
						SystemPrefix: req.SystemPrefix,
						Messages:     msgs,
					})
					if err != nil {
						return agentgo.ToolCallResult{}, err
					}
					return agentgo.ToolCallResult{Selected: false, Text: chatResp.Text}, nil
				}
				return agentgo.ToolCallResult{Selected: false, Text: resp.Text}, nil
			}

			tc := resp.ToolCalls[0]
			t, ok := toolMap[tc.Name]
			if !ok {
				return agentgo.ToolCallResult{}, fmt.Errorf("unknown tool %q", tc.Name)
			}
			if traceEnabled.Load() {
				fmt.Printf("  [tool] selected=%s (needsGen=%v)\n", t.Name(), t.NeedsGeneration())
			}

			args := tc.Arguments
			if t.NeedsGeneration() {
				argModel := req.ArgModel
				if argModel == "" {
					argModel = chatModel
				}
				genMsgs := append(msgs, gateway.Message{
					Role:    gateway.MsgUser,
					Content: fmt.Sprintf("Generate valid JSON arguments for tool %q (%s).", t.Name(), t.Description()),
				})
				genResp, err := ctx.Call(gateway.Request{
					Model:          argModel,
					SystemPrefix:   req.SystemPrefix,
					Messages:       genMsgs,
					ResponseSchema: t.Schema(),
				})
				if err != nil {
					return agentgo.ToolCallResult{}, fmt.Errorf("generate args for %q: %w", t.Name(), err)
				}
				args = json.RawMessage(genResp.Text)
			}

			out, execErr := t.Execute(ctx, args)
			if execErr != nil {
				out = map[string]any{"error": execErr.Error()}
			}
			state.Update(func(s *ChatState) { s.ToolRuns++ })
			outJSON, _ := json.Marshal(out)

			tc.Arguments = args
			msgs = append(msgs,
				gateway.Message{Role: gateway.MsgAssistant, ToolCalls: []gateway.ToolCall{tc}},
				gateway.Message{Role: gateway.MsgTool, ToolCallID: tc.ID, Name: tc.Name, Content: string(outJSON)},
			)
		}
		return agentgo.ToolCallResult{}, fmt.Errorf("too many tool turns")
	})

	ag := agentgo.New[ChatState](
		agentgo.WithID[ChatState]("luachat-root"),
		agentgo.WithProject[ChatState]("luachat"),
		agentgo.WithGateway[ChatState](gw),
		agentgo.WithModel[ChatState](chatModel),
		agentgo.WithSystemPrefix[ChatState](
			"You are a transparent assistant with full Lua execution (lua_exec) and child agent spawning (spawn_worker). Always include exact tool outputs in your final answer.",
		),
		agentgo.WithTools[ChatState](luaExec, spawnWorker),
		agentgo.WithToolInvoker[ChatState](chatInvoker),
		agentgo.WithSkipPlanning[ChatState](true),
	)
	ag.Memory = state
	ag.ArgModel = chatModel
	ag.Pipeline.Execute = func(ctx agentgo.Context, a *agentgo.AI[ChatState]) error {
		res, err := chatInvoker.Invoke(ctx, agentgo.ToolCallRequest{
			ProjectID:    a.ProjectID,
			Model:        a.Model,
			ArgModel:     a.ArgModel,
			SystemPrefix: a.SystemPrefix,
			Messages:     a.Memory.Get().History,
			Tools:        a.Tools,
		})
		if err != nil {
			return err
		}
		a.State.Output = res.Text
		return nil
	}

	fmt.Printf("Microchat ready (chat: %s, decision: %s).\n", chatModel, decisionModel)
	fmt.Println("Commands: /state, /memory, /usage, /trace, /exit")

	for {
		fmt.Print("\nYou> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		switch line {
		case "/exit", "/quit":
			fmt.Printf("\nTotal usage: %+v\n", gw.UsageByProject("luachat"))
			return
		case "/usage":
			fmt.Printf("Usage: %+v\n", gw.UsageByProject("luachat"))
			continue
		case "/trace":
			next := !traceEnabled.Load()
			traceEnabled.Store(next)
			fmt.Printf("Trace logging: %v\n", next)
			continue
		case "/state":
			b, _ := json.MarshalIndent(ag.State, "", "  ")
			fmt.Printf("Agent State:\n%s\n", string(b))
			continue
		case "/memory":
			b, _ := json.MarshalIndent(ag.Memory.Get(), "", "  ")
			fmt.Printf("Agent Memory:\n%s\n", string(b))
			continue
		}

		state.Update(func(s *ChatState) {
			s.History = append(s.History, gateway.Message{Role: gateway.MsgUser, Content: line})
		})

		out, err := ag.Run(context.Background())
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		reply := fmt.Sprint(out)
		state.Update(func(s *ChatState) {
			s.History = append(s.History, gateway.Message{Role: gateway.MsgAssistant, Content: reply})
		})
		fmt.Printf("AI> %s\n", reply)
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

	L := lua.NewState() // all standard libraries enabled
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
