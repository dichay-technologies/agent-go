# Overview, Philosophy & Quickstart

`agent-go` is a composable, **zero-dependency** Go framework by **DiChay Technologies** (`proj.dichay.tech/agent-go`) for building hierarchical micro-agent graphs (`AI[S]`), stateful conversational coordinators, and centralized multi-project model gateways (`gateway.AIGateway`).

---

## 1. Core Design Principles

1. **Mechanisms over Domain Policies:** The core library provides deterministic finite-state machines (FSM), DAG schedulers, 3-attempt guardrails, monotonic permission inheritance, managed workspaces, and an inter-agent message bus without locking your application into a rigid database or domain schema.
2. **Unified `AIGateway` Across Projects:** Agents never call upstream provider HTTP APIs directly. Every call flows through `gateway.AIGateway`, which handles model routing, content-addressed caching, singleflight deduplication, cross-project micro-batching, quotas, SSE streaming, and per-project token accounting.
3. **Token Economics & KV-Cache Preservation:** Lightweight `decision` classifiers (such as Jev) and fast `micro` LLMs handle ~95% of routing, selective context picking, and structured argument generation, while heavy `Plan` models are invoked only for multi-step planning and 3-strike error escalation. Static `SystemPrefix` instructions are kept strictly separate from dynamic turns to maximize provider prompt/KV cache hit rates.
4. **Zero-Dependency Core:** The root Go module relies exclusively on the Go standard library.

---

## 2. The Four State Layers

| Layer | Type | Lifecycle & Concurrency Contract |
| --- | --- | --- |
| **Agent Configuration** | `*AI[S]` | **Immutable** reusable blueprint: stage models, `SystemPrefix`, tools, permissions, spawn limits, `Pipeline`, `Hooks`. Safe to share across concurrent goroutines via `Run`, `RunTask`, `Chat`, `StreamChat`. |
| **Execution State (FSM)** | `RunState` | **Per-run isolated and JSON-serializable** state: current `Status`, `Task`, `Analysis`, `ContextData`, `Plan` DAG, `StepResults`, `PendingEvents`, retry `History`, `Failures` backlog, and `PausedAtGate`. Returned via `RunDetailed` and resumed via `RunFrom` / `Resume`. |
| **Agent Workspace** | `*MemoryBank` | **Thread-safe managed store** for key-value facts (`MemoryEntry`), controllable conversation turns (`ChatEntry`), and self-managed tasks (`TaskItem`). Accessible both from host Go code and by the agent itself via `WorkspaceTools()`. |
| **Host Application State** | `*LocalState[S]` | **Typed Go struct `S`** guarded by `sync.RWMutex` (`Get`, `Set`, `Update`) for domain state owned by your Go application. |

---

## 3. Repository Layout

```text
agent-go/
├── ai.go, agent.go      # AI[S] config, Fork, Run/RunDetailed/Resume/RunTask/Chat/StreamChat
├── context.go           # Unified agentgo.Context, Future, DataSource, templates, slicing & compression
├── scheduler.go         # SequentialScheduler & DAGParallelScheduler (dynamic DAG + SubFuture joins)
├── tool.go              # Tool, Func[In,Out], DefaultToolInvoker, LoopToolInvoker, PermissionMiddleware
├── guardrail.go         # 3-attempt retry loop, verification, escalation, FailurePolicy & failure backlog
├── memory.go            # MemoryBank (facts, chat, tasks) + MemoryTools / ChatTools / TaskTools / WorkspaceTools
├── bus.go               # Inter-agent Bus, mailboxes, memory transfer, Tell, BusTools
├── snapshot.go          # Atomic SaveSnapshot & LoadSnapshot for ProjectSnapshot (RunState + MemoryBank)
├── gateway/             # AIGateway, TableRouter/ClassifierRouter, MemoryCache/FileCache, ReadSSE, SchemaFor[T]
│   ├── gemini/          # Google Gemini (generateContent, SSE Stream, cachedContents, Batch API, Audio/TTS)
│   ├── openai/          # OpenAI-compatible APIs (Chat Completions, SSE Stream, Batch API, Audio, strict JSON)
│   ├── anthropic/       # Anthropic Messages API (SSE Stream, prompt cache, tool_use/tool_result, Batch API)
│   ├── jev/             # TypeSafe System One / Jev (fast Choice / Score / Noul classifier)
│   ├── cloudflare/      # Cloudflare Workers AI & AI Gateway with dialects (openai, anthropic, gemini, workersai, jev)
│   ├── middleware/      # RateLimit, Budget, Retry, Fallback
│   └── mock/            # In-memory test provider
├── verify/              # Three-valued Verdict (Pass / Fail / Unsure), EnsembleVerifier, ACLToolMiddleware
├── mcp/                 # Model Context Protocol client (Initialize, pagination, LoadTools, HTTP & Stdio transports)
├── otel/                # Dependency-free OpenTelemetry-compatible tracing for gateway and tools
└── examples/            # Separate Go module with quickstart and interactive luachat (gopher-lua)
```

---

## 4. Quickstart

```go
package main

import (
	"context"
	"fmt"
	"log"

	"proj.dichay.tech/agent-go"
	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/gemini"
)

type AppState struct {
	Completed int
}

type GreetInput struct {
	Name string `json:"name" desc:"User name"`
}

func main() {
	p, err := gemini.New(gemini.Config{}) // reads GEMINI_API_KEY from env
	if err != nil {
		log.Fatal(err)
	}

	gw := gateway.New(gateway.Config{EnableCache: true})
	gw.Register("gemini", p)
	gw.AddRoute("fast", "gemini", "gemini-2.5-flash")

	greet := agentgo.Func("greet", "Greets a user by name", func(ctx agentgo.Context, in GreetInput) (string, error) {
		return "Hello, " + in.Name + "!", nil
	})

	ag := agentgo.New[AppState](
		agentgo.WithID[AppState]("assistant"),
		agentgo.WithProject[AppState]("demo"),
		agentgo.WithGateway[AppState](gw),
		agentgo.WithModel[AppState]("fast"),
		agentgo.WithSystemPrefix[AppState]("You are a concise assistant."),
		agentgo.WithWorkspaceTools[AppState](), // memory + chat + tasks + bus tools
		agentgo.WithTools[AppState](greet),
		agentgo.WithToolInvoker[AppState](agentgo.LoopToolInvoker{MaxIterations: 5}),
	)

	// Conversational turn with live SSE token streaming:
	for delta, err := range ag.StreamChat(context.Background(), "Greet Alice and remember her name") {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(delta)
	}
	fmt.Println()
}
```

Continue reading:
- [Agent FSM Lifecycle, Gates, Snapshots & DAG Schedulers](./lifecycle-and-dag.md)
- [Tools, 3-Attempt Guardrails & Two-Level Permissions](./tools-guardrails-permissions.md)
- [Managed Memory, Chat Mode & Inter-Agent Bus](./memory-chat-bus.md)
- [Centralized AIGateway, Providers & Integrations](./gateway-and-integrations.md)

