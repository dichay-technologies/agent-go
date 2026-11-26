# agent-go

**A composable, zero-dependency Go framework for hierarchical AI agent graphs, stateful conversational coordinators, and multi-provider model gateways.**  
Built by **DiChay Technologies** (`proj.dichay.tech/agent-go`).

📖 **Documentation / Документация:** **[English (`doc/en/`)](./doc/en/README.md)** · **[Русский (`doc/ru/`)](./doc/ru/README.md)** · **[Index (`doc/`)](./doc/README.md)**

> [!WARNING]
> **Experimental & 100% Vibe-Coded Notice**  
> This project is **100% vibe-coded**. Its public APIs, internals, and architecture may be heavily refactored or rewritten at any moment without notice, and edge cases may not always behave the way you expect. Pin commits if you depend on it and verify behavior in your own environment.
>
> **Дисклеймер:** проект **полностью завайбкоджен**, активно экспериментируется, может быть радикально перелопачен в любой момент без гарантий обратной совместимости и местами может работать не так, как вы ожидаете.

---

## Key Capabilities

- **Zero-Dependency Core:** Root module (`proj.dichay.tech/agent-go`) uses only the Go standard library.
- **Four Clean State Layers:**
  - Immutable `*AI[S]` blueprint (safe for concurrent reuse via `Run`, `RunTask`, `Chat`, `StreamChat`).
  - Serializable `RunState` FSM (`StatusAnalyzing` → `StatusGatheringContext` → `StatusPlanning` → `StatusExecuting` → `StatusWaitingDeps`, plus checkpoint `Gates`, `ErrGatePaused`, `Resume`, and atomic `SaveSnapshot` / `LoadSnapshot`).
  - Managed `*MemoryBank` workspace (key-value facts with `Pinned` protection, controllable conversation history with compaction, and self-managed task list) accessible both from Go and by the agent via `WorkspaceTools()`.
  - Thread-safe host application state `*LocalState[S]`.
- **Non-Blocking Subagent Graphs & Dynamic DAGs:**
  - `SequentialScheduler` and `DAGParallelScheduler` with live mid-run `AppendPlanSteps`.
  - Fan out 50+ background subagents (`SpawnAsync` / `SpawnNamed`) bounded by `MaxActiveChildren` and `MaxSpawnDepth`, with point-to-point `DependsOn` synchronization on named `Future`s.
- **Inter-Agent Message `Bus`:**
  - Shared automatically across `Fork` and `SpawnAsync`; supports direct messaging, `"parent"` alias, `"*"` broadcast, reactive `Subscribe`, external `ag.Tell(...)` events, and cross-agent memory transfer (`share_memory_keys`).
- **Two-Model Tooling, 3-Attempt Guardrails & Two-Level Permissions:**
  - Typed `Func[In, Out]` with automatic `gateway.SchemaFor[T]()`, two-stage parameter synthesis (`decision` classifier + `ArgModel` via `NeedsGeneration()`), and multi-turn `LoopToolInvoker` with `ReplyModel` and self-correction (`ContinueOnToolError`).
  - 3-attempt retry loop with `verify.EnsembleVerifier` (`Pass` / `Fail` / `Unsure`), escalation to `EscalateModel`, `FailurePolicy` (`FailOnError`, `SkipOnError`, `FallbackValue`), and diagnostic `RunState.Failures` backlog.
  - Monotonic top-down `Permissions` narrowing on `Fork`/`Spawn` + `PermissionMiddleware` and `ToolWithPermission`.
- **Centralized Multi-Project `AIGateway`:**
  - Unified providers for **Google Gemini**, **OpenAI** (and compatible endpoints: OpenRouter, DeepSeek, Groq, Ollama, vLLM), **Anthropic**, **TypeSafe Jev (System One)**, and **Cloudflare Workers AI / AI Gateway**.
  - Native **SSE Streaming** (`StreamProvider` + `ag.StreamChat`), content-addressed `MemoryCache` & persistent `FileCache`, singleflight deduplication, cross-project micro-batching (`CoalesceWindow`), 50%-discount native Batch APIs, provider KV/context caching, and per-project token accounting.

---

## Installation

```bash
go get proj.dichay.tech/agent-go
```

---

## Quickstart

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
	prov, err := gemini.New(gemini.Config{}) // reads GEMINI_API_KEY from env
	if err != nil {
		log.Fatal(err)
	}

	gw := gateway.New(gateway.Config{EnableCache: true})
	gw.Register("gemini", prov)
	gw.AddRoute("fast", "gemini", "gemini-2.5-flash")

	greet := agentgo.Func("greet", "Greets a user", func(ctx agentgo.Context, in GreetInput) (string, error) {
		return "Hello, " + in.Name + "!", nil
	})

	ag := agentgo.New[AppState](
		agentgo.WithID[AppState]("coordinator"),
		agentgo.WithProject[AppState]("demo"),
		agentgo.WithGateway[AppState](gw),
		agentgo.WithModel[AppState]("fast"),
		agentgo.WithSystemPrefix[AppState]("You are a concise assistant."),
		agentgo.WithWorkspaceTools[AppState](), // memory_*, chat_*, task_*, agent_* tools
		agentgo.WithTools[AppState](greet),
		agentgo.WithToolInvoker[AppState](agentgo.LoopToolInvoker{
			MaxIterations:       6,
			ContinueOnToolError: true,
		}),
	)

	// Stream a stateful conversational turn in real time:
	for delta, err := range ag.StreamChat(context.Background(), "Greet Alice and save her name in memory") {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(delta)
	}
	fmt.Println()
}
```

---

## Documentation (`doc/`)

| Topic | English | Русский |
| --- | --- | --- |
| **Overview, State Layers & Quickstart** | [doc/en/README.md](./doc/en/README.md) | [doc/ru/README.md](./doc/ru/README.md) |
| **Agent FSM Lifecycle, Gates, Snapshots & DAGs** | [doc/en/lifecycle-and-dag.md](./doc/en/lifecycle-and-dag.md) | [doc/ru/lifecycle-and-dag.md](./doc/ru/lifecycle-and-dag.md) |
| **Tools, 3-Attempt Guardrails & Permissions** | [doc/en/tools-guardrails-permissions.md](./doc/en/tools-guardrails-permissions.md) | [doc/ru/tools-guardrails-permissions.md](./doc/ru/tools-guardrails-permissions.md) |
| **Managed Memory, Chat Mode & Inter-Agent Bus** | [doc/en/memory-chat-bus.md](./doc/en/memory-chat-bus.md) | [doc/ru/memory-chat-bus.md](./doc/ru/memory-chat-bus.md) |
| **AIGateway, Providers, Streaming, MCP & OTel** | [doc/en/gateway-and-integrations.md](./doc/en/gateway-and-integrations.md) | [doc/ru/gateway-and-integrations.md](./doc/ru/gateway-and-integrations.md) |

---

## Packages Overview

- `agentgo` (root): `AI[S]`, `Fork`, `Run`, `RunDetailed`, `Resume`, `RunTask`, `Chat`, `StreamChat`, `RunAs[T]`, `MemoryBank`, `WorkspaceTools`, `Bus`, `Tell`, `SaveSnapshot` / `LoadSnapshot`, `SequentialScheduler`, `DAGParallelScheduler`, `LoopToolInvoker`, `PermissionMiddleware`, `Func[In, Out]`.
- `gateway`: `AIGateway`, `TableRouter`, `ClassifierRouter`, `MemoryCache`, `FileCache`, `StreamProvider` (`ReadSSE`), `SchemaFor[T]()`, typed `ProviderError`.
- `gateway/{gemini,openai,anthropic,jev,cloudflare,mock}`: Provider implementations & Cloudflare AI Gateway dialects.
- `gateway/middleware`: `RateLimit`, `Budget`, `Retry`, `Fallback`.
- `verify`: `EnsembleVerifier` (`Pass` / `Fail` / `Unsure`), `ACLToolMiddleware`.
- `mcp`: JSON-RPC 2.0 MCP `Client` (`Initialize`, `LoadTools`), `HTTPTransport`, `StdioTransport`.
- `otel`: Dependency-free OpenTelemetry `GatewayMiddleware` and `ToolMiddleware`.
- `examples`: Standalone Go module featuring `quickstart` and `luachat` (interactive CLI agent with `gopher-lua`, live SSE streaming, `MemoryBank`, and subagent spawning).

---

## License

Released under the [MIT License](./LICENSE).  
Copyright © 2026 **DiChay Technologies**.
