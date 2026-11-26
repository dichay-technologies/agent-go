# Centralized `AIGateway`, Providers & Integrations

---

## 1. Request Pipeline Inside `gateway.AIGateway`

All model calls (`ctx.Call`, `ctx.Stream`, planner, and tool invokers) flow through `gateway.AIGateway`:

1. **Middleware Chain (`gw.Use(...)`):** `RateLimit`, `Budget`, `Retry`, `Fallback`, `otel.GatewayMiddleware`, and custom interceptors.
2. **Routing (`ModelRouter`):**
   - `TableRouter` (default): resolves explicit aliases (`gw.AddRoute("fast", "gemini", "gemini-2.5-flash")`), `"provider/model"` shorthand, and well-known model prefixes (`gemini-*`, `gpt-*`/`o1*`/`o3*`/`o4*`, `claude-*`, `@cf/*`, `jev-*`).
   - `ClassifierRouter`: when `req.Model` is `""` or `"auto"`, queries a fast classifier provider (e.g. `jev`) to pick the best route dynamically.
3. **Content-Addressed Caching (`gateway.Cache`):**
   Keys are hashed from canonical request content excluding `ProjectID`, allowing identical prompts across projects to share cached results.
   - `MemoryCache` — bounded thread-safe in-memory cache with TTL, slice cloning, and FIFO eviction;
   - `FileCache` (`gateway.NewFileCache(dir)`) — zero-dependency atomic JSON disk cache.
4. **Singleflight Deduplication:**
   Identical concurrent requests share a single upstream HTTP call. Canceling the leader's context does not abort the upstream call while other waiters remain active.
5. **Cross-Project Micro-Batching (`CoalesceWindow` + `req.Coalesce = true`):**
   Collects compatible requests within a 50–150 ms window and dispatches them together via `BatchProvider.BatchComplete` (canceling automatically if all waiting contexts cancel).
6. **Structured Output Validation (`ResponseSchema`):**
   Validates model JSON responses in Go via `JSONSchema.Validate`.
7. **Token Accounting (`UsageByProject`, `AllUsage`, `UsageSink`):**
   Tracks input, output, and cached tokens per `ProjectID` or custom `UsageKey(req)` (such as `req.Meta["tenant"]`).

---

## 2. Providers, KV Caching, Native Batch APIs & SSE Streaming

Every provider falls back to standard environment variables when `Config.APIKey` is empty and classifies HTTP errors via `gateway.ClassifyStatusCode` (`ErrRateLimit`, `ErrRetryable`, `ErrAuth`, `ErrContextLength`):

| Provider | Env Variable | Highlights |
| --- | --- | --- |
| `gateway/gemini` | `GEMINI_API_KEY` | `generateContent`, SSE `streamGenerateContent`, multimodal (`TEXT`, `IMAGE`, `AUDIO`, TTS `SpeechConfig`), automatic `cachedContents` (`EnableContextCache: true`), 50%-discount `batchGenerateContent` (`EnableNativeBatch: true`), `functionCall` / `functionResponse`. |
| `gateway/openai` | `OPENAI_API_KEY` | Works with OpenAI, OpenRouter, DeepSeek, Groq, Ollama, vLLM. SSE streaming with `include_usage`, automatic strict schema normalization (`OpenAINormalize`), audio I/O, and `/v1/batches` (`EnableNativeBatch: true`). |
| `gateway/anthropic` | `ANTHROPIC_API_KEY` | Messages API, SSE streaming (`text_delta` + `input_json_delta`), `cache_control: ephemeral` (`EnablePromptCache: true`), `tool_use` / `tool_result` blocks, and `/v1/messages/batches`. |
| `gateway/jev` | `TYPESAFE_API_KEY` | TypeSafe System One API (Jev): maps `Tools` to `choice` questions with a `"none"` escape hatch and `Enum` schemas to atomic classifier questions. |
| `gateway/cloudflare` | — | Workers AI & Cloudflare AI Gateway (`cf-aig-cache-ttl`, `x-session-affinity`), SSE streaming, and pluggable dialects (`openai`, `anthropic`, `gemini`, `workersai` with `?queueRequest=true`, `jev`). |

---

## 3. Middleware, MCP & OpenTelemetry

### `gateway/middleware`
```go
gw.Use(
    middleware.RateLimit(20, 40),
    middleware.Budget(500_000, nil), // max tokens per ProjectID
    middleware.Retry(3, 100*time.Millisecond),
    middleware.Fallback(gateway.RouteTarget{Provider: "gemini", Model: "gemini-2.5-flash"}),
)
```

### `mcp` (Model Context Protocol)
Zero-dependency MCP client, transports, and `agentgo.Tool` adapter:
- **`mcp.NewHTTPTransport(mcp.HTTPConfig{Endpoint: url})`** — Streamable HTTP JSON-RPC 2.0 supporting both `application/json` and `text/event-stream` responses plus automatic `Mcp-Session-Id` propagation.
- **`mcp.NewStdioTransport(exec.Command(...))`** — spawns a local MCP subprocess and communicates over newline-delimited JSON-RPC via `stdin`/`stdout`.
- **`client.Initialize(ctx)`** & **`mcp.LoadTools(ctx, client)`** — protocol handshake, automatic `nextCursor` pagination on `tools/list`, and `isError: true` propagation into Go errors so agent guardrails can react.

### `otel` (OpenTelemetry-Compatible Tracing)
`otel.GatewayMiddleware(tracer)` and `otel.ToolMiddleware(tracer)` instrument spans using minimal `Tracer` and `Span` interfaces without adding external SDK dependencies to `go.mod`, preserving all `agentgo.Context` capabilities (`WithStdContext`).

