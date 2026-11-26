# Централизованный `AIGateway`, провайдеры и интеграции

---

## 1. Конвейер обработки запросов в `gateway.AIGateway`

Все обращения к моделям (`ctx.Call`, `ctx.Stream`, вызовы планировщика и инвокеров тулов) проходят через `gateway.AIGateway`:

1. **Middleware-цепочка (`gw.Use(...)`):** `RateLimit`, `Budget`, `Retry`, `Fallback`, `otel.GatewayMiddleware` и пользовательские перехватчики.
2. **Маршрутизация (`ModelRouter`):**
   - `TableRouter` (по умолчанию): разрешает явные маршруты (`gw.AddRoute("fast", "gemini", "gemini-2.5-flash")`), запись `"provider/model"` и префиксы моделей (`gemini-*`, `gpt-*`/`o1*`/`o3*`/`o4*`, `claude-*`, `@cf/*`, `jev-*`).
   - `ClassifierRouter`: когда `req.Model == ""` или `"auto"`, вызывает быстрый классификатор (например, `jev`) для выбора оптимального маршрута под конкретный запрос.
3. **Кэширование ответов (`gateway.Cache`):**
   Хэш ключа вычисляется по каноническому содержимому запроса без `ProjectID`, чтобы одинаковые системные проверки из разных проектов переиспользовали кэш.
   - `MemoryCache` — потокобезопасный кэш в памяти с TTL, клонированием буферов и FIFO-вытеснением;
   - `FileCache` (`gateway.NewFileCache(dir)`) — персистентный атомарный JSON-кэш на диске без внешних зависимостей.
4. **Singleflight-дедупликация:**
   Идентичные одновременные запросы разделяют один физический вызов провайдера. Отмена контекста первого запроса не прерывает сетевой вызов, пока результата ждут остальные участники.
5. **Кросс-проектный микробатчинг (`CoalesceWindow` + `req.Coalesce = true`):**
   Удерживает однотипные запросы из разных проектов в окне 50–150 мс и отправляет их единым батчем через `BatchProvider.BatchComplete`. Если все ожидающие клиенты отменили свои контексты, контекст батча также автоматически отменяется.
6. **Валидация схемы (`ResponseSchema`):**
   При наличии `ResponseSchema` JSON-ответ модели проверяется через `JSONSchema.Validate` на стороне Go.
7. **Биллинг и учёт токенов (`UsageByProject`, `AllUsage`, `UsageSink`):**
   Входные, выходные и кэшированные токены учитываются в разрезе `ProjectID` или кастомной функции `UsageKey(req)` (например, по `req.Meta["user_id"]`).

---

## 2. Провайдеры, KV-кеш, нативный Batch API и SSE-стриминг

Все провайдеры читают API-ключи из переменных окружения, если `Config.APIKey` не передан явно, и классифицируют HTTP-ошибки через `gateway.ClassifyStatusCode` (`ErrRateLimit`, `ErrRetryable`, `ErrAuth`, `ErrContextLength`):

| Провайдер | Env-переменная | Особенности реализации |
| --- | --- | --- |
| `gateway/gemini` | `GEMINI_API_KEY` | `generateContent`, SSE `streamGenerateContent`, мультимодальность (`TEXT`, `IMAGE`, `AUDIO`, TTS `SpeechConfig`), авто-управление `cachedContents` (`EnableContextCache: true`), нативный `batchGenerateContent` со скидкой 50% (`EnableNativeBatch: true`), `functionCall` / `functionResponse`. |
| `gateway/openai` | `OPENAI_API_KEY` | Совместим с OpenAI, OpenRouter, DeepSeek, Groq, Ollama, vLLM. SSE-стриминг с `include_usage`, авто-нормализация `JSONSchema` под `strict` режим (`OpenAINormalize`), аудио ввод/вывод и асинхронный `/v1/batches` (`EnableNativeBatch: true`). |
| `gateway/anthropic` | `ANTHROPIC_API_KEY` | Messages API, SSE-стриминг (`text_delta` + `input_json_delta`), `cache_control: ephemeral` (`EnablePromptCache: true`), блоки `tool_use` / `tool_result`, нативный `/v1/messages/batches`. |
| `gateway/jev` | `TYPESAFE_API_KEY` | TypeSafe System One API (Jev): авто-конвертация списка `Tools` в `choice`-вопрос с вариантом `"none"` и `Enum`-схем в атомарные вопросы классификатора. |
| `gateway/cloudflare` | — | Workers AI и Cloudflare AI Gateway (`cf-aig-cache-ttl`, `x-session-affinity`), SSE-стриминг и сменные диалекты (`openai`, `anthropic`, `gemini`, `workersai` с `?queueRequest=true`, `jev`). |

---

## 3. Middleware, MCP и OpenTelemetry

### `gateway/middleware`
```go
gw.Use(
    middleware.RateLimit(20, 40),
    middleware.Budget(500_000, nil), // лимит токенов на каждый ProjectID
    middleware.Retry(3, 100*time.Millisecond),
    middleware.Fallback(gateway.RouteTarget{Provider: "gemini", Model: "gemini-2.5-flash"}),
)
```

### `mcp` (Model Context Protocol)
Пакет `mcp` содержит готовые транспорты на стандартной библиотеке и адаптер инструментов для `AI[S]`:
- **`mcp.NewHTTPTransport(mcp.HTTPConfig{Endpoint: url})`** — Streamable HTTP JSON-RPC 2.0 (поддерживает ответы `application/json` и `text/event-stream`, автоматически сохраняет и передаёт `Mcp-Session-Id`).
- **`mcp.NewStdioTransport(exec.Command(...))`** — запуск локального MCP-сервера дочерним процессом и потокобезопасный обмен через `stdin`/`stdout`.
- **`client.Initialize(ctx)`** и **`mcp.LoadTools(ctx, client)`** — рукопожатие протокола, автоматический обход пагинации `nextCursor` в `tools/list` и трансляция `isError: true` из `tools/call` в ошибки Go для корректной работы guardrail агента.

### `otel` (OpenTelemetry-совместимый трейсинг)
`otel.GatewayMiddleware(tracer)` и `otel.ToolMiddleware(tracer)` работают через минимальные интерфейсы `Tracer` и `Span`, не затягивая внешние зависимости в `go.mod`, и полностью сохраняют все возможности `agentgo.Context` (спавн субагентов, доступ к `Bus`, `MemoryBank` и правам).

