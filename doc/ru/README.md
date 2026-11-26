# Обзор, философия и быстрый старт

`agent-go` — компонуемая Go-библиотека от **DiChay Technologies** (`proj.dichay.tech/agent-go`) **без внешних зависимостей в ядре** для построения графов микро-агентов (`AI[S]`), интерактивных диалоговых координаторов и мультипроектного шлюза моделей (`gateway.AIGateway`).

---

## 1. Базовые принципы проектирования

1. **Механизмы отдельно от доменных политик:** ядро предоставляет детерминированный конечный автомат (FSM), планировщики графов (DAG), защиту от зацикливания, двухуровневую систему прав, управляемую память и межагентную шину, не привязывая приложение к конкретной БД или бизнес-логике.
2. **Единый `AIGateway` поверх всех проектов:** ни один агент не обращается к HTTP API моделей напрямую. Все вызовы проходят через `gateway.AIGateway`, который отвечает за роутинг, кэш, singleflight-дедупликацию, кросс-проектный микробатчинг, квоты, SSE-стриминг и разнесение биллинга токенов по проектам.
3. **Экономика токенов и защита KV-кеша:** дешёвые классификаторы (`jev`) и быстрые `micro`-модели закрывают рутинный выбор инструментов, отбор контекста и генерацию аргументов, тогда как тяжёлая `Plan`-модель подключается только для планирования и эскалации после 3 неудачных попыток. Статический `SystemPrefix` при этом не смешивается с динамическими данными, сохраняя 100% попадание в провайдерский Prompt/KV-кеш.
4. **Zero-dependency ядро:** корневой `go.mod` использует исключительно стандартную библиотеку Go.

---

## 2. Четыре слоя состояния

| Слой | Тип | Назначение и потокобезопасность |
| --- | --- | --- |
| **Конфигурация агента** | `*AI[S]` | **Неизменяемая (immutable)** конфигурация: модели по стадиям, `SystemPrefix`, инструменты, права, лимиты спавна, `Pipeline`, `Hooks`. Один экземпляр `*AI[S]` можно безопасно вызывать из множества горутин через `Run`, `RunTask`, `Chat`, `StreamChat`. |
| **Состояние запуска (FSM)** | `RunState` | **Изолированное на каждый запуск и JSON-сериализуемое** состояние: фаза `Status`, `Task`, `Analysis`, `ContextData`, план `Plan`, результаты шагов `StepResults`, входящие сигналы `PendingEvents`, лог ретраев `History`, бэклог сбоев `Failures` и точка паузы `PausedAtGate`. |
| **Рабочая память агента** | `*MemoryBank` | **Потокобезопасное управляемое хранилище** фактов (`MemoryEntry`), буфера диалога (`ChatEntry`) и списка задач (`TaskItem`). Управляется как из Go-кода, так и самим агентом через встроенные инструменты (`WorkspaceTools()`). |
| **Прикладной стейт Go** | `*LocalState[S]` | **Пользовательская Go-структура `S`** под `sync.RWMutex` (`Get`, `Set`, `Update`) для доменных объектов хост-приложения. |

---

## 3. Структура пакетов репозитория

```text
agent-go/
├── ai.go, agent.go      # Конфигурация AI[S], Fork, Run/RunDetailed/Resume/RunTask/Chat/StreamChat
├── context.go           # Единый agentgo.Context, Future, DataSource, шаблоны, вырезка и сжатие контекста
├── scheduler.go         # SequentialScheduler и DAGParallelScheduler (динамический DAG + join по SubFuture)
├── tool.go              # Tool, Func[In,Out], DefaultToolInvoker, LoopToolInvoker, PermissionMiddleware
├── guardrail.go         # Правило 3 попыток, верификация, эскалация, FailurePolicy и бэклог сбоев
├── memory.go            # MemoryBank (факты, чат, задачи) + MemoryTools / ChatTools / TaskTools / WorkspaceTools
├── bus.go               # Межагентная шина Bus, почтовые ящики, пересылка памяти, Tell, BusTools
├── snapshot.go          # Атомарное сохранение и загрузка ProjectSnapshot (RunState + MemoryBank) на диск
├── gateway/             # AIGateway, TableRouter/ClassifierRouter, MemoryCache/FileCache, ReadSSE, SchemaFor[T]
│   ├── gemini/          # Google Gemini (generateContent, SSE Stream, cachedContents, Batch API, Audio/TTS)
│   ├── openai/          # OpenAI-совместимые API (Chat Completions, SSE Stream, Batch API, Audio, strict JSON)
│   ├── anthropic/       # Anthropic Messages API (SSE Stream, prompt cache, tool_use/tool_result, Batch API)
│   ├── jev/             # TypeSafe System One / Jev (классификатор Choice / Score / Noul)
│   ├── cloudflare/      # Cloudflare Workers AI и AI Gateway с диалектами (openai, anthropic, gemini, workersai, jev)
│   ├── middleware/      # RateLimit, Budget, Retry, Fallback
│   └── mock/            # In-memory провайдер для тестов
├── verify/              # Трёхзначный вердикт (Pass / Fail / Unsure), EnsembleVerifier, ACLToolMiddleware
├── mcp/                 # Клиент MCP (Initialize, пагинация, LoadTools, HTTPTransport и StdioTransport)
├── otel/                # OpenTelemetry-совместимый трейсинг для шлюза и инструментов
└── examples/            # Примеры quickstart и интерактивный терминальный агент luachat (gopher-lua)
```

---

## 4. Быстрый старт

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
	Name string `json:"name" desc:"Имя пользователя"`
}

func main() {
	p, err := gemini.New(gemini.Config{}) // читает GEMINI_API_KEY из окружения
	if err != nil {
		log.Fatal(err)
	}

	gw := gateway.New(gateway.Config{EnableCache: true})
	gw.Register("gemini", p)
	gw.AddRoute("fast", "gemini", "gemini-2.5-flash")

	greet := agentgo.Func("greet", "Приветствует пользователя", func(ctx agentgo.Context, in GreetInput) (string, error) {
		return "Привет, " + in.Name + "!", nil
	})

	ag := agentgo.New[AppState](
		agentgo.WithID[AppState]("assistant"),
		agentgo.WithProject[AppState]("demo"),
		agentgo.WithGateway[AppState](gw),
		agentgo.WithModel[AppState]("fast"),
		agentgo.WithSystemPrefix[AppState]("Ты лаконичный ассистент."),
		agentgo.WithWorkspaceTools[AppState](), // память + чат + задачи + шина
		agentgo.WithTools[AppState](greet),
		agentgo.WithToolInvoker[AppState](agentgo.LoopToolInvoker{MaxIterations: 5}),
	)

	// Диалоговый режим со стримингом токенов:
	for delta, err := range ag.StreamChat(context.Background(), "Поздоровайся с Алисой и запомни её имя") {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(delta)
	}
	fmt.Println()
}
```

Далее см. подробные разделы:
- [Жизненный цикл FSM, Gates, снапшоты и планировщики DAG](./lifecycle-and-dag.md)
- [Инструменты, Guardrails и система прав](./tools-guardrails-permissions.md)
- [Управляемая память, чат-режим и шина Bus](./memory-chat-bus.md)
- [Централизованный AIGateway, провайдеры и интеграции](./gateway-and-integrations.md)
