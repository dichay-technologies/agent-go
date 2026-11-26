# agent-go Documentation / Документация

**DiChay Technologies** (`proj.dichay.tech/agent-go`)

> [!WARNING]
> **Notice / Дисклеймер:** This project is **100% vibe-coded** — APIs and internals can be heavily refactored at any time and may not always behave as expected. / Проект **полностью завайбкоджен**, может быть перелопачен в любой момент и местами работать не так, как ожидается.

Complete architecture, API, and mechanism documentation for `agent-go` in **English** and **Russian**.

Полная документация архитектуры, API и всех внутренних механизмов `agent-go` на **русском** и **английском** языках.

---

## Русский (`doc/ru/`)

1. **[Обзор, философия и быстрый старт](./ru/README.md)** — архитектура пакетов, 4 слоя состояния (`AI[S]`, `RunState`, `MemoryBank`, `LocalState[S]`), базовые примеры.
2. **[Жизненный цикл FSM, Gates, снапшоты и планировщики DAG](./ru/lifecycle-and-dag.md)** — 4 фазы агента, селективный сбор контекста, контрольные точки (`Gates` / `Resume`), атомарные снапшоты, `SequentialScheduler` и `DAGParallelScheduler`, веерный спавн субагентов и точечный `Await` по `SubFuture`.
3. **[Инструменты, Guardrails (3 попытки) и система прав](./ru/tools-guardrails-permissions.md)** — `Func[In, Out]`, `SchemaFor[T]`, двухмодельный вызов (`decision` + `ArgModel` через `NeedsGeneration`), `LoopToolInvoker`, верификация `verify`, эскалация на тяжёлую модель, бэклог сбоев и двухуровневые `Permissions`.
4. **[Управляемая память, чат-режим и межагентная шина](./ru/memory-chat-bus.md)** — `MemoryBank` (факты, задачи, управление буфером диалога), `Chat` и `StreamChat`, шина `Bus`, `Tell`, передача памяти между агентами и `WorkspaceTools`.
5. **[Централизованный `AIGateway`, провайдеры и интеграции](./ru/gateway-and-integrations.md)** — `TableRouter` и `ClassifierRouter` (Jev), кэши (`MemoryCache`, `FileCache`, провайдерский KV-кеш), singleflight, микробатчинг и нативные Batch API, SSE-стриминг, `gateway/middleware`, `mcp` (`HTTPTransport`, `StdioTransport`) и `otel`.

---

## English (`doc/en/`)

1. **[Overview, Philosophy & Quickstart](./en/README.md)** — package layout, the 4 state layers (`AI[S]`, `RunState`, `MemoryBank`, `LocalState[S]`), and core usage examples.
2. **[Agent FSM Lifecycle, Gates, Snapshots & DAG Schedulers](./en/lifecycle-and-dag.md)** — the 4 lifecycle stages, selective context gathering, checkpoint `Gates` & `Resume`, atomic snapshots, `SequentialScheduler` & `DAGParallelScheduler`, non-blocking subagent fan-out, and point-to-point `SubFuture` joins.
3. **[Tools, 3-Attempt Guardrails & Two-Level Permissions](./en/tools-guardrails-permissions.md)** — `Func[In, Out]`, `SchemaFor[T]`, two-model tool invocation (`decision` + `ArgModel` via `NeedsGeneration`), `LoopToolInvoker`, `verify` ensembles, escalation, failure backlogs, and monotonic `Permissions`.
4. **[Managed Memory, Chat Mode & Inter-Agent Bus](./en/memory-chat-bus.md)** — `MemoryBank` (facts, tasks, controllable conversation history), `Chat` & `StreamChat`, inter-agent `Bus`, `Tell`, cross-agent memory transfer, and `WorkspaceTools`.
5. **[Centralized `AIGateway`, Providers & Integrations](./en/gateway-and-integrations.md)** — `TableRouter` & `ClassifierRouter` (Jev), caching (`MemoryCache`, `FileCache`, provider KV caches), singleflight, cross-project coalescing & native Batch APIs, SSE streaming, `gateway/middleware`, `mcp` (`HTTPTransport`, `StdioTransport`), and `otel`.
