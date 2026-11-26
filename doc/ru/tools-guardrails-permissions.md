# Инструменты, Guardrails (3 попытки) и система прав

---

## 1. Объявление инструментов (`Tool`)

Интерфейс `Tool` минимален (`Name`, `Description`, `Schema`, `NeedsGeneration`, `Execute`). Для удобства предусмотрены три конструктора:

1. **`Func[In, Out](name, desc, fn)`** — автоматически строит `JSONSchema` из Go-структуры `In` через рефлексию (`gateway.SchemaFor[In]()`), учитывая теги `json:"...,omitempty"` и `desc:"..."`:
   ```go
   type TrimIn struct {
       ClipID string  `json:"clip_id" desc:"ID клипа"`
       Dur    float64 `json:"dur" desc:"Длительность в секундах"`
   }
   trim := agentgo.Func("trim_clip", "Обрезает клип", func(ctx agentgo.Context, in TrimIn) (string, error) {
       return fmt.Sprintf("%s:%.2fs", in.ClipID, in.Dur), nil
   })
   ```
2. **`NewTypedTool[In](name, desc, schema, needsGeneration, fn)`** — позволяет передать явную схему и включить двухмодельный режим `needsGeneration = true`.
3. **`NewTool(ToolDef{...})`** — низкоуровневый конструктор с сырым `json.RawMessage`.

---

## 2. Двухмодельная механика вызова (`decision` + `micro`) и `LoopToolInvoker`

В `tool.go` реализованы два инвокера инструментов:

### `DefaultToolInvoker` (одноходовой)
1. **Выбор инструмента:** модель `Model` (например, быстрый классификатор Jev) выбирает подходящий инструмент из списка (или отказывается от вызова тула).
2. **Генерация сложных параметров (`NeedsGeneration() == true`):** если выбранный инструмент требует генерации кода или сложной структуры, инвокер делает второй запрос к `ArgModel` с жёсткой `ResponseSchema: tool.Schema()`.
3. **Валидация и вызов:** JSON-аргументы проверяются через `schema.Validate(args)` на стороне Go до вызова `tool.Execute(ctx, args)`.

### `LoopToolInvoker` (многоходовой ReAct-цикл)
Полноценный цикл с обратной связью через `gateway.MsgTool`:
- **`MaxIterations`** (по умолчанию 10) — защита от бесконечного вызова инструментов.
- **`NeedsGeneration()` + `ArgModel`** — прозрачная генерация сложных аргументов второй моделью внутри многоходового диалога.
- **`ReplyModel`** — если на роли `Model` стоит чистый классификатор (Jev), выбирающий только имя тула или `"none"`, то финальный текстовый ответ пользователю автоматически генерирует `ReplyModel` (включая потоковый вывод при `StreamChat`).
- **`ContinueOnToolError: true`** — ошибки валидации схемы или выполнения инструмента не обрывают шаг, а отправляются модели обратно в `MsgTool` (`{"error": "..."}`), чтобы она сама исправила параметры на следующей итерации.

---

## 3. Защита от зацикливания: правило 3 попыток, `verify` и эскалация

Каждый шаг плана исполняется внутри `RunWithGuardrail`:

1. **Попытки `1..MaxAttempts` (по умолчанию 3):**
   При ошибке исполнения или провале проверки `Hooks.OnVerify` ошибка записывается в `RunState.History` и передаётся в следующую попытку (`LastError`).
2. **Приёмка результата (`verify.EnsembleVerifier`):**
   Пакет `verify` предоставляет трёхзначный вердикт `Pass` / `Fail` / `Unsure`:
   ```go
   verifier := verify.EnsembleVerifier{
       RequireAllPass: true, // вердикт Unsure трактуется как отказ, а не как Pass
       Checks: []verify.CheckFunc{checkClipping, checkDuration},
   }
   ```
3. **Эскалация (`StatusEscalated`):**
   После 3 неудач вызывается `Hooks.OnEscalate`, а задача со всей историей ошибок передаётся тяжёлой `EscalateModel` (`Plan`-модели), чей ответ также проходит `OnVerify`.
4. **Выход без зависания (`FailurePolicy` и бэклог `RunState.Failures`):**
   Система никогда не уходит в 4-й круг ретраев. Если эскалация не помогла:
   - полная карточка инцидента `FailureRecord` сохраняется в `RunState.Failures` (список ручных правок для пользователя и технический бэклог разработчика);
   - политика решает судьбу узла: `FailOnError` (остановить запуск), `SkipOnError` (пропустить узел) или `FallbackValue(val)` (подставить безопасную заглушку и продолжить сборку остальных узлов).

---

## 4. Двухуровневая система прав (`Permissions`)

1. **Наследование сверху вниз без повышения привилегий:**
   При вызове `Fork(parent, opts...)` и `SpawnAsync` список прав дочернего агента автоматически пересекается с правами родителя. Дочерний агент может получить только равный или урезанный набор прав, но **никогда не получит право, которого нет у родителя**.
   Дополнительно `WithAllowSubagents(false)` запрещает агенту порождать собственных субагентов, а `MaxActiveChildren` (по умолчанию 50) и `MaxSpawnDepth` (по умолчанию 5) защищают систему от fork-бомбы.
2. **Проверка прав до и во время вызова инструмента:**
   - `PermissionMiddleware(map[string]string{"lua_exec": "lua.exec"})` автоматически убирает запрещённые инструменты из списка `Tools` перед отправкой запроса в модель.
   - `ToolWithPermission("lua.exec", tool)` и `ctx.HasPermission("lua.exec")` проверяют права повторно непосредственно в момент вызова `Execute`.
   - `verify.ACLToolMiddleware(allowFn)` позволяет задавать динамические правила доступа на основе ролей исходного пользователя в `context.Context`.

