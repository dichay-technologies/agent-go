# Tools, 3-Attempt Guardrails & Two-Level Permissions

---

## 1. Declaring Tools (`Tool`)

`agent-go` provides three ergonomic constructors around the minimal `Tool` interface:

1. **`Func[In, Out](name, desc, fn)`** — derives a `JSONSchema` automatically from Go struct `In` via reflection (`gateway.SchemaFor[In]()`), honoring `json:"...,omitempty"` and `desc:"..."` tags:
   ```go
   type TrimIn struct {
       ClipID string  `json:"clip_id" desc:"Target clip ID"`
       Dur    float64 `json:"dur" desc:"Duration in seconds"`
   }
   trim := agentgo.Func("trim_clip", "Trims a media clip", func(ctx agentgo.Context, in TrimIn) (string, error) {
       return fmt.Sprintf("%s:%.2fs", in.ClipID, in.Dur), nil
   })
   ```
2. **`NewTypedTool[In](name, desc, schema, needsGeneration, fn)`** — accepts an explicit schema and supports two-stage parameter generation (`needsGeneration = true`).
3. **`NewTool(ToolDef{...})`** — low-level constructor taking raw `json.RawMessage`.

---

## 2. Two-Model Tool Invocation (`decision` + `micro`) & `LoopToolInvoker`

### `DefaultToolInvoker` (Single-Turn)
1. **Selection:** `Model` (e.g. a fast Jev classifier) picks the best tool or `"none"`.
2. **Structured Argument Generation (`NeedsGeneration() == true`):** when the selected tool requires complex code or nested JSON, a second structured call is dispatched to `ArgModel` with `ResponseSchema: tool.Schema()`.
3. **Validation & Execution:** `schema.Validate(args)` runs in Go before `tool.Execute(ctx, args)` is called.

### `LoopToolInvoker` (Multi-Turn ReAct Loop)
Full multi-turn loop feeding tool outputs back via `gateway.MsgTool`:
- **`MaxIterations`** (default 10) prevents runaway tool loops.
- **`NeedsGeneration()` + `ArgModel`** seamlessly handles two-stage argument synthesis inside multi-turn loops.
- **`ReplyModel`** — when `Model` is a pure classifier (like Jev), `ReplyModel` automatically generates the natural-language reply once no further tool calls are needed (including live SSE streaming during `StreamChat`).
- **`ContinueOnToolError: true`** — schema validation and tool execution errors are returned to the model as `MsgTool` (`{"error": "..."}`) so the model can self-correct on the next turn.

---

## 3. 3-Attempt Guardrail, Verification (`verify`) & Escalation

Every plan step runs inside `RunWithGuardrail`:

1. **Attempts `1..MaxAttempts` (default 3):**
   Execution or `Hooks.OnVerify` failures are appended to `RunState.History` and passed into the next retry (`LastError`).
2. **Three-Valued Verification (`verify.EnsembleVerifier`):**
   Aggregates multiple `CheckFunc` predicates returning `Pass`, `Fail`, or `Unsure`:
   ```go
   verifier := verify.EnsembleVerifier{
       RequireAllPass: true, // treats Unsure as rejection rather than Pass
       Checks: []verify.CheckFunc{checkClipping, checkSync},
   }
   ```
3. **Escalation (`StatusEscalated`):**
   After 3 failed attempts, `Hooks.OnEscalate` fires and `EscalateModel` (`Plan` model) receives the full `AttemptLog` history for a single recovery attempt (also checked by `OnVerify`).
4. **Bounded Exit (`FailurePolicy` & `RunState.Failures` Backlog):**
   The agent never enters a 4th retry loop. If escalation fails:
   - a `FailureRecord` is appended to `RunState.Failures` (serving as a human review list and developer backlog);
   - `FailurePolicy` applies `FailOnError` (abort run), `SkipOnError` (skip node), or `FallbackValue(placeholder)` (substitute a safe placeholder and continue building remaining nodes).

---

## 4. Two-Level `Permissions` & Fork-Bomb Guards

1. **Monotonic Top-Down Inheritance:**
   `Fork(parent, opts...)` and `SpawnAsync` automatically intersect child permissions with parent permissions. A subagent may narrow its permissions, **never escalate beyond its parent**.
   `WithAllowSubagents(false)` disables dynamic child spawning completely, while `MaxActiveChildren` (default 50) and `MaxSpawnDepth` (default 5) prevent fork bombs.
2. **Pre-Filter & Execution-Time Checks:**
   - `PermissionMiddleware(map[string]string{"lua_exec": "lua.exec"})` strips unauthorized tools before the model sees them.
   - `ToolWithPermission("lua.exec", tool)` and `ctx.HasPermission("lua.exec")` re-verify permissions right before `Execute`.
   - `verify.ACLToolMiddleware(allowFn)` enables dynamic caller/role checks from `context.Context`.

