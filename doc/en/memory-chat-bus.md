# Managed Memory, Chat Mode & Inter-Agent Bus

---

## 1. Agent Workspace (`MemoryBank`)

`MemoryBank` (`memory.go`) is a thread-safe, JSON-serializable store divided into three areas that **both host Go code and the agent itself (via built-in tools)** can inspect and mutate.

### A. Key-Value Facts & Artifacts (`MemoryEntry`)
- **From Go:**
  - `bank.Set(key, value, tags...)` — insert or update a fact;
  - `bank.SetPinned(key, value, true, tags...)` — pin an entry so cross-agent imports cannot overwrite it;
  - `bank.Get(key)` / `bank.Value(key)` / `bank.Entries(tagFilter...)` / `bank.Delete(key)`;
  - `bank.CopyTo(otherBank, keys...)` — pass selected (or all) facts into another agent's `MemoryBank`.
- **Agent Self-Management Tools (`MemoryTools()`):**
  - `memory_set`, `memory_get`, `memory_delete`.

### B. Controllable Conversation Buffer ("Chat" — `ChatEntry`)
Every turn in `MemoryBank` has a stable `ID` (`m_1`, `m_2`, ...), a `Pinned` flag, and a `Summary` marker.
- **From Go:**
  - `bank.AppendChat(role, content)` and `bank.AppendMessage(msg, pinned)`;
  - `bank.PinMessage(id, true)` — pinned instructions survive compaction and sliding-window trimming;
  - `bank.EditMessage(id, newContent)` and `bank.DeleteMessages(ids...)`;
  - `bank.CompactChat(summary, keepLast)` — collapses older unpinned turns into a single `[Chat Summary]` entry while preserving all `Pinned` messages and the most recent `keepLast` turns;
  - `bank.SetMaxChatMessages(limit)` — automatic sliding window;
  - `bank.ClearChat(keepPinned)`.
- **Agent Self-Management Tools (`ChatTools()`):**
  - `chat_inspect` — inspect message IDs, roles, pinned flags, and estimated token counts;
  - `chat_pin` — pin or unpin critical user instructions;
  - `chat_manage` — compact older turns into a concise `summary` (keeping `keep_last` recent turns) or delete noisy message IDs via `delete_ids`;
  - `chat_clear` — clear conversation history while keeping pinned rules.

### C. Self-Managed Task List (`TaskItem`)
- **From Go:** `bank.AddTask(...)`, `bank.UpdateTask(id, status, notes, assignee)`, `bank.DeleteTask(id)`, `bank.Tasks(statusFilter...)`.
- **Agent Self-Management Tools (`TaskTools()`):**
  - `task_add` — creates a task; setting `"add_to_plan": true` also appends it dynamically as a live `PlanStep` in the active `Scheduler` run;
  - `task_update` — transitions status (`pending`, `in_progress`, `done`, `blocked`, `skipped`), assignee, or notes;
  - `task_list` — lists tasks optionally filtered by status.

Enable the entire self-management suite in one option via **`WithWorkspaceTools[S]()`**.

---

## 2. Conversational Mode (`ag.Chat` & `ag.StreamChat`)

Any `AI[S]` works out-of-the-box as a stateful chat coordinator without mutating `AI[S]`:

```go
// Synchronous chat turn:
reply, err := coord.Chat(ctx, "Remember FPS=60 and add an intro task")

// Streaming chat turn (yields live SSE deltas):
for delta, err := range coord.StreamChat(ctx, "Make scene 3 faster") {
    if err != nil {
        log.Fatal(err)
    }
    fmt.Print(delta)
}
```

Each turn automatically appends the user message to `MemoryBank`, keeps `SystemPrefix` byte-identical for KV caching, injects workspace facts/tasks + unread `Bus` events + `ChatMessages()`, and records the assistant's reply.

---

## 3. Inter-Agent Message `Bus`, Memory Sharing & `Tell`

`Bus` (`bus.go`) connects parent coordinators, subagents, and external host code through `Envelope` mailboxes:

- **Automatic Inheritance:** `Fork` and `SpawnAsync` automatically attach child agents to the parent's `Bus` and register their `ID` and `MemoryBank`.
- **Inter-Agent Tools (`BusTools()`):**
  - `agent_send` — send a message to a specific agent `ID`, `"parent"`, or `"*"` broadcast. Passing `"share_memory_keys": ["fps", "beats"]` automatically exports those entries from the sender's `MemoryBank` into the recipient's `MemoryBank` (respecting `Pinned` entries).
  - `agent_recv` — drain unread messages immediately or block up to `wait_ms` waiting for a reply.
  - `agent_peers` — list active peers on the bus and parent ID.
- **External Signals (`ag.Tell`):**
  `ag.Tell("editor-ui", "Scene 2 locked by user")` queues an external signal into the agent's mailbox; on the next step it is drained into `RunState.PendingEvents` and surfaced to the model.
- **Reactive Subscriptions (`bus.Subscribe`):**
  `unsub := bus.Subscribe("*", func(env agentgo.Envelope) { ... })` streams all inter-agent traffic to logs or WebSocket/SSE clients.

