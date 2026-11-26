package agentgo

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"proj.dichay.tech/agent-go/gateway"
)

// MemoryEntry is a structured fact, note, or artifact stored in an agent's MemoryBank.
type MemoryEntry struct {
	Key       string    `json:"key"`
	Value     any       `json:"value"`
	Tags      []string  `json:"tags,omitempty"`
	Source    string    `json:"source,omitempty"`
	Pinned    bool      `json:"pinned,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Clone returns a copy of MemoryEntry with cloned Tags.
func (m MemoryEntry) Clone() MemoryEntry {
	out := m
	if len(m.Tags) > 0 {
		out.Tags = append([]string(nil), m.Tags...)
	}
	return out
}

// ChatEntry is an addressable message in an agent's managed conversation buffer.
type ChatEntry struct {
	ID        string          `json:"id"`
	Message   gateway.Message `json:"message"`
	Pinned    bool            `json:"pinned,omitempty"`
	Summary   bool            `json:"summary,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// TaskStatus represents the lifecycle state of a self-managed agent task.
type TaskStatus string

const (
	TaskPending    TaskStatus = "pending"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskBlocked    TaskStatus = "blocked"
	TaskSkipped    TaskStatus = "skipped"
)

// TaskItem is an entry in an agent's self-managed task list / backlog.
type TaskItem struct {
	ID        string     `json:"id"`
	Title     string     `json:"title"`
	Status    TaskStatus `json:"status"`
	Assignee  string     `json:"assignee,omitempty"`
	DependsOn []string   `json:"depends_on,omitempty"`
	Notes     string     `json:"notes,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// Clone returns a copy of TaskItem.
func (t TaskItem) Clone() TaskItem {
	out := t
	if len(t.DependsOn) > 0 {
		out.DependsOn = append([]string(nil), t.DependsOn...)
	}
	return out
}

// MemorySnapshot is a JSON-serializable snapshot of a MemoryBank.
type MemorySnapshot struct {
	Entries []MemoryEntry `json:"entries,omitempty"`
	Chat    []ChatEntry   `json:"chat,omitempty"`
	Tasks   []TaskItem    `json:"tasks,omitempty"`
}

// MemoryBank is a thread-safe managed store for an agent's key-value facts/artifacts,
// controllable conversation ("chat") history, and self-managed task list.
// Both host Go code and the agent itself (via MemoryTools / ChatTools / TaskTools)
// can inspect, add, edit, compact, pin, and transfer entries.
type MemoryBank struct {
	mu              sync.RWMutex
	entries         map[string]MemoryEntry
	chat            []ChatEntry
	tasks           []TaskItem
	maxChatMessages int
	msgSeq          atomic.Uint64
	taskSeq         atomic.Uint64
}

// NewMemoryBank creates an empty MemoryBank.
func NewMemoryBank() *MemoryBank {
	return &MemoryBank{
		entries: make(map[string]MemoryEntry),
	}
}

// SetMaxChatMessages configures an optional sliding-window cap on unpinned chat entries.
// Pinned messages and summaries are preserved when trimming. Zero disables auto-trimming.
func (mb *MemoryBank) SetMaxChatMessages(maxMessages int) {
	mb.mu.Lock()
	mb.maxChatMessages = maxMessages
	mb.enforceChatCapLocked()
	mb.mu.Unlock()
}

// ============================================================================
// Key-Value Facts & Artifacts
// ============================================================================

// Set stores or updates a key-value fact in the bank.
func (mb *MemoryBank) Set(key string, value any, tags ...string) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	prev, exists := mb.entries[key]
	entry := MemoryEntry{
		Key:       key,
		Value:     value,
		Tags:      append([]string(nil), tags...),
		Source:    prev.Source,
		Pinned:    exists && prev.Pinned,
		UpdatedAt: time.Now(),
	}
	if len(tags) == 0 && len(prev.Tags) > 0 {
		entry.Tags = append([]string(nil), prev.Tags...)
	}
	mb.entries[key] = entry
}

// SetEntry inserts or replaces a full MemoryEntry.
func (mb *MemoryBank) SetEntry(entry MemoryEntry) {
	if entry.Key == "" {
		return
	}
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = time.Now()
	}
	mb.mu.Lock()
	mb.entries[entry.Key] = entry.Clone()
	mb.mu.Unlock()
}

// SetPinned sets a fact and marks whether it is pinned against external overwrite.
func (mb *MemoryBank) SetPinned(key string, value any, pinned bool, tags ...string) {
	mb.SetEntry(MemoryEntry{
		Key:       key,
		Value:     value,
		Tags:      tags,
		Pinned:    pinned,
		UpdatedAt: time.Now(),
	})
}

// Get retrieves a MemoryEntry by key.
func (mb *MemoryBank) Get(key string) (MemoryEntry, bool) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	e, ok := mb.entries[key]
	if !ok {
		return MemoryEntry{}, false
	}
	return e.Clone(), true
}

// Value returns the raw stored value for key.
func (mb *MemoryBank) Value(key string) (any, bool) {
	e, ok := mb.Get(key)
	if !ok {
		return nil, false
	}
	return e.Value, true
}

// Delete removes a key from the bank.
func (mb *MemoryBank) Delete(key string) bool {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if _, ok := mb.entries[key]; !ok {
		return false
	}
	delete(mb.entries, key)
	return true
}

// Entries returns all stored facts (sorted by key), optionally filtered by tag.
func (mb *MemoryBank) Entries(tagFilter ...string) []MemoryEntry {
	mb.mu.RLock()
	out := make([]MemoryEntry, 0, len(mb.entries))
	for _, e := range mb.entries {
		if len(tagFilter) > 0 && !hasAnyTag(e.Tags, tagFilter) {
			continue
		}
		out = append(out, e.Clone())
	}
	mb.mu.RUnlock()
	slices.SortFunc(out, func(a, b MemoryEntry) int {
		return strings.Compare(a.Key, b.Key)
	})
	return out
}

// Export returns a map of selected keys (or all keys when keys is empty) for passing to another agent.
func (mb *MemoryBank) Export(keys ...string) map[string]MemoryEntry {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	if len(keys) == 0 {
		out := make(map[string]MemoryEntry, len(mb.entries))
		for k, v := range mb.entries {
			out[k] = v.Clone()
		}
		return out
	}
	out := make(map[string]MemoryEntry, len(keys))
	for _, k := range keys {
		if v, ok := mb.entries[k]; ok {
			out[k] = v.Clone()
		}
	}
	return out
}

// Import merges external entries into mb. Pinned entries in mb are preserved unless overwritePinned is true.
func (mb *MemoryBank) Import(entries map[string]MemoryEntry, overwritePinned bool) int {
	if len(entries) == 0 {
		return 0
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()
	imported := 0
	for k, v := range entries {
		if k == "" {
			continue
		}
		if cur, exists := mb.entries[k]; exists && cur.Pinned && !overwritePinned {
			continue
		}
		cp := v.Clone()
		cp.Key = k
		if cp.UpdatedAt.IsZero() {
			cp.UpdatedAt = time.Now()
		}
		mb.entries[k] = cp
		imported++
	}
	return imported
}

// CopyTo copies selected keys (or all keys when keys is empty) into dst.
func (mb *MemoryBank) CopyTo(dst *MemoryBank, keys ...string) int {
	if mb == nil || dst == nil {
		return 0
	}
	return dst.Import(mb.Export(keys...), false)
}

// ============================================================================
// Controllable Chat / Conversation Buffer
// ============================================================================

// AppendChat appends a text message to the managed chat buffer and returns its assigned ID.
func (mb *MemoryBank) AppendChat(role gateway.MessageRole, content string) string {
	return mb.AppendMessage(gateway.Message{Role: role, Content: content}, false)
}

// AppendMessage appends a gateway.Message to the chat buffer and returns its assigned ID.
func (mb *MemoryBank) AppendMessage(msg gateway.Message, pinned bool) string {
	id := fmt.Sprintf("m_%d", mb.msgSeq.Add(1))
	mb.mu.Lock()
	mb.chat = append(mb.chat, ChatEntry{
		ID:        id,
		Message:   msg,
		Pinned:    pinned,
		CreatedAt: time.Now(),
	})
	mb.enforceChatCapLocked()
	mb.mu.Unlock()
	return id
}

// ChatEntries returns a copy of all ChatEntry records with metadata.
func (mb *MemoryBank) ChatEntries() []ChatEntry {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	out := make([]ChatEntry, len(mb.chat))
	copy(out, mb.chat)
	return out
}

// ChatMessages returns normalized gateway.Messages suitable for gateway.Request.Messages.
func (mb *MemoryBank) ChatMessages() []gateway.Message {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	out := make([]gateway.Message, len(mb.chat))
	for i, c := range mb.chat {
		out[i] = c.Message
	}
	return out
}

// PinMessage pins or unpins a chat message by ID so it survives compaction and auto-trimming.
func (mb *MemoryBank) PinMessage(id string, pinned bool) bool {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	for i := range mb.chat {
		if mb.chat[i].ID == id {
			mb.chat[i].Pinned = pinned
			return true
		}
	}
	return false
}

// EditMessage updates the text content of an existing chat entry by ID.
func (mb *MemoryBank) EditMessage(id, content string) bool {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	for i := range mb.chat {
		if mb.chat[i].ID == id {
			mb.chat[i].Message.Content = content
			return true
		}
	}
	return false
}

// DeleteMessages deletes unpinned chat entries matching any of ids and returns the count removed.
func (mb *MemoryBank) DeleteMessages(ids ...string) int {
	if len(ids) == 0 {
		return 0
	}
	drop := make(map[string]bool, len(ids))
	for _, id := range ids {
		drop[id] = true
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()
	kept := mb.chat[:0]
	removed := 0
	for _, c := range mb.chat {
		if drop[c.ID] && !c.Pinned {
			removed++
			continue
		}
		kept = append(kept, c)
	}
	mb.chat = kept
	return removed
}

// CompactChat replaces older unpinned messages with a single summary entry while keeping
// all pinned messages and the most recent keepLast messages intact.
// Returns the number of entries compacted.
func (mb *MemoryBank) CompactChat(summary string, keepLast int) int {
	if keepLast < 0 {
		keepLast = 0
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()

	cutoff := len(mb.chat) - keepLast
	if cutoff <= 0 {
		return 0
	}

	var next []ChatEntry
	compacted := 0
	summaryInserted := false

	for i := 0; i < cutoff; i++ {
		c := mb.chat[i]
		if c.Pinned {
			next = append(next, c)
			continue
		}
		if !summaryInserted && strings.TrimSpace(summary) != "" {
			next = append(next, ChatEntry{
				ID:        fmt.Sprintf("m_%d", mb.msgSeq.Add(1)),
				Message:   gateway.Message{Role: gateway.MsgUser, Content: "[Chat Summary]\n" + summary},
				Summary:   true,
				CreatedAt: time.Now(),
			})
			summaryInserted = true
		}
		compacted++
	}
	next = append(next, mb.chat[cutoff:]...)
	mb.chat = next
	return compacted
}

// ClearChat removes chat history. When keepPinned is true, pinned entries are preserved.
func (mb *MemoryBank) ClearChat(keepPinned bool) int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if !keepPinned {
		n := len(mb.chat)
		mb.chat = nil
		return n
	}
	var kept []ChatEntry
	removed := 0
	for _, c := range mb.chat {
		if c.Pinned {
			kept = append(kept, c)
		} else {
			removed++
		}
	}
	mb.chat = kept
	return removed
}

func (mb *MemoryBank) enforceChatCapLocked() {
	if mb.maxChatMessages <= 0 || len(mb.chat) <= mb.maxChatMessages {
		return
	}
	excess := len(mb.chat) - mb.maxChatMessages
	kept := make([]ChatEntry, 0, len(mb.chat))
	for _, c := range mb.chat {
		if excess > 0 && !c.Pinned && !c.Summary {
			excess--
			continue
		}
		kept = append(kept, c)
	}
	mb.chat = kept
}

// ============================================================================
// Self-Managed Task List
// ============================================================================

// AddTask appends a task to the bank's task list, generating an ID if empty.
func (mb *MemoryBank) AddTask(item TaskItem) TaskItem {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if item.ID == "" {
		item.ID = fmt.Sprintf("task_%d", mb.taskSeq.Add(1))
	}
	if item.Status == "" {
		item.Status = TaskPending
	}
	item.UpdatedAt = time.Now()
	for i := range mb.tasks {
		if mb.tasks[i].ID == item.ID {
			mb.tasks[i] = item.Clone()
			return mb.tasks[i].Clone()
		}
	}
	mb.tasks = append(mb.tasks, item.Clone())
	return item.Clone()
}

// UpdateTask updates status, notes, and/or assignee of an existing task by ID.
func (mb *MemoryBank) UpdateTask(id string, status TaskStatus, notes, assignee string) (TaskItem, bool) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	for i := range mb.tasks {
		if mb.tasks[i].ID == id {
			if status != "" {
				mb.tasks[i].Status = status
			}
			if notes != "" {
				mb.tasks[i].Notes = notes
			}
			if assignee != "" {
				mb.tasks[i].Assignee = assignee
			}
			mb.tasks[i].UpdatedAt = time.Now()
			return mb.tasks[i].Clone(), true
		}
	}
	return TaskItem{}, false
}

// DeleteTask removes a task by ID.
func (mb *MemoryBank) DeleteTask(id string) bool {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	for i := range mb.tasks {
		if mb.tasks[i].ID == id {
			mb.tasks = append(mb.tasks[:i], mb.tasks[i+1:]...)
			return true
		}
	}
	return false
}

// Tasks returns tasks, optionally filtered by status.
func (mb *MemoryBank) Tasks(statusFilter ...TaskStatus) []TaskItem {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	out := make([]TaskItem, 0, len(mb.tasks))
	for _, t := range mb.tasks {
		if len(statusFilter) > 0 && !slices.Contains(statusFilter, t.Status) {
			continue
		}
		out = append(out, t.Clone())
	}
	return out
}

// ============================================================================
// Serialization & DataSource Contract
// ============================================================================

// Snapshot exports a serializable snapshot of all facts, chat entries, and tasks.
func (mb *MemoryBank) Snapshot() MemorySnapshot {
	if mb == nil {
		return MemorySnapshot{}
	}
	return MemorySnapshot{
		Entries: mb.Entries(),
		Chat:    mb.ChatEntries(),
		Tasks:   mb.Tasks(),
	}
}

// Restore replaces the bank's contents with snap.
func (mb *MemoryBank) Restore(snap MemorySnapshot) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.entries = make(map[string]MemoryEntry, len(snap.Entries))
	for _, e := range snap.Entries {
		mb.entries[e.Key] = e.Clone()
	}
	mb.chat = make([]ChatEntry, len(snap.Chat))
	copy(mb.chat, snap.Chat)
	mb.tasks = make([]TaskItem, len(snap.Tasks))
	for i, t := range snap.Tasks {
		mb.tasks[i] = t.Clone()
	}
}

// Clone creates an independent deep copy of the MemoryBank.
func (mb *MemoryBank) Clone() *MemoryBank {
	cp := NewMemoryBank()
	if mb != nil {
		cp.Restore(mb.Snapshot())
	}
	return cp
}

// Name implements DataSource.
func (mb *MemoryBank) Name() string { return "memory_bank" }

// Content renders stored facts and tasks into compact text for model context.
func (mb *MemoryBank) Content(_ Context) (string, error) {
	if mb == nil {
		return "", nil
	}
	entries := mb.Entries()
	tasks := mb.Tasks()
	if len(entries) == 0 && len(tasks) == 0 {
		return "", nil
	}
	var sb strings.Builder
	if len(entries) > 0 {
		sb.WriteString("Memory Facts:\n")
		for _, e := range entries {
			valBytes, err := json.Marshal(e.Value)
			valStr := fmt.Sprint(e.Value)
			if err == nil {
				valStr = string(valBytes)
			}
			pin := ""
			if e.Pinned {
				pin = " [pinned]"
			}
			fmt.Fprintf(&sb, "- %s%s = %s\n", e.Key, pin, valStr)
		}
	}
	if len(tasks) > 0 {
		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString("Tasks:\n")
		for _, t := range tasks {
			extra := ""
			if t.Assignee != "" {
				extra += " @" + t.Assignee
			}
			if t.Notes != "" {
				extra += " (" + t.Notes + ")"
			}
			fmt.Fprintf(&sb, "- [%s] %s: %s%s\n", t.Status, t.ID, t.Title, extra)
		}
	}
	return strings.TrimRight(sb.String(), "\n"), nil
}

func hasAnyTag(have, want []string) bool {
	for _, w := range want {
		if slices.Contains(have, w) {
			return true
		}
	}
	return false
}

func resolveBank(ctx Context, explicit []*MemoryBank) (*MemoryBank, error) {
	if len(explicit) > 0 && explicit[0] != nil {
		return explicit[0], nil
	}
	if ctx != nil && ctx.Bank() != nil {
		return ctx.Bank(), nil
	}
	return nil, fmt.Errorf("memory: no MemoryBank attached to agent or tool")
}

// ============================================================================
// Self-Management Toolkits (Memory, Chat, Tasks, Workspace)
// ============================================================================

// MemoryTools returns tools allowing an agent to read, write, list, and delete
// facts in its MemoryBank (resolved from explicit bank or ctx.Bank()).
func MemoryTools(explicit ...*MemoryBank) []Tool {
	type setIn struct {
		Key    string   `json:"key" desc:"Unique memory key"`
		Value  any      `json:"value" desc:"Value to remember (string, number, object, or array)"`
		Tags   []string `json:"tags,omitempty" desc:"Optional categorization tags"`
		Pinned bool     `json:"pinned,omitempty" desc:"Pin entry to protect from external overwrite"`
	}
	setTool := Func("memory_set", "Saves or updates a fact/artifact in the agent's memory bank", func(ctx Context, in setIn) (map[string]any, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return nil, err
		}
		if in.Key == "" {
			return nil, fmt.Errorf("memory_set: key is required")
		}
		src := ""
		if ctx != nil && ctx.Agent() != nil {
			src = "agent:" + agentIdentifier(ctx.Agent())
		}
		bank.SetEntry(MemoryEntry{
			Key:       in.Key,
			Value:     in.Value,
			Tags:      in.Tags,
			Source:    src,
			Pinned:    in.Pinned,
			UpdatedAt: time.Now(),
		})
		return map[string]any{"ok": true, "key": in.Key}, nil
	})

	type getIn struct {
		Key string `json:"key,omitempty" desc:"Specific key to retrieve; omit to list all entries"`
		Tag string `json:"tag,omitempty" desc:"Optional tag filter when listing entries"`
	}
	getTool := Func("memory_get", "Reads a specific fact by key or lists all stored facts in memory", func(ctx Context, in getIn) (any, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return nil, err
		}
		if in.Key != "" {
			e, ok := bank.Get(in.Key)
			return map[string]any{"found": ok, "entry": e}, nil
		}
		if in.Tag != "" {
			return map[string]any{"entries": bank.Entries(in.Tag)}, nil
		}
		return map[string]any{"entries": bank.Entries()}, nil
	})

	type delIn struct {
		Key string `json:"key" desc:"Memory key to delete"`
	}
	delTool := Func("memory_delete", "Deletes an obsolete fact from the agent's memory bank", func(ctx Context, in delIn) (map[string]any, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return nil, err
		}
		return map[string]any{"deleted": bank.Delete(in.Key)}, nil
	})

	return []Tool{setTool, getTool, delTool}
}

// ChatTools returns tools allowing an agent to inspect, pin, compact/summarize,
// edit, or prune its own conversation ("chat") buffer.
func ChatTools(explicit ...*MemoryBank) []Tool {
	type inspectIn struct{}
	inspectTool := Func("chat_inspect", "Lists messages in the conversation buffer with IDs, roles, pinned flags, and token estimate", func(ctx Context, _ inspectIn) (map[string]any, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return nil, err
		}
		entries := bank.ChatEntries()
		totalTokens := 0
		type row struct {
			ID      string              `json:"id"`
			Role    gateway.MessageRole `json:"role"`
			Preview string              `json:"preview"`
			Pinned  bool                `json:"pinned,omitempty"`
			Summary bool                `json:"summary,omitempty"`
			Tokens  int                 `json:"est_tokens"`
		}
		rows := make([]row, len(entries))
		for i, c := range entries {
			tok := EstimateTokens(c.Message.Content)
			totalTokens += tok
			prev := c.Message.Content
			if len(prev) > 120 {
				prev = prev[:120] + "..."
			}
			rows[i] = row{
				ID:      c.ID,
				Role:    c.Message.Role,
				Preview: prev,
				Pinned:  c.Pinned,
				Summary: c.Summary,
				Tokens:  tok,
			}
		}
		return map[string]any{
			"count":      len(rows),
			"est_tokens": totalTokens,
			"messages":   rows,
		}, nil
	})

	type pinIn struct {
		ID     string `json:"id" desc:"Chat message ID (e.g. m_1)"`
		Pinned bool   `json:"pinned" desc:"true to pin, false to unpin"`
	}
	pinTool := Func("chat_pin", "Pins or unpins a chat message so important instructions survive compaction", func(ctx Context, in pinIn) (map[string]any, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return nil, err
		}
		return map[string]any{"updated": bank.PinMessage(in.ID, in.Pinned)}, nil
	})

	type compactIn struct {
		Summary   string   `json:"summary,omitempty" desc:"Concise summary replacing older unpinned messages"`
		KeepLast  int      `json:"keep_last,omitempty" desc:"Number of most recent messages to keep intact when compacting"`
		DeleteIDs []string `json:"delete_ids,omitempty" desc:"Optional specific unpinned message IDs to delete"`
	}
	compactTool := Func("chat_manage", "Compacts older chat turns into a summary or deletes noisy message IDs while preserving pinned messages", func(ctx Context, in compactIn) (map[string]any, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return nil, err
		}
		deleted := 0
		if len(in.DeleteIDs) > 0 {
			deleted = bank.DeleteMessages(in.DeleteIDs...)
		}
		compacted := 0
		if in.Summary != "" || in.KeepLast > 0 {
			compacted = bank.CompactChat(in.Summary, in.KeepLast)
		}
		return map[string]any{
			"deleted":   deleted,
			"compacted": compacted,
			"remaining": len(bank.ChatEntries()),
		}, nil
	})

	type clearIn struct {
		KeepPinned bool `json:"keep_pinned,omitempty" desc:"Keep pinned messages when clearing chat"`
	}
	clearTool := Func("chat_clear", "Clears conversation history (optionally keeping pinned messages)", func(ctx Context, in clearIn) (map[string]any, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return nil, err
		}
		return map[string]any{"removed": bank.ClearChat(in.KeepPinned)}, nil
	})

	return []Tool{inspectTool, pinTool, compactTool, clearTool}
}

// TaskTools returns tools allowing an agent to manage its own task list and
// optionally enqueue dynamic PlanSteps into the active execution DAG.
func TaskTools(explicit ...*MemoryBank) []Tool {
	type addIn struct {
		ID        string   `json:"id,omitempty" desc:"Optional task ID; auto-generated if omitted"`
		Title     string   `json:"title" desc:"Task description"`
		Assignee  string   `json:"assignee,omitempty" desc:"Optional assignee agent ID"`
		DependsOn []string `json:"depends_on,omitempty" desc:"Optional IDs of prerequisite tasks or subagents"`
		Notes     string   `json:"notes,omitempty" desc:"Optional notes"`
		AddToPlan bool     `json:"add_to_plan,omitempty" desc:"If true, also schedules this task as a dynamic step in the current Run plan"`
	}
	addTool := Func("task_add", "Adds a task to the agent's task list (and optionally to the live execution plan)", func(ctx Context, in addIn) (TaskItem, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return TaskItem{}, err
		}
		if in.Title == "" {
			return TaskItem{}, fmt.Errorf("task_add: title is required")
		}
		created := bank.AddTask(TaskItem{
			ID:        in.ID,
			Title:     in.Title,
			Status:    TaskPending,
			Assignee:  in.Assignee,
			DependsOn: in.DependsOn,
			Notes:     in.Notes,
		})
		if in.AddToPlan && ctx != nil {
			AppendPlanSteps(ctx, PlanStep{
				ID:        created.ID,
				Task:      created.Title,
				DependsOn: created.DependsOn,
			})
		}
		return created, nil
	})

	type updateIn struct {
		ID       string     `json:"id" desc:"Task ID to update"`
		Status   TaskStatus `json:"status,omitempty" desc:"New status: pending, in_progress, done, blocked, or skipped"`
		Notes    string     `json:"notes,omitempty" desc:"Updated notes or result summary"`
		Assignee string     `json:"assignee,omitempty" desc:"Updated assignee"`
	}
	updateTool := Func("task_update", "Updates a task's status, notes, or assignee", func(ctx Context, in updateIn) (map[string]any, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return nil, err
		}
		updated, ok := bank.UpdateTask(in.ID, in.Status, in.Notes, in.Assignee)
		return map[string]any{"found": ok, "task": updated}, nil
	})

	type listIn struct {
		Status TaskStatus `json:"status,omitempty" desc:"Optional status filter"`
	}
	listTool := Func("task_list", "Lists tasks in the agent's task list", func(ctx Context, in listIn) (map[string]any, error) {
		bank, err := resolveBank(ctx, explicit)
		if err != nil {
			return nil, err
		}
		if in.Status != "" {
			return map[string]any{"tasks": bank.Tasks(in.Status)}, nil
		}
		return map[string]any{"tasks": bank.Tasks()}, nil
	})

	return []Tool{addTool, updateTool, listTool}
}

// WorkspaceTools returns the complete self-management toolset:
// MemoryTools + ChatTools + TaskTools + BusTools.
func WorkspaceTools() []Tool {
	var all []Tool
	all = append(all, MemoryTools()...)
	all = append(all, ChatTools()...)
	all = append(all, TaskTools()...)
	all = append(all, BusTools()...)
	return all
}

