package agentgo

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"proj.dichay.tech/agent-go/gateway"
)

// AgentStatus represents the current FSM phase of an agent.
type AgentStatus string

const (
	StatusIdle             AgentStatus = "idle"
	StatusAnalyzing        AgentStatus = "analyzing"
	StatusGatheringContext AgentStatus = "gathering_context"
	StatusPlanning         AgentStatus = "planning"
	StatusExecuting        AgentStatus = "executing"
	StatusWaitingDeps      AgentStatus = "waiting_deps"
	StatusVerifying        AgentStatus = "verifying"
	StatusEscalated        AgentStatus = "escalated"
	StatusPaused           AgentStatus = "paused"
	StatusDone             AgentStatus = "done"
	StatusFailed           AgentStatus = "failed"
)

// ContextPlacement controls where gathered DataSource content is placed in model requests.
type ContextPlacement int

const (
	// ContextAsMessage places gathered DataSource content in a separate user message,
	// keeping SystemPrefix stable for provider KV/prompt caching (default).
	ContextAsMessage ContextPlacement = iota
	// ContextInSystemPrefix appends gathered DataSource content to SystemPrefix.
	ContextInSystemPrefix
)

// PlanStep represents a node in the agent's execution plan DAG.
type PlanStep struct {
	ID        string   `json:"id"`
	Task      string   `json:"task"`
	DependsOn []string `json:"depends_on,omitempty"`
}

// FailureRecord captures diagnostic details when a step exhausts all attempts
// and escalation (Spec §5.3 manual-review list & developer backlog).
type FailureRecord struct {
	StepID   string        `json:"step_id"`
	Attempts int           `json:"attempts"`
	Error    string        `json:"error"`
	Action   FailureAction `json:"action"`
	History  []AttemptLog  `json:"history,omitempty"`
}

// RunResult is the complete outcome of RunDetailed / RunFromDetailed, including
// a serializable RunState snapshot suitable for persistence, inspection, or resumption.
type RunResult struct {
	Output any      `json:"output,omitempty"`
	State  RunState `json:"state"`
	Paused bool     `json:"paused,omitempty"`
	Gate   string   `json:"gate,omitempty"`
}

// RunState holds the mutable, JSON-serializable execution state of a single agent run.
// It is isolated from the immutable AI[S] configuration so an AI[S] can be reused
// across sequential or concurrent Run calls and resumed via RunFrom / Resume.
type RunState struct {
	Status          AgentStatus     `json:"status"`
	Task            string          `json:"task,omitempty"`
	CurrentStepID   string          `json:"current_step_id,omitempty"`
	PausedAtGate    string          `json:"paused_at_gate,omitempty"`
	CompletedStages []string        `json:"completed_stages,omitempty"`
	Attempt         int             `json:"attempt,omitempty"`
	SpawnDepth      int             `json:"spawn_depth,omitempty"`
	Analysis        string          `json:"analysis,omitempty"`
	ContextData     string          `json:"context_data,omitempty"`
	Plan            []PlanStep      `json:"plan,omitempty"`
	StepResults     map[string]any  `json:"step_results,omitempty"`
	PendingEvents   []Envelope      `json:"pending_events,omitempty"`
	History         []AttemptLog    `json:"history,omitempty"`
	Failures        []FailureRecord `json:"failures,omitempty"`
	Output          any             `json:"output,omitempty"`
}

// Clone returns a deep-enough copy of RunState safe for serialization or resumption.
func (rs RunState) Clone() RunState {
	out := rs
	if len(rs.CompletedStages) > 0 {
		out.CompletedStages = append([]string(nil), rs.CompletedStages...)
	}
	if len(rs.Plan) > 0 {
		out.Plan = make([]PlanStep, len(rs.Plan))
		for i, p := range rs.Plan {
			cp := p
			if len(p.DependsOn) > 0 {
				cp.DependsOn = append([]string(nil), p.DependsOn...)
			}
			out.Plan[i] = cp
		}
	}
	if len(rs.StepResults) > 0 {
		out.StepResults = make(map[string]any, len(rs.StepResults))
		for k, v := range rs.StepResults {
			out.StepResults[k] = v
		}
	}
	if len(rs.PendingEvents) > 0 {
		out.PendingEvents = make([]Envelope, len(rs.PendingEvents))
		for i, e := range rs.PendingEvents {
			out.PendingEvents[i] = e.Clone()
		}
	}
	if len(rs.History) > 0 {
		out.History = append([]AttemptLog(nil), rs.History...)
	}
	if len(rs.Failures) > 0 {
		out.Failures = make([]FailureRecord, len(rs.Failures))
		for i, f := range rs.Failures {
			cf := f
			if len(f.History) > 0 {
				cf.History = append([]AttemptLog(nil), f.History...)
			}
			out.Failures[i] = cf
		}
	}
	return out
}

// LocalState wraps the user-defined application state S with a RWMutex
// so concurrent child agents and hooks can safely read and update it.
type LocalState[S any] struct {
	mu    sync.RWMutex
	value S
}

// NewLocalState creates a LocalState initialized with v.
func NewLocalState[S any](v S) *LocalState[S] {
	return &LocalState[S]{value: v}
}

// Get returns a shallow copy of the current state.
func (ls *LocalState[S]) Get() S {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.value
}

// Set replaces the current state.
func (ls *LocalState[S]) Set(v S) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.value = v
}

// Update mutates the state under an exclusive write lock.
func (ls *LocalState[S]) Update(fn func(*S)) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	fn(&ls.value)
}

// Hooks defines optional lifecycle callbacks for an AI[S] agent.
type Hooks[S any] struct {
	OnStep     func(ctx Context, state *LocalState[S], stepID string, res ToolCallResult)
	OnStream   func(ctx Context, chunk gateway.StreamChunk)
	OnVerify   func(ctx Context, state *LocalState[S], output any) error
	OnEscalate func(ctx Context, state *LocalState[S], history []AttemptLog) error
	// OnGate is called after a gated stage completes; returning false pauses the run at that checkpoint.
	OnGate func(ctx Context, state *LocalState[S], gate string) (proceed bool, err error)
	OnFail func(ctx Context, state *LocalState[S], err error)
}

// StageFunc is a single overridable phase of an agent's lifecycle.
type StageFunc[S any] func(ctx Context, a *AI[S]) error

// Stage represents a named FSM phase in a custom pipeline.
type Stage[S any] struct {
	Status AgentStatus
	Name   string
	Run    StageFunc[S]
}

// Pipeline allows overriding individual lifecycle phases or providing a Custom stage list.
type Pipeline[S any] struct {
	Analyze       StageFunc[S]
	GatherContext StageFunc[S]
	Plan          StageFunc[S]
	Execute       StageFunc[S]
	WaitDeps      StageFunc[S]
	Custom        []Stage[S]
}

// PromptConfig allows overriding the built-in Go templates used by lifecycle phases.
type PromptConfig struct {
	Analyze  string
	Plan     string
	Escalate string
}

const (
	defaultAnalyzePrompt  = "Analyze task: {{.Task}}"
	defaultPlanPrompt     = "Create an execution plan as JSON {\"steps\":[{\"id\":\"...\",\"task\":\"...\",\"depends_on\":[]}]} for:\n{{.Task}}\n\nContext:\n{{.Context}}{{if .Analysis}}\n\nAnalysis:\n{{.Analysis}}{{end}}"
	defaultEscalatePrompt = "Step {{.StepID}} failed {{.Attempts}} times. History:\n{{.History}}\nProvide a final recovery response or explain why it cannot proceed."
)

// AI is the reusable configuration of a generic agent parameterized by application state S.
// Per-run mutable execution state lives in RunState, while structured memory/chat/tasks live in Bank.
type AI[S any] struct {
	ID        string
	ProjectID string
	Gateway   *gateway.AIGateway

	// Model selection (routed via AIGateway's ModelRouter).
	Model         string
	PlanModel     string
	ArgModel      string
	EscalateModel string
	StageModels   map[AgentStatus]string
	Temperature   *float64
	MaxTokens     int

	Memory *LocalState[S]
	Bank   *MemoryBank // optional managed KV memory, controllable chat history, and task list
	Bus    *Bus        // optional inter-agent message bus (auto-shared across Fork/Spawn)

	Task             string
	Prompt           string // optional Go text/template
	SystemPrefix     string // static prefix for provider KV-caching
	MaxContextTokens int
	ContextPlacement ContextPlacement
	Prompts          PromptConfig

	Tools           []Tool
	ToolInvoker     ToolInvoker
	ToolMiddlewares []ToolMiddleware
	Permissions     []string // inherited top-down without privilege escalation on Fork/Spawn
	Data            []DataSource

	ParentAgent    Agent
	BaseSpawnDepth int
	Children       map[string]Agent
	AllowSubagents *bool // nil defaults to true; set false to forbid dynamic child spawning

	EnableAnalyze   bool     // opt-in DefaultAnalyze stage (disabled by default to avoid wasted tokens)
	EnablePlan      bool     // opt-in DefaultPlan stage (disabled by default to avoid wasted tokens)
	SelectiveGather bool     // opt-in selective DataSource picking by decision model in GatherContext
	Gates           []string // stage names (e.g. "analyze", "plan") where execution pauses for approval
	SkipPlanning    bool     // kept for compatibility; planning is already opt-in unless EnablePlan or Pipeline.Plan is set

	MaxActiveChildren int // semaphore cap (default 50)
	MaxSpawnDepth     int // fork-bomb guard (default 5)
	MaxAttempts       int // guardrail retry limit (default 3)

	Scheduler     Scheduler     // defaults to SequentialScheduler
	FailurePolicy FailurePolicy // defaults to FailureFail

	Pipeline Pipeline[S]
	Hooks    Hooks[S]

	initMu sync.Mutex
}

// AgentID returns the agent's string identifier.
func (a *AI[S]) AgentID() string {
	if a == nil {
		return ""
	}
	return a.ID
}

// EnsureBank returns a.Bank, lazily initializing one if nil.
func (a *AI[S]) EnsureBank() *MemoryBank {
	a.initMu.Lock()
	defer a.initMu.Unlock()
	if a.Bank == nil {
		a.Bank = NewMemoryBank()
	}
	if a.Bus != nil && a.ID != "" {
		a.Bus.Register(a.ID, a.Bank)
	}
	return a.Bank
}

// EnsureBus returns a.Bus, lazily initializing one if nil and registering this agent.
func (a *AI[S]) EnsureBus() *Bus {
	a.initMu.Lock()
	defer a.initMu.Unlock()
	if a.Bus == nil {
		a.Bus = NewBus()
	}
	if a.ID != "" {
		a.Bus.Register(a.ID, a.Bank)
	}
	return a.Bus
}

// Tell delivers an external message/instruction into this agent's Bus mailbox.
func (a *AI[S]) Tell(from, content string, data ...map[string]any) (Envelope, error) {
	if a.ID == "" {
		return Envelope{}, fmt.Errorf("agentgo: cannot Tell agent with empty ID")
	}
	var payload map[string]any
	if len(data) > 0 {
		payload = data[0]
	}
	return a.EnsureBus().Send(Envelope{
		From:    from,
		To:      a.ID,
		Kind:    "tell",
		Content: content,
		Data:    payload,
	})
}

// Config is a declarative configuration struct for constructing an AI[S] via FromConfig
// without repeating type parameter [S] on every option.
type Config[S any] struct {
	ID                string
	ProjectID         string
	Gateway           *gateway.AIGateway
	Model             string
	PlanModel         string
	ArgModel          string
	EscalateModel     string
	StageModels       map[AgentStatus]string
	Temperature       *float64
	MaxTokens         int
	State             S
	Memory            *LocalState[S]
	Bank              *MemoryBank
	Bus               *Bus
	Task              string
	Prompt            string
	SystemPrefix      string
	MaxContextTokens  int
	ContextPlacement  ContextPlacement
	Prompts           PromptConfig
	Tools             []Tool
	ToolInvoker       ToolInvoker
	ToolMiddlewares   []ToolMiddleware
	Permissions       []string
	Data              []DataSource
	Children          map[string]Agent
	AllowSubagents    *bool
	EnableAnalyze     bool
	EnablePlan        bool
	SelectiveGather   bool
	Gates             []string
	SkipPlanning      bool
	MaxActiveChildren int
	MaxSpawnDepth     int
	MaxAttempts       int
	Scheduler         Scheduler
	FailurePolicy     FailurePolicy
	Pipeline          Pipeline[S]
	Hooks             Hooks[S]
}

// FromConfig creates an AI[S] from a Config[S] struct.
func FromConfig[S any](cfg Config[S]) *AI[S] {
	mem := cfg.Memory
	if mem == nil {
		mem = NewLocalState(cfg.State)
	}
	stageModels := cfg.StageModels
	if stageModels == nil {
		stageModels = make(map[AgentStatus]string)
	}
	children := cfg.Children
	if children == nil {
		children = make(map[string]Agent)
	}
	ag := &AI[S]{
		ID:                cfg.ID,
		ProjectID:         cfg.ProjectID,
		Gateway:           cfg.Gateway,
		Model:             cfg.Model,
		PlanModel:         cfg.PlanModel,
		ArgModel:          cfg.ArgModel,
		EscalateModel:     cfg.EscalateModel,
		StageModels:       stageModels,
		Temperature:       cfg.Temperature,
		MaxTokens:         cfg.MaxTokens,
		Memory:            mem,
		Bank:              cfg.Bank,
		Bus:               cfg.Bus,
		Task:              cfg.Task,
		Prompt:            cfg.Prompt,
		SystemPrefix:      cfg.SystemPrefix,
		MaxContextTokens:  cfg.MaxContextTokens,
		ContextPlacement:  cfg.ContextPlacement,
		Prompts:           cfg.Prompts,
		Tools:             append([]Tool(nil), cfg.Tools...),
		ToolInvoker:       cfg.ToolInvoker,
		ToolMiddlewares:   append([]ToolMiddleware(nil), cfg.ToolMiddlewares...),
		Permissions:       append([]string(nil), cfg.Permissions...),
		Data:              append([]DataSource(nil), cfg.Data...),
		Children:          children,
		AllowSubagents:    cfg.AllowSubagents,
		EnableAnalyze:     cfg.EnableAnalyze,
		EnablePlan:        cfg.EnablePlan,
		SelectiveGather:   cfg.SelectiveGather,
		Gates:             append([]string(nil), cfg.Gates...),
		SkipPlanning:      cfg.SkipPlanning,
		MaxActiveChildren: cfg.MaxActiveChildren,
		MaxSpawnDepth:     cfg.MaxSpawnDepth,
		MaxAttempts:       cfg.MaxAttempts,
		Scheduler:         cfg.Scheduler,
		FailurePolicy:     cfg.FailurePolicy,
		Pipeline:          cfg.Pipeline,
		Hooks:             cfg.Hooks,
	}
	if ag.Bus != nil && ag.ID != "" {
		ag.Bus.Register(ag.ID, ag.Bank)
	}
	return ag
}

// Option configures an AI[S] instance in New or Fork.
type Option[S any] func(*AI[S])

// New creates a configured AI[S] agent using functional options.
func New[S any](opts ...Option[S]) *AI[S] {
	var zero S
	a := &AI[S]{
		Memory:      NewLocalState(zero),
		StageModels: make(map[AgentStatus]string),
		Children:    make(map[string]Agent),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	if a.Bus != nil && a.ID != "" {
		a.Bus.Register(a.ID, a.Bank)
	}
	return a
}

// intersectPermissions enforces top-down permission narrowing (Spec §5.1):
// a child agent never receives permissions outside its parent's permission set.
func intersectPermissions(parentPerms, childPerms []string) []string {
	if len(parentPerms) == 0 {
		return append([]string(nil), childPerms...)
	}
	if slices.Contains(parentPerms, "*") {
		if len(childPerms) == 0 {
			return append([]string(nil), parentPerms...)
		}
		return append([]string(nil), childPerms...)
	}
	if len(childPerms) == 0 {
		return append([]string(nil), parentPerms...)
	}
	var allowed []string
	for _, cp := range childPerms {
		if cp == "*" {
			return append([]string(nil), parentPerms...)
		}
		if slices.Contains(parentPerms, cp) {
			allowed = append(allowed, cp)
		}
	}
	if allowed == nil {
		// Return non-nil empty slice so caller knows permissions are restricted to zero.
		return []string{}
	}
	return allowed
}

// Fork creates a child AI[C] inheriting Gateway, ProjectID, Bus, narrowed Permissions,
// Model, ArgModel, SystemPrefix, Temperature, MaxTokens, Tools, ToolInvoker, ToolMiddlewares,
// and incremented BaseSpawnDepth from parent, unless explicitly overridden in opts.
// Each forked child receives its own isolated MemoryBank unless WithBank is explicitly passed.
func Fork[P any, C any](parent *AI[P], opts ...Option[C]) *AI[C] {
	child := New(opts...)
	if parent != nil {
		child.ParentAgent = parent
		child.BaseSpawnDepth = parent.BaseSpawnDepth + 1
		if child.Gateway == nil {
			child.Gateway = parent.Gateway
		}
		if child.ProjectID == "" {
			child.ProjectID = parent.ProjectID
		}
		if child.Bus == nil {
			child.Bus = parent.EnsureBus()
		}
		if child.Bank == nil && parent.Bank != nil {
			child.Bank = NewMemoryBank()
		}
		child.Permissions = intersectPermissions(parent.Permissions, child.Permissions)
		if child.Model == "" {
			child.Model = parent.Model
		}
		if child.ArgModel == "" {
			child.ArgModel = parent.ArgModel
		}
		if child.SystemPrefix == "" {
			child.SystemPrefix = parent.SystemPrefix
		}
		if child.Temperature == nil {
			child.Temperature = parent.Temperature
		}
		if child.MaxTokens == 0 {
			child.MaxTokens = parent.MaxTokens
		}
		if len(child.Tools) == 0 && len(parent.Tools) > 0 {
			child.Tools = append([]Tool(nil), parent.Tools...)
		}
		if child.ToolInvoker == nil {
			child.ToolInvoker = parent.ToolInvoker
		}
		if len(child.ToolMiddlewares) == 0 && len(parent.ToolMiddlewares) > 0 {
			child.ToolMiddlewares = append([]ToolMiddleware(nil), parent.ToolMiddlewares...)
		}
		if child.Bus != nil && child.ID != "" {
			child.Bus.Register(child.ID, child.Bank)
		}
	}
	return child
}

func WithID[S any](id string) Option[S]                { return func(a *AI[S]) { a.ID = id } }
func WithProject[S any](p string) Option[S]            { return func(a *AI[S]) { a.ProjectID = p } }
func WithGateway[S any](g *gateway.AIGateway) Option[S] { return func(a *AI[S]) { a.Gateway = g } }
func WithModel[S any](m string) Option[S]              { return func(a *AI[S]) { a.Model = m } }
func WithTask[S any](t string) Option[S]               { return func(a *AI[S]) { a.Task = t } }
func WithPrompt[S any](p string) Option[S]             { return func(a *AI[S]) { a.Prompt = p } }
func WithSystemPrefix[S any](s string) Option[S]       { return func(a *AI[S]) { a.SystemPrefix = s } }
func WithTools[S any](tools ...Tool) Option[S]         { return func(a *AI[S]) { a.Tools = append(a.Tools, tools...) } }
func WithToolInvoker[S any](inv ToolInvoker) Option[S] {
	return func(a *AI[S]) { a.ToolInvoker = inv }
}
func WithToolMiddleware[S any](mws ...ToolMiddleware) Option[S] {
	return func(a *AI[S]) { a.ToolMiddlewares = append(a.ToolMiddlewares, mws...) }
}
func WithData[S any](ds ...DataSource) Option[S] { return func(a *AI[S]) { a.Data = append(a.Data, ds...) } }
func WithState[S any](s S) Option[S]             { return func(a *AI[S]) { a.Memory = NewLocalState(s) } }
func WithBank[S any](mb *MemoryBank) Option[S]   { return func(a *AI[S]) { a.Bank = mb } }
func WithBus[S any](b *Bus) Option[S]            { return func(a *AI[S]) { a.Bus = b } }
func WithPermissions[S any](perms ...string) Option[S] {
	return func(a *AI[S]) { a.Permissions = append([]string(nil), perms...) }
}
func WithAllowSubagents[S any](allow bool) Option[S] {
	return func(a *AI[S]) { a.AllowSubagents = &allow }
}
// WithWorkspaceTools equips the agent with its own MemoryBank, Bus, and the full
// self-management toolset (memory_*, chat_*, task_*, agent_*).
func WithWorkspaceTools[S any]() Option[S] {
	return func(a *AI[S]) {
		if a.Bank == nil {
			a.Bank = NewMemoryBank()
		}
		if a.Bus == nil {
			a.Bus = NewBus()
		}
		a.Tools = append(a.Tools, WorkspaceTools()...)
	}
}
func WithHooks[S any](h Hooks[S]) Option[S] { return func(a *AI[S]) { a.Hooks = h } }
func WithPipeline[S any](p Pipeline[S]) Option[S] {
	return func(a *AI[S]) { a.Pipeline = p }
}
func WithPrompts[S any](p PromptConfig) Option[S] { return func(a *AI[S]) { a.Prompts = p } }
func WithContextPlacement[S any](cp ContextPlacement) Option[S] {
	return func(a *AI[S]) { a.ContextPlacement = cp }
}
func WithEnableAnalyze[S any](enable bool) Option[S] {
	return func(a *AI[S]) { a.EnableAnalyze = enable }
}
func WithEnablePlan[S any](enable bool) Option[S] {
	return func(a *AI[S]) { a.EnablePlan = enable }
}
func WithSelectiveGather[S any](enable bool) Option[S] {
	return func(a *AI[S]) { a.SelectiveGather = enable }
}
func WithGates[S any](gates ...string) Option[S] {
	return func(a *AI[S]) { a.Gates = append([]string(nil), gates...) }
}
func WithSkipPlanning[S any](skip bool) Option[S] {
	return func(a *AI[S]) { a.SkipPlanning = skip }
}
func WithScheduler[S any](s Scheduler) Option[S] {
	return func(a *AI[S]) { a.Scheduler = s }
}
func WithFailurePolicy[S any](p FailurePolicy) Option[S] {
	return func(a *AI[S]) { a.FailurePolicy = p }
}
func WithStageModel[S any](status AgentStatus, model string) Option[S] {
	return func(a *AI[S]) {
		if a.StageModels == nil {
			a.StageModels = make(map[AgentStatus]string)
		}
		a.StageModels[status] = model
	}
}
func WithLimits[S any](maxActiveChildren, maxSpawnDepth, maxAttempts int) Option[S] {
	return func(a *AI[S]) {
		a.MaxActiveChildren = maxActiveChildren
		a.MaxSpawnDepth = maxSpawnDepth
		a.MaxAttempts = maxAttempts
	}
}
func WithChild[S any](name string, child Agent) Option[S] {
	return func(a *AI[S]) {
		if a.Children == nil {
			a.Children = make(map[string]Agent)
		}
		a.Children[name] = child
	}
}

// ModelFor returns the configured model for a given lifecycle status,
// checking StageModels first and falling back to role-specific or default Model.
func (a *AI[S]) ModelFor(status AgentStatus) string {
	if a.StageModels != nil {
		if m, ok := a.StageModels[status]; ok && m != "" {
			return m
		}
	}
	switch status {
	case StatusPlanning:
		if a.PlanModel != "" {
			return a.PlanModel
		}
	case StatusEscalated:
		if a.EscalateModel != "" {
			return a.EscalateModel
		}
	}
	return a.Model
}

// RunAs runs an Agent and decodes its output into typed value T.
// If the raw output is already of type T, it is returned directly;
// if it is a JSON string/bytes or another struct/map, it is JSON-decoded into T.
func RunAs[T any](ctx context.Context, a Agent) (T, error) {
	var zero T
	if a == nil {
		return zero, fmt.Errorf("agentgo: agent is nil")
	}
	raw, err := a.Run(ctx)
	if err != nil {
		return zero, err
	}
	if typed, ok := raw.(T); ok {
		return typed, nil
	}
	var rawBytes []byte
	switch v := raw.(type) {
	case string:
		rawBytes = []byte(v)
	case []byte:
		rawBytes = v
	case json.RawMessage:
		rawBytes = v
	default:
		b, merr := json.Marshal(v)
		if merr != nil {
			return zero, fmt.Errorf("agentgo: RunAs marshal output: %w", merr)
		}
		rawBytes = b
	}
	var out T
	if err := json.Unmarshal(rawBytes, &out); err != nil {
		return zero, fmt.Errorf("agentgo: RunAs decode into %T: %w", out, err)
	}
	return out, nil
}
