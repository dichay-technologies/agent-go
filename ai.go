package agentgo

import (
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
	StatusDone             AgentStatus = "done"
	StatusFailed           AgentStatus = "failed"
)

// PlanStep represents a node in the agent's execution plan DAG.
type PlanStep struct {
	ID        string   `json:"id"`
	Task      string   `json:"task"`
	DependsOn []string `json:"depends_on,omitempty"`
}

// AgentState holds the mutable execution state of an AI[S] instance.
type AgentState struct {
	Status        AgentStatus
	CurrentStepID string
	Attempt       int
	ActiveFutures map[string]*Future
	SpawnDepth    int
	History       []AttemptLog
	Plan          []PlanStep
	ContextData   string
	Output        any
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
	OnVerify   func(ctx Context, state *LocalState[S], output any) error
	OnEscalate func(ctx Context, state *LocalState[S], history []AttemptLog) error
	OnFail     func(ctx Context, state *LocalState[S], err error)
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
	defaultPlanPrompt     = "Create an execution plan as JSON {\"steps\":[{\"id\":\"...\",\"task\":\"...\",\"depends_on\":[]}]} for:\n{{.Task}}\n\nContext:\n{{.Context}}"
	defaultEscalatePrompt = "Step {{.StepID}} failed {{.Attempts}} times. History:\n{{.History}}\nProvide a final recovery response or explain why it cannot proceed."
)

// AI is the core generic agent instance parameterized by application state S.
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

	State  AgentState
	Memory *LocalState[S]

	Task             string
	Prompt           string // optional Go text/template
	SystemPrefix     string // static prefix for provider KV-caching
	MaxContextTokens int
	Prompts          PromptConfig

	Tools       []Tool
	ToolInvoker ToolInvoker
	Data        []DataSource

	ParentAgent  Agent
	Children     map[string]Agent
	SkipPlanning bool // skip phases 1-3 and start directly at StatusExecuting

	MaxActiveChildren int // semaphore cap (default 50)
	MaxSpawnDepth     int // fork-bomb guard (default 5)
	MaxAttempts       int // guardrail retry limit (default 3)

	Pipeline Pipeline[S]
	Hooks    Hooks[S]

	mu       sync.Mutex
	spawnSem chan struct{}
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
	a.initRuntime()
	return a
}

// Fork creates a child AI[C] inheriting Gateway, ProjectID, Model, SystemPrefix,
// Temperature, MaxTokens, and incremented SpawnDepth from parent.
func Fork[P any, C any](parent *AI[P], opts ...Option[C]) *AI[C] {
	child := New(opts...)
	if parent != nil {
		child.ParentAgent = parent
		parent.mu.Lock()
		child.State.SpawnDepth = parent.State.SpawnDepth + 1
		parent.mu.Unlock()
		if child.Gateway == nil {
			child.Gateway = parent.Gateway
		}
		if child.ProjectID == "" {
			child.ProjectID = parent.ProjectID
		}
		if child.Model == "" {
			child.Model = parent.Model
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
	}
	return child
}

func WithID[S any](id string) Option[S]          { return func(a *AI[S]) { a.ID = id } }
func WithProject[S any](p string) Option[S]      { return func(a *AI[S]) { a.ProjectID = p } }
func WithGateway[S any](g *gateway.AIGateway) Option[S] { return func(a *AI[S]) { a.Gateway = g } }
func WithModel[S any](m string) Option[S]        { return func(a *AI[S]) { a.Model = m } }
func WithTask[S any](t string) Option[S]         { return func(a *AI[S]) { a.Task = t } }
func WithPrompt[S any](p string) Option[S]       { return func(a *AI[S]) { a.Prompt = p } }
func WithSystemPrefix[S any](s string) Option[S] { return func(a *AI[S]) { a.SystemPrefix = s } }
func WithTools[S any](tools ...Tool) Option[S]   { return func(a *AI[S]) { a.Tools = append(a.Tools, tools...) } }
func WithToolInvoker[S any](inv ToolInvoker) Option[S] {
	return func(a *AI[S]) { a.ToolInvoker = inv }
}
func WithData[S any](ds ...DataSource) Option[S] { return func(a *AI[S]) { a.Data = append(a.Data, ds...) } }
func WithState[S any](s S) Option[S]             { return func(a *AI[S]) { a.Memory = NewLocalState(s) } }
func WithHooks[S any](h Hooks[S]) Option[S]      { return func(a *AI[S]) { a.Hooks = h } }
func WithPipeline[S any](p Pipeline[S]) Option[S] {
	return func(a *AI[S]) { a.Pipeline = p }
}
func WithPrompts[S any](p PromptConfig) Option[S] { return func(a *AI[S]) { a.Prompts = p } }
func WithSkipPlanning[S any](skip bool) Option[S] {
	return func(a *AI[S]) { a.SkipPlanning = skip }
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
