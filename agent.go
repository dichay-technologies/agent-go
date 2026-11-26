package agentgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"proj.dichay.tech/agent-go/gateway"
)

// ErrGatePaused is returned by Run / RunDetailed / RunFromDetailed when execution
// stops at a configured checkpoint Gate awaiting approval (resume via Resume or RunFrom).
var ErrGatePaused = errors.New("agentgo: execution paused at gate")

const (
	defaultMaxActiveChildren = 50
	defaultMaxSpawnDepth     = 5
)

type runSessionKey struct{}
type sessionHandleKey struct{}
type chatTurnKey struct{}
type streamHookKey struct{}

// sessionHandle exposes type-erased session operations to non-generic schedulers and tools.
type sessionHandle interface {
	appendPlanSteps(steps []PlanStep)
	livePlan() []PlanStep
	hasFuture(name string) bool
	awaitFuture(ctx context.Context, name string) (any, bool, error)
}

type spawnContext struct {
	parent      Agent
	depth       int
	gw          *gateway.AIGateway
	projectID   string
	bus         *Bus
	permissions []string
}

type spawnContextKey struct{}

// runSession holds the per-execution mutable state for a single Run / RunFrom call,
// keeping AI[S] immutable and safely reusable across sequential or concurrent runs.
type runSession[S any] struct {
	mu            sync.Mutex
	state         RunState
	activeFutures map[string]*Future
	namedFutures  map[string]*Future
	spawnSem      chan struct{}
	childSeq      atomic.Uint64
	memory        *LocalState[S]
	bank          *MemoryBank
	bus           *Bus
	permissions   []string
	parent        Agent
	gw            *gateway.AIGateway
	projectID     string
	chatTurn      bool
}

func newRunSession[S any](a *AI[S], initial RunState, sc *spawnContext) *runSession[S] {
	capSize := a.MaxActiveChildren
	if capSize <= 0 {
		capSize = defaultMaxActiveChildren
	}
	st := initial.Clone()
	if st.StepResults == nil {
		st.StepResults = make(map[string]any)
	}
	if st.Status == "" {
		st.Status = StatusIdle
	}
	if st.Task == "" {
		st.Task = a.Task
	}

	mem := a.Memory
	if mem == nil {
		var zero S
		mem = NewLocalState(zero)
	}

	parent := a.ParentAgent
	gw := a.Gateway
	projectID := a.ProjectID
	bus := a.Bus
	perms := append([]string(nil), a.Permissions...)

	if sc != nil {
		if parent == nil {
			parent = sc.parent
		}
		if st.SpawnDepth == 0 && sc.depth > 0 {
			st.SpawnDepth = sc.depth
		}
		if gw == nil {
			gw = sc.gw
		}
		if projectID == "" {
			projectID = sc.projectID
		}
		if bus == nil {
			bus = sc.bus
		}
		perms = intersectPermissions(sc.permissions, perms)
	}
	if st.SpawnDepth == 0 && a.BaseSpawnDepth > 0 {
		st.SpawnDepth = a.BaseSpawnDepth
	}
	if bus != nil && a.ID != "" {
		bus.Register(a.ID, a.Bank)
	}

	return &runSession[S]{
		state:         st,
		activeFutures: make(map[string]*Future),
		namedFutures:  make(map[string]*Future),
		spawnSem:      make(chan struct{}, capSize),
		memory:        mem,
		bank:          a.Bank,
		bus:           bus,
		permissions:   perms,
		parent:        parent,
		gw:            gw,
		projectID:     projectID,
	}
}

func (s *runSession[S]) snapshot() RunState {
	s.mu.Lock()
	st := s.state.Clone()
	bus := s.bus
	s.mu.Unlock()
	if bus != nil {
		// Reflect unread inbox messages alongside drained events without mutating the mailbox.
	}
	return st
}

func (s *runSession[S]) setState(status AgentStatus, stepID string, attempt int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Status = status
	s.state.CurrentStepID = stepID
	s.state.Attempt = attempt
}

func (s *runSession[S]) appendAttemptLog(entry AttemptLog) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.History = append(s.state.History, entry)
}

func (s *runSession[S]) appendFailureRecord(rec FailureRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Failures = append(s.state.Failures, rec)
}

func (s *runSession[S]) markStageCompleted(stageName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Contains(s.state.CompletedStages, stageName) {
		s.state.CompletedStages = append(s.state.CompletedStages, stageName)
	}
}

func (s *runSession[S]) isStageCompleted(stageName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Contains(s.state.CompletedStages, stageName)
}

func (s *runSession[S]) attemptHistory() []AttemptLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AttemptLog, len(s.state.History))
	copy(out, s.state.History)
	return out
}

func (s *runSession[S]) snapshotPlan() []PlanStep {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.state.Plan) == 0 {
		return nil
	}
	out := make([]PlanStep, len(s.state.Plan))
	for i, p := range s.state.Plan {
		cp := p
		if len(p.DependsOn) > 0 {
			cp.DependsOn = append([]string(nil), p.DependsOn...)
		}
		out[i] = cp
	}
	return out
}

func (s *runSession[S]) livePlan() []PlanStep {
	return s.snapshotPlan()
}

func (s *runSession[S]) appendPlanSteps(steps []PlanStep) {
	if len(steps) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := make(map[string]bool, len(s.state.Plan))
	for _, p := range s.state.Plan {
		existing[p.ID] = true
	}
	for _, st := range steps {
		if st.ID == "" || existing[st.ID] {
			continue
		}
		existing[st.ID] = true
		cp := st
		if len(st.DependsOn) > 0 {
			cp.DependsOn = append([]string(nil), st.DependsOn...)
		}
		s.state.Plan = append(s.state.Plan, cp)
	}
}

func (s *runSession[S]) hasFuture(name string) bool {
	if name == "" {
		return false
	}
	s.mu.Lock()
	_, ok := s.namedFutures[name]
	s.mu.Unlock()
	return ok
}

func (s *runSession[S]) awaitFuture(ctx context.Context, name string) (any, bool, error) {
	s.mu.Lock()
	fut, ok := s.namedFutures[name]
	if ok {
		select {
		case <-fut.Done():
		default:
			s.state.Status = StatusWaitingDeps
		}
	}
	s.mu.Unlock()
	if !ok {
		return nil, false, nil
	}
	val, err := fut.Await(ctx)
	return val, true, err
}

func (s *runSession[S]) snapshotStepResults() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.state.StepResults) == 0 {
		return nil
	}
	out := make(map[string]any, len(s.state.StepResults))
	for k, v := range s.state.StepResults {
		out[k] = v
	}
	return out
}

func (s *runSession[S]) setStepResult(stepID string, val any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.StepResults == nil {
		s.state.StepResults = make(map[string]any)
	}
	s.state.StepResults[stepID] = val
}

func (s *runSession[S]) task() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Task
}

// SetPlan sets the execution plan for the current run represented by ctx.
func SetPlan[S any](ctx Context, a *AI[S], steps []PlanStep) {
	sess := sessionFromContext(ctx, a)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.state.Plan = append([]PlanStep(nil), steps...)
}

// AppendPlanSteps dynamically appends new PlanSteps to the active run's plan DAG.
// Any step whose ID already exists in the plan is ignored.
func AppendPlanSteps(ctx context.Context, steps ...PlanStep) {
	if ctx == nil || len(steps) == 0 {
		return
	}
	if h, ok := ctx.Value(sessionHandleKey{}).(sessionHandle); ok && h != nil {
		h.appendPlanSteps(steps)
	}
}

func livePlanFromContext(ctx context.Context) []PlanStep {
	if ctx == nil {
		return nil
	}
	if h, ok := ctx.Value(sessionHandleKey{}).(sessionHandle); ok && h != nil {
		return h.livePlan()
	}
	return nil
}

func hasNamedFuture(ctx context.Context, name string) bool {
	if ctx == nil || name == "" {
		return false
	}
	if h, ok := ctx.Value(sessionHandleKey{}).(sessionHandle); ok && h != nil {
		return h.hasFuture(name)
	}
	return false
}

func awaitNamedFuture(ctx context.Context, name string) (any, bool, error) {
	if ctx == nil || name == "" {
		return nil, false, nil
	}
	if h, ok := ctx.Value(sessionHandleKey{}).(sessionHandle); ok && h != nil {
		return h.awaitFuture(ctx, name)
	}
	return nil, false, nil
}

func sessionFromContext[S any](ctx Context, a *AI[S]) *runSession[S] {
	if ctx != nil {
		if s, ok := ctx.Value(runSessionKey{}).(*runSession[S]); ok && s != nil {
			return s
		}
	}
	return newRunSession(a, RunState{}, nil)
}

func (a *AI[S]) memoryFor(sess *runSession[S]) *LocalState[S] {
	if sess != nil && sess.memory != nil {
		return sess.memory
	}
	if a.Memory != nil {
		return a.Memory
	}
	var zero S
	return NewLocalState(zero)
}

// Parent returns the parent Agent if this agent was forked from another agent, or nil.
func (a *AI[S]) Parent() Agent {
	return a.ParentAgent
}

// Run executes the agent's FSM lifecycle (or custom Pipeline) with a fresh RunState
// and returns the final output. AI[S] is not mutated and can be safely reused.
func (a *AI[S]) Run(parentCtx context.Context) (any, error) {
	res, err := a.RunFromDetailed(parentCtx, RunState{})
	return res.Output, err
}

// RunDetailed executes the agent with a fresh RunState and returns both Output and the final RunState snapshot.
func (a *AI[S]) RunDetailed(parentCtx context.Context) (RunResult, error) {
	return a.RunFromDetailed(parentCtx, RunState{})
}

// RunTask executes the agent on a specific task string without mutating a.Task,
// keeping AI[S] safe for concurrent reuse across different tasks.
func (a *AI[S]) RunTask(parentCtx context.Context, task string) (any, error) {
	res, err := a.RunFromDetailed(parentCtx, RunState{Task: task})
	return res.Output, err
}

// Resume approves a paused gate in pausedState and continues execution from the next stage.
func (a *AI[S]) Resume(parentCtx context.Context, pausedState RunState) (RunResult, error) {
	next := pausedState.Clone()
	next.PausedAtGate = ""
	if next.Status == StatusPaused {
		next.Status = StatusIdle
	}
	return a.RunFromDetailed(parentCtx, next)
}

// Chat executes a stateful conversational turn using the agent's MemoryBank:
// it appends userMessage to the bank's chat buffer, runs the agent with full
// conversation history + stored facts/tasks + inbox events, appends the assistant's
// response to the chat buffer, and returns the text reply.
func (a *AI[S]) Chat(parentCtx context.Context, userMessage string) (string, error) {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	bank := a.EnsureBank()
	bank.AppendChat(gateway.MsgUser, userMessage)

	turnCtx := context.WithValue(parentCtx, chatTurnKey{}, true)
	out, err := a.RunFrom(turnCtx, RunState{Task: userMessage})
	if err != nil {
		return "", err
	}
	reply := fmt.Sprint(out)
	bank.AppendChat(gateway.MsgAssistant, reply)
	return reply, nil
}

// StreamChat executes a stateful conversational turn like Chat while streaming incremental
// text deltas via iter.Seq2 as they arrive from the provider (including across tool-calling loops).
func (a *AI[S]) StreamChat(parentCtx context.Context, userMessage string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithCancel(parentCtx)
		defer cancel()

		type item struct {
			text string
			err  error
			done bool
		}
		ch := make(chan item, 32)

		streamFn := func(chunk gateway.StreamChunk) {
			if chunk.Text == "" {
				return
			}
			select {
			case ch <- item{text: chunk.Text}:
			case <-ctx.Done():
			}
		}

		go func() {
			sctx := context.WithValue(ctx, streamHookKey{}, streamFn)
			reply, err := a.Chat(sctx, userMessage)
			ch <- item{text: reply, err: err, done: true}
			close(ch)
		}()

		emittedAny := false
		for it := range ch {
			if it.done {
				if it.err != nil {
					yield("", it.err)
					return
				}
				if !emittedAny && it.text != "" {
					yield(it.text, nil)
				}
				return
			}
			emittedAny = true
			if !yield(it.text, nil) {
				cancel()
				return
			}
		}
	}
}

// RunFrom resumes or starts an agent execution from an existing RunState snapshot.
func (a *AI[S]) RunFrom(parentCtx context.Context, initial RunState) (any, error) {
	res, err := a.RunFromDetailed(parentCtx, initial)
	return res.Output, err
}

// RunFromDetailed resumes or starts an agent execution from initial and returns
// the full RunResult (including the updated RunState snapshot even on gate pause or error).
func (a *AI[S]) RunFromDetailed(parentCtx context.Context, initial RunState) (RunResult, error) {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	var sc *spawnContext
	if v, ok := parentCtx.Value(spawnContextKey{}).(*spawnContext); ok {
		sc = v
	}
	sess := newRunSession(a, initial, sc)
	if v, _ := parentCtx.Value(chatTurnKey{}).(bool); v {
		sess.chatTurn = true
	}

	ctxWithSess := context.WithValue(parentCtx, runSessionKey{}, sess)
	ctxWithSess = context.WithValue(ctxWithSess, sessionHandleKey{}, sessionHandle(sess))

	makeCfg := func() ContextConfig {
		return ContextConfig{
			ProjectID:    sess.projectID,
			DefaultModel: a.Model,
			Gateway:      sess.gw,
			Agent:        a,
			Children:     a.Children,
			Bus:          sess.bus,
			Bank:         sess.bank,
			Permissions:  sess.permissions,
			RunStateFn:   sess.snapshot,
			SpawnFn: func(c context.Context, child Agent) (*Future, error) {
				ac, ok := ContextFrom(c)
				if !ok {
					ac = NewContext(c, ContextConfig{
						ProjectID:    sess.projectID,
						DefaultModel: a.Model,
						Gateway:      sess.gw,
						Agent:        a,
						Children:     a.Children,
						Bus:          sess.bus,
						Bank:         sess.bank,
						Permissions:  sess.permissions,
						RunStateFn:   sess.snapshot,
					})
				}
				return a.spawnWithSession(ac, sess, "", child)
			},
			SpawnNamedFn: func(c context.Context, name string, child Agent) (*Future, error) {
				ac, ok := ContextFrom(c)
				if !ok {
					ac = NewContext(c, ContextConfig{
						ProjectID:    sess.projectID,
						DefaultModel: a.Model,
						Gateway:      sess.gw,
						Agent:        a,
						Children:     a.Children,
						Bus:          sess.bus,
						Bank:         sess.bank,
						Permissions:  sess.permissions,
						RunStateFn:   sess.snapshot,
					})
				}
				return a.spawnWithSession(ac, sess, name, child)
			},
		}
	}
	ctx := NewContext(ctxWithSess, makeCfg())

	stages := a.resolveStages()
	for _, st := range stages {
		if sess.isStageCompleted(st.Name) {
			continue
		}
		sess.setState(st.Status, st.Name, 0)
		if err := st.Run(ctx, a); err != nil {
			a.failSession(ctx, sess, err)
			return RunResult{State: sess.snapshot()}, err
		}
		sess.markStageCompleted(st.Name)

		if slices.Contains(a.Gates, st.Name) {
			proceed := false
			if a.Hooks.OnGate != nil {
				ok, gerr := a.Hooks.OnGate(ctx, a.memoryFor(sess), st.Name)
				if gerr != nil {
					a.failSession(ctx, sess, gerr)
					return RunResult{State: sess.snapshot()}, gerr
				}
				proceed = ok
			}
			if !proceed {
				sess.mu.Lock()
				sess.state.Status = StatusPaused
				sess.state.PausedAtGate = st.Name
				sess.mu.Unlock()
				return RunResult{
					State:  sess.snapshot(),
					Paused: true,
					Gate:   st.Name,
				}, fmt.Errorf("%w: %s", ErrGatePaused, st.Name)
			}
		}
	}

	sess.setState(StatusDone, "done", 0)
	snap := sess.snapshot()
	return RunResult{Output: snap.Output, State: snap}, nil
}

func (a *AI[S]) resolveStages() []Stage[S] {
	if len(a.Pipeline.Custom) > 0 {
		return a.Pipeline.Custom
	}

	var stages []Stage[S]

	if a.Pipeline.Analyze != nil {
		stages = append(stages, Stage[S]{Status: StatusAnalyzing, Name: "analyze", Run: a.Pipeline.Analyze})
	} else if a.EnableAnalyze && !a.SkipPlanning {
		stages = append(stages, Stage[S]{Status: StatusAnalyzing, Name: "analyze", Run: DefaultAnalyze[S]})
	}

	if a.Pipeline.GatherContext != nil {
		stages = append(stages, Stage[S]{Status: StatusGatheringContext, Name: "gather", Run: a.Pipeline.GatherContext})
	} else if !a.SkipPlanning {
		stages = append(stages, Stage[S]{Status: StatusGatheringContext, Name: "gather", Run: DefaultGatherContext[S]})
	}

	if a.Pipeline.Plan != nil {
		stages = append(stages, Stage[S]{Status: StatusPlanning, Name: "plan", Run: a.Pipeline.Plan})
	} else if a.EnablePlan && !a.SkipPlanning {
		stages = append(stages, Stage[S]{Status: StatusPlanning, Name: "plan", Run: DefaultPlan[S]})
	}

	exec := a.Pipeline.Execute
	if exec == nil {
		exec = DefaultExecute[S]
	}
	wait := a.Pipeline.WaitDeps
	if wait == nil {
		wait = DefaultWaitDeps[S]
	}

	stages = append(stages,
		Stage[S]{Status: StatusExecuting, Name: "execute", Run: exec},
		Stage[S]{Status: StatusWaitingDeps, Name: "wait_deps", Run: wait},
	)
	return stages
}

// DefaultAnalyze is the built-in StatusAnalyzing stage.
// It stores the analysis text in RunState.Analysis (skipping if already populated on resume).
func DefaultAnalyze[S any](ctx Context, a *AI[S]) error {
	sess := sessionFromContext(ctx, a)
	sess.mu.Lock()
	alreadyAnalyzed := sess.state.Analysis != ""
	taskText := sess.state.Task
	sess.mu.Unlock()
	if alreadyAnalyzed || sess.gw == nil || taskText == "" {
		return nil
	}
	tmpl := a.Prompts.Analyze
	if tmpl == "" {
		tmpl = defaultAnalyzePrompt
	}
	msg, err := RenderPrompt(tmpl, map[string]any{"Task": taskText, "State": a.memoryFor(sess).Get()})
	if err != nil {
		return err
	}
	resp, err := ctx.Call(gateway.Request{
		Model:        a.ModelFor(StatusAnalyzing),
		SystemPrefix: a.SystemPrefix,
		Temperature:  a.Temperature,
		MaxTokens:    a.MaxTokens,
		Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: msg}},
	})
	if err != nil {
		return err
	}
	sess.mu.Lock()
	sess.state.Analysis = resp.Text
	sess.mu.Unlock()
	return nil
}

// DefaultGatherContext is the built-in StatusGatheringContext stage.
// When a.SelectiveGather is true and multiple DataSources exist, the gathering model
// selects only the relevant DataSource names before loading their contents (Spec §2.2).
func DefaultGatherContext[S any](ctx Context, a *AI[S]) error {
	sess := sessionFromContext(ctx, a)
	sess.mu.Lock()
	alreadyGathered := sess.state.ContextData != ""
	taskText := sess.state.Task
	sess.mu.Unlock()
	if alreadyGathered || len(a.Data) == 0 {
		return nil
	}

	sources := a.Data
	if a.SelectiveGather && len(sources) > 1 && sess.gw != nil && taskText != "" {
		names := make([]any, len(sources))
		nameList := make([]string, len(sources))
		for i, ds := range sources {
			names[i] = ds.Name()
			nameList[i] = ds.Name()
		}
		selResp, err := ctx.Call(gateway.Request{
			Model: a.ModelFor(StatusGatheringContext),
			Messages: []gateway.Message{{
				Role:    gateway.MsgUser,
				Content: fmt.Sprintf("Select only the data sources needed for task: %s\nAvailable: %s", taskText, strings.Join(nameList, ", ")),
			}},
			ResponseSchema: &gateway.JSONSchema{
				Type:     "object",
				Required: []string{"sources"},
				Properties: map[string]*gateway.JSONSchema{
					"sources": {
						Type:  "array",
						Items: &gateway.JSONSchema{Type: "string", Enum: names},
					},
				},
			},
		})
		if err == nil {
			var picked struct {
				Sources []string `json:"sources"`
			}
			if json.Unmarshal([]byte(stripCodeFences(selResp.Text)), &picked) == nil {
				allow := make(map[string]bool, len(picked.Sources))
				for _, n := range picked.Sources {
					allow[n] = true
				}
				filtered := make([]DataSource, 0, len(picked.Sources))
				for _, ds := range sources {
					if allow[ds.Name()] {
						filtered = append(filtered, ds)
					}
				}
				sources = filtered
			}
		}
	}

	var gathered []string
	for _, ds := range sources {
		content, err := ds.Content(ctx)
		if err != nil {
			return err
		}
		gathered = append(gathered, fmt.Sprintf("[%s]\n%s", ds.Name(), content))
	}
	dynamicCtx := strings.Join(gathered, "\n\n")
	if err := ValidateWindow(a.SystemPrefix, dynamicCtx, a.MaxContextTokens); err != nil {
		return err
	}
	sess.mu.Lock()
	sess.state.ContextData = dynamicCtx
	sess.mu.Unlock()
	return nil
}

// DefaultPlan is the built-in StatusPlanning stage.
func DefaultPlan[S any](ctx Context, a *AI[S]) error {
	sess := sessionFromContext(ctx, a)
	sess.mu.Lock()
	dynamicCtx := sess.state.ContextData
	analysis := sess.state.Analysis
	hasPlan := len(sess.state.Plan) > 0
	taskText := sess.state.Task
	sess.mu.Unlock()

	mem := a.memoryFor(sess)
	planPrompt := taskText
	if a.Prompt != "" {
		rendered, err := RenderPrompt(a.Prompt, map[string]any{
			"Task":     taskText,
			"Context":  dynamicCtx,
			"Analysis": analysis,
			"State":    mem.Get(),
		})
		if err != nil {
			return err
		}
		if rendered != "" {
			planPrompt = rendered
		}
	}

	if sess.gw != nil && !hasPlan && planPrompt != "" {
		tmpl := a.Prompts.Plan
		if tmpl == "" {
			tmpl = defaultPlanPrompt
		}
		msg, err := RenderPrompt(tmpl, map[string]any{
			"Task":     planPrompt,
			"Context":  dynamicCtx,
			"Analysis": analysis,
			"State":    mem.Get(),
		})
		if err != nil {
			return err
		}
		if planResp, err := ctx.Call(gateway.Request{
			Model:        a.ModelFor(StatusPlanning),
			SystemPrefix: a.SystemPrefix,
			Temperature:  a.Temperature,
			MaxTokens:    a.MaxTokens,
			Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: msg}},
		}); err == nil {
			var parsed struct {
				Steps []PlanStep `json:"steps"`
			}
			rawJSON := stripCodeFences(planResp.Text)
			if json.Unmarshal([]byte(rawJSON), &parsed) == nil && len(parsed.Steps) > 0 {
				sess.mu.Lock()
				sess.state.Plan = parsed.Steps
				sess.mu.Unlock()
			}
		}
	}
	return nil
}

func stripCodeFences(s string) string {
	trimmed := strings.TrimSpace(s)
	if strings.HasPrefix(trimmed, "```") {
		if nl := strings.IndexByte(trimmed, '\n'); nl >= 0 {
			trimmed = trimmed[nl+1:]
		}
		if end := strings.LastIndex(trimmed, "```"); end >= 0 {
			trimmed = trimmed[:end]
		}
	}
	return strings.TrimSpace(trimmed)
}

// DefaultExecute is the built-in StatusExecuting stage.
func DefaultExecute[S any](ctx Context, a *AI[S]) error {
	sess := sessionFromContext(ctx, a)
	invoker := ChainToolMiddlewares(a.ToolInvoker, a.ToolMiddlewares...)

	mem := a.memoryFor(sess)
	taskText := sess.task()
	defaultPrompt := taskText
	if a.Prompt != "" {
		if rendered, err := RenderPrompt(a.Prompt, map[string]any{
			"Task":  taskText,
			"State": mem.Get(),
		}); err == nil && rendered != "" {
			defaultPrompt = rendered
		}
	}

	steps := sess.snapshotPlan()
	if len(steps) == 0 {
		steps = []PlanStep{{ID: "step-1", Task: defaultPrompt}}
	}

	sched := a.Scheduler
	if sched == nil {
		sched = SequentialScheduler{}
	}

	precomputed := sess.snapshotStepResults()
	_, out, err := sched.Schedule(ctx, steps, precomputed, func(stepCtx Context, step PlanStep, depOutputs map[string]any) (any, error) {
		stepPrompt := step.Task
		if len(depOutputs) > 0 {
			var depContext []string
			for _, dep := range step.DependsOn {
				if val, ok := depOutputs[dep]; ok {
					depContext = append(depContext, fmt.Sprintf("%s: %v", dep, val))
				}
			}
			if len(depContext) > 0 {
				stepPrompt += "\n\nCompleted dependencies:\n" + strings.Join(depContext, "\n")
			}
		}
		res, stepErr := a.runStep(stepCtx, sess, invoker, step.ID, stepPrompt)
		if stepErr == nil {
			sess.setStepResult(step.ID, res)
		}
		return res, stepErr
	})
	if err != nil {
		return err
	}

	sess.mu.Lock()
	sess.state.Output = out
	sess.mu.Unlock()
	return nil
}

// DefaultWaitDeps is the built-in StatusWaitingDeps stage.
func DefaultWaitDeps[S any](ctx Context, a *AI[S]) error {
	return a.WaitDeps(ctx)
}

func (a *AI[S]) runStep(ctx Context, sess *runSession[S], invoker ToolInvoker, stepID, promptText string) (any, error) {
	sess.mu.Lock()
	dynamicCtx := sess.state.ContextData
	analysis := sess.state.Analysis
	taskText := sess.state.Task
	chatTurn := sess.chatTurn
	bank := sess.bank
	bus := sess.bus
	sess.mu.Unlock()

	sysPrefix := a.SystemPrefix
	var msgs []gateway.Message

	if dynamicCtx != "" {
		if a.ContextPlacement == ContextInSystemPrefix {
			if sysPrefix != "" {
				sysPrefix += "\n\n" + dynamicCtx
			} else {
				sysPrefix = dynamicCtx
			}
		} else {
			msgs = append(msgs, gateway.Message{
				Role:    gateway.MsgUser,
				Content: "Context:\n" + dynamicCtx,
			})
		}
	}

	if bank != nil {
		if bankCtx, err := bank.Content(ctx); err == nil && bankCtx != "" {
			msgs = append(msgs, gateway.Message{
				Role:    gateway.MsgUser,
				Content: "Workspace:\n" + bankCtx,
			})
		}
	}

	if bus != nil && a.ID != "" {
		if incoming := bus.Drain(a.ID); len(incoming) > 0 {
			sess.mu.Lock()
			sess.state.PendingEvents = append(sess.state.PendingEvents, incoming...)
			sess.mu.Unlock()

			var evLines []string
			for _, ev := range incoming {
				sender := ev.From
				if sender == "" {
					sender = "external"
				}
				evLines = append(evLines, fmt.Sprintf("- from %s (%s): %s", sender, ev.Kind, ev.Content))
			}
			msgs = append(msgs, gateway.Message{
				Role:    gateway.MsgUser,
				Content: "Incoming events:\n" + strings.Join(evLines, "\n"),
			})
		}
	}

	if analysis != "" {
		msgs = append(msgs, gateway.Message{
			Role:    gateway.MsgUser,
			Content: "Analysis:\n" + analysis,
		})
	}

	appendPrompt := true
	if bank != nil {
		chatHist := bank.ChatMessages()
		if len(chatHist) > 0 {
			msgs = append(msgs, chatHist...)
			if chatTurn && promptText == taskText && chatHist[len(chatHist)-1].Role == gateway.MsgUser && chatHist[len(chatHist)-1].Content == promptText {
				appendPrompt = false
			}
		}
	}
	if appendPrompt {
		msgs = append(msgs, gateway.Message{Role: gateway.MsgUser, Content: promptText})
	}

	var streamCb func(gateway.StreamChunk)
	if cb, ok := ctx.Value(streamHookKey{}).(func(gateway.StreamChunk)); ok && cb != nil {
		streamCb = cb
	}
	if a.Hooks.OnStream != nil {
		prev := streamCb
		streamCb = func(ch gateway.StreamChunk) {
			a.Hooks.OnStream(ctx, ch)
			if prev != nil {
				prev(ch)
			}
		}
	}

	mem := a.memoryFor(sess)
	return RunWithGuardrail(ctx, a, stepID, func(attempt int, lastErr error) (any, error) {
		if len(a.Tools) > 0 {
			res, err := invoker.Invoke(ctx, ToolCallRequest{
				ProjectID:    sess.projectID,
				Model:        a.ModelFor(StatusExecuting),
				ArgModel:     a.ArgModel,
				SystemPrefix: sysPrefix,
				Messages:     msgs,
				Tools:        a.Tools,
				Attempt:      attempt,
				LastError:    lastErr,
				OnStream:     streamCb,
			})
			if err != nil {
				return nil, err
			}
			if a.Hooks.OnStep != nil {
				a.Hooks.OnStep(ctx, mem, stepID, res)
			}
			if res.Selected {
				return res.Output, nil
			}
			return res.Text, nil
		}

		if streamCb != nil {
			var textBuf strings.Builder
			for ch, serr := range ctx.Stream(gateway.Request{
				Model:        a.ModelFor(StatusExecuting),
				SystemPrefix: sysPrefix,
				Temperature:  a.Temperature,
				MaxTokens:    a.MaxTokens,
				Messages:     msgs,
			}) {
				if serr != nil {
					return nil, serr
				}
				if ch.Text != "" {
					textBuf.WriteString(ch.Text)
					streamCb(ch)
				}
			}
			finalText := textBuf.String()
			if a.Hooks.OnStep != nil {
				a.Hooks.OnStep(ctx, mem, stepID, ToolCallResult{Text: finalText})
			}
			return finalText, nil
		}

		resp, err := ctx.Call(gateway.Request{
			Model:        a.ModelFor(StatusExecuting),
			SystemPrefix: sysPrefix,
			Temperature:  a.Temperature,
			MaxTokens:    a.MaxTokens,
			Messages:     msgs,
		})
		if err != nil {
			return nil, err
		}
		if a.Hooks.OnStep != nil {
			a.Hooks.OnStep(ctx, mem, stepID, ToolCallResult{Text: resp.Text})
		}
		return resp.Text, nil
	})
}

// SpawnAsync launches a child Agent in a separate goroutine, enforcing
// AllowSubagents, MaxActiveChildren semaphore, and MaxSpawnDepth fork-bomb protection.
func (a *AI[S]) SpawnAsync(ctx Context, name string, child Agent) (*Future, error) {
	sess := sessionFromContext(ctx, a)
	return a.spawnWithSession(ctx, sess, name, child)
}

func (a *AI[S]) spawnWithSession(ctx Context, sess *runSession[S], name string, child Agent) (*Future, error) {
	if child == nil {
		return nil, fmt.Errorf("agent: child is nil")
	}
	if a.AllowSubagents != nil && !*a.AllowSubagents {
		return nil, fmt.Errorf("agent: spawning subagents is disabled for %q", a.ID)
	}

	maxDepth := a.MaxSpawnDepth
	if maxDepth <= 0 {
		maxDepth = defaultMaxSpawnDepth
	}
	sess.mu.Lock()
	curDepth := sess.state.SpawnDepth
	sess.mu.Unlock()
	if a.BaseSpawnDepth > curDepth {
		curDepth = a.BaseSpawnDepth
	}
	if curDepth >= maxDepth {
		return nil, fmt.Errorf("agent: max spawn depth %d reached", maxDepth)
	}

	select {
	case sess.spawnSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	fut := &Future{
		Name: name,
		done: make(chan struct{}),
	}

	sess.mu.Lock()
	if sess.activeFutures == nil {
		sess.activeFutures = make(map[string]*Future)
	}
	if sess.namedFutures == nil {
		sess.namedFutures = make(map[string]*Future)
	}
	key := name
	if key == "" {
		key = fmt.Sprintf("child_%d", sess.childSeq.Add(1))
	} else {
		sess.namedFutures[name] = fut
	}
	sess.activeFutures[key] = fut
	sess.mu.Unlock()

	childCtx := context.WithValue(ctx, spawnContextKey{}, &spawnContext{
		parent:      a,
		depth:       curDepth + 1,
		gw:          sess.gw,
		projectID:   sess.projectID,
		bus:         sess.bus,
		permissions: sess.permissions,
	})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fut.val = nil
				fut.err = fmt.Errorf("agent: panic in child %q: %v", name, r)
			}
			sess.mu.Lock()
			delete(sess.activeFutures, key)
			sess.mu.Unlock()
			<-sess.spawnSem
			close(fut.done)
		}()
		fut.val, fut.err = child.Run(childCtx)
	}()

	return fut, nil
}

// WaitDeps transitions to StatusWaitingDeps and awaits all currently active Futures.
func (a *AI[S]) WaitDeps(ctx Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sess := sessionFromContext(ctx, a)
	sess.mu.Lock()
	if len(sess.activeFutures) == 0 {
		sess.mu.Unlock()
		return nil
	}
	futures := make([]*Future, 0, len(sess.activeFutures))
	for _, f := range sess.activeFutures {
		futures = append(futures, f)
	}
	sess.state.Status = StatusWaitingDeps
	sess.mu.Unlock()

	for _, f := range futures {
		if _, err := f.Await(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (a *AI[S]) failSession(ctx Context, sess *runSession[S], err error) {
	sess.mu.Lock()
	sess.state.Status = StatusFailed
	sess.mu.Unlock()
	if a.Hooks.OnFail != nil {
		a.Hooks.OnFail(ctx, a.memoryFor(sess), err)
	}
}
