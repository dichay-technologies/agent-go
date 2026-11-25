package agentgo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"proj.dichay.tech/agent-go/gateway"
)

const (
	defaultMaxActiveChildren = 50
	defaultMaxSpawnDepth     = 5
)

type childConfigurer interface {
	attachParent(parent Agent, depth int, gw *gateway.AIGateway, projectID string)
}

func (a *AI[S]) attachParent(parent Agent, depth int, gw *gateway.AIGateway, projectID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ParentAgent == nil {
		a.ParentAgent = parent
	}
	if a.State.SpawnDepth == 0 && depth > 0 {
		a.State.SpawnDepth = depth
	}
	if a.Gateway == nil {
		a.Gateway = gw
	}
	if a.ProjectID == "" {
		a.ProjectID = projectID
	}
}

// Parent returns the parent Agent if this agent was spawned by another agent, or nil.
func (a *AI[S]) Parent() Agent {
	return a.ParentAgent
}

// Run executes the agent's FSM lifecycle (or custom Pipeline) and returns the final output.
func (a *AI[S]) Run(parentCtx context.Context) (any, error) {
	if a.Memory == nil {
		var zero S
		a.Memory = NewLocalState(zero)
	}
	a.initRuntime()

	ctx := NewContext(parentCtx, ContextConfig{
		ProjectID:    a.ProjectID,
		DefaultModel: a.Model,
		Gateway:      a.Gateway,
		Agent:        a,
		Children:     a.Children,
		SpawnFn: func(c context.Context, child Agent) (*Future, error) {
			ac, ok := ContextFrom(c)
			if !ok {
				ac = NewContext(c, ContextConfig{
					ProjectID:    a.ProjectID,
					DefaultModel: a.Model,
					Gateway:      a.Gateway,
					Agent:        a,
					Children:     a.Children,
				})
			}
			return a.SpawnAsync(ac, "", child)
		},
	})

	stages := a.resolveStages()
	for _, st := range stages {
		a.setState(st.Status, st.Name, 0)
		if err := st.Run(ctx, a); err != nil {
			a.fail(ctx, err)
			return nil, err
		}
	}

	a.setState(StatusDone, "done", 0)
	a.mu.Lock()
	out := a.State.Output
	a.mu.Unlock()
	return out, nil
}

func (a *AI[S]) resolveStages() []Stage[S] {
	if len(a.Pipeline.Custom) > 0 {
		return a.Pipeline.Custom
	}

	analyze := a.Pipeline.Analyze
	if analyze == nil {
		analyze = DefaultAnalyze[S]
	}
	gather := a.Pipeline.GatherContext
	if gather == nil {
		gather = DefaultGatherContext[S]
	}
	plan := a.Pipeline.Plan
	if plan == nil {
		plan = DefaultPlan[S]
	}
	exec := a.Pipeline.Execute
	if exec == nil {
		exec = DefaultExecute[S]
	}
	wait := a.Pipeline.WaitDeps
	if wait == nil {
		wait = DefaultWaitDeps[S]
	}

	if a.SkipPlanning {
		return []Stage[S]{
			{Status: StatusExecuting, Name: "execute", Run: exec},
			{Status: StatusWaitingDeps, Name: "wait_deps", Run: wait},
		}
	}
	return []Stage[S]{
		{Status: StatusAnalyzing, Name: "analyze", Run: analyze},
		{Status: StatusGatheringContext, Name: "gather", Run: gather},
		{Status: StatusPlanning, Name: "plan", Run: plan},
		{Status: StatusExecuting, Name: "execute", Run: exec},
		{Status: StatusWaitingDeps, Name: "wait_deps", Run: wait},
	}
}

// DefaultAnalyze is the built-in StatusAnalyzing stage.
func DefaultAnalyze[S any](ctx Context, a *AI[S]) error {
	if a.Gateway == nil || a.Task == "" {
		return nil
	}
	tmpl := a.Prompts.Analyze
	if tmpl == "" {
		tmpl = defaultAnalyzePrompt
	}
	msg, err := RenderPrompt(tmpl, map[string]any{"Task": a.Task, "State": a.Memory.Get()})
	if err != nil {
		return err
	}
	_, err = ctx.Call(gateway.Request{
		Model:        a.ModelFor(StatusAnalyzing),
		SystemPrefix: a.SystemPrefix,
		Temperature:  a.Temperature,
		MaxTokens:    a.MaxTokens,
		Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: msg}},
	})
	return err
}

// DefaultGatherContext is the built-in StatusGatheringContext stage.
func DefaultGatherContext[S any](ctx Context, a *AI[S]) error {
	var gathered []string
	for _, ds := range a.Data {
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
	a.mu.Lock()
	a.State.ContextData = dynamicCtx
	a.mu.Unlock()
	return nil
}

// DefaultPlan is the built-in StatusPlanning stage.
func DefaultPlan[S any](ctx Context, a *AI[S]) error {
	a.mu.Lock()
	dynamicCtx := a.State.ContextData
	hasPlan := len(a.State.Plan) > 0
	a.mu.Unlock()

	planPrompt := a.Task
	if a.Prompt != "" {
		rendered, err := RenderPrompt(a.Prompt, map[string]any{
			"Task":    a.Task,
			"Context": dynamicCtx,
			"State":   a.Memory.Get(),
		})
		if err != nil {
			return err
		}
		if rendered != "" {
			planPrompt = rendered
		}
	}

	if a.Gateway != nil && !hasPlan && planPrompt != "" {
		tmpl := a.Prompts.Plan
		if tmpl == "" {
			tmpl = defaultPlanPrompt
		}
		msg, err := RenderPrompt(tmpl, map[string]any{
			"Task":    planPrompt,
			"Context": dynamicCtx,
			"State":   a.Memory.Get(),
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
			if json.Unmarshal([]byte(planResp.Text), &parsed) == nil && len(parsed.Steps) > 0 {
				a.mu.Lock()
				a.State.Plan = parsed.Steps
				a.mu.Unlock()
			}
		}
	}
	return nil
}

// DefaultExecute is the built-in StatusExecuting stage.
func DefaultExecute[S any](ctx Context, a *AI[S]) error {
	invoker := a.ToolInvoker
	if invoker == nil {
		invoker = DefaultToolInvoker{}
	}

	defaultPrompt := a.Task
	if a.Prompt != "" {
		if rendered, err := RenderPrompt(a.Prompt, map[string]any{
			"Task":  a.Task,
			"State": a.Memory.Get(),
		}); err == nil && rendered != "" {
			defaultPrompt = rendered
		}
	}

	steps := a.snapshotPlan()
	if len(steps) == 0 {
		steps = []PlanStep{{ID: "step-1", Task: defaultPrompt}}
	}

	out, err := a.executePlan(ctx, invoker, steps)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.State.Output = out
	a.mu.Unlock()
	return nil
}

// DefaultWaitDeps is the built-in StatusWaitingDeps stage.
func DefaultWaitDeps[S any](ctx Context, a *AI[S]) error {
	return a.WaitDeps(ctx)
}

func (a *AI[S]) executePlan(ctx Context, invoker ToolInvoker, steps []PlanStep) (any, error) {
	done := make(map[string]bool, len(steps))
	failed := make(map[string]bool, len(steps))
	outputs := make(map[string]any, len(steps))
	var lastOut any
	var lastErr error
	var succeeded int

	for len(done)+len(failed) < len(steps) {
		progressed := false
		for _, step := range steps {
			if done[step.ID] || failed[step.ID] {
				continue
			}
			blockedByFail := false
			ready := true
			var depContext []string
			for _, dep := range step.DependsOn {
				if failed[dep] {
					blockedByFail = true
					break
				}
				if !done[dep] {
					ready = false
					break
				}
				depContext = append(depContext, fmt.Sprintf("%s: %v", dep, outputs[dep]))
			}
			if blockedByFail {
				failed[step.ID] = true
				progressed = true
				continue
			}
			if !ready {
				continue
			}

			progressed = true
			stepPrompt := step.Task
			if len(depContext) > 0 {
				stepPrompt += "\n\nCompleted dependencies:\n" + strings.Join(depContext, "\n")
			}

			out, err := a.runStep(ctx, invoker, step.ID, stepPrompt)
			if err != nil {
				failed[step.ID] = true
				lastErr = err
				continue
			}
			done[step.ID] = true
			outputs[step.ID] = out
			lastOut = out
			succeeded++
		}
		if !progressed {
			return nil, fmt.Errorf("agent: cyclic or unresolvable plan dependencies")
		}
	}

	if succeeded == 0 && lastErr != nil {
		return nil, lastErr
	}
	return lastOut, nil
}

func (a *AI[S]) runStep(ctx Context, invoker ToolInvoker, stepID, promptText string) (any, error) {
	return RunWithGuardrail(ctx, a, stepID, func(attempt int, lastErr error) (any, error) {
		if len(a.Tools) > 0 {
			res, err := invoker.Invoke(ctx, ToolCallRequest{
				ProjectID:    a.ProjectID,
				Model:        a.ModelFor(StatusExecuting),
				ArgModel:     a.ArgModel,
				SystemPrefix: a.SystemPrefix,
				Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: promptText}},
				Tools:        a.Tools,
				Attempt:      attempt,
				LastError:    lastErr,
			})
			if err != nil {
				return nil, err
			}
			if a.Hooks.OnStep != nil {
				a.Hooks.OnStep(ctx, a.Memory, stepID, res)
			}
			if res.Selected {
				return res.Output, nil
			}
			return res.Text, nil
		}

		if a.Gateway == nil {
			return promptText, nil
		}
		msgs := []gateway.Message{{Role: gateway.MsgUser, Content: promptText}}
		if lastErr != nil {
			msgs = append(msgs, gateway.Message{
				Role:    gateway.MsgUser,
				Content: fmt.Sprintf("Previous attempt %d failed: %v", attempt, lastErr),
			})
		}
		resp, err := ctx.Call(gateway.Request{
			Model:        a.ModelFor(StatusExecuting),
			SystemPrefix: a.SystemPrefix,
			Temperature:  a.Temperature,
			MaxTokens:    a.MaxTokens,
			Messages:     msgs,
		})
		if err != nil {
			return nil, err
		}
		if a.Hooks.OnStep != nil {
			a.Hooks.OnStep(ctx, a.Memory, stepID, ToolCallResult{Text: resp.Text})
		}
		return resp.Text, nil
	})
}

func (a *AI[S]) snapshotPlan() []PlanStep {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.State.Plan) == 0 {
		return nil
	}
	out := make([]PlanStep, len(a.State.Plan))
	copy(out, a.State.Plan)
	return out
}

// SpawnAsync launches a child Agent in a separate goroutine, enforcing
// MaxActiveChildren semaphore and MaxSpawnDepth fork-bomb protection.
func (a *AI[S]) SpawnAsync(ctx Context, name string, child Agent) (*Future, error) {
	if child == nil {
		return nil, fmt.Errorf("agent: child is nil")
	}
	a.initRuntime()

	maxDepth := a.MaxSpawnDepth
	if maxDepth <= 0 {
		maxDepth = defaultMaxSpawnDepth
	}
	a.mu.Lock()
	curDepth := a.State.SpawnDepth
	a.mu.Unlock()
	if curDepth >= maxDepth {
		return nil, fmt.Errorf("agent: max spawn depth %d reached", maxDepth)
	}

	if cfgChild, ok := child.(childConfigurer); ok {
		cfgChild.attachParent(a, curDepth+1, a.Gateway, a.ProjectID)
	}

	select {
	case a.spawnSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	fut := &Future{
		Name: name,
		done: make(chan struct{}),
	}

	a.mu.Lock()
	if a.State.ActiveFutures == nil {
		a.State.ActiveFutures = make(map[string]*Future)
	}
	key := name
	if key == "" {
		key = fmt.Sprintf("child_%d", len(a.State.ActiveFutures)+1)
	}
	a.State.ActiveFutures[key] = fut
	a.mu.Unlock()

	go func() {
		defer func() {
			<-a.spawnSem
			close(fut.done)
		}()
		fut.val, fut.err = child.Run(ctx)
	}()

	return fut, nil
}

// WaitDeps transitions to StatusWaitingDeps and awaits all currently active Futures.
func (a *AI[S]) WaitDeps(ctx Context) error {
	a.mu.Lock()
	if len(a.State.ActiveFutures) == 0 {
		a.mu.Unlock()
		return nil
	}
	futures := make([]*Future, 0, len(a.State.ActiveFutures))
	for _, f := range a.State.ActiveFutures {
		futures = append(futures, f)
	}
	a.State.Status = StatusWaitingDeps
	a.mu.Unlock()

	for _, f := range futures {
		if _, err := f.Await(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (a *AI[S]) initRuntime() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.spawnSem == nil {
		capSize := a.MaxActiveChildren
		if capSize <= 0 {
			capSize = defaultMaxActiveChildren
		}
		a.spawnSem = make(chan struct{}, capSize)
	}
	if a.State.ActiveFutures == nil {
		a.State.ActiveFutures = make(map[string]*Future)
	}
}

func (a *AI[S]) setState(status AgentStatus, stepID string, attempt int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.State.Status = status
	a.State.CurrentStepID = stepID
	a.State.Attempt = attempt
}

func (a *AI[S]) appendAttemptLog(entry AttemptLog) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.State.History = append(a.State.History, entry)
}

func (a *AI[S]) attemptHistory() []AttemptLog {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]AttemptLog, len(a.State.History))
	copy(out, a.State.History)
	return out
}

func (a *AI[S]) fail(ctx Context, err error) {
	a.mu.Lock()
	a.State.Status = StatusFailed
	a.mu.Unlock()
	if a.Hooks.OnFail != nil {
		a.Hooks.OnFail(ctx, a.Memory, err)
	}
}
