package agentgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// StepRunner executes a single plan step given the outputs of its completed dependencies.
type StepRunner func(ctx Context, step PlanStep, depOutputs map[string]any) (any, error)

// Scheduler controls how a plan's DAG of steps is executed.
type Scheduler interface {
	Schedule(ctx Context, steps []PlanStep, precomputed map[string]any, run StepRunner) (outputs map[string]any, lastOut any, err error)
}

func mergeDynamicSteps(steps []PlanStep, known map[string]bool, ctx Context) []PlanStep {
	live := livePlanFromContext(ctx)
	for _, s := range live {
		if s.ID != "" && !known[s.ID] {
			known[s.ID] = true
			steps = append(steps, s)
		}
	}
	return steps
}

// SequentialScheduler executes ready plan steps one at a time in topological order.
// It supports dynamic steps appended mid-run via AppendPlanSteps and awaiting
// named background SubFutures referenced in PlanStep.DependsOn.
type SequentialScheduler struct{}

func (SequentialScheduler) Schedule(ctx Context, initialSteps []PlanStep, precomputed map[string]any, run StepRunner) (map[string]any, any, error) {
	steps := append([]PlanStep(nil), initialSteps...)
	knownSteps := make(map[string]bool, len(steps))
	for _, s := range steps {
		knownSteps[s.ID] = true
	}

	done := make(map[string]bool, len(steps))
	failed := make(map[string]bool, len(steps))
	outputs := make(map[string]any, len(steps))
	var lastOut any
	var stepErrs []error

	for k, val := range precomputed {
		done[k] = true
		outputs[k] = val
		if knownSteps[k] {
			lastOut = val
		}
	}

	for {
		steps = mergeDynamicSteps(steps, knownSteps, ctx)
		completedSteps := 0
		for _, s := range steps {
			if done[s.ID] || failed[s.ID] {
				completedSteps++
			}
		}
		if completedSteps >= len(steps) {
			break
		}
		if err := ctx.Err(); err != nil {
			return outputs, lastOut, err
		}

		progressed := false
		for i := 0; i < len(steps); i++ {
			step := steps[i]
			if done[step.ID] || failed[step.ID] {
				continue
			}
			blockedByFail := false
			ready := true
			depOutputs := make(map[string]any, len(step.DependsOn))
			for _, dep := range step.DependsOn {
				if failed[dep] {
					blockedByFail = true
					break
				}
				if !done[dep] && !knownSteps[dep] {
					// Point-to-point join on a named background Future (Spec §2.3).
					if val, found, ferr := awaitNamedFuture(ctx, dep); found {
						progressed = true
						if ferr != nil {
							failed[dep] = true
							stepErrs = append(stepErrs, fmt.Errorf("dependency %q: %w", dep, ferr))
							blockedByFail = true
							break
						}
						done[dep] = true
						outputs[dep] = val
					}
				}
				if !done[dep] {
					ready = false
					break
				}
				depOutputs[dep] = outputs[dep]
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
			out, err := run(ctx, step, depOutputs)
			steps = mergeDynamicSteps(steps, knownSteps, ctx)
			if err != nil {
				failed[step.ID] = true
				stepErrs = append(stepErrs, fmt.Errorf("step %q: %w", step.ID, err))
				continue
			}
			done[step.ID] = true
			outputs[step.ID] = out
			lastOut = out
		}
		if !progressed {
			return outputs, lastOut, fmt.Errorf("agent: cyclic or unresolvable plan dependencies")
		}
	}

	if len(stepErrs) > 0 {
		return outputs, lastOut, errors.Join(stepErrs...)
	}
	return outputs, lastOut, nil
}

// DAGParallelScheduler executes independent ready plan steps concurrently up to MaxConcurrency.
// It supports dynamic steps appended via AppendPlanSteps and non-blocking joins on named Futures.
type DAGParallelScheduler struct {
	// MaxConcurrency limits parallel step execution per Run. Defaults to 4 when <= 0.
	MaxConcurrency int
}

func (s DAGParallelScheduler) Schedule(ctx Context, initialSteps []PlanStep, precomputed map[string]any, run StepRunner) (map[string]any, any, error) {
	maxConc := s.MaxConcurrency
	if maxConc <= 0 {
		maxConc = 4
	}
	sem := make(chan struct{}, maxConc)

	steps := append([]PlanStep(nil), initialSteps...)
	knownSteps := make(map[string]bool, len(steps))
	for _, st := range steps {
		knownSteps[st.ID] = true
	}

	done := make(map[string]bool, len(steps))
	failed := make(map[string]bool, len(steps))
	running := make(map[string]bool, len(steps))
	outputs := make(map[string]any, len(steps))
	var lastOut any
	var stepErrs []error

	for k, val := range precomputed {
		done[k] = true
		outputs[k] = val
		if knownSteps[k] {
			lastOut = val
		}
	}

	type stepOutcome struct {
		id     string
		out    any
		err    error
		isStep bool
	}
	outcomes := make(chan stepOutcome, max(len(steps)+4, maxConc*2))
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(stopCh)
		wg.Wait()
	}()

	for {
		steps = mergeDynamicSteps(steps, knownSteps, ctx)
		completedSteps := 0
		for _, st := range steps {
			if done[st.ID] || failed[st.ID] {
				completedSteps++
			}
		}
		if completedSteps >= len(steps) && len(running) == 0 {
			break
		}
		if err := ctx.Err(); err != nil {
			return outputs, lastOut, err
		}

		launched := 0
		for _, step := range steps {
			if done[step.ID] || failed[step.ID] || running[step.ID] {
				continue
			}
			blockedByFail := false
			ready := true
			depOutputs := make(map[string]any, len(step.DependsOn))
			for _, dep := range step.DependsOn {
				if failed[dep] {
					blockedByFail = true
					break
				}
				if !done[dep] && !running[dep] && !knownSteps[dep] {
					if hasNamedFuture(ctx, dep) {
						running[dep] = true
						launched++
						depName := dep
						wg.Add(1)
						go func() {
							defer wg.Done()
							val, _, ferr := awaitNamedFuture(ctx, depName)
							select {
							case outcomes <- stepOutcome{id: depName, out: val, err: ferr, isStep: false}:
							case <-stopCh:
							}
						}()
					}
				}
				if !done[dep] {
					ready = false
					break
				}
				depOutputs[dep] = outputs[dep]
			}
			if blockedByFail {
				failed[step.ID] = true
				launched++
				continue
			}
			if !ready {
				continue
			}

			running[step.ID] = true
			launched++

			st := step
			deps := depOutputs
			wg.Add(1)
			go func() {
				defer wg.Done()
				var out any
				var err error
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
					func() {
						defer func() {
							if r := recover(); r != nil {
								out = nil
								err = fmt.Errorf("agent: panic in step %q: %v", st.ID, r)
							}
						}()
						out, err = run(ctx, st, deps)
					}()
				case <-ctx.Done():
					err = ctx.Err()
				case <-stopCh:
					err = context.Canceled
				}
				select {
				case outcomes <- stepOutcome{id: st.ID, out: out, err: err, isStep: true}:
				case <-stopCh:
				}
			}()
		}

		if len(running) == 0 {
			if launched == 0 {
				return outputs, lastOut, fmt.Errorf("agent: cyclic or unresolvable plan dependencies")
			}
			continue
		}

		select {
		case res := <-outcomes:
			delete(running, res.id)
			if res.err != nil {
				failed[res.id] = true
				stepErrs = append(stepErrs, fmt.Errorf("step %q: %w", res.id, res.err))
			} else {
				done[res.id] = true
				outputs[res.id] = res.out
				if res.isStep {
					lastOut = res.out
				}
			}
		case <-ctx.Done():
			return outputs, lastOut, ctx.Err()
		}
	}

	if len(stepErrs) > 0 {
		return outputs, lastOut, errors.Join(stepErrs...)
	}
	return outputs, lastOut, nil
}
