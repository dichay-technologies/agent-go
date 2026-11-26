package agentgo

import (
	"fmt"
	"strings"

	"proj.dichay.tech/agent-go/gateway"
)

const defaultMaxAttempts = 3

// AttemptLog records a single failed attempt for escalation diagnostics.
type AttemptLog struct {
	Attempt int    `json:"attempt"`
	StepID  string `json:"step_id"`
	Error   string `json:"error"`
}

// FailureAction specifies how the agent should handle an unrecoverable step failure.
type FailureAction int

const (
	// FailureFail fails the step and transitions the run to StatusFailed (default).
	FailureFail FailureAction = iota
	// FailureSkip skips the failed step with nil output and continues the run.
	FailureSkip
	// FailureFallback substitutes FailureDecision.Value as the step's output.
	FailureFallback
)

// FailureDecision is returned by a FailurePolicy when a step fails after escalation.
type FailureDecision struct {
	Action FailureAction
	Value  any
	Err    error
}

// FailurePolicy decides what to do when a step fails all attempts and escalation.
type FailurePolicy func(ctx Context, stepID string, history []AttemptLog, err error) FailureDecision

// FailOnError is the default FailurePolicy that fails the step with err.
func FailOnError(_ Context, _ string, _ []AttemptLog, err error) FailureDecision {
	return FailureDecision{Action: FailureFail, Err: err}
}

// SkipOnError is a FailurePolicy that skips failed steps and continues execution.
func SkipOnError(_ Context, _ string, _ []AttemptLog, _ error) FailureDecision {
	return FailureDecision{Action: FailureSkip}
}

// FallbackValue returns a FailurePolicy that substitutes val when a step fails.
func FallbackValue(val any) FailurePolicy {
	return func(_ Context, _ string, _ []AttemptLog, _ error) FailureDecision {
		return FailureDecision{Action: FailureFallback, Value: val}
	}
}

// RunWithGuardrail executes stepFn up to maxAttempts (default 3) times.
// If all attempts fail, it transitions the run to StatusEscalated, invokes
// Hooks.OnEscalate, and gives the escalation model one chance to recover.
// If escalation also fails, FailurePolicy decides whether to fail, skip, or fallback.
func RunWithGuardrail[S any](
	ctx Context,
	a *AI[S],
	stepID string,
	stepFn func(attempt int, lastErr error) (any, error),
) (any, error) {
	sess := sessionFromContext(ctx, a)
	mem := a.memoryFor(sess)

	maxAttempts := a.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		sess.setState(StatusExecuting, stepID, attempt)
		out, err := stepFn(attempt, lastErr)
		if err == nil {
			sess.setState(StatusVerifying, stepID, attempt)
			if a.Hooks.OnVerify != nil {
				if verr := a.Hooks.OnVerify(ctx, mem, out); verr != nil {
					err = verr
				}
			}
			if err == nil {
				return out, nil
			}
		}

		lastErr = err
		sess.appendAttemptLog(AttemptLog{
			Attempt: attempt,
			StepID:  stepID,
			Error:   err.Error(),
		})
	}

	// Escalate after maxAttempts failures.
	sess.setState(StatusEscalated, stepID, maxAttempts)
	history := sess.attemptHistory()
	if a.Hooks.OnEscalate != nil {
		if herr := a.Hooks.OnEscalate(ctx, mem, history); herr != nil {
			return applyFailurePolicy(ctx, a, sess, stepID, history, herr)
		}
	}

	escModel := a.ModelFor(StatusEscalated)

	var histBuf strings.Builder
	for _, h := range history {
		fmt.Fprintf(&histBuf, "- attempt %d: %s\n", h.Attempt, h.Error)
	}
	escTmpl := a.Prompts.Escalate
	if escTmpl == "" {
		escTmpl = defaultEscalatePrompt
	}
	escPrompt, err := RenderPrompt(escTmpl, map[string]any{
		"StepID":   stepID,
		"Attempts": maxAttempts,
		"History":  histBuf.String(),
		"Task":     sess.task(),
	})
	if err != nil {
		escPrompt = fmt.Sprintf("Step %q failed %d times:\n%s", stepID, maxAttempts, histBuf.String())
	}

	resp, escErr := ctx.Call(gateway.Request{
		Model:        escModel,
		SystemPrefix: a.SystemPrefix,
		Temperature:  a.Temperature,
		MaxTokens:    a.MaxTokens,
		Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: escPrompt}},
	})
	if escErr != nil {
		finalErr := fmt.Errorf("guardrail: escalation failed after %d attempts (%v): %w", maxAttempts, lastErr, escErr)
		return applyFailurePolicy(ctx, a, sess, stepID, history, finalErr)
	}

	if a.Hooks.OnVerify != nil {
		if verr := a.Hooks.OnVerify(ctx, mem, resp.Text); verr != nil {
			finalErr := fmt.Errorf("guardrail: escalated output rejected: %w", verr)
			return applyFailurePolicy(ctx, a, sess, stepID, history, finalErr)
		}
	}

	return resp.Text, nil
}

func applyFailurePolicy[S any](
	ctx Context,
	a *AI[S],
	sess *runSession[S],
	stepID string,
	history []AttemptLog,
	err error,
) (any, error) {
	action := FailureFail
	var fallbackVal any
	if a.FailurePolicy != nil {
		decision := a.FailurePolicy(ctx, stepID, history, err)
		action = decision.Action
		fallbackVal = decision.Value
		if action == FailureFail && decision.Err != nil {
			err = decision.Err
		}
	}
	sess.appendFailureRecord(FailureRecord{
		StepID:   stepID,
		Attempts: len(history),
		Error:    err.Error(),
		Action:   action,
		History:  history,
	})
	switch action {
	case FailureSkip:
		return nil, nil
	case FailureFallback:
		return fallbackVal, nil
	default:
		a.failSession(ctx, sess, err)
		return nil, err
	}
}
