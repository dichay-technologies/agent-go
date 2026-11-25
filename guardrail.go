package agentgo

import (
	"fmt"
	"strings"

	"proj.dichay.tech/agent-go/gateway"
)

const defaultMaxAttempts = 3

// AttemptLog records a single failed attempt for escalation diagnostics.
type AttemptLog struct {
	Attempt int
	StepID  string
	Error   string
}

// RunWithGuardrail executes stepFn up to maxAttempts (default 3) times.
// If all attempts fail, it transitions the agent to StatusEscalated, invokes
// Hooks.OnEscalate, and gives the escalation model one chance to recover.
// If escalation also fails, it transitions to StatusFailed and invokes Hooks.OnFail.
func RunWithGuardrail[S any](
	ctx Context,
	a *AI[S],
	stepID string,
	stepFn func(attempt int, lastErr error) (any, error),
) (any, error) {
	maxAttempts := a.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		a.setState(StatusExecuting, stepID, attempt)
		out, err := stepFn(attempt, lastErr)
		if err == nil {
			a.setState(StatusVerifying, stepID, attempt)
			if a.Hooks.OnVerify != nil {
				if verr := a.Hooks.OnVerify(ctx, a.Memory, out); verr != nil {
					err = verr
				}
			}
			if err == nil {
				return out, nil
			}
		}

		lastErr = err
		a.appendAttemptLog(AttemptLog{
			Attempt: attempt,
			StepID:  stepID,
			Error:   err.Error(),
		})
	}

	// Escalate after maxAttempts failures.
	a.setState(StatusEscalated, stepID, maxAttempts)
	history := a.attemptHistory()
	if a.Hooks.OnEscalate != nil {
		if herr := a.Hooks.OnEscalate(ctx, a.Memory, history); herr != nil {
			a.fail(ctx, herr)
			return nil, herr
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
		"Task":     a.Task,
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
		a.fail(ctx, finalErr)
		return nil, finalErr
	}

	if a.Hooks.OnVerify != nil {
		if verr := a.Hooks.OnVerify(ctx, a.Memory, resp.Text); verr != nil {
			finalErr := fmt.Errorf("guardrail: escalated output rejected: %w", verr)
			a.fail(ctx, finalErr)
			return nil, finalErr
		}
	}

	return resp.Text, nil
}

