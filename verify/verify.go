// Package verify provides optional verification helpers (three-valued verdicts,
// question ensembles, and context-based tool ACL middleware) built on top of agentgo hooks.
package verify

import (
	"errors"
	"fmt"

	agentgo "proj.dichay.tech/agent-go"
)

// Verdict is a three-valued verification outcome.
type Verdict string

const (
	Pass   Verdict = "pass"
	Fail   Verdict = "fail"
	Unsure Verdict = "unsure"
)

// CheckFunc evaluates an agent output and returns a Verdict and optional reason.
type CheckFunc func(ctx agentgo.Context, output any) (Verdict, string, error)

// EnsembleVerifier runs multiple CheckFuncs and aggregates their verdicts.
// Any Fail verdict rejects immediately; if RequireAllPass is true, Unsure also rejects.
type EnsembleVerifier struct {
	Checks         []CheckFunc
	RequireAllPass bool
}

// Verify implements a reusable verification function suitable for Hooks.OnVerify.
func (e EnsembleVerifier) Verify(ctx agentgo.Context, output any) error {
	for i, check := range e.Checks {
		if check == nil {
			continue
		}
		v, reason, err := check(ctx, output)
		if err != nil {
			return fmt.Errorf("verify: check %d error: %w", i, err)
		}
		switch v {
		case Fail:
			return fmt.Errorf("verify: check %d failed: %s", i, reason)
		case Unsure:
			if e.RequireAllPass {
				return fmt.Errorf("verify: check %d unsure: %s", i, reason)
			}
		case Pass:
		default:
			return fmt.Errorf("verify: check %d returned unknown verdict %q", i, v)
		}
	}
	return nil
}

// ErrToolDenied is returned by ACLToolMiddleware when a tool call is not permitted.
var ErrToolDenied = errors.New("verify: tool execution denied by ACL")

// ACLToolMiddleware returns an agentgo.ToolMiddleware that enforces per-call
// tool authorization using a predicate (typically reading caller/role from ctx).
func ACLToolMiddleware(allow func(ctx agentgo.Context, toolName string) bool) agentgo.ToolMiddleware {
	return func(next agentgo.ToolInvoker) agentgo.ToolInvoker {
		return agentgo.ToolInvokerFunc(func(ctx agentgo.Context, req agentgo.ToolCallRequest) (agentgo.ToolCallResult, error) {
			if allow != nil {
				filtered := make([]agentgo.Tool, 0, len(req.Tools))
				for _, t := range req.Tools {
					if allow(ctx, t.Name()) {
						filtered = append(filtered, t)
					}
				}
				if len(filtered) == 0 && len(req.Tools) > 0 {
					return agentgo.ToolCallResult{}, ErrToolDenied
				}
				req.Tools = filtered
			}
			return next.Invoke(ctx, req)
		})
	}
}

