// Package otel provides optional OpenTelemetry-compatible tracing middleware
// for gateway.AIGateway and agentgo.ToolInvoker without adding external SDK
// dependencies to the core module.
package otel

import (
	"context"

	agentgo "proj.dichay.tech/agent-go"
	"proj.dichay.tech/agent-go/gateway"
)

// Span represents an active tracing span.
type Span interface {
	SetAttribute(key string, val any)
	RecordError(err error)
	End()
}

// Tracer starts spans for gateway and tool operations.
type Tracer interface {
	Start(ctx context.Context, name string) (context.Context, Span)
}

// GatewayMiddleware instruments AIGateway.Complete calls with tracing spans.
func GatewayMiddleware(tracer Tracer) gateway.Middleware {
	return func(next gateway.CompleteFunc) gateway.CompleteFunc {
		return func(ctx context.Context, req gateway.Request) (gateway.Response, error) {
			if tracer == nil {
				return next(ctx, req)
			}
			spanCtx, span := tracer.Start(ctx, "gateway.complete")
			span.SetAttribute("llm.model", req.Model)
			span.SetAttribute("llm.provider", req.Provider)
			span.SetAttribute("llm.project_id", req.ProjectID)
			resp, err := next(spanCtx, req)
			if err != nil {
				span.RecordError(err)
			} else {
				span.SetAttribute("llm.input_tokens", resp.Usage.InputTokens)
				span.SetAttribute("llm.output_tokens", resp.Usage.OutputTokens)
				span.SetAttribute("llm.cached", resp.Cached)
			}
			span.End()
			return resp, err
		}
	}
}

// ToolMiddleware instruments ToolInvoker.Invoke calls with tracing spans.
func ToolMiddleware(tracer Tracer) agentgo.ToolMiddleware {
	return func(next agentgo.ToolInvoker) agentgo.ToolInvoker {
		return agentgo.ToolInvokerFunc(func(ctx agentgo.Context, req agentgo.ToolCallRequest) (agentgo.ToolCallResult, error) {
			if tracer == nil {
				return next.Invoke(ctx, req)
			}
			spanCtx, span := tracer.Start(ctx, "agent.tool_invoke")
			span.SetAttribute("agent.model", req.Model)
			span.SetAttribute("agent.attempt", req.Attempt)
			wrappedCtx := ctx.WithStdContext(spanCtx)
			res, err := next.Invoke(wrappedCtx, req)
			if err != nil {
				span.RecordError(err)
			} else if res.Selected {
				span.SetAttribute("agent.tool_name", res.ToolName)
			}
			span.End()
			return res, err
		})
	}
}

