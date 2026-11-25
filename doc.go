// Package agentgo provides a minimal, composable Go library for building
// AI agent graphs with pluggable model routing, unified telebot-style Context,
// overridable lifecycle stages, 3-attempt guardrails, and bounded async spawning.
//
// Core building blocks:
//   - AI[S] & New[S](...Option[S]): generic agent parameterized by application state S.
//   - Fork[P, C](parent, ...Option[C]): creates a child agent inheriting Gateway, ProjectID, and depth.
//   - Context (context.go): unified execution context passed to tools, hooks, and stages.
//   - Tool / NewTool / NewTypedTool / ToolInvoker (tool.go): extensible tool execution.
//   - Pipeline[S] / Stage[S] (ai.go, agent.go): overridable FSM phases.
//   - gateway.AIGateway (gateway/): centralized model router, cache, coalescing, and providers.
package agentgo

