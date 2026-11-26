package agentgo

import (
	"encoding/json"
	"fmt"

	"proj.dichay.tech/agent-go/gateway"
)

// Tool is the minimal contract for an external capability exposed to an agent.
type Tool interface {
	Name() string
	Description() string
	Schema() *gateway.JSONSchema
	// NeedsGeneration reports whether arguments must be generated in a follow-up
	// structured call after the tool is selected.
	NeedsGeneration() bool
	Execute(ctx Context, args json.RawMessage) (any, error)
}

// ToolCallRequest carries the context needed by a ToolInvoker to select,
// parameterize, and execute a tool.
type ToolCallRequest struct {
	ProjectID    string
	Model        string
	ArgModel     string // optional model for NeedsGeneration(); defaults to Model
	SystemPrefix string
	Messages     []gateway.Message
	Tools        []Tool
	Attempt      int
	LastError    error
	OnStream     func(chunk gateway.StreamChunk)
}

// ToolCallResult is the outcome of a single ToolInvoker.Invoke call.
type ToolCallResult struct {
	Selected bool
	ToolName string
	Args     json.RawMessage
	Output   any
	Text     string
}

// ToolInvoker defines how an agent selects, parameterizes, validates, and executes tools.
type ToolInvoker interface {
	Invoke(ctx Context, req ToolCallRequest) (ToolCallResult, error)
}

// ToolInvokerFunc adapts a plain function to the ToolInvoker interface.
type ToolInvokerFunc func(ctx Context, req ToolCallRequest) (ToolCallResult, error)

func (f ToolInvokerFunc) Invoke(ctx Context, req ToolCallRequest) (ToolCallResult, error) {
	return f(ctx, req)
}

// DefaultToolInvoker implements the standard tool pipeline:
//  1. Model selects a tool (and direct args if NeedsGeneration == false).
//  2. Optional structured arg generation when NeedsGeneration == true.
//  3. JSONSchema validation before Tool.Execute.
type DefaultToolInvoker struct{}

func (DefaultToolInvoker) Invoke(ctx Context, req ToolCallRequest) (ToolCallResult, error) {
	gw := ctx.AI()
	if gw == nil {
		return ToolCallResult{}, fmt.Errorf("tool: AIGateway is nil in context")
	}
	if len(req.Tools) == 0 {
		return ToolCallResult{Selected: false}, nil
	}

	toolMap := make(map[string]Tool, len(req.Tools))
	specs := make([]gateway.ToolSpec, len(req.Tools))
	for i, t := range req.Tools {
		toolMap[t.Name()] = t
		specs[i] = gateway.ToolSpec{
			Name:        t.Name(),
			Description: t.Description(),
			Parameters:  t.Schema(),
		}
	}

	msgs := make([]gateway.Message, len(req.Messages))
	copy(msgs, req.Messages)
	if req.LastError != nil {
		msgs = append(msgs, gateway.Message{
			Role:    gateway.MsgUser,
			Content: fmt.Sprintf("Previous tool attempt %d failed with error: %v. Please fix.", req.Attempt, req.LastError),
		})
	}

	decResp, err := gw.Complete(ctx, gateway.Request{
		ProjectID:    req.ProjectID,
		Model:        req.Model,
		SystemPrefix: req.SystemPrefix,
		Messages:     msgs,
		Tools:        specs,
	})
	if err != nil {
		return ToolCallResult{}, fmt.Errorf("tool selection: %w", err)
	}

	if len(decResp.ToolCalls) == 0 {
		return ToolCallResult{
			Selected: false,
			Text:     decResp.Text,
		}, nil
	}

	tc := decResp.ToolCalls[0]
	selectedTool, ok := toolMap[tc.Name]
	if !ok {
		return ToolCallResult{}, fmt.Errorf("tool: unknown tool %q selected", tc.Name)
	}

	args := tc.Arguments
	if selectedTool.NeedsGeneration() {
		argModel := req.ArgModel
		if argModel == "" {
			argModel = req.Model
		}
		genMsgs := append(msgs, gateway.Message{
			Role:    gateway.MsgUser,
			Content: fmt.Sprintf("Generate valid JSON arguments for tool %q (%s).", selectedTool.Name(), selectedTool.Description()),
		})
		genResp, err := gw.Complete(ctx, gateway.Request{
			ProjectID:      req.ProjectID,
			Model:          argModel,
			SystemPrefix:   req.SystemPrefix,
			Messages:       genMsgs,
			ResponseSchema: selectedTool.Schema(),
		})
		if err != nil {
			return ToolCallResult{Selected: true, ToolName: selectedTool.Name()}, fmt.Errorf("tool arg generation (%s): %w", selectedTool.Name(), err)
		}
		args = json.RawMessage(genResp.Text)
	}

	if schema := selectedTool.Schema(); schema != nil {
		if len(args) == 0 {
			args = json.RawMessage(`{}`)
		}
		if err := schema.Validate(args); err != nil {
			return ToolCallResult{Selected: true, ToolName: selectedTool.Name(), Args: args}, fmt.Errorf("tool schema validation (%s): %w", selectedTool.Name(), err)
		}
	}

	out, err := selectedTool.Execute(ctx, args)
	if err != nil {
		return ToolCallResult{Selected: true, ToolName: selectedTool.Name(), Args: args}, fmt.Errorf("tool execute (%s): %w", selectedTool.Name(), err)
	}

	return ToolCallResult{
		Selected: true,
		ToolName: selectedTool.Name(),
		Args:     args,
		Output:   out,
	}, nil
}

// ToolDef configures a lightweight function-backed Tool created with NewTool.
type ToolDef struct {
	Name            string
	Description     string
	Schema          *gateway.JSONSchema
	NeedsGeneration bool
	Execute         func(ctx Context, args json.RawMessage) (any, error)
}

type funcTool struct {
	def ToolDef
}

func (f funcTool) Name() string               { return f.def.Name }
func (f funcTool) Description() string        { return f.def.Description }
func (f funcTool) Schema() *gateway.JSONSchema { return f.def.Schema }
func (f funcTool) NeedsGeneration() bool      { return f.def.NeedsGeneration }
func (f funcTool) Execute(ctx Context, args json.RawMessage) (any, error) {
	if f.def.Execute == nil {
		return nil, fmt.Errorf("tool %q has nil Execute handler", f.def.Name)
	}
	return f.def.Execute(ctx, args)
}

// NewTool constructs a Tool from a ToolDef without boilerplate struct methods.
func NewTool(def ToolDef) Tool {
	return funcTool{def: def}
}

// NewTypedTool constructs a Tool that automatically unmarshals json.RawMessage into In.
func NewTypedTool[In any](
	name, description string,
	schema *gateway.JSONSchema,
	needsGeneration bool,
	fn func(ctx Context, in In) (any, error),
) Tool {
	return NewTool(ToolDef{
		Name:            name,
		Description:     description,
		Schema:          schema,
		NeedsGeneration: needsGeneration,
		Execute: func(ctx Context, raw json.RawMessage) (any, error) {
			var in In
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &in); err != nil {
					return nil, fmt.Errorf("tool %q: decode args: %w", name, err)
				}
			}
			return fn(ctx, in)
		},
	})
}

// Func constructs a typed Tool whose JSONSchema is automatically derived from In.
func Func[In any, Out any](
	name, description string,
	fn func(ctx Context, in In) (Out, error),
) Tool {
	return NewTypedTool[In](
		name,
		description,
		gateway.SchemaFor[In](),
		false,
		func(ctx Context, in In) (any, error) {
			return fn(ctx, in)
		},
	)
}

// ToolMiddleware wraps a ToolInvoker for cross-cutting concerns (logging, ACLs, auditing).
type ToolMiddleware func(next ToolInvoker) ToolInvoker

// ChainToolMiddlewares wraps base with middlewares in order (mws[0] is outermost).
func ChainToolMiddlewares(base ToolInvoker, mws ...ToolMiddleware) ToolInvoker {
	if base == nil {
		base = DefaultToolInvoker{}
	}
	inv := base
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] != nil {
			inv = mws[i](inv)
		}
	}
	return inv
}

// ErrPermissionDenied is returned when an agent attempts to execute a tool without the required permission.
var ErrPermissionDenied = fmt.Errorf("tool: permission denied")

// ToolWithPermission wraps a Tool so Execute verifies that ctx.HasPermission(perm) holds
// before running (defense-in-depth per Spec §5.1).
func ToolWithPermission(perm string, base Tool) Tool {
	if base == nil || perm == "" {
		return base
	}
	return NewTool(ToolDef{
		Name:            base.Name(),
		Description:     base.Description(),
		Schema:          base.Schema(),
		NeedsGeneration: base.NeedsGeneration(),
		Execute: func(ctx Context, args json.RawMessage) (any, error) {
			if ctx != nil && !ctx.HasPermission(perm) {
				return nil, fmt.Errorf("%w: tool %q requires %q", ErrPermissionDenied, base.Name(), perm)
			}
			return base.Execute(ctx, args)
		},
	})
}

// PermissionMiddleware filters out tools from ToolCallRequest whose required permission
// in requiredPerms (toolName -> perm) is not granted to ctx, and guards execution.
func PermissionMiddleware(requiredPerms map[string]string) ToolMiddleware {
	return func(next ToolInvoker) ToolInvoker {
		return ToolInvokerFunc(func(ctx Context, req ToolCallRequest) (ToolCallResult, error) {
			if len(requiredPerms) > 0 && ctx != nil {
				allowed := make([]Tool, 0, len(req.Tools))
				for _, t := range req.Tools {
					reqPerm, restricted := requiredPerms[t.Name()]
					if restricted && reqPerm != "" {
						if !ctx.HasPermission(reqPerm) {
							continue
						}
						t = ToolWithPermission(reqPerm, t)
					}
					allowed = append(allowed, t)
				}
				if len(allowed) == 0 && len(req.Tools) > 0 {
					return ToolCallResult{}, ErrPermissionDenied
				}
				req.Tools = allowed
			}
			return next.Invoke(ctx, req)
		})
	}
}

// LoopToolInvoker implements a multi-turn ReAct tool-calling loop.
// It repeatedly sends model tool results back as MsgTool messages until the
// model produces a final text response without tool calls or MaxIterations is reached.
type LoopToolInvoker struct {
	MaxIterations int // defaults to 10 when <= 0
	// ReplyModel optionally specifies a conversational/generation model used when
	// req.Model is a lightweight decision classifier (e.g. Jev) that selects tools or "none".
	ReplyModel string
	// ContinueOnToolError feeds schema/execution errors back to the model as MsgTool
	// messages so the model can self-correct within the loop instead of aborting immediately.
	ContinueOnToolError bool
}

func (l LoopToolInvoker) Invoke(ctx Context, req ToolCallRequest) (ToolCallResult, error) {
	gw := ctx.AI()
	if gw == nil {
		return ToolCallResult{}, fmt.Errorf("tool: AIGateway is nil in context")
	}

	maxIter := l.MaxIterations
	if maxIter <= 0 {
		maxIter = 10
	}

	toolMap := make(map[string]Tool, len(req.Tools))
	specs := make([]gateway.ToolSpec, 0, len(req.Tools))
	for _, t := range req.Tools {
		toolMap[t.Name()] = t
		specs = append(specs, gateway.ToolSpec{
			Name:        t.Name(),
			Description: t.Description(),
			Parameters:  t.Schema(),
		})
	}

	msgs := make([]gateway.Message, len(req.Messages))
	copy(msgs, req.Messages)
	if req.LastError != nil {
		msgs = append(msgs, gateway.Message{
			Role:    gateway.MsgUser,
			Content: fmt.Sprintf("Previous attempt %d failed with error: %v. Please fix.", req.Attempt, req.LastError),
		})
	}

	var lastResult ToolCallResult
	for iter := 0; iter < maxIter; iter++ {
		turnModel := req.Model
		if iter > 0 && l.ReplyModel != "" {
			turnModel = l.ReplyModel
		}
		var resp gateway.Response
		var err error
		if req.OnStream != nil {
			var textBuf []byte
			var calls []gateway.ToolCall
			for ch, serr := range gw.Stream(ctx, gateway.Request{
				ProjectID:    req.ProjectID,
				Model:        turnModel,
				SystemPrefix: req.SystemPrefix,
				Messages:     msgs,
				Tools:        specs,
			}) {
				if serr != nil {
					err = serr
					break
				}
				if ch.Text != "" {
					textBuf = append(textBuf, ch.Text...)
					req.OnStream(ch)
				}
				if len(ch.ToolCalls) > 0 {
					calls = append(calls, ch.ToolCalls...)
				}
			}
			resp = gateway.Response{Text: string(textBuf), ToolCalls: calls}
		} else {
			resp, err = gw.Complete(ctx, gateway.Request{
				ProjectID:    req.ProjectID,
				Model:        turnModel,
				SystemPrefix: req.SystemPrefix,
				Messages:     msgs,
				Tools:        specs,
			})
		}
		if err != nil {
			return ToolCallResult{}, err
		}

		if len(resp.ToolCalls) == 0 {
			finalText := resp.Text
			if l.ReplyModel != "" && turnModel != l.ReplyModel {
				if req.OnStream != nil {
					var rbuf []byte
					for ch, serr := range gw.Stream(ctx, gateway.Request{
						ProjectID:    req.ProjectID,
						Model:        l.ReplyModel,
						SystemPrefix: req.SystemPrefix,
						Messages:     msgs,
					}) {
						if serr != nil {
							return ToolCallResult{}, serr
						}
						if ch.Text != "" {
							rbuf = append(rbuf, ch.Text...)
							req.OnStream(ch)
						}
					}
					finalText = string(rbuf)
				} else {
					replyResp, rerr := gw.Complete(ctx, gateway.Request{
						ProjectID:    req.ProjectID,
						Model:        l.ReplyModel,
						SystemPrefix: req.SystemPrefix,
						Messages:     msgs,
					})
					if rerr != nil {
						return ToolCallResult{}, rerr
					}
					finalText = replyResp.Text
				}
			}
			return ToolCallResult{
				Selected: false,
				ToolName: lastResult.ToolName,
				Args:     lastResult.Args,
				Output:   lastResult.Output,
				Text:     finalText,
			}, nil
		}

		resolvedCalls := make([]gateway.ToolCall, len(resp.ToolCalls))
		copy(resolvedCalls, resp.ToolCalls)

		var toolMsgs []gateway.Message
		for i, tc := range resolvedCalls {
			tool, ok := toolMap[tc.Name]
			if !ok {
				if l.ContinueOnToolError {
					toolMsgs = append(toolMsgs, gateway.Message{
						Role:       gateway.MsgTool,
						Name:       tc.Name,
						ToolCallID: tc.ID,
						Content:    fmt.Sprintf(`{"error":"unknown tool %q"}`, tc.Name),
					})
					continue
				}
				return ToolCallResult{}, fmt.Errorf("tool: model selected unknown tool %q", tc.Name)
			}

			args := tc.Arguments
			if tool.NeedsGeneration() {
				argModel := req.ArgModel
				if argModel == "" {
					if l.ReplyModel != "" {
						argModel = l.ReplyModel
					} else {
						argModel = turnModel
					}
				}
				genMsgs := append(msgs, gateway.Message{
					Role:    gateway.MsgUser,
					Content: fmt.Sprintf("Generate valid JSON arguments for tool %q (%s).", tool.Name(), tool.Description()),
				})
				genResp, gerr := gw.Complete(ctx, gateway.Request{
					ProjectID:      req.ProjectID,
					Model:          argModel,
					SystemPrefix:   req.SystemPrefix,
					Messages:       genMsgs,
					ResponseSchema: tool.Schema(),
				})
				if gerr != nil {
					return ToolCallResult{Selected: true, ToolName: tool.Name()}, fmt.Errorf("tool arg generation (%s): %w", tool.Name(), gerr)
				}
				args = json.RawMessage(genResp.Text)
				resolvedCalls[i].Arguments = args
			}

			if s := tool.Schema(); s != nil {
				if len(args) == 0 {
					args = json.RawMessage(`{}`)
					resolvedCalls[i].Arguments = args
				}
				if verr := s.Validate(args); verr != nil {
					if l.ContinueOnToolError {
						errJSON, _ := json.Marshal(map[string]string{"error": "invalid arguments: " + verr.Error()})
						toolMsgs = append(toolMsgs, gateway.Message{
							Role:       gateway.MsgTool,
							Name:       tc.Name,
							ToolCallID: tc.ID,
							Content:    string(errJSON),
						})
						continue
					}
					return ToolCallResult{}, fmt.Errorf("tool %q: invalid arguments: %w", tc.Name, verr)
				}
			}

			out, execErr := tool.Execute(ctx, args)
			if execErr != nil {
				if l.ContinueOnToolError {
					errJSON, _ := json.Marshal(map[string]string{"error": execErr.Error()})
					toolMsgs = append(toolMsgs, gateway.Message{
						Role:       gateway.MsgTool,
						Name:       tc.Name,
						ToolCallID: tc.ID,
						Content:    string(errJSON),
					})
					continue
				}
				return ToolCallResult{}, execErr
			}

			var resultStr string
			switch v := out.(type) {
			case string:
				resultStr = v
			default:
				if b, err := json.Marshal(v); err == nil {
					resultStr = string(b)
				} else {
					resultStr = fmt.Sprint(v)
				}
			}

			lastResult = ToolCallResult{
				Selected: true,
				ToolName: tc.Name,
				Args:     args,
				Output:   out,
			}
			toolMsgs = append(toolMsgs, gateway.Message{
				Role:       gateway.MsgTool,
				Name:       tc.Name,
				Content:    resultStr,
				ToolCallID: tc.ID,
			})
		}

		msgs = append(msgs, gateway.Message{
			Role:      gateway.MsgAssistant,
			Content:   resp.Text,
			ToolCalls: resolvedCalls,
		})
		msgs = append(msgs, toolMsgs...)
	}

	return ToolCallResult{}, fmt.Errorf("tool: LoopToolInvoker exceeded max iterations (%d)", maxIter)
}
