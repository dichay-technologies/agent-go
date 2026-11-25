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
