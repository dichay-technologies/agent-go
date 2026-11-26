// Package mcp provides an optional Model Context Protocol (MCP) client adapter
// that exposes MCP server tools as agentgo.Tool implementations.
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	agentgo "proj.dichay.tech/agent-go"
	"proj.dichay.tech/agent-go/gateway"
)

// Transport sends a JSON-RPC 2.0 request to an MCP server and returns the raw response.
type Transport interface {
	RoundTrip(ctx context.Context, payload []byte) ([]byte, error)
}

// ToolDescriptor describes a tool returned by MCP tools/list.
type ToolDescriptor struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	InputSchema *gateway.JSONSchema `json:"inputSchema"`
}

// Client is a minimal JSON-RPC 2.0 MCP client.
type Client struct {
	transport Transport
	seq       atomic.Uint64
}

// NewClient creates a new MCP Client backed by transport.
func NewClient(transport Transport) *Client {
	return &Client{transport: transport}
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Initialize performs the MCP protocol handshake (initialize + notifications/initialized).
func (c *Client) Initialize(ctx context.Context) error {
	_, err := c.call(ctx, "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo": map[string]string{
			"name":    "agent-go",
			"version": "1.0.0",
		},
	})
	if err != nil {
		return err
	}
	if c.transport != nil {
		notif, _ := json.Marshal(rpcRequest{
			JSONRPC: "2.0",
			Method:  "notifications/initialized",
		})
		_, _ = c.transport.RoundTrip(ctx, notif)
	}
	return nil
}

func (c *Client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if c.transport == nil {
		return nil, fmt.Errorf("mcp: transport is nil")
	}
	reqBytes, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      c.seq.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return nil, err
	}
	respBytes, err := c.transport.RoundTrip(ctx, reqBytes)
	if err != nil {
		return nil, err
	}
	var resp rpcResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("mcp: decode response: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("mcp: rpc error %d: %s", resp.Error.Code, resp.Error.Message)
	}
	return resp.Result, nil
}

// ListTools queries the MCP server's tools/list endpoint, following nextCursor pagination if present.
func (c *Client) ListTools(ctx context.Context) ([]ToolDescriptor, error) {
	var all []ToolDescriptor
	var cursor string
	for page := 0; page < 100; page++ {
		var params any = struct{}{}
		if cursor != "" {
			params = map[string]string{"cursor": cursor}
		}
		raw, err := c.call(ctx, "tools/list", params)
		if err != nil {
			return nil, err
		}
		var out struct {
			Tools      []ToolDescriptor `json:"tools"`
			NextCursor string           `json:"nextCursor"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("mcp: decode tools/list: %w", err)
		}
		all = append(all, out.Tools...)
		if out.NextCursor == "" || out.NextCursor == cursor {
			break
		}
		cursor = out.NextCursor
	}
	return all, nil
}

// CallTool invokes a named tool on the MCP server via tools/call.
// If the MCP response carries isError=true, CallTool returns a Go error so agent guardrails can react.
func (c *Client) CallTool(ctx context.Context, name string, args json.RawMessage) (any, error) {
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}
	raw, err := c.call(ctx, "tools/call", map[string]any{
		"name":      name,
		"arguments": args,
	})
	if err != nil {
		return nil, err
	}
	var mcpRes struct {
		IsError bool `json:"isError"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(raw, &mcpRes) == nil && mcpRes.IsError {
		var parts []string
		for _, item := range mcpRes.Content {
			if item.Text != "" {
				parts = append(parts, item.Text)
			}
		}
		msg := strings.Join(parts, "\n")
		if msg == "" {
			msg = string(raw)
		}
		return nil, fmt.Errorf("mcp: tool %q returned error: %s", name, msg)
	}
	var res any
	if err := json.Unmarshal(raw, &res); err != nil {
		return string(raw), nil
	}
	return res, nil
}

// LoadTools discovers tools from the MCP server and adapts each one to agentgo.Tool.
func LoadTools(ctx context.Context, c *Client) ([]agentgo.Tool, error) {
	descs, err := c.ListTools(ctx)
	if err != nil {
		return nil, err
	}
	tools := make([]agentgo.Tool, 0, len(descs))
	for _, d := range descs {
		desc := d
		tools = append(tools, agentgo.NewTool(agentgo.ToolDef{
			Name:        desc.Name,
			Description: desc.Description,
			Schema:      desc.InputSchema,
			Execute: func(tctx agentgo.Context, args json.RawMessage) (any, error) {
				execCtx := context.Background()
				if tctx != nil {
					execCtx = tctx
				}
				return c.CallTool(execCtx, desc.Name, args)
			},
		}))
	}
	return tools, nil
}
