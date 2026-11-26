package anthropic_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/cloudflare/anthropic"
)

func TestDialectToolUseAndToolResult(t *testing.T) {
	d := anthropic.Dialect{APIKey: "k", EnablePromptCache: true}
	suffix, hdrs, body, err := d.BuildRequest("claude-sonnet-4-20250514", gateway.Request{
		SystemPrefix: "sys",
		Messages: []gateway.Message{
			{Role: gateway.MsgUser, Content: "ping"},
			{Role: gateway.MsgAssistant, ToolCalls: []gateway.ToolCall{{ID: "tu1", Name: "ping", Arguments: json.RawMessage(`{"host":"1.1.1.1"}`)}}},
			{Role: gateway.MsgTool, ToolCallID: "tu1", Content: "pong"},
		},
		Tools: []gateway.ToolSpec{{Name: "ping", Description: "pings"}},
	})
	if err != nil || suffix != "v1/messages" || hdrs["x-api-key"] != "k" {
		t.Fatalf("BuildRequest failed: suffix=%q hdrs=%+v err=%v", suffix, hdrs, err)
	}
	if !bytes.Contains(body, []byte(`"tool_use"`)) || !bytes.Contains(body, []byte(`"tool_result"`)) {
		t.Fatalf("expected tool_use and tool_result blocks in payload: %s", string(body))
	}
	resp, err := d.ParseResponse("claude", []byte(`{
		"stop_reason": "tool_use",
		"content": [{"type": "tool_use", "id": "tu1", "name": "ping", "input": {"host": "1.1.1.1"}}],
		"usage": {"input_tokens": 9, "output_tokens": 3}
	}`))
	if err != nil || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "ping" {
		t.Fatalf("ParseResponse failed: resp=%+v err=%v", resp, err)
	}
}

