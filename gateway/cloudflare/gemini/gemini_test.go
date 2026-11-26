package gemini_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/cloudflare/gemini"
)

func TestDialectToolsAndFunctionResponse(t *testing.T) {
	d := gemini.Dialect{}
	suffix, _, body, err := d.BuildRequest("gemini-2.5-flash", gateway.Request{
		SystemPrefix: "sys",
		Messages: []gateway.Message{
			{Role: gateway.MsgUser, Content: "calc"},
			{Role: gateway.MsgAssistant, ToolCalls: []gateway.ToolCall{{ID: "1", Name: "add", Arguments: json.RawMessage(`{"a":1}`)}}},
			{Role: gateway.MsgTool, Name: "add", Content: `{"sum":2}`},
		},
		Tools: []gateway.ToolSpec{{Name: "add", Description: "add numbers"}},
	})
	if err != nil || suffix != "v1beta/models/gemini-2.5-flash:generateContent" {
		t.Fatalf("BuildRequest failed: suffix=%q err=%v", suffix, err)
	}
	if !bytes.Contains(body, []byte(`"functionDeclarations"`)) || !bytes.Contains(body, []byte(`"functionResponse"`)) {
		t.Fatalf("expected functionDeclarations and functionResponse in payload: %s", string(body))
	}
	resp, err := d.ParseResponse("gemini-2.5-flash", []byte(`{
		"candidates": [{"finishReason": "STOP", "content": {"parts": [{"functionCall": {"name": "add", "args": {"a": 1}}}]}}]
	}`))
	if err != nil || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "add" {
		t.Fatalf("ParseResponse failed: resp=%+v err=%v", resp, err)
	}
}

