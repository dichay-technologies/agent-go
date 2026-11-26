package openai_test

import (
	"encoding/json"
	"testing"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/cloudflare/openai"
)

func TestDialectBuildAndParse(t *testing.T) {
	d := openai.Dialect{ReasoningEffort: "medium"}
	if d.Name() != "openai" {
		t.Fatalf("unexpected name: %q", d.Name())
	}
	suffix, _, body, err := d.BuildRequest("gpt-4o-mini", gateway.Request{
		SystemPrefix: "sys",
		Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: "hi"}},
		ResponseSchema: &gateway.JSONSchema{
			Type:       "object",
			Required:   []string{"ok"},
			Properties: map[string]*gateway.JSONSchema{"ok": {Type: "boolean"}},
		},
	})
	if err != nil || suffix != "v1/chat/completions" || len(body) == 0 {
		t.Fatalf("BuildRequest failed: suffix=%q err=%v", suffix, err)
	}
	resp, err := d.ParseResponse("gpt-4o-mini", []byte(`{
		"model": "gpt-4o-mini",
		"choices": [{"message": {"role": "assistant", "content": "{\"ok\":true}"}, "finish_reason": "stop"}],
		"usage": {"prompt_tokens": 5, "completion_tokens": 2}
	}`))
	if err != nil || resp.Text != `{"ok":true}` || resp.Usage.InputTokens != 5 {
		t.Fatalf("ParseResponse failed: resp=%+v err=%v", resp, err)
	}
	var reqMap map[string]any
	_ = json.Unmarshal(body, &reqMap)
	if reqMap["reasoning_effort"] != "medium" {
		t.Fatalf("expected reasoning_effort=medium")
	}
}

