package jev_test

import (
	"testing"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/cloudflare/jev"
)

func TestDialectToolChoiceAndFallback(t *testing.T) {
	d := jev.Dialect{}
	suffix, _, body, err := d.BuildRequest("", gateway.Request{
		Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "pick tool"}},
		Tools:    []gateway.ToolSpec{{Name: "cut", Description: "cut clip"}},
	})
	if err != nil || suffix != "v1/systemone" || len(body) == 0 {
		t.Fatalf("BuildRequest failed: suffix=%q err=%v", suffix, err)
	}
	resp, err := d.ParseResponse("jev-latest", []byte(`{
		"model": "jev-latest",
		"answers": {
			"tool": {"type": "choice", "choice": "cut"},
			"quality": {"type": "score", "score": 0.92}
		},
		"usage": {"input_tokens": 10, "output_tokens": 1}
	}`))
	if err != nil || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "cut" {
		t.Fatalf("ParseResponse failed: resp=%+v err=%v", resp, err)
	}
}

