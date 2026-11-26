package anthropic_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/anthropic"
)

func TestAnthropicCompleteAndNativeBatch(t *testing.T) {
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "ant-secret" {
			t.Fatalf("unexpected x-api-key: %q", r.Header.Get("x-api-key"))
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/messages":
			_, _ = w.Write([]byte(`{
				"model": "claude-sonnet-4-20250514",
				"stop_reason": "end_turn",
				"content": [{"type": "text", "text": "hello"}],
				"usage": {"input_tokens": 10, "output_tokens": 3, "cache_read_input_tokens": 7}
			}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/messages/batches":
			_, _ = w.Write([]byte(`{"id": "msgbatch_1"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/messages/batches/msgbatch_1":
			if polls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"processing_status": "in_progress"}`))
				return
			}
			_, _ = w.Write([]byte(`{"processing_status": "ended", "results_url": "/v1/messages/batches/msgbatch_1/results"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/messages/batches/msgbatch_1/results":
			_, _ = w.Write([]byte(
				`{"custom_id":"req_1","result":{"type":"succeeded","message":{"model":"claude-sonnet-4-20250514","content":[{"type":"text","text":"two"}]}}}` + "\n" +
					`{"custom_id":"req_0","result":{"type":"succeeded","message":{"model":"claude-sonnet-4-20250514","content":[{"type":"text","text":"one"}]}}}` + "\n",
			))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	p, err := anthropic.New(anthropic.Config{
		APIKey:            "ant-secret",
		BaseURL:           srv.URL,
		EnablePromptCache: true,
		EnableNativeBatch: true,
		BatchPollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	gw := gateway.New(gateway.Config{})
	gw.Register("anthropic", p)

	resp, err := gw.Complete(context.Background(), gateway.Request{
		ProjectID:    "proj-ant",
		Model:        "claude-sonnet-4-20250514",
		SystemPrefix: "sys",
		Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	if resp.Text != "hello" || resp.Usage.CachedTokens != 7 {
		t.Fatalf("unexpected response: %+v", resp)
	}

	resps, errs := p.BatchComplete(context.Background(), []gateway.Request{
		{Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "1"}}},
		{Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "2"}}},
	})
	if len(errs) != 2 || errs[0] != nil || errs[1] != nil {
		t.Fatalf("unexpected batch errs: %+v", errs)
	}
	if resps[0].Text != "one" || resps[1].Text != "two" {
		t.Fatalf("unexpected batch responses: %+v", resps)
	}
}

func TestAnthropicSSEStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(
			"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":11,\"cache_read_input_tokens\":5}}}\n\n" +
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Claude \"}}\n\n" +
				"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu_1\",\"name\":\"lookup\"}}\n\n" +
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\":\\\"go\\\"}\"}}\n\n" +
				"event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":4}}\n\n",
		))
	}))
	defer srv.Close()

	p, err := anthropic.New(anthropic.Config{APIKey: "ant-secret", BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var text string
	var calls []gateway.ToolCall
	var usage *gateway.Usage
	for ch, serr := range p.Stream(context.Background(), gateway.Request{Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "hi"}}}) {
		if serr != nil {
			t.Fatalf("Stream: %v", serr)
		}
		text += ch.Text
		calls = append(calls, ch.ToolCalls...)
		if ch.Usage != nil {
			usage = ch.Usage
		}
	}
	if text != "Claude " || len(calls) != 1 || calls[0].Name != "lookup" || usage == nil || usage.InputTokens != 11 || usage.OutputTokens != 4 {
		t.Fatalf("unexpected anthropic stream: text=%q calls=%+v usage=%+v", text, calls, usage)
	}
}

