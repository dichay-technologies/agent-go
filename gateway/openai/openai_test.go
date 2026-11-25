package openai_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/openai"
)

func TestOpenAICompleteAndRetry(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sk-test" {
			t.Fatalf("unexpected auth: %q", r.Header.Get("Authorization"))
		}
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("unexpected path: %q", r.URL.Path)
		}
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`rate limited`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model": "gpt-4o-mini",
			"choices": [{
				"message": {
					"role": "assistant",
					"content": "hi there",
					"tool_calls": [{
						"id": "call_1",
						"type": "function",
						"function": {"name": "ping", "arguments": "{}"}
					}]
				},
				"finish_reason": "tool_calls"
			}],
			"usage": {
				"prompt_tokens": 25,
				"completion_tokens": 6,
				"prompt_tokens_details": {"cached_tokens": 12}
			}
		}`))
	}))
	defer srv.Close()

	p, err := openai.New(openai.Config{
		APIKey:     "sk-test",
		BaseURL:    srv.URL,
		MaxRetries: 2,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	gw := gateway.New(gateway.Config{})
	gw.Register("openai", p)

	resp, err := gw.Complete(context.Background(), gateway.Request{
		ProjectID:    "proj-oai",
		SystemPrefix: "sys",
		Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	if resp.Text != "hi there" || len(resp.ToolCalls) != 1 || resp.Usage.CachedTokens != 12 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts (1 retry), got %d", attempts.Load())
	}
}

func TestOpenAINativeBatch(t *testing.T) {
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/files":
			_, _ = w.Write([]byte(`{"id": "file-in-1"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/batches":
			_, _ = w.Write([]byte(`{"id": "batch-1"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/batches/batch-1":
			if polls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"status": "in_progress"}`))
				return
			}
			_, _ = w.Write([]byte(`{"status": "completed", "output_file_id": "file-out-1"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/files/file-out-1/content":
			_, _ = w.Write([]byte(
				`{"custom_id":"req_1","response":{"status_code":200,"body":{"model":"gpt-4o-mini","choices":[{"message":{"content":"two"},"finish_reason":"stop"}]}}}` + "\n" +
					`{"custom_id":"req_0","response":{"status_code":200,"body":{"model":"gpt-4o-mini","choices":[{"message":{"content":"one"},"finish_reason":"stop"}]}}}` + "\n",
			))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	p, err := openai.New(openai.Config{
		APIKey:            "sk-test",
		BaseURL:           srv.URL,
		EnableNativeBatch: true,
		BatchPollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
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

func TestOpenAIDirectVoiceIO(t *testing.T) {
	audioOut := []byte{0x52, 0x49, 0x46, 0x46}
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model": "gpt-4o-audio-preview",
			"choices": [{
				"message": {
					"role": "assistant",
					"audio": {
						"data": "` + base64.StdEncoding.EncodeToString(audioOut) + `",
						"transcript": "hello back"
					}
				},
				"finish_reason": "stop"
			}]
		}`))
	}))
	defer srv.Close()

	p, err := openai.New(openai.Config{APIKey: "sk-test", BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	resp, err := p.Complete(context.Background(), gateway.Request{
		Model:              "gpt-4o-audio-preview",
		ResponseModalities: []gateway.Modality{gateway.ModalityText, gateway.ModalityAudio},
		SpeechConfig:       &gateway.SpeechConfig{VoiceName: "nova"},
		Messages: []gateway.Message{{
			Role:  gateway.MsgUser,
			Parts: []gateway.Part{{MIMEType: "audio/wav", Data: []byte{1, 2, 3}}},
		}},
	})
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	if resp.Text != "hello back" || len(resp.Parts) != 1 || !bytes.Equal(resp.Parts[0].Data, audioOut) {
		t.Fatalf("unexpected voice response: %+v", resp)
	}
	if gotBody["audio"] == nil || gotBody["modalities"] == nil {
		t.Fatalf("expected modalities & audio config in request, got: %+v", gotBody)
	}
}

