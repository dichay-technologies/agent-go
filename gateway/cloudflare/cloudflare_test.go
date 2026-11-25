package cloudflare_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/cloudflare"
	"proj.dichay.tech/agent-go/gateway/cloudflare/anthropic"
	"proj.dichay.tech/agent-go/gateway/cloudflare/gemini"
	"proj.dichay.tech/agent-go/gateway/cloudflare/jev"
	"proj.dichay.tech/agent-go/gateway/cloudflare/openai"
	"proj.dichay.tech/agent-go/gateway/cloudflare/workersai"
)

func TestCloudflareOpenAIDialectAndCacheHeaders(t *testing.T) {
	var gotPath, gotAuth, gotAIGAuth, gotCacheTTL, gotAffinity string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotAIGAuth = r.Header.Get("cf-aig-authorization")
		gotCacheTTL = r.Header.Get("cf-aig-cache-ttl")
		gotAffinity = r.Header.Get("x-session-affinity")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model": "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
			"choices": [{
				"message": {
					"role": "assistant",
					"content": "",
					"tool_calls": [{
						"id": "call_1",
						"type": "function",
						"function": {"name": "search", "arguments": "{\"q\":\"go\"}"}
					}]
				},
				"finish_reason": "tool_calls"
			}],
			"usage": {
				"prompt_tokens": 42,
				"completion_tokens": 11,
				"prompt_tokens_details": {"cached_tokens": 20}
			}
		}`))
	}))
	defer srv.Close()

	cf, err := cloudflare.New(cloudflare.Config{
		APIToken:              "cf-secret",
		AIGatewayToken:        "aig-secret",
		BaseURL:               srv.URL,
		Dialect:               openai.Dialect{},
		CacheTTL:              5 * time.Minute,
		EnableSessionAffinity: true,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	gw := gateway.New(gateway.Config{})
	gw.Register("cloudflare", cf)

	resp, err := gw.Complete(context.Background(), gateway.Request{
		ProjectID:    "proj-cf",
		SystemPrefix: "You are a router.",
		Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: "find go docs"}},
		Tools: []gateway.ToolSpec{{
			Name:        "search",
			Description: "search docs",
			Parameters: &gateway.JSONSchema{
				Type:       "object",
				Required:   []string{"q"},
				Properties: map[string]*gateway.JSONSchema{"q": {Type: "string"}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	if gotPath != "/v1/chat/completions" {
		t.Fatalf("unexpected path: %q", gotPath)
	}
	if gotAuth != "Bearer cf-secret" || gotAIGAuth != "Bearer aig-secret" {
		t.Fatalf("unexpected auth headers: %q, %q", gotAuth, gotAIGAuth)
	}
	if gotCacheTTL != "300" || gotAffinity == "" {
		t.Fatalf("expected cache headers, got ttl=%q affinity=%q", gotCacheTTL, gotAffinity)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "search" {
		t.Fatalf("unexpected tool calls: %+v", resp.ToolCalls)
	}
	if resp.Usage.InputTokens != 42 || resp.Usage.OutputTokens != 11 || resp.Usage.CachedTokens != 20 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
}

func TestCloudflareWorkersAIAsyncBatch(t *testing.T) {
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.RawQuery == "queueRequest=true" {
			_, _ = w.Write([]byte(`{"success": true, "result": {"id": "batch-123", "status": "queued"}}`))
			return
		}
		if r.Method == http.MethodGet && r.URL.RawQuery == "requestId=batch-123" {
			if polls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"success": true, "result": {"status": "running"}}`))
				return
			}
			_, _ = w.Write([]byte(`{
				"success": true,
				"result": {
					"status": "completed",
					"responses": [
						{"external_reference": "req_1", "result": {"response": "ans-2", "usage": {"prompt_tokens": 4, "completion_tokens": 2}}},
						{"external_reference": "req_0", "result": {"response": "ans-1", "usage": {"prompt_tokens": 3, "completion_tokens": 1}}}
					]
				}
			}`))
			return
		}
		t.Fatalf("unexpected request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
	}))
	defer srv.Close()

	cf, err := cloudflare.New(cloudflare.Config{
		APIToken:          "cf-secret",
		BaseURL:           srv.URL,
		Dialect:           workersai.Dialect{},
		EnableAsyncBatch:  true,
		BatchPollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	resps, errs := cf.BatchComplete(context.Background(), []gateway.Request{
		{Model: "@cf/qwen/qwen2.5-coder-32b-instruct", Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "1"}}},
		{Model: "@cf/qwen/qwen2.5-coder-32b-instruct", Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "2"}}},
	})
	if len(errs) != 2 || errs[0] != nil || errs[1] != nil {
		t.Fatalf("unexpected batch errs: %+v", errs)
	}
	if resps[0].Text != "ans-1" || resps[1].Text != "ans-2" {
		t.Fatalf("unexpected batch responses (order mismatch?): %+v", resps)
	}
}

func TestCloudflareAnthropicAndGeminiDialects(t *testing.T) {
	t.Run("anthropic with prompt cache", func(t *testing.T) {
		var gotPath, gotKey, gotVer string
		var gotBody map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotKey = r.Header.Get("x-api-key")
			gotVer = r.Header.Get("anthropic-version")
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			_, _ = w.Write([]byte(`{
				"model": "claude-sonnet-4-20250514",
				"stop_reason": "end_turn",
				"content": [{"type": "text", "text": "hello from claude"}],
				"usage": {"input_tokens": 12, "output_tokens": 4, "cache_read_input_tokens": 8}
			}`))
		}))
		defer srv.Close()

		cf, err := cloudflare.New(cloudflare.Config{
			BaseURL: srv.URL,
			Dialect: anthropic.Dialect{APIKey: "ant-key", EnablePromptCache: true},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		resp, err := cf.Complete(context.Background(), gateway.Request{
			Model:        "claude-sonnet-4-20250514",
			SystemPrefix: "sys",
			Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: "hi"}},
		})
		if err != nil {
			t.Fatalf("Complete failed: %v", err)
		}
		if gotPath != "/v1/messages" || gotKey != "ant-key" || gotVer != "2023-06-01" {
			t.Fatalf("unexpected request metadata: path=%q key=%q ver=%q", gotPath, gotKey, gotVer)
		}
		if resp.Text != "hello from claude" || resp.Usage.CachedTokens != 8 {
			t.Fatalf("unexpected response: %+v", resp)
		}
	})

	t.Run("gemini", func(t *testing.T) {
		var gotPath string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			_, _ = w.Write([]byte(`{
				"candidates": [{
					"finishReason": "STOP",
					"content": {"parts": [{"text": "hello from gemini"}]}
				}],
				"usageMetadata": {"promptTokenCount": 9, "candidatesTokenCount": 5, "cachedContentTokenCount": 4}
			}`))
		}))
		defer srv.Close()

		cf, err := cloudflare.New(cloudflare.Config{
			BaseURL: srv.URL,
			Dialect: gemini.Dialect{},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		resp, err := cf.Complete(context.Background(), gateway.Request{
			Model:    "gemini-2.5-flash",
			Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "hi"}},
		})
		if err != nil {
			t.Fatalf("Complete failed: %v", err)
		}
		if gotPath != "/v1beta/models/gemini-2.5-flash:generateContent" {
			t.Fatalf("unexpected path: %q", gotPath)
		}
		if resp.Text != "hello from gemini" || resp.Usage.CachedTokens != 4 {
			t.Fatalf("unexpected response: %+v", resp)
		}
	})

	t.Run("jev", func(t *testing.T) {
		var gotPath string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			_, _ = w.Write([]byte(`{
				"model": "jev-latest",
				"answers": {
					"action": {"type": "choice", "choice": "run", "confidence": 0.95}
				},
				"usage": {"input_tokens": 15, "output_tokens": 2}
			}`))
		}))
		defer srv.Close()

		cf, err := cloudflare.New(cloudflare.Config{
			BaseURL: srv.URL,
			Dialect: jev.Dialect{},
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		resp, err := cf.Complete(context.Background(), gateway.Request{
			Model:    "jev-latest",
			Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "should we run?"}},
			ResponseSchema: &gateway.JSONSchema{
				Type: "object",
				Properties: map[string]*gateway.JSONSchema{
					"action": {Type: "string", Enum: []any{"run", "skip"}},
				},
			},
		})
		if err != nil {
			t.Fatalf("Complete failed: %v", err)
		}
		if gotPath != "/v1/systemone" {
			t.Fatalf("unexpected path: %q", gotPath)
		}
		if resp.Text != `{"action":"run"}` || resp.Usage.InputTokens != 15 {
			t.Fatalf("unexpected response: %+v", resp)
		}
	})
}
