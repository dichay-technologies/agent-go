package gemini_test

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
	"proj.dichay.tech/agent-go/gateway/gemini"
)

func TestGeminiProviderCompleteAndTools(t *testing.T) {
	var gotPath, gotKey string
	var gotReq map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKey = r.Header.Get("x-goog-api-key")
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"candidates": [{
				"finishReason": "STOP",
				"content": {
					"parts": [{
						"functionCall": {
							"name": "lookup",
							"args": {"id": "42"}
						}
					}]
				}
			}],
			"usageMetadata": {
				"promptTokenCount": 30,
				"candidatesTokenCount": 8,
				"cachedContentTokenCount": 15
			}
		}`))
	}))
	defer srv.Close()

	p, err := gemini.New(gemini.Config{
		APIKey:  "gem-secret",
		BaseURL: srv.URL,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	gw := gateway.New(gateway.Config{})
	gw.Register("gemini", p)

	resp, err := gw.Complete(context.Background(), gateway.Request{
		ProjectID:    "proj-gem",
		SystemPrefix: "Be concise.",
		Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: "lookup 42"}},
		Tools: []gateway.ToolSpec{{
			Name:        "lookup",
			Description: "lookup record",
			Parameters: &gateway.JSONSchema{
				Type:       "object",
				Required:   []string{"id"},
				Properties: map[string]*gateway.JSONSchema{"id": {Type: "string"}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	if gotPath != "/v1beta/models/gemini-2.5-flash:generateContent" {
		t.Fatalf("unexpected path: %q", gotPath)
	}
	if gotKey != "gem-secret" {
		t.Fatalf("unexpected x-goog-api-key: %q", gotKey)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "lookup" {
		t.Fatalf("unexpected tool calls: %+v", resp.ToolCalls)
	}
	if resp.Usage.InputTokens != 30 || resp.Usage.OutputTokens != 8 || resp.Usage.CachedTokens != 15 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
}

func TestGeminiMultimodalImageAndAudio(t *testing.T) {
	imgOut := []byte{0x89, 0x50, 0x4E, 0x47}
	audioOut := []byte{0x52, 0x49, 0x46, 0x46}

	t.Run("image input and output", func(t *testing.T) {
		var gotBody map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"candidates": [{
					"finishReason": "STOP",
					"content": {
						"parts": [
							{"text": "edited"},
							{"inlineData": {"mimeType": "image/png", "data": "` + base64.StdEncoding.EncodeToString(imgOut) + `"}}
						]
					}
				}]
			}`))
		}))
		defer srv.Close()

		p, err := gemini.New(gemini.Config{APIKey: "k", BaseURL: srv.URL})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}

		resp, err := p.Complete(context.Background(), gateway.Request{
			Model:              "gemini-2.5-flash-image",
			ResponseModalities: []gateway.Modality{gateway.ModalityText, gateway.ModalityImage},
			Messages: []gateway.Message{{
				Role: gateway.MsgUser,
				Parts: []gateway.Part{
					{Text: "edit this"},
					{MIMEType: "image/jpeg", Data: []byte{0xFF, 0xD8, 0xFF}},
				},
			}},
		})
		if err != nil {
			t.Fatalf("Complete failed: %v", err)
		}
		if resp.Text != "edited" || len(resp.Parts) != 2 {
			t.Fatalf("unexpected multimodal response: %+v", resp)
		}
		if resp.Parts[1].MIMEType != "image/png" || !bytes.Equal(resp.Parts[1].Data, imgOut) {
			t.Fatalf("unexpected image part: %+v", resp.Parts[1])
		}
	})

	t.Run("audio tts output", func(t *testing.T) {
		var gotBody map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"candidates": [{
					"finishReason": "STOP",
					"content": {
						"parts": [
							{"inlineData": {"mimeType": "audio/wav", "data": "` + base64.StdEncoding.EncodeToString(audioOut) + `"}}
						]
					}
				}]
			}`))
		}))
		defer srv.Close()

		p, err := gemini.New(gemini.Config{APIKey: "k", BaseURL: srv.URL})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}

		resp, err := p.Complete(context.Background(), gateway.Request{
			Model:              "gemini-2.5-flash-preview-tts",
			ResponseModalities: []gateway.Modality{gateway.ModalityAudio},
			SpeechConfig:       &gateway.SpeechConfig{VoiceName: "Kore"},
			Messages:           []gateway.Message{{Role: gateway.MsgUser, Content: "hello"}},
		})
		if err != nil {
			t.Fatalf("Complete failed: %v", err)
		}
		if len(resp.Parts) != 1 || resp.Parts[0].MIMEType != "audio/wav" || !bytes.Equal(resp.Parts[0].Data, audioOut) {
			t.Fatalf("unexpected audio part: %+v", resp.Parts)
		}
		genCfg, _ := gotBody["generationConfig"].(map[string]any)
		if genCfg["speechConfig"] == nil {
			t.Fatalf("expected speechConfig in generationConfig, got: %+v", genCfg)
		}
	})

	t.Run("direct voice-to-voice", func(t *testing.T) {
		var gotBody map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"candidates": [{
					"finishReason": "STOP",
					"content": {
						"parts": [
							{"inlineData": {"mimeType": "audio/pcm", "data": "` + base64.StdEncoding.EncodeToString(audioOut) + `"}}
						]
					}
				}]
			}`))
		}))
		defer srv.Close()

		p, err := gemini.New(gemini.Config{APIKey: "k", BaseURL: srv.URL})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}

		resp, err := p.Complete(context.Background(), gateway.Request{
			Model:              "gemini-2.5-flash-native-audio-preview-12-2025",
			ResponseModalities: []gateway.Modality{gateway.ModalityAudio},
			SpeechConfig:       &gateway.SpeechConfig{VoiceName: "Puck"},
			Messages: []gateway.Message{{
				Role:  gateway.MsgUser,
				Parts: []gateway.Part{{MIMEType: "audio/pcm", Data: []byte{1, 2, 3, 4}}},
			}},
		})
		if err != nil {
			t.Fatalf("Complete failed: %v", err)
		}
		if len(resp.Parts) != 1 || resp.Parts[0].MIMEType != "audio/pcm" || !bytes.Equal(resp.Parts[0].Data, audioOut) {
			t.Fatalf("unexpected voice response: %+v", resp.Parts)
		}
	})
}

func TestGeminiContextCachingAndNativeBatch(t *testing.T) {
	t.Run("auto context cache", func(t *testing.T) {
		var cacheCreates atomic.Int32
		var gotCachedContent string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/v1beta/cachedContents" {
				cacheCreates.Add(1)
				_, _ = w.Write([]byte(`{"name": "cachedContents/sys-123"}`))
				return
			}
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			gotCachedContent, _ = body["cachedContent"].(string)
			_, _ = w.Write([]byte(`{
				"candidates": [{"finishReason": "STOP", "content": {"parts": [{"text": "ok"}]}}],
				"usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 1, "cachedContentTokenCount": 9}
			}`))
		}))
		defer srv.Close()

		p, err := gemini.New(gemini.Config{
			APIKey:             "k",
			BaseURL:            srv.URL,
			EnableContextCache: true,
			ContextCacheTTL:    time.Minute,
		})
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}

		for range 2 {
			resp, err := p.Complete(context.Background(), gateway.Request{
				SystemPrefix: "huge static system prompt",
				Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: "hi"}},
			})
			if err != nil {
				t.Fatalf("Complete failed: %v", err)
			}
			if resp.Usage.CachedTokens != 9 {
				t.Fatalf("expected cached tokens 9, got %d", resp.Usage.CachedTokens)
			}
		}
		if cacheCreates.Load() != 1 {
			t.Fatalf("expected cachedContents to be created once and reused, got %d", cacheCreates.Load())
		}
		if gotCachedContent != "cachedContents/sys-123" {
			t.Fatalf("expected cachedContent in generateContent, got %q", gotCachedContent)
		}
	})

	t.Run("native batchGenerateContent", func(t *testing.T) {
		var polls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodPost && r.URL.Path == "/v1beta/models/gemini-2.5-flash:batchGenerateContent" {
				_, _ = w.Write([]byte(`{"name": "batches/b-1", "state": "BATCH_STATE_PENDING"}`))
				return
			}
			if r.Method == http.MethodGet && r.URL.Path == "/v1beta/batches/b-1" {
				if polls.Add(1) == 1 {
					_, _ = w.Write([]byte(`{"name": "batches/b-1", "state": "BATCH_STATE_RUNNING"}`))
					return
				}
				_, _ = w.Write([]byte(`{
					"name": "batches/b-1",
					"state": "BATCH_STATE_SUCCEEDED",
					"output": {
						"inlinedResponses": {
							"inlinedResponses": [
								{"metadata": {"custom_id": "req_1"}, "response": {"candidates": [{"finishReason": "STOP", "content": {"parts": [{"text": "two"}]}}]}},
								{"metadata": {"custom_id": "req_0"}, "response": {"candidates": [{"finishReason": "STOP", "content": {"parts": [{"text": "one"}]}}]}}
							]
						}
					}
				}`))
				return
			}
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer srv.Close()

		p, err := gemini.New(gemini.Config{
			APIKey:            "k",
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
	})
}

func TestGeminiSSEStreamAndMsgTool(t *testing.T) {
	var seenFuncResp bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if contents, _ := body["contents"].([]any); len(contents) > 0 {
			raw, _ := json.Marshal(contents)
			if bytes.Contains(raw, []byte("functionResponse")) {
				seenFuncResp = true
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(
			"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Part1 \"}]}}]}\n\n" +
				"data: {\"candidates\":[{\"finishReason\":\"STOP\",\"content\":{\"parts\":[{\"text\":\"Part2\"}]}}],\"usageMetadata\":{\"promptTokenCount\":6,\"candidatesTokenCount\":2}}\n\n",
		))
	}))
	defer srv.Close()

	p, err := gemini.New(gemini.Config{APIKey: "k", BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var text string
	var finalUsage *gateway.Usage
	for ch, serr := range p.Stream(context.Background(), gateway.Request{
		Messages: []gateway.Message{
			{Role: gateway.MsgUser, Content: "calc"},
			{Role: gateway.MsgAssistant, ToolCalls: []gateway.ToolCall{{ID: "1", Name: "add", Arguments: json.RawMessage(`{"a":1}`)}}},
			{Role: gateway.MsgTool, Name: "add", ToolCallID: "1", Content: `{"sum":2}`},
		},
	}) {
		if serr != nil {
			t.Fatalf("Stream: %v", serr)
		}
		text += ch.Text
		if ch.Usage != nil {
			finalUsage = ch.Usage
		}
	}
	if !seenFuncResp || text != "Part1 Part2" || finalUsage == nil || finalUsage.InputTokens != 6 {
		t.Fatalf("unexpected gemini stream: funcResp=%v text=%q usage=%+v", seenFuncResp, text, finalUsage)
	}
}
