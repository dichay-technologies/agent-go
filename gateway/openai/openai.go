package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"proj.dichay.tech/agent-go/gateway"
)

const (
	DefaultBaseURL     = "https://api.openai.com/v1"
	DefaultModel       = "gpt-4o-mini"
	defaultTimeout     = 60 * time.Second
	defaultMaxRetries  = 3
	defaultConcurrency = 16
	defaultPoll        = 500 * time.Millisecond
)

// APIError represents a non-2xx HTTP response from an OpenAI-compatible API.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("openai: HTTP %d: %s", e.Status, e.Body)
}

// Unwrap exposes typed gateway.ProviderError categories (ErrRateLimit, ErrRetryable, ErrAuth, ErrContextLength).
func (e *APIError) Unwrap() error {
	return gateway.ClassifyStatusCode("openai", e.Status, e.Body)
}

// Retryable reports whether the status code (429 or 5xx) should be retried.
func (e *APIError) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status >= 500
}

// Config configures the standalone OpenAI-compatible provider
// (works with OpenAI, OpenRouter, Groq, DeepSeek, Ollama, vLLM, etc.).
type Config struct {
	// Name overrides the registered provider name (defaults to "openai").
	Name string
	// APIKey is the bearer token. If empty, reads OPENAI_API_KEY from env.
	// Optional when BaseURL points to a local server (e.g. Ollama, vLLM).
	APIKey string
	// BaseURL overrides DefaultBaseURL / OPENAI_BASE_URL.
	BaseURL string
	// DefaultModel overrides DefaultModel / OPENAI_MODEL.
	DefaultModel string
	// Headers adds extra HTTP headers (e.g. HTTP-Referer for OpenRouter).
	Headers map[string]string
	// HTTPClient overrides the default http.Client.
	HTTPClient *http.Client
	// MaxRetries controls retries on 429/5xx responses (defaults to 3; <0 disables).
	MaxRetries int
	// MaxBatchConcurrency caps concurrent HTTP calls in fan-out BatchComplete (defaults to 16).
	MaxBatchConcurrency int
	// EnableNativeBatch uses OpenAI's asynchronous Batch API (/files + /batches, 50% discount)
	// in BatchComplete instead of concurrent fan-out.
	EnableNativeBatch bool
	// BatchPollInterval controls how often batch job status is polled (defaults to 500ms).
	BatchPollInterval time.Duration
}

// Provider implements gateway.Provider and gateway.BatchProvider for OpenAI-compatible APIs.
type Provider struct {
	name       string
	cfg        Config
	baseURL    string
	httpClient *http.Client
}

// New creates a standalone OpenAI-compatible Provider.
func New(cfg Config) (*Provider, error) {
	if cfg.APIKey == "" {
		cfg.APIKey = os.Getenv("OPENAI_API_KEY")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = os.Getenv("OPENAI_BASE_URL")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")

	if cfg.APIKey == "" && cfg.BaseURL == DefaultBaseURL {
		return nil, fmt.Errorf("openai: APIKey is required for %s", DefaultBaseURL)
	}
	if cfg.Name == "" {
		cfg.Name = "openai"
	}
	if cfg.DefaultModel == "" {
		if m := os.Getenv("OPENAI_MODEL"); m != "" {
			cfg.DefaultModel = m
		} else {
			cfg.DefaultModel = DefaultModel
		}
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.MaxBatchConcurrency <= 0 {
		cfg.MaxBatchConcurrency = defaultConcurrency
	}
	if cfg.BatchPollInterval <= 0 {
		cfg.BatchPollInterval = defaultPoll
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}

	return &Provider{
		name:       cfg.Name,
		cfg:        cfg,
		baseURL:    cfg.BaseURL,
		httpClient: client,
	}, nil
}

func (p *Provider) Name() string { return p.name }

func (p *Provider) Complete(ctx context.Context, req gateway.Request) (gateway.Response, error) {
	model := req.Model
	if model == "" {
		model = p.cfg.DefaultModel
	}

	body, err := json.Marshal(buildChatRequest(model, req))
	if err != nil {
		return gateway.Response{}, fmt.Errorf("openai: marshal request: %w", err)
	}

	raw, err := p.doWithRetry(ctx, http.MethodPost, p.baseURL+"/chat/completions", "application/json", body)
	if err != nil {
		return gateway.Response{}, err
	}
	return parseChatResponse(model, raw)
}

// Stream implements gateway.StreamProvider over Server-Sent Events (/chat/completions with stream=true).
func (p *Provider) Stream(ctx context.Context, req gateway.Request) iter.Seq2[gateway.StreamChunk, error] {
	return func(yield func(gateway.StreamChunk, error) bool) {
		model := req.Model
		if model == "" {
			model = p.cfg.DefaultModel
		}
		payload := buildChatRequest(model, req)
		payload.Stream = true
		payload.StreamOptions = &oaiStreamOptions{IncludeUsage: true}

		body, err := json.Marshal(payload)
		if err != nil {
			yield(gateway.StreamChunk{}, fmt.Errorf("openai: marshal stream request: %w", err))
			return
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			yield(gateway.StreamChunk{}, fmt.Errorf("openai: create stream request: %w", err))
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "text/event-stream")
		if p.cfg.APIKey != "" {
			httpReq.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
		}
		for k, v := range p.cfg.Headers {
			httpReq.Header.Set(k, v)
		}

		httpResp, err := p.httpClient.Do(httpReq)
		if err != nil {
			yield(gateway.StreamChunk{}, fmt.Errorf("openai: stream request failed: %w", err))
			return
		}
		defer func() { _ = httpResp.Body.Close() }()

		if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
			errBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20))
			yield(gateway.StreamChunk{}, &APIError{Status: httpResp.StatusCode, Body: strings.TrimSpace(string(errBody))})
			return
		}

		type accTool struct {
			id   string
			name string
			args strings.Builder
		}
		toolAcc := make(map[int]*accTool)
		var maxToolIdx = -1
		var finalUsage *gateway.Usage

		for ev, sseErr := range gateway.ReadSSE(httpResp.Body) {
			if sseErr != nil {
				yield(gateway.StreamChunk{}, sseErr)
				return
			}
			data := strings.TrimSpace(ev.Data)
			if data == "" {
				continue
			}
			if data == "[DONE]" {
				break
			}
			var frame struct {
				Choices []struct {
					Delta struct {
						Content   string `json:"content"`
						ToolCalls []struct {
							Index    int    `json:"index"`
							ID       string `json:"id"`
							Function struct {
								Name      string `json:"name"`
								Arguments string `json:"arguments"`
							} `json:"function"`
						} `json:"tool_calls"`
					} `json:"delta"`
				} `json:"choices"`
				Usage *struct {
					PromptTokens        int64 `json:"prompt_tokens"`
					CompletionTokens    int64 `json:"completion_tokens"`
					PromptTokensDetails struct {
						CachedTokens int64 `json:"cached_tokens"`
					} `json:"prompt_tokens_details"`
				} `json:"usage"`
			}
			if err := json.Unmarshal([]byte(data), &frame); err != nil {
				continue
			}
			if frame.Usage != nil {
				finalUsage = &gateway.Usage{
					InputTokens:  frame.Usage.PromptTokens,
					OutputTokens: frame.Usage.CompletionTokens,
					CachedTokens: frame.Usage.PromptTokensDetails.CachedTokens,
				}
			}
			if len(frame.Choices) > 0 {
				d := frame.Choices[0].Delta
				for _, tc := range d.ToolCalls {
					entry, ok := toolAcc[tc.Index]
					if !ok {
						entry = &accTool{}
						toolAcc[tc.Index] = entry
						if tc.Index > maxToolIdx {
							maxToolIdx = tc.Index
						}
					}
					if tc.ID != "" {
						entry.id = tc.ID
					}
					if tc.Function.Name != "" {
						entry.name = tc.Function.Name
					}
					entry.args.WriteString(tc.Function.Arguments)
				}
				if d.Content != "" {
					if !yield(gateway.StreamChunk{Text: d.Content}, nil) {
						return
					}
				}
			}
		}

		var calls []gateway.ToolCall
		for idx := 0; idx <= maxToolIdx; idx++ {
			if entry, ok := toolAcc[idx]; ok {
				rawArgs := entry.args.String()
				if rawArgs == "" {
					rawArgs = "{}"
				}
				calls = append(calls, gateway.ToolCall{
					ID:        entry.id,
					Name:      entry.name,
					Arguments: json.RawMessage(rawArgs),
				})
			}
		}
		yield(gateway.StreamChunk{
			ToolCalls: calls,
			Usage:     finalUsage,
			Done:      true,
		}, nil)
	}
}

func (p *Provider) BatchComplete(ctx context.Context, reqs []gateway.Request) ([]gateway.Response, []error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if p.cfg.EnableNativeBatch {
		return p.batchNative(ctx, reqs)
	}

	resps := make([]gateway.Response, len(reqs))
	errs := make([]error, len(reqs))
	sem := make(chan struct{}, p.cfg.MaxBatchConcurrency)
	var wg sync.WaitGroup

	for i, req := range reqs {
		wg.Add(1)
		go func(idx int, r gateway.Request) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errs[idx] = ctx.Err()
				return
			}
			resps[idx], errs[idx] = p.Complete(ctx, r)
		}(i, req)
	}
	wg.Wait()
	return resps, errs
}

func (p *Provider) batchNative(ctx context.Context, reqs []gateway.Request) ([]gateway.Response, []error) {
	var jsonl bytes.Buffer
	for i, r := range reqs {
		model := r.Model
		if model == "" {
			model = p.cfg.DefaultModel
		}
		line, err := json.Marshal(struct {
			CustomID string      `json:"custom_id"`
			Method   string      `json:"method"`
			URL      string      `json:"url"`
			Body     chatRequest `json:"body"`
		}{
			CustomID: fmt.Sprintf("req_%d", i),
			Method:   http.MethodPost,
			URL:      "/v1/chat/completions",
			Body:     buildChatRequest(model, r),
		})
		if err != nil {
			return failAll(len(reqs), fmt.Errorf("openai: marshal batch line: %w", err))
		}
		jsonl.Write(line)
		jsonl.WriteByte('\n')
	}

	// 1. Upload JSONL file.
	var mpBuf bytes.Buffer
	mw := multipart.NewWriter(&mpBuf)
	_ = mw.WriteField("purpose", "batch")
	fw, err := mw.CreateFormFile("file", "batch.jsonl")
	if err != nil {
		return failAll(len(reqs), err)
	}
	_, _ = fw.Write(jsonl.Bytes())
	_ = mw.Close()

	rawFile, err := p.doWithRetry(ctx, http.MethodPost, p.baseURL+"/files", mw.FormDataContentType(), mpBuf.Bytes())
	if err != nil {
		return failAll(len(reqs), err)
	}
	var uploaded struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rawFile, &uploaded); err != nil || uploaded.ID == "" {
		return failAll(len(reqs), fmt.Errorf("openai: decode uploaded file id: %w", err))
	}

	// 2. Create batch job.
	batchBody, _ := json.Marshal(struct {
		InputFileID      string `json:"input_file_id"`
		Endpoint         string `json:"endpoint"`
		CompletionWindow string `json:"completion_window"`
	}{
		InputFileID:      uploaded.ID,
		Endpoint:         "/v1/chat/completions",
		CompletionWindow: "24h",
	})
	rawBatch, err := p.doWithRetry(ctx, http.MethodPost, p.baseURL+"/batches", "application/json", batchBody)
	if err != nil {
		return failAll(len(reqs), err)
	}
	var job struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rawBatch, &job); err != nil || job.ID == "" {
		return failAll(len(reqs), fmt.Errorf("openai: decode batch job id: %w", err))
	}

	// 3. Poll until completed.
	ticker := time.NewTicker(p.cfg.BatchPollInterval)
	defer ticker.Stop()
	var outputFileID string
	for {
		select {
		case <-ctx.Done():
			return failAll(len(reqs), ctx.Err())
		case <-ticker.C:
			rawStatus, err := p.doWithRetry(ctx, http.MethodGet, p.baseURL+"/batches/"+job.ID, "", nil)
			if err != nil {
				return failAll(len(reqs), err)
			}
			var st struct {
				Status       string `json:"status"`
				OutputFileID string `json:"output_file_id"`
			}
			if err := json.Unmarshal(rawStatus, &st); err != nil {
				return failAll(len(reqs), fmt.Errorf("openai: decode batch status: %w", err))
			}
			switch st.Status {
			case "completed":
				outputFileID = st.OutputFileID
			case "failed", "expired", "cancelled":
				return failAll(len(reqs), fmt.Errorf("openai: batch %s ended with status %s", job.ID, st.Status))
			default:
				continue
			}
		}
		if outputFileID != "" {
			break
		}
	}

	// 4. Download output JSONL and map by custom_id.
	rawOut, err := p.doWithRetry(ctx, http.MethodGet, p.baseURL+"/files/"+outputFileID+"/content", "", nil)
	if err != nil {
		return failAll(len(reqs), err)
	}

	resps := make([]gateway.Response, len(reqs))
	errs := make([]error, len(reqs))
	for i := range errs {
		errs[i] = fmt.Errorf("openai: batch missing response %d", i)
	}

	scanner := bufio.NewScanner(bytes.NewReader(rawOut))
	scanner.Buffer(make([]byte, 64*1024), 16<<20)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var item struct {
			CustomID string `json:"custom_id"`
			Response *struct {
				StatusCode int             `json:"status_code"`
				Body       json.RawMessage `json:"body"`
			} `json:"response"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(line, &item); err != nil {
			continue
		}
		var idx int
		if _, scanErr := fmt.Sscanf(item.CustomID, "req_%d", &idx); scanErr != nil || idx < 0 || idx >= len(reqs) {
			continue
		}
		if item.Error != nil && item.Error.Message != "" {
			errs[idx] = fmt.Errorf("openai: batch item error: %s", item.Error.Message)
			continue
		}
		if item.Response == nil {
			continue
		}
		if item.Response.StatusCode >= 300 {
			errs[idx] = &APIError{Status: item.Response.StatusCode, Body: string(item.Response.Body)}
			continue
		}
		model := reqs[idx].Model
		if model == "" {
			model = p.cfg.DefaultModel
		}
		resps[idx], errs[idx] = parseChatResponse(model, item.Response.Body)
	}
	return resps, errs
}

func (p *Provider) doWithRetry(ctx context.Context, method, endpoint, contentType string, body []byte) ([]byte, error) {
	backoff := 500 * time.Millisecond
	maxRetries := p.cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}

	for attempt := 0; ; attempt++ {
		raw, err := p.doOnce(ctx, method, endpoint, contentType, body)
		var apiErr *APIError
		if err == nil || !errors.As(err, &apiErr) || !apiErr.Retryable() || attempt >= maxRetries {
			return raw, err
		}
		wait := backoff<<attempt + time.Duration(rand.Int64N(int64(backoff)))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *Provider) doOnce(ctx context.Context, method, endpoint, contentType string, body []byte) ([]byte, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("openai: create request: %w", err)
	}
	if contentType != "" {
		httpReq.Header.Set("Content-Type", contentType)
	}
	if p.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	}
	for k, v := range p.cfg.Headers {
		httpReq.Header.Set(k, v)
	}

	httpResp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("openai: request failed: %w", err)
	}
	defer func() { _ = httpResp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(httpResp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("openai: read response: %w", err)
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, &APIError{Status: httpResp.StatusCode, Body: strings.TrimSpace(string(respBody))}
	}
	return respBody, nil
}

func buildChatRequest(model string, req gateway.Request) chatRequest {
	msgs := make([]oaiMessage, 0, len(req.Messages)+1)
	if req.SystemPrefix != "" {
		msgs = append(msgs, oaiMessage{Role: string(gateway.MsgSystem), Content: req.SystemPrefix})
	}
	for _, m := range req.Messages {
		cm := oaiMessage{
			Role:       string(m.Role),
			ToolCallID: m.ToolCallID,
			Name:       m.Name,
		}
		if len(m.Parts) > 0 {
			parts := make([]any, 0, 1+len(m.Parts))
			if m.Content != "" {
				parts = append(parts, map[string]any{"type": "text", "text": m.Content})
			}
			for _, pt := range m.Parts {
				switch {
				case pt.Text != "":
					parts = append(parts, map[string]any{"type": "text", "text": pt.Text})
				case len(pt.Data) > 0 && strings.HasPrefix(pt.MIMEType, "audio/"):
					format := strings.TrimPrefix(pt.MIMEType, "audio/")
					if format == "mpeg" {
						format = "mp3"
					}
					parts = append(parts, map[string]any{
						"type": "input_audio",
						"input_audio": map[string]string{
							"data":   base64.StdEncoding.EncodeToString(pt.Data),
							"format": format,
						},
					})
				case len(pt.Data) > 0:
					dataURL := fmt.Sprintf("data:%s;base64,%s", pt.MIMEType, base64.StdEncoding.EncodeToString(pt.Data))
					parts = append(parts, map[string]any{
						"type":      "image_url",
						"image_url": map[string]string{"url": dataURL},
					})
				case pt.FileURI != "":
					parts = append(parts, map[string]any{
						"type":      "image_url",
						"image_url": map[string]string{"url": pt.FileURI},
					})
				}
			}
			cm.Content = parts
		} else {
			cm.Content = m.Content
		}
		if len(m.ToolCalls) > 0 {
			cm.ToolCalls = make([]oaiToolCall, len(m.ToolCalls))
			for i, tc := range m.ToolCalls {
				cm.ToolCalls[i] = oaiToolCall{
					ID:   tc.ID,
					Type: "function",
					Function: oaiToolCallFunction{
						Name:      tc.Name,
						Arguments: string(tc.Arguments),
					},
				}
			}
		}
		msgs = append(msgs, cm)
	}

	payload := chatRequest{
		Model:       model,
		Messages:    msgs,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}
	if len(req.ResponseModalities) > 0 {
		mods := make([]string, len(req.ResponseModalities))
		hasAudio := false
		for i, m := range req.ResponseModalities {
			mods[i] = strings.ToLower(string(m))
			if m == gateway.ModalityAudio {
				hasAudio = true
			}
		}
		payload.Modalities = mods
		if hasAudio {
			voice := "alloy"
			if req.SpeechConfig != nil && req.SpeechConfig.VoiceName != "" {
				voice = req.SpeechConfig.VoiceName
			}
			payload.Audio = &oaiAudioConfig{Voice: voice, Format: "wav"}
		}
	}
	if len(req.Tools) > 0 {
		payload.Tools = make([]oaiTool, len(req.Tools))
		for i, t := range req.Tools {
			payload.Tools[i] = oaiTool{
				Type: "function",
				Function: oaiToolFunction{
					Name:        t.Name,
					Description: t.Description,
					Parameters:  t.Parameters,
				},
			}
		}
	}
	if req.ResponseSchema != nil {
		normSchema, strictOK := req.ResponseSchema.OpenAINormalize()
		payload.ResponseFormat = &oaiResponseFormat{
			Type: "json_schema",
			JSONSchema: &oaiJSONSchemaSpec{
				Name:   "response",
				Strict: strictOK,
				Schema: normSchema,
			},
		}
	}
	return payload
}

func parseChatResponse(model string, raw []byte) (gateway.Response, error) {
	var parsed struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content   string        `json:"content"`
				ToolCalls []oaiToolCall `json:"tool_calls"`
				Audio     *struct {
					Data       string `json:"data"`
					Transcript string `json:"transcript"`
				} `json:"audio"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens        int64 `json:"prompt_tokens"`
			CompletionTokens    int64 `json:"completion_tokens"`
			PromptTokensDetails struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
		Error *struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return gateway.Response{}, fmt.Errorf("openai: decode response: %w", err)
	}
	if parsed.Error != nil && parsed.Error.Message != "" {
		return gateway.Response{}, fmt.Errorf("openai: api error (%s): %s", parsed.Error.Type, parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 {
		return gateway.Response{}, fmt.Errorf("openai: empty choices in response")
	}

	choice := parsed.Choices[0]
	out := gateway.Response{
		Text:         choice.Message.Content,
		FinishReason: choice.FinishReason,
		Model:        parsed.Model,
		Usage: gateway.Usage{
			InputTokens:  parsed.Usage.PromptTokens,
			OutputTokens: parsed.Usage.CompletionTokens,
			CachedTokens: parsed.Usage.PromptTokensDetails.CachedTokens,
		},
	}
	if choice.Message.Audio != nil {
		if out.Text == "" {
			out.Text = choice.Message.Audio.Transcript
		}
		if choice.Message.Audio.Data != "" {
			rawAudio, err := base64.StdEncoding.DecodeString(choice.Message.Audio.Data)
			if err != nil {
				return gateway.Response{}, fmt.Errorf("openai: decode audio: %w", err)
			}
			out.Parts = append(out.Parts, gateway.Part{
				MIMEType: "audio/wav",
				Data:     rawAudio,
			})
		}
	}
	if out.Model == "" {
		out.Model = model
	}
	if len(choice.Message.ToolCalls) > 0 {
		out.ToolCalls = make([]gateway.ToolCall, len(choice.Message.ToolCalls))
		for i, tc := range choice.Message.ToolCalls {
			out.ToolCalls[i] = gateway.ToolCall{
				ID:        tc.ID,
				Name:      tc.Function.Name,
				Arguments: json.RawMessage(tc.Function.Arguments),
			}
		}
	}
	return out, nil
}

func failAll(n int, err error) ([]gateway.Response, []error) {
	resps := make([]gateway.Response, n)
	errs := make([]error, n)
	for i := range errs {
		errs[i] = err
	}
	return resps, errs
}

type chatRequest struct {
	Model          string             `json:"model"`
	Messages       []oaiMessage       `json:"messages"`
	Tools          []oaiTool          `json:"tools,omitempty"`
	ResponseFormat *oaiResponseFormat `json:"response_format,omitempty"`
	Modalities     []string           `json:"modalities,omitempty"`
	Audio          *oaiAudioConfig    `json:"audio,omitempty"`
	Temperature    *float64           `json:"temperature,omitempty"`
	MaxTokens      int                `json:"max_tokens,omitempty"`
	Stream         bool               `json:"stream,omitempty"`
	StreamOptions  *oaiStreamOptions  `json:"stream_options,omitempty"`
}

type oaiStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type oaiAudioConfig struct {
	Voice  string `json:"voice"`
	Format string `json:"format"`
}

type oaiMessage struct {
	Role       string        `json:"role"`
	Content    any           `json:"content"`
	ToolCalls  []oaiToolCall `json:"tool_calls,omitempty"`
	ToolCallID string        `json:"tool_call_id,omitempty"`
	Name       string        `json:"name,omitempty"`
}

type oaiTool struct {
	Type     string          `json:"type"`
	Function oaiToolFunction `json:"function"`
}

type oaiToolFunction struct {
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Parameters  *gateway.JSONSchema `json:"parameters,omitempty"`
}

type oaiResponseFormat struct {
	Type       string             `json:"type"`
	JSONSchema *oaiJSONSchemaSpec `json:"json_schema,omitempty"`
}

type oaiJSONSchemaSpec struct {
	Name   string              `json:"name"`
	Strict bool                `json:"strict,omitempty"`
	Schema *gateway.JSONSchema `json:"schema"`
}

type oaiToolCall struct {
	ID       string              `json:"id"`
	Type     string              `json:"type"`
	Function oaiToolCallFunction `json:"function"`
}

type oaiToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

