package gemini

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"proj.dichay.tech/agent-go/gateway"
)

const (
	DefaultBaseURL           = "https://generativelanguage.googleapis.com"
	DefaultModel             = "gemini-2.5-flash"
	defaultTimeout           = 60 * time.Second
	defaultConcurrency       = 16
	defaultBatchPollInterval = 500 * time.Millisecond
	defaultContextCacheTTL   = 5 * time.Minute
)

// Config configures the standalone Google Gemini provider.
type Config struct {
	// Name overrides the registered provider name (defaults to "gemini").
	Name string
	// APIKey is the Gemini API key. If empty, reads GEMINI_API_KEY from env.
	APIKey string
	// BaseURL overrides DefaultBaseURL (useful for testing or proxies).
	BaseURL string
	// DefaultModel overrides DefaultModel / GEMINI_MODEL.
	DefaultModel string
	// HTTPClient overrides the default http.Client.
	HTTPClient *http.Client
	// MaxBatchConcurrency caps concurrent HTTP calls in fan-out BatchComplete (defaults to 16).
	MaxBatchConcurrency int

	// EnableContextCache automatically creates/reuses Gemini cachedContents resources
	// for SystemPrefix (+ Tools) when SystemPrefix is non-empty.
	EnableContextCache bool
	// ContextCacheTTL sets the TTL for auto-created cachedContents (defaults to 5m).
	ContextCacheTTL time.Duration

	// EnableNativeBatch uses Gemini's asynchronous batchGenerateContent API in BatchComplete
	// (50% cost discount, higher latency) instead of concurrent fan-out.
	EnableNativeBatch bool
	// BatchPollInterval controls how often batch job status is polled (defaults to 500ms).
	BatchPollInterval time.Duration
}

type cachedRef struct {
	name      string
	expiresAt time.Time
}

// Provider implements gateway.Provider and gateway.BatchProvider for Google Gemini.
type Provider struct {
	name       string
	cfg        Config
	baseURL    string
	httpClient *http.Client

	cacheMu sync.Mutex
	caches  map[string]cachedRef
}

// New creates a standalone Gemini Provider.
func New(cfg Config) (*Provider, error) {
	if cfg.APIKey == "" {
		cfg.APIKey = os.Getenv("GEMINI_API_KEY")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("gemini: APIKey is required")
	}
	if cfg.Name == "" {
		cfg.Name = "gemini"
	}
	if cfg.DefaultModel == "" {
		if m := os.Getenv("GEMINI_MODEL"); m != "" {
			cfg.DefaultModel = m
		} else {
			cfg.DefaultModel = DefaultModel
		}
	}
	if cfg.MaxBatchConcurrency <= 0 {
		cfg.MaxBatchConcurrency = defaultConcurrency
	}
	if cfg.ContextCacheTTL <= 0 {
		cfg.ContextCacheTTL = defaultContextCacheTTL
	}
	if cfg.BatchPollInterval <= 0 {
		cfg.BatchPollInterval = defaultBatchPollInterval
	}
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}

	return &Provider{
		name:       cfg.Name,
		cfg:        cfg,
		baseURL:    baseURL,
		httpClient: client,
		caches:     make(map[string]cachedRef),
	}, nil
}

func (p *Provider) Name() string { return p.name }

func (p *Provider) Complete(ctx context.Context, req gateway.Request) (gateway.Response, error) {
	model := req.Model
	if model == "" {
		model = p.cfg.DefaultModel
	}

	payload := buildRequest(req)
	if p.cfg.EnableContextCache && !req.BypassCache && req.SystemPrefix != "" {
		if cacheName, err := p.ensureContextCache(ctx, model, payload.SystemInstruction, payload.Tools); err == nil && cacheName != "" {
			payload.CachedContent = cacheName
			payload.SystemInstruction = nil
			payload.Tools = nil
		}
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return gateway.Response{}, fmt.Errorf("gemini: marshal request: %w", err)
	}

	endpoint := fmt.Sprintf("%s/v1beta/models/%s:generateContent", p.baseURL, strings.TrimLeft(model, "/"))
	respBody, err := p.doHTTP(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return gateway.Response{}, err
	}
	return parseResponse(model, respBody)
}

// Stream implements gateway.StreamProvider using Gemini's streamGenerateContent?alt=sse endpoint.
func (p *Provider) Stream(ctx context.Context, req gateway.Request) iter.Seq2[gateway.StreamChunk, error] {
	return func(yield func(gateway.StreamChunk, error) bool) {
		model := req.Model
		if model == "" {
			model = p.cfg.DefaultModel
		}
		payload := buildRequest(req)
		if p.cfg.EnableContextCache && !req.BypassCache && req.SystemPrefix != "" {
			if cacheName, err := p.ensureContextCache(ctx, model, payload.SystemInstruction, payload.Tools); err == nil && cacheName != "" {
				payload.CachedContent = cacheName
				payload.SystemInstruction = nil
				payload.Tools = nil
			}
		}
		body, err := json.Marshal(payload)
		if err != nil {
			yield(gateway.StreamChunk{}, fmt.Errorf("gemini: marshal stream request: %w", err))
			return
		}

		endpoint := fmt.Sprintf("%s/v1beta/models/%s:streamGenerateContent?alt=sse", p.baseURL, strings.TrimLeft(model, "/"))
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			yield(gateway.StreamChunk{}, fmt.Errorf("gemini: create stream request: %w", err))
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "text/event-stream")
		httpReq.Header.Set("x-goog-api-key", p.cfg.APIKey)

		httpResp, err := p.httpClient.Do(httpReq)
		if err != nil {
			yield(gateway.StreamChunk{}, fmt.Errorf("gemini: stream request failed: %w", err))
			return
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
			errBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20))
			yield(gateway.StreamChunk{}, gateway.ClassifyStatusCode(p.name, httpResp.StatusCode, strings.TrimSpace(string(errBody))))
			return
		}

		var calls []gateway.ToolCall
		var lastUsage *gateway.Usage

		for ev, sseErr := range gateway.ReadSSE(httpResp.Body) {
			if sseErr != nil {
				yield(gateway.StreamChunk{}, sseErr)
				return
			}
			data := strings.TrimSpace(ev.Data)
			if data == "" {
				continue
			}
			parsed, perr := parseResponse(model, []byte(data))
			if perr != nil {
				continue
			}
			if parsed.Usage.InputTokens > 0 || parsed.Usage.OutputTokens > 0 || parsed.Usage.CachedTokens > 0 {
				u := parsed.Usage
				lastUsage = &u
			}
			if len(parsed.ToolCalls) > 0 {
				calls = append(calls, parsed.ToolCalls...)
			}
			if parsed.Text != "" {
				if !yield(gateway.StreamChunk{Text: parsed.Text}, nil) {
					return
				}
			}
		}

		yield(gateway.StreamChunk{
			ToolCalls: calls,
			Usage:     lastUsage,
			Done:      true,
		}, nil)
	}
}

// CreateContextCache explicitly creates a Gemini cachedContents resource and returns its name.
func (p *Provider) CreateContextCache(ctx context.Context, model, systemPrefix string, contents []gateway.Message, tools []gateway.ToolSpec, ttl time.Duration) (string, error) {
	if model == "" {
		model = p.cfg.DefaultModel
	}
	if ttl <= 0 {
		ttl = p.cfg.ContextCacheTTL
	}
	req := buildRequest(gateway.Request{
		SystemPrefix: systemPrefix,
		Messages:     contents,
		Tools:        tools,
	})
	return p.createCacheResource(ctx, model, req.SystemInstruction, req.Contents, req.Tools, ttl)
}

func (p *Provider) ensureContextCache(ctx context.Context, model string, sysInst *gemContent, tools []gemTool) (string, error) {
	keyRaw, _ := json.Marshal(struct {
		Model string
		Sys   *gemContent
		Tools []gemTool
	}{Model: model, Sys: sysInst, Tools: tools})
	sum := sha256.Sum256(keyRaw)
	key := hex.EncodeToString(sum[:16])

	p.cacheMu.Lock()
	if ref, ok := p.caches[key]; ok && time.Now().Before(ref.expiresAt) {
		p.cacheMu.Unlock()
		return ref.name, nil
	}
	p.cacheMu.Unlock()

	name, err := p.createCacheResource(ctx, model, sysInst, nil, tools, p.cfg.ContextCacheTTL)
	if err != nil {
		return "", err
	}

	p.cacheMu.Lock()
	p.caches[key] = cachedRef{
		name:      name,
		expiresAt: time.Now().Add(p.cfg.ContextCacheTTL - 5*time.Second),
	}
	p.cacheMu.Unlock()
	return name, nil
}

func (p *Provider) createCacheResource(ctx context.Context, model string, sysInst *gemContent, contents []gemContent, tools []gemTool, ttl time.Duration) (string, error) {
	modelPath := "models/" + strings.TrimPrefix(strings.TrimLeft(model, "/"), "models/")
	payload := struct {
		Model             string       `json:"model"`
		SystemInstruction *gemContent  `json:"systemInstruction,omitempty"`
		Contents          []gemContent `json:"contents,omitempty"`
		Tools             []gemTool    `json:"tools,omitempty"`
		TTL               string       `json:"ttl"`
	}{
		Model:             modelPath,
		SystemInstruction: sysInst,
		Contents:          contents,
		Tools:             tools,
		TTL:               fmt.Sprintf("%ds", max(1, int(ttl.Seconds()))),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	raw, err := p.doHTTP(ctx, http.MethodPost, p.baseURL+"/v1beta/cachedContents", body)
	if err != nil {
		return "", err
	}
	var out struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("gemini: decode cachedContents: %w", err)
	}
	return out.Name, nil
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
	model := reqs[0].Model
	if model == "" {
		model = p.cfg.DefaultModel
	}
	modelID := strings.TrimPrefix(strings.TrimLeft(model, "/"), "models/")

	type inlinedReq struct {
		Request  gemRequest        `json:"request"`
		Metadata map[string]string `json:"metadata,omitempty"`
	}
	inlined := make([]inlinedReq, len(reqs))
	for i, r := range reqs {
		inlined[i] = inlinedReq{
			Request:  buildRequest(r),
			Metadata: map[string]string{"custom_id": fmt.Sprintf("req_%d", i)},
		}
	}

	payload := struct {
		Batch struct {
			DisplayName string `json:"displayName,omitempty"`
			InputConfig struct {
				Requests struct {
					Requests []inlinedReq `json:"requests"`
				} `json:"requests"`
			} `json:"inputConfig"`
		} `json:"batch"`
	}{}
	payload.Batch.DisplayName = "agentgo-batch"
	payload.Batch.InputConfig.Requests.Requests = inlined

	body, err := json.Marshal(payload)
	if err != nil {
		return failAll(len(reqs), fmt.Errorf("gemini: marshal batch: %w", err))
	}

	submitURL := fmt.Sprintf("%s/v1beta/models/%s:batchGenerateContent", p.baseURL, modelID)
	rawSubmit, err := p.doHTTP(ctx, http.MethodPost, submitURL, body)
	if err != nil {
		return failAll(len(reqs), err)
	}

	var batchJob struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}
	if err := json.Unmarshal(rawSubmit, &batchJob); err != nil {
		return failAll(len(reqs), fmt.Errorf("gemini: decode batch submit: %w", err))
	}
	if batchJob.Name == "" {
		return failAll(len(reqs), fmt.Errorf("gemini: batch submit missing job name"))
	}

	pollURL := fmt.Sprintf("%s/v1beta/%s", p.baseURL, strings.TrimLeft(batchJob.Name, "/"))
	ticker := time.NewTicker(p.cfg.BatchPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return failAll(len(reqs), ctx.Err())
		case <-ticker.C:
			rawPoll, err := p.doHTTP(ctx, http.MethodGet, pollURL, nil)
			if err != nil {
				return failAll(len(reqs), err)
			}
			done, resps, errs, err := parseBatchPoll(model, len(reqs), rawPoll)
			if err != nil {
				return failAll(len(reqs), err)
			}
			if done {
				return resps, errs
			}
		}
	}
}

func parseBatchPoll(model string, count int, raw []byte) (bool, []gateway.Response, []error, error) {
	var job struct {
		State string `json:"state"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error,omitempty"`
		Output *struct {
			InlinedResponses *struct {
				InlinedResponses []struct {
					Metadata map[string]string `json:"metadata"`
					Response json.RawMessage   `json:"response"`
					Error    *struct {
						Message string `json:"message"`
					} `json:"error,omitempty"`
				} `json:"inlinedResponses"`
			} `json:"inlinedResponses"`
		} `json:"output,omitempty"`
	}
	if err := json.Unmarshal(raw, &job); err != nil {
		return false, nil, nil, fmt.Errorf("gemini: decode batch poll: %w", err)
	}

	switch job.State {
	case "", "BATCH_STATE_PENDING", "BATCH_STATE_RUNNING", "BATCH_STATE_UNSPECIFIED":
		return false, nil, nil, nil
	case "BATCH_STATE_FAILED", "BATCH_STATE_CANCELLED", "BATCH_STATE_EXPIRED":
		msg := job.State
		if job.Error != nil && job.Error.Message != "" {
			msg += ": " + job.Error.Message
		}
		return true, nil, nil, fmt.Errorf("gemini: batch job %s", msg)
	}

	resps := make([]gateway.Response, count)
	errs := make([]error, count)
	for i := range errs {
		errs[i] = fmt.Errorf("gemini: batch missing response %d", i)
	}

	if job.Output != nil && job.Output.InlinedResponses != nil {
		for i, item := range job.Output.InlinedResponses.InlinedResponses {
			idx := i
			if cid, ok := item.Metadata["custom_id"]; ok {
				var parsedIdx int
				if _, scanErr := fmt.Sscanf(cid, "req_%d", &parsedIdx); scanErr == nil {
					idx = parsedIdx
				}
			}
			if idx < 0 || idx >= count {
				continue
			}
			if item.Error != nil && item.Error.Message != "" {
				errs[idx] = fmt.Errorf("gemini: batch item error: %s", item.Error.Message)
				continue
			}
			r, err := parseResponse(model, item.Response)
			resps[idx] = r
			errs[idx] = err
		}
	}
	return true, resps, errs, nil
}

func (p *Provider) doHTTP(ctx context.Context, method, endpoint string, body []byte) ([]byte, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("gemini: create request: %w", err)
	}
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	httpReq.Header.Set("x-goog-api-key", p.cfg.APIKey)

	httpResp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("gemini: request failed: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(httpResp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("gemini: read response: %w", err)
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, gateway.ClassifyStatusCode(p.name, httpResp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return respBody, nil
}

func failAll(n int, err error) ([]gateway.Response, []error) {
	resps := make([]gateway.Response, n)
	errs := make([]error, n)
	for i := range errs {
		errs[i] = err
	}
	return resps, errs
}

func buildRequest(req gateway.Request) gemRequest {
	var sysInst *gemContent
	if req.SystemPrefix != "" {
		sysInst = &gemContent{Parts: []gemPart{{Text: req.SystemPrefix}}}
	}

	contents := make([]gemContent, 0, len(req.Messages))
	for _, m := range req.Messages {
		if m.Role == gateway.MsgSystem {
			if sysInst == nil {
				sysInst = &gemContent{}
			}
			sysInst.Parts = append(sysInst.Parts, gemPart{Text: m.Content})
			continue
		}
		if m.Role == gateway.MsgTool {
			fnName := m.Name
			if fnName == "" {
				fnName = m.ToolCallID
			}
			respObj := map[string]any{"result": m.Content}
			var decoded map[string]any
			if json.Unmarshal([]byte(m.Content), &decoded) == nil && decoded != nil {
				respObj = decoded
			}
			respBytes, _ := json.Marshal(respObj)
			contents = append(contents, gemContent{
				Role: "user",
				Parts: []gemPart{{
					FunctionResponse: &gemFunctionResponse{
						Name:     fnName,
						Response: respBytes,
					},
				}},
			})
			continue
		}
		role := "user"
		if m.Role == gateway.MsgAssistant {
			role = "model"
		}
		parts := make([]gemPart, 0, 1+len(m.Parts)+len(m.ToolCalls))
		if m.Content != "" {
			parts = append(parts, gemPart{Text: m.Content})
		}
		for _, p := range m.Parts {
			gp := gemPart{Text: p.Text}
			if len(p.Data) > 0 {
				gp.InlineData = &gemBlob{
					MIMEType: p.MIMEType,
					Data:     base64.StdEncoding.EncodeToString(p.Data),
				}
			} else if p.FileURI != "" {
				gp.FileData = &gemFileData{
					MIMEType: p.MIMEType,
					FileURI:  p.FileURI,
				}
			}
			parts = append(parts, gp)
		}
		for _, tc := range m.ToolCalls {
			parts = append(parts, gemPart{
				FunctionCall: &gemFunctionCall{
					Name: tc.Name,
					Args: tc.Arguments,
				},
			})
		}
		if len(parts) > 0 {
			contents = append(contents, gemContent{Role: role, Parts: parts})
		}
	}

	var tools []gemTool
	if len(req.Tools) > 0 {
		decls := make([]gemFuncDecl, len(req.Tools))
		for i, t := range req.Tools {
			decls[i] = gemFuncDecl{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  t.Parameters,
			}
		}
		tools = []gemTool{{FunctionDeclarations: decls}}
	}

	var genCfg *gemGenConfig
	if req.Temperature != nil || req.MaxTokens > 0 || req.ResponseSchema != nil || len(req.ResponseModalities) > 0 || req.SpeechConfig != nil {
		genCfg = &gemGenConfig{
			Temperature:     req.Temperature,
			MaxOutputTokens: req.MaxTokens,
		}
		if req.ResponseSchema != nil {
			genCfg.ResponseMimeType = "application/json"
			genCfg.ResponseSchema = req.ResponseSchema
		}
		if len(req.ResponseModalities) > 0 {
			genCfg.ResponseModalities = make([]string, len(req.ResponseModalities))
			for i, m := range req.ResponseModalities {
				genCfg.ResponseModalities[i] = string(m)
			}
		}
		if req.SpeechConfig != nil && req.SpeechConfig.VoiceName != "" {
			genCfg.SpeechConfig = &gemSpeechConfig{
				VoiceConfig: &gemVoiceConfig{
					PrebuiltVoiceConfig: &gemPrebuiltVoice{
						VoiceName: req.SpeechConfig.VoiceName,
					},
				},
			}
		}
	}

	return gemRequest{
		SystemInstruction: sysInst,
		Contents:          contents,
		Tools:             tools,
		GenerationConfig:  genCfg,
	}
}

func parseResponse(model string, raw []byte) (gateway.Response, error) {
	var parsed struct {
		Candidates []struct {
			FinishReason string `json:"finishReason"`
			Content      struct {
				Parts []gemPart `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		UsageMetadata struct {
			PromptTokenCount        int64 `json:"promptTokenCount"`
			CandidatesTokenCount    int64 `json:"candidatesTokenCount"`
			CachedContentTokenCount int64 `json:"cachedContentTokenCount"`
		} `json:"usageMetadata"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return gateway.Response{}, fmt.Errorf("gemini: decode response: %w", err)
	}
	if parsed.Error != nil && parsed.Error.Message != "" {
		return gateway.Response{}, fmt.Errorf("gemini: api error: %s", parsed.Error.Message)
	}
	if len(parsed.Candidates) == 0 {
		return gateway.Response{}, fmt.Errorf("gemini: empty candidates in response")
	}

	cand := parsed.Candidates[0]
	var texts []string
	var outParts []gateway.Part
	var calls []gateway.ToolCall
	for i, part := range cand.Content.Parts {
		if part.Text != "" {
			texts = append(texts, part.Text)
			outParts = append(outParts, gateway.Part{Text: part.Text})
		}
		if part.InlineData != nil {
			rawBytes, err := base64.StdEncoding.DecodeString(part.InlineData.Data)
			if err != nil {
				return gateway.Response{}, fmt.Errorf("gemini: decode inlineData: %w", err)
			}
			outParts = append(outParts, gateway.Part{
				MIMEType: part.InlineData.MIMEType,
				Data:     rawBytes,
			})
		}
		if part.FileData != nil {
			outParts = append(outParts, gateway.Part{
				MIMEType: part.FileData.MIMEType,
				FileURI:  part.FileData.FileURI,
			})
		}
		if part.FunctionCall != nil {
			calls = append(calls, gateway.ToolCall{
				ID:        fmt.Sprintf("gem_call_%d", i),
				Name:      part.FunctionCall.Name,
				Arguments: part.FunctionCall.Args,
			})
		}
	}

	return gateway.Response{
		Text:         strings.Join(texts, ""),
		Parts:        outParts,
		ToolCalls:    calls,
		FinishReason: cand.FinishReason,
		Model:        model,
		Usage: gateway.Usage{
			InputTokens:  parsed.UsageMetadata.PromptTokenCount,
			OutputTokens: parsed.UsageMetadata.CandidatesTokenCount,
			CachedTokens: parsed.UsageMetadata.CachedContentTokenCount,
		},
	}, nil
}

type gemRequest struct {
	CachedContent     string        `json:"cachedContent,omitempty"`
	SystemInstruction *gemContent   `json:"systemInstruction,omitempty"`
	Contents          []gemContent  `json:"contents"`
	Tools             []gemTool     `json:"tools,omitempty"`
	GenerationConfig  *gemGenConfig `json:"generationConfig,omitempty"`
}

type gemContent struct {
	Role  string    `json:"role,omitempty"`
	Parts []gemPart `json:"parts"`
}

type gemPart struct {
	Text             string               `json:"text,omitempty"`
	InlineData       *gemBlob             `json:"inlineData,omitempty"`
	FileData         *gemFileData         `json:"fileData,omitempty"`
	FunctionCall     *gemFunctionCall     `json:"functionCall,omitempty"`
	FunctionResponse *gemFunctionResponse `json:"functionResponse,omitempty"`
}

type gemFunctionResponse struct {
	Name     string          `json:"name"`
	Response json.RawMessage `json:"response"`
}

type gemBlob struct {
	MIMEType string `json:"mimeType"`
	Data     string `json:"data"`
}

type gemFileData struct {
	MIMEType string `json:"mimeType,omitempty"`
	FileURI  string `json:"fileUri"`
}

type gemFunctionCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
}

type gemTool struct {
	FunctionDeclarations []gemFuncDecl `json:"functionDeclarations"`
}

type gemFuncDecl struct {
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Parameters  *gateway.JSONSchema `json:"parameters,omitempty"`
}

type gemGenConfig struct {
	Temperature        *float64            `json:"temperature,omitempty"`
	MaxOutputTokens    int                 `json:"maxOutputTokens,omitempty"`
	ResponseMimeType   string              `json:"responseMimeType,omitempty"`
	ResponseSchema     *gateway.JSONSchema `json:"responseSchema,omitempty"`
	ResponseModalities []string            `json:"responseModalities,omitempty"`
	SpeechConfig       *gemSpeechConfig    `json:"speechConfig,omitempty"`
}

type gemSpeechConfig struct {
	VoiceConfig *gemVoiceConfig `json:"voiceConfig,omitempty"`
}

type gemVoiceConfig struct {
	PrebuiltVoiceConfig *gemPrebuiltVoice `json:"prebuiltVoiceConfig,omitempty"`
}

type gemPrebuiltVoice struct {
	VoiceName string `json:"voiceName"`
}

