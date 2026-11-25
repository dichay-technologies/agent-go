package gemini

import (
	"encoding/json"
	"fmt"
	"strings"

	"proj.dichay.tech/agent-go/gateway"
)

// Dialect implements cloudflare.Dialect for Google Gemini
// routed through Cloudflare AI Gateway (/google-ai-studio/v1beta/models/{model}:generateContent).
type Dialect struct{}

func (d Dialect) Name() string { return "gemini" }

func (d Dialect) BuildRequest(model string, req gateway.Request) (string, map[string]string, []byte, error) {
	type gemPart struct {
		Text string `json:"text,omitempty"`
	}
	type gemContent struct {
		Role  string    `json:"role,omitempty"`
		Parts []gemPart `json:"parts"`
	}
	type gemGenConfig struct {
		Temperature      *float64            `json:"temperature,omitempty"`
		MaxOutputTokens  int                 `json:"maxOutputTokens,omitempty"`
		ResponseMimeType string              `json:"responseMimeType,omitempty"`
		ResponseSchema   *gateway.JSONSchema `json:"responseSchema,omitempty"`
	}

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
		role := "user"
		if m.Role == gateway.MsgAssistant {
			role = "model"
		}
		contents = append(contents, gemContent{
			Role:  role,
			Parts: []gemPart{{Text: m.Content}},
		})
	}

	var genCfg *gemGenConfig
	if req.Temperature != nil || req.MaxTokens > 0 || req.ResponseSchema != nil {
		genCfg = &gemGenConfig{
			Temperature:     req.Temperature,
			MaxOutputTokens: req.MaxTokens,
		}
		if req.ResponseSchema != nil {
			genCfg.ResponseMimeType = "application/json"
			genCfg.ResponseSchema = req.ResponseSchema
		}
	}

	payload := struct {
		SystemInstruction *gemContent   `json:"systemInstruction,omitempty"`
		Contents          []gemContent  `json:"contents"`
		GenerationConfig  *gemGenConfig `json:"generationConfig,omitempty"`
	}{
		SystemInstruction: sysInst,
		Contents:          contents,
		GenerationConfig:  genCfg,
	}

	b, err := json.Marshal(payload)
	suffix := fmt.Sprintf("v1beta/models/%s:generateContent", strings.TrimLeft(model, "/"))
	return suffix, nil, b, err
}

func (d Dialect) ParseResponse(model string, raw []byte) (gateway.Response, error) {
	var parsed struct {
		Candidates []struct {
			FinishReason string `json:"finishReason"`
			Content      struct {
				Parts []struct {
					Text         string `json:"text"`
					FunctionCall *struct {
						Name string          `json:"name"`
						Args json.RawMessage `json:"args"`
					} `json:"functionCall"`
				} `json:"parts"`
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
		return gateway.Response{}, fmt.Errorf("decode gemini response: %w", err)
	}
	if parsed.Error != nil && parsed.Error.Message != "" {
		return gateway.Response{}, fmt.Errorf("gemini error: %s", parsed.Error.Message)
	}
	if len(parsed.Candidates) == 0 {
		return gateway.Response{}, fmt.Errorf("empty candidates in gemini response")
	}

	cand := parsed.Candidates[0]
	var texts []string
	var calls []gateway.ToolCall
	for i, part := range cand.Content.Parts {
		if part.Text != "" {
			texts = append(texts, part.Text)
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

