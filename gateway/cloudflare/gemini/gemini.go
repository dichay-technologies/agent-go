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
	type gemFuncCall struct {
		Name string          `json:"name"`
		Args json.RawMessage `json:"args,omitempty"`
	}
	type gemFuncResp struct {
		Name     string          `json:"name"`
		Response json.RawMessage `json:"response"`
	}
	type gemPart struct {
		Text             string       `json:"text,omitempty"`
		FunctionCall     *gemFuncCall `json:"functionCall,omitempty"`
		FunctionResponse *gemFuncResp `json:"functionResponse,omitempty"`
	}
	type gemContent struct {
		Role  string    `json:"role,omitempty"`
		Parts []gemPart `json:"parts"`
	}
	type gemFuncDecl struct {
		Name        string              `json:"name"`
		Description string              `json:"description,omitempty"`
		Parameters  *gateway.JSONSchema `json:"parameters,omitempty"`
	}
	type gemTool struct {
		FunctionDeclarations []gemFuncDecl `json:"functionDeclarations"`
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
					FunctionResponse: &gemFuncResp{Name: fnName, Response: respBytes},
				}},
			})
			continue
		}
		role := "user"
		if m.Role == gateway.MsgAssistant {
			role = "model"
		}
		var parts []gemPart
		if m.Content != "" {
			parts = append(parts, gemPart{Text: m.Content})
		}
		for _, tc := range m.ToolCalls {
			parts = append(parts, gemPart{
				FunctionCall: &gemFuncCall{Name: tc.Name, Args: tc.Arguments},
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
		Tools             []gemTool     `json:"tools,omitempty"`
		GenerationConfig  *gemGenConfig `json:"generationConfig,omitempty"`
	}{
		SystemInstruction: sysInst,
		Contents:          contents,
		Tools:             tools,
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
