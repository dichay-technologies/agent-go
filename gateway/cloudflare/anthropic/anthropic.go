package anthropic

import (
	"encoding/json"
	"fmt"
	"strings"

	"proj.dichay.tech/agent-go/gateway"
)

// Dialect implements cloudflare.Dialect for Anthropic Messages API
// routed through Cloudflare AI Gateway (/anthropic/v1/messages).
type Dialect struct {
	// APIKey sets x-api-key header.
	APIKey string
	// Version sets anthropic-version header (defaults to "2023-06-01").
	Version string
	// EnablePromptCache attaches ephemeral cache_control to SystemPrefix blocks.
	EnablePromptCache bool
}

func (d Dialect) Name() string { return "anthropic" }

func (d Dialect) BuildRequest(model string, req gateway.Request) (string, map[string]string, []byte, error) {
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	version := d.Version
	if version == "" {
		version = "2023-06-01"
	}

	type cacheControl struct {
		Type string `json:"type"`
	}
	type systemBlock struct {
		Type         string        `json:"type"`
		Text         string        `json:"text"`
		CacheControl *cacheControl `json:"cache_control,omitempty"`
	}
	type antTool struct {
		Name        string              `json:"name"`
		Description string              `json:"description,omitempty"`
		InputSchema *gateway.JSONSchema `json:"input_schema,omitempty"`
	}
	type antMsg struct {
		Role    string `json:"role"`
		Content any    `json:"content"`
	}

	var sysBlocks []systemBlock
	if req.SystemPrefix != "" {
		sb := systemBlock{Type: "text", Text: req.SystemPrefix}
		if d.EnablePromptCache {
			sb.CacheControl = &cacheControl{Type: "ephemeral"}
		}
		sysBlocks = append(sysBlocks, sb)
	}

	msgs := make([]antMsg, 0, len(req.Messages))
	for _, m := range req.Messages {
		if m.Role == gateway.MsgSystem {
			sysBlocks = append(sysBlocks, systemBlock{Type: "text", Text: m.Content})
			continue
		}
		if m.Role == gateway.MsgTool {
			msgs = append(msgs, antMsg{
				Role: "user",
				Content: []map[string]any{{
					"type":        "tool_result",
					"tool_use_id": m.ToolCallID,
					"content":     m.Content,
				}},
			})
			continue
		}
		if len(m.ToolCalls) > 0 {
			blocks := make([]map[string]any, 0, 1+len(m.ToolCalls))
			if m.Content != "" {
				blocks = append(blocks, map[string]any{"type": "text", "text": m.Content})
			}
			for _, tc := range m.ToolCalls {
				input := tc.Arguments
				if len(input) == 0 {
					input = json.RawMessage(`{}`)
				}
				blocks = append(blocks, map[string]any{
					"type":  "tool_use",
					"id":    tc.ID,
					"name":  tc.Name,
					"input": input,
				})
			}
			msgs = append(msgs, antMsg{Role: "assistant", Content: blocks})
			continue
		}
		msgs = append(msgs, antMsg{Role: string(m.Role), Content: m.Content})
	}

	payload := struct {
		Model       string        `json:"model"`
		MaxTokens   int           `json:"max_tokens"`
		System      []systemBlock `json:"system,omitempty"`
		Messages    []antMsg      `json:"messages"`
		Tools       []antTool     `json:"tools,omitempty"`
		Temperature *float64      `json:"temperature,omitempty"`
	}{
		Model:       model,
		MaxTokens:   maxTokens,
		System:      sysBlocks,
		Messages:    msgs,
		Temperature: req.Temperature,
	}

	if len(req.Tools) > 0 {
		payload.Tools = make([]antTool, len(req.Tools))
		for i, t := range req.Tools {
			payload.Tools[i] = antTool{
				Name:        t.Name,
				Description: t.Description,
				InputSchema: t.Parameters,
			}
		}
	}

	headers := map[string]string{
		"anthropic-version": version,
	}
	if d.APIKey != "" {
		headers["x-api-key"] = d.APIKey
	}

	b, err := json.Marshal(payload)
	return "v1/messages", headers, b, err
}

func (d Dialect) ParseResponse(model string, raw []byte) (gateway.Response, error) {
	var parsed struct {
		Model      string `json:"model"`
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type  string          `json:"type"`
			Text  string          `json:"text"`
			ID    string          `json:"id"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content"`
		Usage struct {
			InputTokens          int64 `json:"input_tokens"`
			OutputTokens         int64 `json:"output_tokens"`
			CacheReadInputTokens int64 `json:"cache_read_input_tokens"`
		} `json:"usage"`
		Error *struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return gateway.Response{}, fmt.Errorf("decode anthropic response: %w", err)
	}
	if parsed.Error != nil && parsed.Error.Message != "" {
		return gateway.Response{}, fmt.Errorf("anthropic error (%s): %s", parsed.Error.Type, parsed.Error.Message)
	}

	var texts []string
	var calls []gateway.ToolCall
	for _, block := range parsed.Content {
		switch block.Type {
		case "text":
			texts = append(texts, block.Text)
		case "tool_use":
			calls = append(calls, gateway.ToolCall{
				ID:        block.ID,
				Name:      block.Name,
				Arguments: block.Input,
			})
		}
	}
	outModel := parsed.Model
	if outModel == "" {
		outModel = model
	}
	return gateway.Response{
		Text:         strings.Join(texts, ""),
		ToolCalls:    calls,
		FinishReason: parsed.StopReason,
		Model:        outModel,
		Usage: gateway.Usage{
			InputTokens:  parsed.Usage.InputTokens,
			OutputTokens: parsed.Usage.OutputTokens,
			CachedTokens: parsed.Usage.CacheReadInputTokens,
		},
	}, nil
}

