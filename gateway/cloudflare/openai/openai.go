package openai

import (
	"encoding/json"
	"fmt"

	"proj.dichay.tech/agent-go/gateway"
)

// Dialect implements cloudflare.Dialect for OpenAI-compatible Chat Completions
// (Workers AI /v1/chat/completions, AI Gateway /openai, /compat).
type Dialect struct {
	// PathSuffix defaults to "v1/chat/completions". For AI Gateway /openai, set "chat/completions".
	PathSuffix string
	// ReasoningEffort sets optional reasoning_effort ("low", "medium", "high") when supported.
	ReasoningEffort string
}

func (d Dialect) Name() string { return "openai" }

func (d Dialect) BuildRequest(model string, req gateway.Request) (string, map[string]string, []byte, error) {
	suffix := d.PathSuffix
	if suffix == "" {
		suffix = "v1/chat/completions"
	}

	payload := chatRequest{
		Model:           model,
		Messages:        buildMessages(req),
		Temperature:     req.Temperature,
		MaxTokens:       req.MaxTokens,
		ReasoningEffort: d.ReasoningEffort,
	}
	if len(req.Tools) > 0 {
		payload.Tools = make([]tool, len(req.Tools))
		for i, t := range req.Tools {
			payload.Tools[i] = tool{
				Type: "function",
				Function: toolFunction{
					Name:        t.Name,
					Description: t.Description,
					Parameters:  t.Parameters,
				},
			}
		}
	}
	if req.ResponseSchema != nil {
		payload.ResponseFormat = &responseFormat{
			Type: "json_schema",
			JSONSchema: &jsonSchemaSpec{
				Name:   "response",
				Strict: true,
				Schema: req.ResponseSchema,
			},
		}
	}

	b, err := json.Marshal(payload)
	return suffix, nil, b, err
}

func (d Dialect) ParseResponse(model string, raw []byte) (gateway.Response, error) {
	var parsed chatResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return gateway.Response{}, fmt.Errorf("decode openai response: %w", err)
	}
	if parsed.Error != nil && parsed.Error.Message != "" {
		return gateway.Response{}, fmt.Errorf("api error (%s): %s", parsed.Error.Type, parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 {
		return gateway.Response{}, fmt.Errorf("empty choices in response")
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

func buildMessages(req gateway.Request) []message {
	msgs := make([]message, 0, len(req.Messages)+1)
	if req.SystemPrefix != "" {
		msgs = append(msgs, message{Role: string(gateway.MsgSystem), Content: req.SystemPrefix})
	}
	for _, m := range req.Messages {
		cm := message{
			Role:       string(m.Role),
			Content:    m.Content,
			ToolCallID: m.ToolCallID,
			Name:       m.Name,
		}
		if len(m.ToolCalls) > 0 {
			cm.ToolCalls = make([]toolCall, len(m.ToolCalls))
			for i, tc := range m.ToolCalls {
				cm.ToolCalls[i] = toolCall{
					ID:   tc.ID,
					Type: "function",
					Function: toolCallFunction{
						Name:      tc.Name,
						Arguments: string(tc.Arguments),
					},
				}
			}
		}
		msgs = append(msgs, cm)
	}
	return msgs
}

type chatRequest struct {
	Model           string          `json:"model"`
	Messages        []message       `json:"messages"`
	Tools           []tool          `json:"tools,omitempty"`
	ResponseFormat  *responseFormat `json:"response_format,omitempty"`
	Temperature     *float64        `json:"temperature,omitempty"`
	MaxTokens       int             `json:"max_tokens,omitempty"`
	ReasoningEffort string          `json:"reasoning_effort,omitempty"`
}

type message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCalls  []toolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	Name       string     `json:"name,omitempty"`
}

type tool struct {
	Type     string       `json:"type"`
	Function toolFunction `json:"function"`
}

type toolFunction struct {
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Parameters  *gateway.JSONSchema `json:"parameters,omitempty"`
}

type responseFormat struct {
	Type       string          `json:"type"`
	JSONSchema *jsonSchemaSpec `json:"json_schema,omitempty"`
}

type jsonSchemaSpec struct {
	Name   string              `json:"name"`
	Strict bool                `json:"strict,omitempty"`
	Schema *gateway.JSONSchema `json:"schema"`
}

type toolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function toolCallFunction `json:"function"`
}

type toolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type chatResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message      message `json:"message"`
		FinishReason string  `json:"finish_reason"`
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

