package workersai

import (
	"encoding/json"
	"fmt"
	"strings"

	"proj.dichay.tech/agent-go/gateway"
)

// Dialect implements cloudflare.Dialect (and native batch payload building)
// for Cloudflare Workers AI's native /run/{model} endpoint.
type Dialect struct{}

func (d Dialect) Name() string { return "workers-ai" }

func (d Dialect) BuildRequest(model string, req gateway.Request) (string, map[string]string, []byte, error) {
	payload := buildSinglePayload(req)
	b, err := json.Marshal(payload)
	return "run/" + strings.TrimLeft(model, "/"), nil, b, err
}

func (d Dialect) ParseResponse(model string, raw []byte) (gateway.Response, error) {
	var env struct {
		Success bool `json:"success"`
		Errors  []struct {
			Message string `json:"message"`
		} `json:"errors"`
		Result resultPayload `json:"result"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return gateway.Response{}, fmt.Errorf("decode workers-ai response: %w", err)
	}
	if !env.Success && len(env.Errors) > 0 {
		return gateway.Response{}, fmt.Errorf("workers-ai error: %s", env.Errors[0].Message)
	}
	return resultToResponse(model, env.Result), nil
}

// BuildBatchRequest builds a native Workers AI Asynchronous Batch API request
// (`{"requests": [...]}` sent to `run/{model}?queueRequest=true`).
func (d Dialect) BuildBatchRequest(model string, reqs []gateway.Request) (string, map[string]string, []byte, error) {
	items := make([]batchItemPayload, len(reqs))
	for i, req := range reqs {
		p := buildSinglePayload(req)
		items[i] = batchItemPayload{
			ExternalReference: fmt.Sprintf("req_%d", i),
			Messages:          p.Messages,
			Tools:             p.Tools,
			ResponseFormat:    p.ResponseFormat,
			Temperature:       p.Temperature,
			MaxTokens:         p.MaxTokens,
		}
	}
	body, err := json.Marshal(struct {
		Requests []batchItemPayload `json:"requests"`
	}{Requests: items})
	if err != nil {
		return "", nil, nil, err
	}
	return "run/" + strings.TrimLeft(model, "/") + "?queueRequest=true", nil, body, nil
}

// ParseBatchSubmit parses the initial async batch submission response (`{"result": {"id": "..."}}`).
func (d Dialect) ParseBatchSubmit(raw []byte) (requestID string, err error) {
	var env struct {
		Success bool `json:"success"`
		Errors  []struct {
			Message string `json:"message"`
		} `json:"errors"`
		Result struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", fmt.Errorf("decode workers-ai batch submit: %w", err)
	}
	if !env.Success && len(env.Errors) > 0 {
		return "", fmt.Errorf("workers-ai batch error: %s", env.Errors[0].Message)
	}
	if env.Result.ID == "" {
		return "", fmt.Errorf("workers-ai batch submit: missing request id")
	}
	return env.Result.ID, nil
}

// BatchPollPath returns the status polling path suffix for a submitted batch requestID.
func (d Dialect) BatchPollPath(model, requestID string) string {
	return fmt.Sprintf("run/%s?requestId=%s", strings.TrimLeft(model, "/"), requestID)
}

// ParseBatchPoll parses a batch poll response. done is true when status is completed/failed.
func (d Dialect) ParseBatchPoll(model string, count int, raw []byte) (done bool, resps []gateway.Response, errs []error, err error) {
	var env struct {
		Success bool `json:"success"`
		Errors  []struct {
			Message string `json:"message"`
		} `json:"errors"`
		Result struct {
			Status    string `json:"status"`
			Responses []struct {
				ExternalReference string        `json:"external_reference"`
				Result            resultPayload `json:"result"`
				Error             string        `json:"error"`
			} `json:"responses"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return false, nil, nil, fmt.Errorf("decode workers-ai batch poll: %w", err)
	}
	if !env.Success && len(env.Errors) > 0 {
		return true, nil, nil, fmt.Errorf("workers-ai batch poll error: %s", env.Errors[0].Message)
	}

	switch strings.ToLower(env.Result.Status) {
	case "queued", "running", "pending":
		return false, nil, nil, nil
	}

	resps = make([]gateway.Response, count)
	errs = make([]error, count)
	for i := range errs {
		errs[i] = fmt.Errorf("workers-ai batch: missing response for index %d", i)
	}

	for _, item := range env.Result.Responses {
		var idx int
		if _, scanErr := fmt.Sscanf(item.ExternalReference, "req_%d", &idx); scanErr != nil || idx < 0 || idx >= count {
			continue
		}
		if item.Error != "" {
			errs[idx] = fmt.Errorf("workers-ai item error: %s", item.Error)
			continue
		}
		errs[idx] = nil
		resps[idx] = resultToResponse(model, item.Result)
	}
	return true, resps, errs, nil
}

func buildSinglePayload(req gateway.Request) singlePayload {
	msgs := make([]msgPayload, 0, len(req.Messages)+1)
	if req.SystemPrefix != "" {
		msgs = append(msgs, msgPayload{Role: string(gateway.MsgSystem), Content: req.SystemPrefix})
	}
	for _, m := range req.Messages {
		msgs = append(msgs, msgPayload{Role: string(m.Role), Content: m.Content})
	}

	p := singlePayload{
		Messages:    msgs,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}
	if len(req.Tools) > 0 {
		p.Tools = make([]toolPayload, len(req.Tools))
		for i, t := range req.Tools {
			p.Tools[i] = toolPayload{
				Type: "function",
				Function: toolFunctionPayload{
					Name:        t.Name,
					Description: t.Description,
					Parameters:  t.Parameters,
				},
			}
		}
	}
	if req.ResponseSchema != nil {
		p.ResponseFormat = &responseFormatPayload{
			Type:       "json_schema",
			JSONSchema: &jsonSchemaSpecPayload{Name: "response", Schema: req.ResponseSchema},
		}
	}
	return p
}

func resultToResponse(model string, res resultPayload) gateway.Response {
	out := gateway.Response{
		Text:  res.Response,
		Model: model,
		Usage: gateway.Usage{
			InputTokens:  res.Usage.PromptTokens,
			OutputTokens: res.Usage.CompletionTokens,
		},
	}
	if len(res.ToolCalls) > 0 {
		out.ToolCalls = make([]gateway.ToolCall, len(res.ToolCalls))
		for i, tc := range res.ToolCalls {
			out.ToolCalls[i] = gateway.ToolCall{
				ID:        fmt.Sprintf("cf_call_%d", i),
				Name:      tc.Name,
				Arguments: tc.Arguments,
			}
		}
	}
	return out
}

type singlePayload struct {
	Messages       []msgPayload           `json:"messages"`
	Tools          []toolPayload          `json:"tools,omitempty"`
	ResponseFormat *responseFormatPayload `json:"response_format,omitempty"`
	Temperature    *float64               `json:"temperature,omitempty"`
	MaxTokens      int                    `json:"max_tokens,omitempty"`
}

type batchItemPayload struct {
	ExternalReference string                 `json:"external_reference"`
	Messages          []msgPayload           `json:"messages"`
	Tools             []toolPayload          `json:"tools,omitempty"`
	ResponseFormat    *responseFormatPayload `json:"response_format,omitempty"`
	Temperature       *float64               `json:"temperature,omitempty"`
	MaxTokens         int                    `json:"max_tokens,omitempty"`
}

type msgPayload struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type toolPayload struct {
	Type     string              `json:"type"`
	Function toolFunctionPayload `json:"function"`
}

type toolFunctionPayload struct {
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Parameters  *gateway.JSONSchema `json:"parameters,omitempty"`
}

type responseFormatPayload struct {
	Type       string                 `json:"type"`
	JSONSchema *jsonSchemaSpecPayload `json:"json_schema,omitempty"`
}

type jsonSchemaSpecPayload struct {
	Name   string              `json:"name"`
	Schema *gateway.JSONSchema `json:"schema"`
}

type resultPayload struct {
	Response  string `json:"response"`
	ToolCalls []struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	} `json:"tool_calls"`
	Usage struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

