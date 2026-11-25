package jev

import (
	"encoding/json"
	"fmt"

	"proj.dichay.tech/agent-go/gateway"
)

const defaultModel = "jev-latest"

// Dialect implements cloudflare.Dialect for TypeSafe System One API (Jev)
// routed through Cloudflare AI Gateway (/v1/systemone).
type Dialect struct {
	// PathSuffix defaults to "v1/systemone".
	PathSuffix string
}

func (d Dialect) Name() string { return "jev" }

func (d Dialect) BuildRequest(model string, req gateway.Request) (string, map[string]string, []byte, error) {
	suffix := d.PathSuffix
	if suffix == "" {
		suffix = "v1/systemone"
	}
	if model == "" {
		model = defaultModel
	}

	questions := make(map[string]wireQuestion)

	// 1. If Tools are provided, create a "tool" choice question.
	if len(req.Tools) > 0 {
		crit := make(map[string]any, len(req.Tools))
		for _, t := range req.Tools {
			if t.Description != "" {
				crit[t.Name] = t.Description
			} else {
				crit[t.Name] = nil
			}
		}
		questions["tool"] = wireQuestion{
			Type:         "choice",
			Instructions: "Select the best tool for the current state.",
			Criteria:     crit,
		}
	}

	// 2. If ResponseSchema has enum properties, map each to a choice question.
	if req.ResponseSchema != nil {
		if len(req.ResponseSchema.Enum) > 0 {
			questions["decision"] = enumQuestion(req.ResponseSchema.Description, req.ResponseSchema.Enum)
		} else {
			for k, prop := range req.ResponseSchema.Properties {
				if prop != nil && len(prop.Enum) > 0 {
					questions[k] = enumQuestion(prop.Description, prop.Enum)
				}
			}
		}
	}

	// 3. Fallback: ask a noul/score or binary choice from the last user message.
	if len(questions) == 0 {
		instr := ""
		if n := len(req.Messages); n > 0 {
			instr = req.Messages[n-1].Content
		}
		questions["decision"] = wireQuestion{
			Type:         "choice",
			Instructions: instr,
			Criteria:     map[string]any{"yes": nil, "no": nil},
		}
	}

	payload := wireRequest{
		Model: model,
		State: map[string]any{
			"system":   req.SystemPrefix,
			"messages": req.Messages,
		},
		Questions: questions,
	}
	b, err := json.Marshal(payload)
	return suffix, nil, b, err
}

func (d Dialect) ParseResponse(model string, raw []byte) (gateway.Response, error) {
	var parsed wireResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return gateway.Response{}, fmt.Errorf("decode jev response: %w", err)
	}

	outModel := parsed.Model
	if outModel == "" {
		outModel = model
	}

	out := gateway.Response{
		Model:        outModel,
		FinishReason: "stop",
		Usage: gateway.Usage{
			InputTokens:  parsed.Usage.InputTokens,
			OutputTokens: parsed.Usage.OutputTokens,
		},
	}

	// Map "tool" choice answer into ToolCalls if present.
	if toolAns, ok := parsed.Answers["tool"]; ok && toolAns.Type == "choice" && toolAns.Choice != "" {
		out.ToolCalls = []gateway.ToolCall{{
			ID:        "jev_tool_0",
			Name:      toolAns.Choice,
			Arguments: json.RawMessage(`{}`),
		}}
		out.FinishReason = "tool_calls"
	}

	// Serialize simplified key -> value map as JSON Text so ResponseSchema validation works cleanly.
	flat := make(map[string]any, len(parsed.Answers))
	for k, ans := range parsed.Answers {
		switch ans.Type {
		case "choice":
			flat[k] = ans.Choice
		case "score":
			flat[k] = ans.Score
		case "noul":
			flat[k] = ans.Noul
		}
	}
	b, _ := json.Marshal(flat)
	out.Text = string(b)
	return out, nil
}

func enumQuestion(desc string, vals []any) wireQuestion {
	crit := make(map[string]any, len(vals))
	for _, v := range vals {
		crit[fmt.Sprint(v)] = nil
	}
	return wireQuestion{
		Type:         "choice",
		Instructions: desc,
		Criteria:     crit,
	}
}

type wireRequest struct {
	Model     string                  `json:"model"`
	State     any                     `json:"state"`
	Questions map[string]wireQuestion `json:"questions"`
}

type wireQuestion struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions,omitempty"`
	Criteria     any    `json:"criteria,omitempty"`
}

type wireResponse struct {
	Model   string                `json:"model"`
	Answers map[string]wireAnswer `json:"answers"`
	Usage   struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
	} `json:"usage"`
}

type wireAnswer struct {
	Type          string             `json:"type"`
	Choice        string             `json:"choice"`
	Score         float64            `json:"score"`
	Noul          float64            `json:"noul"`
	Confidence    float64            `json:"confidence"`
	Probabilities map[string]float64 `json:"probabilities"`
}

