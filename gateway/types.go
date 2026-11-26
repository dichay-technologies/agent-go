package gateway

import (
	"context"
	"encoding/json"
	"iter"
)

// MessageRole represents the author of a chat message.
type MessageRole string

const (
	MsgSystem    MessageRole = "system"
	MsgUser      MessageRole = "user"
	MsgAssistant MessageRole = "assistant"
	MsgTool      MessageRole = "tool"
)

// Modality represents an input or output media modality.
type Modality string

const (
	ModalityText  Modality = "TEXT"
	ModalityImage Modality = "IMAGE"
	ModalityAudio Modality = "AUDIO"
)

// Part represents a multimodal content piece (text, inline binary media, or file URI).
type Part struct {
	Text     string `json:"text,omitempty"`
	MIMEType string `json:"mime_type,omitempty"`
	Data     []byte `json:"data,omitempty"`
	FileURI  string `json:"file_uri,omitempty"`
}

// SpeechConfig configures voice synthesis for audio-capable models (e.g. Gemini TTS).
type SpeechConfig struct {
	VoiceName string `json:"voice_name,omitempty"`
}

// Message is a normalized chat message across all model providers.
type Message struct {
	Role       MessageRole `json:"role"`
	Content    string      `json:"content"`
	Parts      []Part      `json:"parts,omitempty"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
	ToolCallID string      `json:"tool_call_id,omitempty"`
	Name       string      `json:"name,omitempty"`
}

// ToolSpec describes a tool available to the model.
type ToolSpec struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Parameters  *JSONSchema `json:"parameters,omitempty"`
}

// ToolCall represents a tool invocation requested by the model.
type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// Request is a normalized model invocation sent through AIGateway.
type Request struct {
	// ProjectID tags the request for per-project token accounting, quotas, and routing.
	ProjectID string
	// Meta carries arbitrary developer-defined tags (e.g. user_id, run_id, tenant)
	// for UsageSink accounting and middleware without hardcoding schema fields.
	Meta map[string]string
	// Provider optionally selects a registered provider by name.
	// If empty, AIGateway resolves it via its ModelRouter.
	Provider string
	// Model specifies the model ID or alias for this request.
	Model string
	// Coalesce opts this request into cross-project micro-batching when CoalesceWindow > 0.
	Coalesce bool

	// SystemPrefix is the static system prompt prefix marked for provider KV-caching.
	SystemPrefix string
	// Messages is the dynamic conversation turn list.
	Messages []Message
	// Tools lists tools the model may invoke.
	Tools []ToolSpec
	// ResponseSchema, when non-nil, enforces structured JSON output and validates the response.
	ResponseSchema *JSONSchema
	// ResponseModalities requests specific output modalities (e.g. IMAGE, AUDIO, TEXT).
	ResponseModalities []Modality
	// SpeechConfig configures voice output when ModalityAudio is requested.
	SpeechConfig *SpeechConfig

	Temperature *float64
	MaxTokens   int
	// BypassCache skips content-addressed cache lookup for this request.
	BypassCache bool
}

// Usage tracks token consumption for a request or project.
type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	CachedTokens int64 `json:"cached_tokens,omitempty"`
}

// Add accumulates another Usage into this one.
func (u *Usage) Add(other Usage) {
	u.InputTokens += other.InputTokens
	u.OutputTokens += other.OutputTokens
	u.CachedTokens += other.CachedTokens
}

// Response is the normalized result returned by a Provider via AIGateway.
type Response struct {
	Text         string
	Parts        []Part
	ToolCalls    []ToolCall
	FinishReason string
	Model        string
	Usage        Usage
	Cached       bool
}

// Provider is the minimal contract a model backend must implement.
type Provider interface {
	Name() string
	Complete(ctx context.Context, req Request) (Response, error)
}

// BatchProvider is an optional interface for providers that support native batch execution.
// When not implemented, AIGateway fans out coalesced requests concurrently.
type BatchProvider interface {
	Provider
	BatchComplete(ctx context.Context, reqs []Request) ([]Response, []error)
}

// StreamChunk represents a single incremental delta from a streaming provider.
type StreamChunk struct {
	Text      string
	ToolCalls []ToolCall
	Usage     *Usage
	Done      bool
}

// StreamProvider is an optional interface for providers that support streaming responses.
type StreamProvider interface {
	Provider
	Stream(ctx context.Context, req Request) iter.Seq2[StreamChunk, error]
}

// UsageSink receives token usage events for custom accounting or telemetry.
type UsageSink interface {
	Record(ctx context.Context, req Request, usage Usage)
}

// UsageSinkFunc adapts a function to UsageSink.
type UsageSinkFunc func(ctx context.Context, req Request, usage Usage)

func (f UsageSinkFunc) Record(ctx context.Context, req Request, usage Usage) {
	f(ctx, req, usage)
}

