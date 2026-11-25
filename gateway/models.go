package gateway

// ModelFormat identifies the wire protocol a model supports.
type ModelFormat string

const (
	FormatOpenAI    ModelFormat = "openai"
	FormatGemini    ModelFormat = "gemini"
	FormatAnthropic ModelFormat = "anthropic"
	FormatWorkersAI ModelFormat = "workers-ai"
	FormatJev       ModelFormat = "jev"
)

// ModelCapability describes what a model can do.
type ModelCapability string

const (
	CapText         ModelCapability = "text"
	CapTools        ModelCapability = "tools"
	CapJSONSchema   ModelCapability = "json_schema"
	CapImage        ModelCapability = "image"
	CapAudio        ModelCapability = "audio"         // audio input or TTS output
	CapVoice        ModelCapability = "voice"         // direct voice-to-voice / realtime audio
	CapContextCache ModelCapability = "context_cache"
	CapBatch        ModelCapability = "batch"
)

// ModelInfo describes a supported model and the formats/providers it can use.
type ModelInfo struct {
	ID           string
	Provider     string
	Formats      []ModelFormat
	Capabilities []ModelCapability
}

// HasCapability reports whether the model supports the given capability.
func (m ModelInfo) HasCapability(c ModelCapability) bool {
	for _, cap := range m.Capabilities {
		if cap == c {
			return true
		}
	}
	return false
}

// SupportedModels returns the built-in catalog of models supported out of the box,
// including models accessible via the OpenAI-compatible format and direct voice models.
func SupportedModels() []ModelInfo {
	return []ModelInfo{
		// OpenAI (native OpenAI format)
		{ID: "gpt-5", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapContextCache, CapBatch}},
		{ID: "gpt-5-mini", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapContextCache, CapBatch}},
		{ID: "gpt-4o", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapContextCache, CapBatch}},
		{ID: "gpt-4o-mini", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapContextCache, CapBatch}},
		{ID: "gpt-4o-audio-preview", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapAudio, CapVoice, CapTools}},
		{ID: "gpt-4o-realtime-preview", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapVoice, CapAudio, CapText, CapTools}},
		{ID: "gpt-4o-mini-realtime-preview", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapVoice, CapAudio, CapText, CapTools}},
		{ID: "gpt-4o-mini-tts", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapAudio}},
		{ID: "gpt-4.1", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapContextCache, CapBatch}},
		{ID: "gpt-4.1-mini", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapContextCache, CapBatch}},
		{ID: "gpt-4.1-nano", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapContextCache, CapBatch}},
		{ID: "o3-mini", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapBatch}},
		{ID: "o4-mini", Provider: "openai", Formats: []ModelFormat{FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapBatch}},

		// Google Gemini 4 (native Gemini API + OpenAI-compatible endpoint)
		{ID: "gemini-4-neon", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-4-hydrogen", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-4-carbon", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-4-argon", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},

		// Google Gemini 3.x (native Gemini API + OpenAI-compatible endpoint)
		{ID: "gemini-3.8-flash", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-3.6-flash", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-3.5-flash", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-3.5-flash-lite", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-3.1-flash-lite", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapContextCache, CapBatch}},
		{ID: "gemini-3-pro-image", Provider: "gemini", Formats: []ModelFormat{FormatGemini}, Capabilities: []ModelCapability{CapText, CapImage}},

		// Google Gemini 2.5 / 2.0 (including TTS & Live native-audio)
		{ID: "gemini-2.5-flash", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-2.5-pro", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-2.5-flash-lite", Provider: "gemini", Formats: []ModelFormat{FormatGemini, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapImage, CapAudio, CapContextCache, CapBatch}},
		{ID: "gemini-2.5-flash-image", Provider: "gemini", Formats: []ModelFormat{FormatGemini}, Capabilities: []ModelCapability{CapText, CapImage}},
		{ID: "gemini-2.5-flash-preview-tts", Provider: "gemini", Formats: []ModelFormat{FormatGemini}, Capabilities: []ModelCapability{CapAudio}},
		{ID: "gemini-2.5-pro-preview-tts", Provider: "gemini", Formats: []ModelFormat{FormatGemini}, Capabilities: []ModelCapability{CapAudio}},
		{ID: "gemini-2.5-flash-native-audio-preview-12-2025", Provider: "gemini", Formats: []ModelFormat{FormatGemini}, Capabilities: []ModelCapability{CapVoice, CapAudio, CapText, CapTools}},
		{ID: "gemini-2.0-flash-live-001", Provider: "gemini", Formats: []ModelFormat{FormatGemini}, Capabilities: []ModelCapability{CapVoice, CapAudio, CapText, CapTools}},

		// Cloudflare Workers AI (OpenAI-compatible + native /run/{model})
		{ID: "@cf/meta/llama-3.3-70b-instruct-fp8-fast", Provider: "cloudflare", Formats: []ModelFormat{FormatOpenAI, FormatWorkersAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapBatch}},
		{ID: "@cf/meta/llama-3.1-8b-instruct-fast", Provider: "cloudflare", Formats: []ModelFormat{FormatOpenAI, FormatWorkersAI}, Capabilities: []ModelCapability{CapText, CapTools, CapBatch}},
		{ID: "@cf/qwen/qwen2.5-coder-32b-instruct", Provider: "cloudflare", Formats: []ModelFormat{FormatOpenAI, FormatWorkersAI}, Capabilities: []ModelCapability{CapText, CapTools, CapJSONSchema, CapBatch}},
		{ID: "@cf/deepseek-ai/deepseek-r1-distill-qwen-32b", Provider: "cloudflare", Formats: []ModelFormat{FormatOpenAI, FormatWorkersAI}, Capabilities: []ModelCapability{CapText, CapBatch}},
		{ID: "@cf/openai/whisper", Provider: "cloudflare", Formats: []ModelFormat{FormatWorkersAI}, Capabilities: []ModelCapability{CapAudio}},
		{ID: "@cf/myshell-ai/melotts", Provider: "cloudflare", Formats: []ModelFormat{FormatWorkersAI}, Capabilities: []ModelCapability{CapAudio}},
		{ID: "@cf/black-forest-labs/flux-1-schnell", Provider: "cloudflare", Formats: []ModelFormat{FormatWorkersAI}, Capabilities: []ModelCapability{CapImage}},

		// Anthropic (via Cloudflare AI Gateway)
		{ID: "claude-sonnet-4-20250514", Provider: "cloudflare", Formats: []ModelFormat{FormatAnthropic, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapImage, CapContextCache}},
		{ID: "claude-3-7-sonnet-latest", Provider: "cloudflare", Formats: []ModelFormat{FormatAnthropic, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapImage, CapContextCache}},
		{ID: "claude-3-5-haiku-latest", Provider: "cloudflare", Formats: []ModelFormat{FormatAnthropic, FormatOpenAI}, Capabilities: []ModelCapability{CapText, CapTools, CapContextCache}},

		// TypeSafe Jev (System One)
		{ID: "jev-latest", Provider: "jev", Formats: []ModelFormat{FormatJev}, Capabilities: []ModelCapability{CapTools, CapJSONSchema}},
	}
}

// FindModel looks up a model by ID in SupportedModels.
func FindModel(id string) (ModelInfo, bool) {
	for _, m := range SupportedModels() {
		if m.ID == id {
			return m, true
		}
	}
	return ModelInfo{}, false
}

// ModelsByCapability returns all built-in models that support the given capability.
func ModelsByCapability(cap ModelCapability) []ModelInfo {
	var out []ModelInfo
	for _, m := range SupportedModels() {
		if m.HasCapability(cap) {
			out = append(out, m)
		}
	}
	return out
}

