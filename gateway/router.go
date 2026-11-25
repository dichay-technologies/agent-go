package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// ModelRouter resolves a Request to a registered provider name and target model ID.
type ModelRouter interface {
	Route(ctx context.Context, req Request, registered map[string]Provider, defaultProvider string) (providerName string, model string, err error)
}

// ModelRouterFunc adapts a function to ModelRouter.
type ModelRouterFunc func(ctx context.Context, req Request, registered map[string]Provider, defaultProvider string) (string, string, error)

func (f ModelRouterFunc) Route(ctx context.Context, req Request, registered map[string]Provider, defaultProvider string) (string, string, error) {
	return f(ctx, req, registered, defaultProvider)
}

// RouteTarget defines a routing rule target.
type RouteTarget struct {
	Provider    string
	Model       string
	Description string // optional hint used by ClassifierRouter (e.g. Jev)
}

// TableRouter routes requests using explicit aliases/model rules, "provider/model"
// prefixes, built-in model prefixes (e.g. "@cf/", "gemini-", "gpt-", "claude-", "jev-"),
// and falls back to defaultProvider.
type TableRouter struct {
	mu     sync.RWMutex
	routes map[string]RouteTarget
}

// NewTableRouter creates an empty TableRouter.
func NewTableRouter() *TableRouter {
	return &TableRouter{routes: make(map[string]RouteTarget)}
}

// AddRoute maps an alias or model ID (e.g. "fast", "voice", "gpt-4o-mini")
// to a specific registered provider and target model ID.
func (r *TableRouter) AddRoute(alias, provider, model string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes[alias] = RouteTarget{Provider: provider, Model: model}
}

func (r *TableRouter) Route(_ context.Context, req Request, registered map[string]Provider, defaultProvider string) (string, string, error) {
	prov := req.Provider
	model := req.Model

	// 1. Check explicit route table (by model/alias).
	if prov == "" && model != "" {
		r.mu.RLock()
		target, ok := r.routes[model]
		r.mu.RUnlock()
		if ok {
			prov = target.Provider
			if target.Model != "" {
				model = target.Model
			}
		}
	}

	// 2. Support "provider/model" shorthand when Provider is not explicitly set.
	if prov == "" && strings.Contains(model, "/") {
		parts := strings.SplitN(model, "/", 2)
		if _, exists := registered[parts[0]]; exists {
			prov = parts[0]
			model = parts[1]
		}
	}

	// 3. Auto-detect provider from well-known model prefixes when registered.
	if prov == "" && model != "" {
		prov = inferProviderByModel(model, registered)
	}

	if prov == "" {
		prov = defaultProvider
	}
	if _, ok := registered[prov]; !ok {
		return "", "", fmt.Errorf("%w: %q", ErrNoProvider, prov)
	}
	return prov, model, nil
}

func inferProviderByModel(model string, registered map[string]Provider) string {
	switch {
	case strings.HasPrefix(model, "@cf/"):
		if _, ok := registered["cloudflare"]; ok {
			return "cloudflare"
		}
	case strings.HasPrefix(model, "gemini-"):
		if _, ok := registered["gemini"]; ok {
			return "gemini"
		}
	case strings.HasPrefix(model, "jev-"):
		if _, ok := registered["jev"]; ok {
			return "jev"
		}
	case strings.HasPrefix(model, "claude-"):
		if _, ok := registered["anthropic"]; ok {
			return "anthropic"
		}
		if _, ok := registered["cloudflare"]; ok {
			return "cloudflare"
		}
	case strings.HasPrefix(model, "gpt-"), strings.HasPrefix(model, "o1"), strings.HasPrefix(model, "o3"), strings.HasPrefix(model, "o4"):
		if _, ok := registered["openai"]; ok {
			return "openai"
		}
	}
	return ""
}

// ClassifierRouter uses a fast classifier Provider (such as Jev) to dynamically
// pick a route when req.Model is "" or "auto". Explicit models fall back to TableRouter.
type ClassifierRouter struct {
	// Classifier is the provider used to choose a route (e.g. jev.Provider).
	Classifier Provider
	// ClassifierModel overrides the model passed to Classifier (e.g. "jev-latest").
	ClassifierModel string
	// Routes maps route names (e.g. "fast", "coding", "deep") to targets.
	Routes map[string]RouteTarget
	// DefaultRoute is used if classification fails or returns an unknown route.
	DefaultRoute string
	// Fallback handles explicit model names; defaults to NewTableRouter().
	Fallback ModelRouter
}

func (c *ClassifierRouter) Route(ctx context.Context, req Request, registered map[string]Provider, defaultProvider string) (string, string, error) {
	fb := c.Fallback
	if fb == nil {
		fb = NewTableRouter()
	}

	// Only invoke classifier when Model is empty or "auto" and Provider is not pinned.
	if req.Provider != "" || (req.Model != "" && req.Model != "auto") || c.Classifier == nil || len(c.Routes) == 0 {
		return fb.Route(ctx, req, registered, defaultProvider)
	}

	enumVals := make([]any, 0, len(c.Routes))
	var descLines []string
	for name, target := range c.Routes {
		enumVals = append(enumVals, name)
		if target.Description != "" {
			descLines = append(descLines, fmt.Sprintf("- %s: %s", name, target.Description))
		} else {
			descLines = append(descLines, fmt.Sprintf("- %s (%s/%s)", name, target.Provider, target.Model))
		}
	}

	var lastMsg string
	if n := len(req.Messages); n > 0 {
		lastMsg = req.Messages[n-1].Content
	}

	resp, err := c.Classifier.Complete(ctx, Request{
		ProjectID: req.ProjectID,
		Model:     c.ClassifierModel,
		Messages: []Message{{
			Role:    MsgUser,
			Content: fmt.Sprintf("Choose the best route for this request:\n%s\n\nRoutes:\n%s", lastMsg, strings.Join(descLines, "\n")),
		}},
		ResponseSchema: &JSONSchema{
			Type:     "object",
			Required: []string{"route"},
			Properties: map[string]*JSONSchema{
				"route": {Type: "string", Enum: enumVals},
			},
		},
	})

	picked := c.DefaultRoute
	if err == nil {
		var parsed struct {
			Route string `json:"route"`
		}
		if json.Unmarshal([]byte(resp.Text), &parsed) == nil && parsed.Route != "" {
			picked = parsed.Route
		}
	}

	if target, ok := c.Routes[picked]; ok {
		if _, exists := registered[target.Provider]; exists {
			return target.Provider, target.Model, nil
		}
	}
	return fb.Route(ctx, req, registered, defaultProvider)
}
