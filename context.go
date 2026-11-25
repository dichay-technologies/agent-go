package agentgo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"text/template"

	"proj.dichay.tech/agent-go/gateway"
)

// Agent is the common interface for any agent instance (root or child).
// A child agent is simply an Agent with a non-nil Parent().
type Agent interface {
	Run(ctx context.Context) (any, error)
	Parent() Agent
}

// Future represents an asynchronously spawned child agent execution.
type Future struct {
	Name string
	done chan struct{}
	val  any
	err  error
}

// NewFuture starts a child Agent in a background goroutine.
func NewFuture(ctx context.Context, name string, child Agent) *Future {
	f := &Future{
		Name: name,
		done: make(chan struct{}),
	}
	go func() {
		defer close(f.done)
		f.val, f.err = child.Run(ctx)
	}()
	return f
}

// Await blocks until the child agent finishes or ctx is canceled.
func (f *Future) Await(ctx context.Context) (any, error) {
	select {
	case <-f.done:
		return f.val, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Done returns a channel that is closed when the child agent finishes.
func (f *Future) Done() <-chan struct{} {
	return f.done
}

// Context is the unified telebot-style execution context passed to tools,
// ToolInvokers, hooks, and agent lifecycle phases.
type Context interface {
	context.Context

	AI() *gateway.AIGateway
	Agent() Agent
	Child(name string) (Agent, bool)

	Call(req gateway.Request) (gateway.Response, error)
	Run(child Agent) (any, error)
	Spawn(child Agent) (*Future, error)
}

// ContextConfig configures a new Context.
type ContextConfig struct {
	ProjectID    string
	DefaultModel string
	Gateway      *gateway.AIGateway
	Agent        Agent
	Children     map[string]Agent
	SpawnFn      func(ctx context.Context, child Agent) (*Future, error)
}

type agentContext struct {
	context.Context
	cfg ContextConfig
}

type ctxKey struct{}

// NewContext wraps parent with a unified agentgo.Context.
func NewContext(parent context.Context, cfg ContextConfig) Context {
	if parent == nil {
		parent = context.Background()
	}
	ac := &agentContext{cfg: cfg}
	ac.Context = context.WithValue(parent, ctxKey{}, ac)
	return ac
}

// ContextFrom extracts an agentgo.Context from a plain context.Context if present.
func ContextFrom(ctx context.Context) (Context, bool) {
	if c, ok := ctx.(Context); ok {
		return c, true
	}
	c, ok := ctx.Value(ctxKey{}).(Context)
	return c, ok && c != nil
}

func (c *agentContext) AI() *gateway.AIGateway { return c.cfg.Gateway }
func (c *agentContext) Agent() Agent          { return c.cfg.Agent }

func (c *agentContext) Child(name string) (Agent, bool) {
	if c.cfg.Children == nil {
		return nil, false
	}
	a, ok := c.cfg.Children[name]
	return a, ok
}

func (c *agentContext) Call(req gateway.Request) (gateway.Response, error) {
	if c.cfg.Gateway == nil {
		return gateway.Response{}, fmt.Errorf("agentgo: AIGateway is nil in context")
	}
	if req.ProjectID == "" {
		req.ProjectID = c.cfg.ProjectID
	}
	if req.Model == "" {
		req.Model = c.cfg.DefaultModel
	}
	return c.cfg.Gateway.Complete(c, req)
}

func (c *agentContext) Run(child Agent) (any, error) {
	if child == nil {
		return nil, fmt.Errorf("agentgo: child agent is nil")
	}
	return child.Run(c)
}

func (c *agentContext) Spawn(child Agent) (*Future, error) {
	if child == nil {
		return nil, fmt.Errorf("agentgo: child agent is nil")
	}
	if c.cfg.SpawnFn != nil {
		return c.cfg.SpawnFn(c, child)
	}
	return NewFuture(c, "", child), nil
}

// ============================================================================
// DataSource, Templates, Slicing, and KV-Prefix Validation
// ============================================================================

// DataSource provides named textual context to an agent.
type DataSource interface {
	Name() string
	Content(ctx Context) (string, error)
}

type staticDataSource struct {
	name    string
	content string
}

func (s staticDataSource) Name() string                  { return s.name }
func (s staticDataSource) Content(_ Context) (string, error) { return s.content, nil }

// StaticData creates an in-memory DataSource.
func StaticData(name, content string) DataSource {
	return staticDataSource{name: name, content: content}
}

var tmplCache sync.Map // string -> *template.Template

// RenderPrompt compiles (with caching) and executes a Go text/template.
func RenderPrompt(tmplText string, data any) (string, error) {
	if tmplText == "" {
		return "", nil
	}
	var tmpl *template.Template
	if cached, ok := tmplCache.Load(tmplText); ok {
		tmpl = cached.(*template.Template)
	} else {
		parsed, err := template.New("prompt").Parse(tmplText)
		if err != nil {
			return "", fmt.Errorf("context: parse template: %w", err)
		}
		actual, _ := tmplCache.LoadOrStore(tmplText, parsed)
		tmpl = actual.(*template.Template)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("context: execute template: %w", err)
	}
	return buf.String(), nil
}

// SliceByRegex extracts matching substrings from src without rewriting the original text.
func SliceByRegex(src, pattern string, maxMatches int) ([]string, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("context: invalid regex %q: %w", pattern, err)
	}
	return re.FindAllString(src, maxMatches), nil
}

// SliceLines extracts 1-based inclusive line range [startLine, endLine] from src.
func SliceLines(src string, startLine, endLine int) string {
	if startLine < 1 {
		startLine = 1
	}
	lines := strings.Split(src, "\n")
	if startLine > len(lines) {
		return ""
	}
	if endLine <= 0 || endLine > len(lines) {
		endLine = len(lines)
	}
	if endLine < startLine {
		return ""
	}
	return strings.Join(lines[startLine-1:endLine], "\n")
}

// EstimateTokens returns a fast heuristic token count (~4 chars per token).
func EstimateTokens(s string) int {
	if len(s) == 0 {
		return 0
	}
	return (len(s) + 3) / 4
}

// ValidateWindow checks that systemPrefix + dynamicText fit within maxTokens
// while ensuring systemPrefix stays intact for provider KV-caching.
func ValidateWindow(systemPrefix, dynamicText string, maxTokens int) error {
	if maxTokens <= 0 {
		return nil
	}
	sysTokens := EstimateTokens(systemPrefix)
	if sysTokens > maxTokens {
		return fmt.Errorf("context: static system prefix (%d est. tokens) exceeds window (%d)", sysTokens, maxTokens)
	}
	total := sysTokens + EstimateTokens(dynamicText)
	if total > maxTokens {
		return fmt.Errorf("context: prompt (%d est. tokens) exceeds window (%d)", total, maxTokens)
	}
	return nil
}

// CompressContext compresses text to fit within maxTokens using compressModel,
// and verifies with verifyModel that no critical meaning was lost.
// If verification reports missing facts, it retries once with the missing items.
func CompressContext(ctx Context, compressModel, verifyModel, text string, maxTokens int) (string, error) {
	if maxTokens <= 0 || EstimateTokens(text) <= maxTokens {
		return text, nil
	}
	if verifyModel == "" {
		verifyModel = compressModel
	}

	verifySchema := &gateway.JSONSchema{
		Type:     "object",
		Required: []string{"ok"},
		Properties: map[string]*gateway.JSONSchema{
			"ok":      {Type: "boolean"},
			"missing": {Type: "array", Items: &gateway.JSONSchema{Type: "string"}},
		},
	}

	var missingHint string
	for attempt := range 2 {
		prompt := fmt.Sprintf("Compress the following text to under %d tokens while preserving all facts, identifiers, and constraints.", maxTokens)
		if missingHint != "" {
			prompt += "\nMake sure to include: " + missingHint
		}
		compResp, err := ctx.Call(gateway.Request{
			Model: compressModel,
			Messages: []gateway.Message{
				{Role: gateway.MsgSystem, Content: prompt},
				{Role: gateway.MsgUser, Content: text},
			},
		})
		if err != nil {
			return "", fmt.Errorf("context: compress: %w", err)
		}

		verResp, err := ctx.Call(gateway.Request{
			Model:          verifyModel,
			ResponseSchema: verifySchema,
			Messages: []gateway.Message{{
				Role:    gateway.MsgUser,
				Content: fmt.Sprintf("Original:\n%s\n\nCompressed:\n%s\n\nDid compression preserve all critical meaning? Return JSON {ok, missing}.", text, compResp.Text),
			}},
		})
		if err != nil {
			return "", fmt.Errorf("context: verify compression: %w", err)
		}

		var v struct {
			OK      bool     `json:"ok"`
			Missing []string `json:"missing"`
		}
		if err := json.Unmarshal([]byte(verResp.Text), &v); err != nil {
			return "", fmt.Errorf("context: parse compression check: %w", err)
		}
		if v.OK {
			return compResp.Text, nil
		}
		if attempt == 0 && len(v.Missing) > 0 {
			missingHint = strings.Join(v.Missing, ", ")
			continue
		}
		return "", fmt.Errorf("context: compression lost meaning: %v", v.Missing)
	}
	return "", fmt.Errorf("context: compression failed")
}
