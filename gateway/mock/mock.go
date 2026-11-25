package mock

import (
	"context"
	"sync"
	"sync/atomic"

	"proj.dichay.tech/agent-go/gateway"
)

// Handler handles a single request in Provider.
type Handler func(ctx context.Context, req gateway.Request) (gateway.Response, error)

// Provider is an in-memory gateway.Provider and gateway.BatchProvider for testing.
type Provider struct {
	name    string
	handler Handler

	mu         sync.Mutex
	calls      []gateway.Request
	batchCalls [][]gateway.Request
	callCount  atomic.Int64
}

// New creates a mock Provider with the given name and handler.
func New(name string, handler Handler) *Provider {
	if name == "" {
		name = "mock"
	}
	return &Provider{
		name:    name,
		handler: handler,
	}
}

func (m *Provider) Name() string {
	return m.name
}

func (m *Provider) Complete(ctx context.Context, req gateway.Request) (gateway.Response, error) {
	m.callCount.Add(1)
	m.mu.Lock()
	m.calls = append(m.calls, req)
	m.mu.Unlock()

	if m.handler != nil {
		return m.handler(ctx, req)
	}
	return gateway.Response{Text: "ok", Model: req.Model}, nil
}

func (m *Provider) BatchComplete(ctx context.Context, reqs []gateway.Request) ([]gateway.Response, []error) {
	m.callCount.Add(1)
	m.mu.Lock()
	copied := make([]gateway.Request, len(reqs))
	copy(copied, reqs)
	m.batchCalls = append(m.batchCalls, copied)
	m.mu.Unlock()

	resps := make([]gateway.Response, len(reqs))
	errs := make([]error, len(reqs))
	for i, req := range reqs {
		if m.handler != nil {
			resps[i], errs[i] = m.handler(ctx, req)
		} else {
			resps[i] = gateway.Response{Text: "ok", Model: req.Model}
		}
	}
	return resps, errs
}

// Calls returns a snapshot of all single Complete calls.
func (m *Provider) Calls() []gateway.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]gateway.Request, len(m.calls))
	copy(out, m.calls)
	return out
}

// BatchCalls returns a snapshot of all BatchComplete calls.
func (m *Provider) BatchCalls() [][]gateway.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]gateway.Request, len(m.batchCalls))
	copy(out, m.batchCalls)
	return out
}

// TotalInvocations returns how many times Complete or BatchComplete was invoked.
func (m *Provider) TotalInvocations() int64 {
	return m.callCount.Load()
}

