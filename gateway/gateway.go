package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"
)

var (
	ErrNoProvider = errors.New("gateway: no provider registered or matched")
	ErrEmptyBatch = errors.New("gateway: empty batch")
)

// Config configures AIGateway behavior.
type Config struct {
	// EnableCache enables content-addressed response caching.
	EnableCache bool
	// CacheTTL sets how long cached responses remain valid. Zero means no expiration.
	CacheTTL time.Duration
	// MaxCacheEntries caps the default in-memory cache size. Zero defaults to 1024.
	MaxCacheEntries int
	// Cache overrides the default in-memory Cache when EnableCache is true.
	Cache Cache
	// UsageSink receives token usage records for custom accounting.
	UsageSink UsageSink
	// UsageKey derives the accounting key for UsageByProject / AllUsage. Defaults to req.ProjectID.
	UsageKey func(Request) string
	// CoalesceWindow enables cross-project micro-batching for requests with Coalesce=true
	// when > 0 (recommended: 50ms - 150ms).
	CoalesceWindow time.Duration
	// MaxBatchSize caps how many requests are combined in a single coalesced batch.
	// Zero defaults to 32.
	MaxBatchSize int
	// Router resolves (Provider, Model) for each Request. Defaults to NewTableRouter().
	Router ModelRouter
}

// CompleteFunc is the signature of AIGateway.Complete, used by Middleware.
type CompleteFunc func(ctx context.Context, req Request) (Response, error)

// Middleware wraps CompleteFunc for cross-cutting concerns (logging, budgets, tracing).
type Middleware func(next CompleteFunc) CompleteFunc

// AIGateway is the centralized model dispatcher shared across projects and agents.
// It handles provider routing, per-project token accounting, singleflight deduplication,
// content-addressed caching, cross-project micro-batching, and schema validation.
type AIGateway struct {
	cfg    Config
	router ModelRouter

	mu          sync.RWMutex
	providers   map[string]Provider
	defaultProv string
	middlewares []Middleware

	usageMu sync.RWMutex
	usage   map[string]Usage

	flightMu sync.Mutex
	flights  map[string]*inFlightCall

	batchMu sync.Mutex
	batches map[batchKey]*pendingBatch
}

type inFlightCall struct {
	done    chan struct{}
	resp    Response
	err     error
	mu      sync.Mutex
	waiters int
	cancel  context.CancelFunc
}

type batchKey struct {
	provider string
	model    string
}

type batchItem struct {
	ctx  context.Context
	req  Request
	resp chan batchResult
}

type batchResult struct {
	resp Response
	err  error
}

type pendingBatch struct {
	key   batchKey
	items []*batchItem
	timer *time.Timer
}

// New creates a new AIGateway with the given configuration.
func New(cfg Config) *AIGateway {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 32
	}
	if cfg.MaxCacheEntries <= 0 {
		cfg.MaxCacheEntries = 1024
	}
	if cfg.EnableCache && cfg.Cache == nil {
		cfg.Cache = NewMemoryCache(cfg.MaxCacheEntries)
	}
	router := cfg.Router
	if router == nil {
		router = NewTableRouter()
	}
	return &AIGateway{
		cfg:       cfg,
		router:    router,
		providers: make(map[string]Provider),
		usage:     make(map[string]Usage),
		flights:   make(map[string]*inFlightCall),
		batches:   make(map[batchKey]*pendingBatch),
	}
}

// AddRoute registers a model/alias route when the gateway uses a *TableRouter.
func (g *AIGateway) AddRoute(alias, provider, model string) {
	if tr, ok := g.router.(*TableRouter); ok {
		tr.AddRoute(alias, provider, model)
	}
}

// Use appends one or more Middleware interceptors around Complete.
func (g *AIGateway) Use(mw ...Middleware) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.middlewares = append(g.middlewares, mw...)
}

// Register adds a provider under the given name. The first registered provider
// becomes the default unless SetDefault is called.
func (g *AIGateway) Register(name string, p Provider) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.providers[name] = p
	if g.defaultProv == "" {
		g.defaultProv = name
	}
}

// SetDefault sets the fallback provider name when a request does not specify one.
func (g *AIGateway) SetDefault(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.defaultProv = name
}

// UsageByProject returns accumulated token usage for a given projectID.
func (g *AIGateway) UsageByProject(projectID string) Usage {
	g.usageMu.RLock()
	defer g.usageMu.RUnlock()
	return g.usage[projectID]
}

// AllUsage returns a snapshot of token usage across all projects.
func (g *AIGateway) AllUsage() map[string]Usage {
	g.usageMu.RLock()
	defer g.usageMu.RUnlock()
	out := make(map[string]Usage, len(g.usage))
	for k, v := range g.usage {
		out[k] = v
	}
	return out
}

// Complete dispatches a normalized request through registered Middleware, cache,
// singleflight, optional cross-project coalescing, provider execution, schema validation,
// and token accounting.
func (g *AIGateway) Complete(ctx context.Context, req Request) (Response, error) {
	g.mu.RLock()
	mws := make([]Middleware, len(g.middlewares))
	copy(mws, g.middlewares)
	g.mu.RUnlock()

	fn := g.completeCore
	for i := len(mws) - 1; i >= 0; i-- {
		fn = mws[i](fn)
	}
	return fn(ctx, req)
}

func (g *AIGateway) completeCore(ctx context.Context, req Request) (Response, error) {
	prov, resolvedModel, err := g.resolveProvider(ctx, req)
	if err != nil {
		return Response{}, err
	}
	req.Provider = prov.Name()
	req.Model = resolvedModel

	key := hashRequest(req)

	// 1. Content-addressed cache lookup.
	if g.cfg.EnableCache && !req.BypassCache {
		if resp, ok := g.cacheGet(ctx, key); ok {
			resp.Cached = true
			return resp, nil
		}
	}

	// 2. Singleflight deduplication for identical concurrent requests.
	g.flightMu.Lock()
	call, joined := g.flights[key]
	if joined {
		call.mu.Lock()
		call.waiters++
		call.mu.Unlock()
		g.flightMu.Unlock()
	} else {
		flightCtx, cancelFlight := context.WithCancel(context.WithoutCancel(ctx))
		call = &inFlightCall{
			done:    make(chan struct{}),
			waiters: 1,
			cancel:  cancelFlight,
		}
		g.flights[key] = call
		g.flightMu.Unlock()

		go func() {
			defer func() {
				if r := recover(); r != nil {
					call.resp = Response{}
					call.err = fmt.Errorf("gateway: panic in provider %q: %v", prov.Name(), r)
				}
				cancelFlight()
				g.flightMu.Lock()
				delete(g.flights, key)
				g.flightMu.Unlock()
				close(call.done)
			}()

			// 3. Execute directly or via cross-project coalescing window.
			var resp Response
			var callErr error
			if g.shouldCoalesce(req) {
				resp, callErr = g.enqueueCoalesced(flightCtx, prov, req)
			} else {
				resp, callErr = prov.Complete(flightCtx, req)
			}

			if callErr == nil && req.ResponseSchema != nil && len(resp.ToolCalls) == 0 {
				if verr := req.ResponseSchema.Validate(json.RawMessage(resp.Text)); verr != nil {
					callErr = fmt.Errorf("gateway: response schema validation failed: %w", verr)
				}
			}

			call.resp = resp
			call.err = callErr

			if callErr == nil && g.cfg.EnableCache && !req.BypassCache {
				g.cachePut(flightCtx, key, resp)
			}
		}()
	}

	select {
	case <-call.done:
		if call.err == nil {
			if !joined || req.ProjectID != "" || len(req.Meta) > 0 {
				g.recordUsage(ctx, req, call.resp.Usage)
			}
		}
		return call.resp, call.err
	case <-ctx.Done():
		call.mu.Lock()
		call.waiters--
		shouldCancel := call.waiters == 0 && call.cancel != nil
		cancelFn := call.cancel
		call.mu.Unlock()
		if shouldCancel {
			cancelFn()
		}
		return Response{}, ctx.Err()
	}
}

func (g *AIGateway) resolveProvider(ctx context.Context, req Request) (Provider, string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	provName, model, err := g.router.Route(ctx, req, g.providers, g.defaultProv)
	if err != nil {
		return nil, "", err
	}
	p, ok := g.providers[provName]
	if !ok {
		return nil, "", fmt.Errorf("%w: %q", ErrNoProvider, provName)
	}
	return p, model, nil
}

func (g *AIGateway) shouldCoalesce(req Request) bool {
	return g.cfg.CoalesceWindow > 0 && req.Coalesce
}

func (g *AIGateway) enqueueCoalesced(ctx context.Context, prov Provider, req Request) (Response, error) {
	bkey := batchKey{provider: prov.Name(), model: req.Model}
	item := &batchItem{
		ctx:  ctx,
		req:  req,
		resp: make(chan batchResult, 1),
	}

	g.batchMu.Lock()
	b, exists := g.batches[bkey]
	if !exists {
		b = &pendingBatch{key: bkey}
		g.batches[bkey] = b
		b.timer = time.AfterFunc(g.cfg.CoalesceWindow, func() {
			g.flushBatch(bkey, b, prov)
		})
	}
	b.items = append(b.items, item)
	flushNow := len(b.items) >= g.cfg.MaxBatchSize
	if flushNow {
		b.timer.Stop()
		delete(g.batches, bkey)
	}
	g.batchMu.Unlock()

	if flushNow {
		go g.executeBatch(b.items, prov)
	}

	select {
	case res := <-item.resp:
		return res.resp, res.err
	case <-ctx.Done():
		return Response{}, ctx.Err()
	}
}

func (g *AIGateway) flushBatch(bkey batchKey, target *pendingBatch, prov Provider) {
	g.batchMu.Lock()
	cur, ok := g.batches[bkey]
	if !ok || cur != target {
		g.batchMu.Unlock()
		return
	}
	delete(g.batches, bkey)
	g.batchMu.Unlock()

	g.executeBatch(target.items, prov)
}

func (g *AIGateway) executeBatch(items []*batchItem, prov Provider) {
	if len(items) == 0 {
		return
	}

	// Filter out already-canceled items before dispatching.
	active := make([]*batchItem, 0, len(items))
	for _, it := range items {
		if err := it.ctx.Err(); err != nil {
			it.resp <- batchResult{err: err}
			continue
		}
		active = append(active, it)
	}
	if len(active) == 0 {
		return
	}

	// Single item doesn't need batch overhead.
	if len(active) == 1 {
		resp, err := prov.Complete(active[0].ctx, active[0].req)
		active[0].resp <- batchResult{resp: resp, err: err}
		return
	}

	// Use native BatchProvider if available; otherwise fan out concurrently.
	if bp, ok := prov.(BatchProvider); ok {
		reqs := make([]Request, len(active))
		for i, it := range active {
			reqs[i] = it.req
		}
		batchCtx, cancelBatch := mergeBatchContext(active)
		defer cancelBatch()
		resps, errs := bp.BatchComplete(batchCtx, reqs)
		for i, it := range active {
			var r Response
			var e error
			if i < len(resps) {
				r = resps[i]
			}
			if i < len(errs) {
				e = errs[i]
			}
			it.resp <- batchResult{resp: r, err: e}
		}
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(active))
	for _, it := range active {
		go func(item *batchItem) {
			defer wg.Done()
			resp, err := prov.Complete(item.ctx, item.req)
			item.resp <- batchResult{resp: resp, err: err}
		}(it)
	}
	wg.Wait()
}

func mergeBatchContext(items []*batchItem) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		for _, it := range items {
			select {
			case <-it.ctx.Done():
			case <-done:
				return
			}
		}
		cancel()
	}()
	return ctx, func() {
		close(done)
		cancel()
	}
}

// Stream dispatches a streaming request to the routed provider if it implements StreamProvider,
// otherwise falls back to Complete and yields a single chunk.
func (g *AIGateway) Stream(ctx context.Context, req Request) iter.Seq2[StreamChunk, error] {
	return func(yield func(StreamChunk, error) bool) {
		prov, resolvedModel, err := g.resolveProvider(ctx, req)
		if err != nil {
			yield(StreamChunk{}, err)
			return
		}
		req.Provider = prov.Name()
		req.Model = resolvedModel

		if sp, ok := prov.(StreamProvider); ok {
			for chunk, err := range sp.Stream(ctx, req) {
				if err == nil && chunk.Usage != nil {
					g.recordUsage(ctx, req, *chunk.Usage)
				}
				if !yield(chunk, err) || err != nil {
					return
				}
			}
			return
		}

		resp, err := g.Complete(ctx, req)
		if err != nil {
			yield(StreamChunk{}, err)
			return
		}
		u := resp.Usage
		yield(StreamChunk{
			Text:      resp.Text,
			ToolCalls: resp.ToolCalls,
			Usage:     &u,
			Done:      true,
		}, nil)
	}
}

func (g *AIGateway) recordUsage(ctx context.Context, req Request, u Usage) {
	if g.cfg.UsageSink != nil {
		g.cfg.UsageSink.Record(ctx, req, u)
	}
	key := req.ProjectID
	if g.cfg.UsageKey != nil {
		key = g.cfg.UsageKey(req)
	}
	if key == "" {
		key = "_default"
	}
	g.usageMu.Lock()
	cur := g.usage[key]
	cur.Add(u)
	g.usage[key] = cur
	g.usageMu.Unlock()
}

func (g *AIGateway) cacheGet(ctx context.Context, key string) (Response, bool) {
	if g.cfg.Cache == nil {
		return Response{}, false
	}
	return g.cfg.Cache.Get(ctx, key)
}

func (g *AIGateway) cachePut(ctx context.Context, key string, resp Response) {
	if g.cfg.Cache == nil {
		return
	}
	g.cfg.Cache.Set(ctx, key, resp, g.cfg.CacheTTL)
}

func hashRequest(req Request) string {
	// Exclude ProjectID from the content hash so identical prompts across projects
	// can share cache and singleflight deduplication.
	canonical := struct {
		Provider           string
		Model              string
		SystemPrefix       string
		Messages           []Message
		Tools              []ToolSpec
		ResponseSchema     *JSONSchema
		ResponseModalities []Modality
		SpeechConfig       *SpeechConfig
		Temperature        *float64
		MaxTokens          int
	}{
		Provider:           req.Provider,
		Model:              req.Model,
		SystemPrefix:       req.SystemPrefix,
		Messages:           req.Messages,
		Tools:              req.Tools,
		ResponseSchema:     req.ResponseSchema,
		ResponseModalities: req.ResponseModalities,
		SpeechConfig:       req.SpeechConfig,
		Temperature:        req.Temperature,
		MaxTokens:          req.MaxTokens,
	}
	b, _ := json.Marshal(canonical)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
