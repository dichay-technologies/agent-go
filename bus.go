package agentgo

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Envelope is a message or event exchanged between agents (or sent from host/user to an agent).
type Envelope struct {
	ID        string                 `json:"id"`
	From      string                 `json:"from"`
	To        string                 `json:"to"` // target agent ID, "parent", or "*" for broadcast
	Kind      string                 `json:"kind,omitempty"`
	Content   string                 `json:"content"`
	Data      map[string]any         `json:"data,omitempty"`
	Memory    map[string]MemoryEntry `json:"memory,omitempty"`
	ReplyTo   string                 `json:"reply_to,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// Clone returns a deep-enough copy of the Envelope.
func (e Envelope) Clone() Envelope {
	out := e
	if len(e.Data) > 0 {
		out.Data = make(map[string]any, len(e.Data))
		for k, v := range e.Data {
			out.Data[k] = v
		}
	}
	if len(e.Memory) > 0 {
		out.Memory = make(map[string]MemoryEntry, len(e.Memory))
		for k, v := range e.Memory {
			out.Memory[k] = v.Clone()
		}
	}
	return out
}

type mailbox struct {
	queue  []Envelope
	notify chan struct{}
	bank   *MemoryBank
	subs   map[uint64]func(Envelope)
}

func newMailbox() *mailbox {
	return &mailbox{
		notify: make(chan struct{}),
		subs:   make(map[uint64]func(Envelope)),
	}
}

func (m *mailbox) signalLocked() {
	close(m.notify)
	m.notify = make(chan struct{})
}

// Bus is a thread-safe in-memory message bus connecting a graph of agents (parent,
// subagents, peers, and external callers/chat UI).
type Bus struct {
	mu          sync.Mutex
	boxes       map[string]*mailbox
	registered  map[string]bool
	wildcardSub map[uint64]func(Envelope)
	seq         atomic.Uint64
	subSeq      atomic.Uint64
	maxInbox    int
}

// NewBus creates a new inter-agent message Bus.
func NewBus() *Bus {
	return &Bus{
		boxes:       make(map[string]*mailbox),
		registered:  make(map[string]bool),
		wildcardSub: make(map[uint64]func(Envelope)),
		maxInbox:    1024,
	}
}

func (b *Bus) boxLocked(agentID string) *mailbox {
	mb, ok := b.boxes[agentID]
	if !ok {
		mb = newMailbox()
		b.boxes[agentID] = mb
	}
	return mb
}

// Register binds an agent ID (and optional MemoryBank for automatic memory transfer)
// on the bus so it appears in Peers() and receives broadcast ("*") messages.
func (b *Bus) Register(agentID string, bank *MemoryBank) {
	if b == nil || agentID == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	mb := b.boxLocked(agentID)
	if bank != nil {
		mb.bank = bank
	}
	b.registered[agentID] = true
}

// Unregister removes agentID from active broadcast peers while keeping its mailbox.
func (b *Bus) Unregister(agentID string) {
	if b == nil || agentID == "" {
		return
	}
	b.mu.Lock()
	delete(b.registered, agentID)
	b.mu.Unlock()
}

// Peers returns sorted IDs of all registered agents on the bus.
func (b *Bus) Peers() []string {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	out := make([]string, 0, len(b.registered))
	for id := range b.registered {
		out = append(out, id)
	}
	b.mu.Unlock()
	slices.Sort(out)
	return out
}

// Send delivers an Envelope to a target agent ID or broadcasts it when To == "*".
// If env.Memory is populated and the recipient has a registered MemoryBank,
// those entries are automatically imported into the recipient's bank (without overwriting pinned keys).
func (b *Bus) Send(env Envelope) (Envelope, error) {
	if b == nil {
		return env, fmt.Errorf("bus: nil Bus")
	}
	if env.To == "" {
		return env, fmt.Errorf("bus: recipient (To) is required")
	}
	if env.ID == "" {
		env.ID = fmt.Sprintf("msg_%d", b.seq.Add(1))
	}
	if env.Kind == "" {
		env.Kind = "message"
	}
	if env.Timestamp.IsZero() {
		env.Timestamp = time.Now()
	}

	var callbacks []func(Envelope)
	var banksToSync []*MemoryBank

	b.mu.Lock()
	if env.To == "*" {
		for id := range b.registered {
			if id == env.From {
				continue
			}
			mb := b.boxLocked(id)
			b.enqueueLocked(mb, env.Clone())
			if mb.bank != nil && len(env.Memory) > 0 {
				banksToSync = append(banksToSync, mb.bank)
			}
			for _, fn := range mb.subs {
				callbacks = append(callbacks, fn)
			}
		}
	} else {
		mb := b.boxLocked(env.To)
		b.enqueueLocked(mb, env.Clone())
		if mb.bank != nil && len(env.Memory) > 0 {
			banksToSync = append(banksToSync, mb.bank)
		}
		for _, fn := range mb.subs {
			callbacks = append(callbacks, fn)
		}
	}
	for _, fn := range b.wildcardSub {
		callbacks = append(callbacks, fn)
	}
	b.mu.Unlock()

	if len(banksToSync) > 0 {
		tagged := make(map[string]MemoryEntry, len(env.Memory))
		for k, v := range env.Memory {
			cp := v.Clone()
			if cp.Source == "" && env.From != "" {
				cp.Source = "agent:" + env.From
			}
			cp.Pinned = false
			tagged[k] = cp
		}
		for _, bank := range banksToSync {
			bank.Import(tagged, false)
		}
	}

	for _, fn := range callbacks {
		if fn != nil {
			fn(env.Clone())
		}
	}
	return env, nil
}

func (b *Bus) enqueueLocked(mb *mailbox, env Envelope) {
	if b.maxInbox > 0 && len(mb.queue) >= b.maxInbox {
		mb.queue = mb.queue[1:]
	}
	mb.queue = append(mb.queue, env)
	mb.signalLocked()
}

// TryReceive pops the oldest pending message for agentID without blocking.
func (b *Bus) TryReceive(agentID string) (Envelope, bool) {
	if b == nil || agentID == "" {
		return Envelope{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	mb, ok := b.boxes[agentID]
	if !ok || len(mb.queue) == 0 {
		return Envelope{}, false
	}
	msg := mb.queue[0]
	mb.queue = mb.queue[1:]
	return msg, true
}

// Receive blocks until a message arrives for agentID or ctx is canceled.
func (b *Bus) Receive(ctx context.Context, agentID string) (Envelope, error) {
	if b == nil {
		return Envelope{}, fmt.Errorf("bus: nil Bus")
	}
	if agentID == "" {
		return Envelope{}, fmt.Errorf("bus: agentID is empty")
	}
	for {
		b.mu.Lock()
		mb := b.boxLocked(agentID)
		if len(mb.queue) > 0 {
			msg := mb.queue[0]
			mb.queue = mb.queue[1:]
			b.mu.Unlock()
			return msg, nil
		}
		waitCh := mb.notify
		b.mu.Unlock()

		select {
		case <-waitCh:
		case <-ctx.Done():
			return Envelope{}, ctx.Err()
		}
	}
}

// Drain atomically pops and returns all pending messages for agentID in FIFO order.
func (b *Bus) Drain(agentID string) []Envelope {
	if b == nil || agentID == "" {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	mb, ok := b.boxes[agentID]
	if !ok || len(mb.queue) == 0 {
		return nil
	}
	out := make([]Envelope, len(mb.queue))
	for i, m := range mb.queue {
		out[i] = m.Clone()
	}
	mb.queue = nil
	return out
}

// Peek returns a non-destructive copy of pending messages for agentID.
func (b *Bus) Peek(agentID string) []Envelope {
	if b == nil || agentID == "" {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	mb, ok := b.boxes[agentID]
	if !ok || len(mb.queue) == 0 {
		return nil
	}
	out := make([]Envelope, len(mb.queue))
	for i, m := range mb.queue {
		out[i] = m.Clone()
	}
	return out
}

// Subscribe registers a reactive listener for messages sent to agentID (or "*" for all bus traffic).
// Returns an unsubscribe function.
func (b *Bus) Subscribe(agentID string, fn func(Envelope)) func() {
	if b == nil || fn == nil {
		return func() {}
	}
	id := b.subSeq.Add(1)
	b.mu.Lock()
	if agentID == "*" {
		b.wildcardSub[id] = fn
		b.mu.Unlock()
		return func() {
			b.mu.Lock()
			delete(b.wildcardSub, id)
			b.mu.Unlock()
		}
	}
	mb := b.boxLocked(agentID)
	mb.subs[id] = fn
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		if cur, ok := b.boxes[agentID]; ok {
			delete(cur.subs, id)
		}
		b.mu.Unlock()
	}
}

// BusTools returns built-in tools allowing an agent to discover peers, send messages
// (optionally sharing memory keys), and receive messages on the shared Bus.
func BusTools() []Tool {
	type sendIn struct {
		To              string         `json:"to" desc:"Recipient agent ID, 'parent' for parent agent, or '*' for broadcast"`
		Kind            string         `json:"kind,omitempty" desc:"Optional message type (e.g. message, task, result, signal)"`
		Content         string         `json:"content" desc:"Message text content"`
		Data            map[string]any `json:"data,omitempty" desc:"Optional structured payload"`
		ShareMemoryKeys []string       `json:"share_memory_keys,omitempty" desc:"Optional keys from this agent's memory bank to send to the recipient"`
		ReplyTo         string         `json:"reply_to,omitempty" desc:"Optional message ID being replied to"`
	}
	sendTool := Func("agent_send", "Sends a message (and optional memory entries) to another agent, 'parent', or '*' broadcast", func(ctx Context, in sendIn) (map[string]any, error) {
		bus := ctx.Bus()
		if bus == nil {
			return nil, fmt.Errorf("agent_send: no Bus attached to context")
		}
		to := in.To
		if to == "parent" {
			if cur := ctx.Agent(); cur != nil && cur.Parent() != nil {
				if pid := agentIdentifier(cur.Parent()); pid != "" {
					to = pid
				}
			}
			if to == "parent" {
				return nil, fmt.Errorf("agent_send: current agent has no addressable parent")
			}
		}
		var shared map[string]MemoryEntry
		if len(in.ShareMemoryKeys) > 0 && ctx.Bank() != nil {
			shared = ctx.Bank().Export(in.ShareMemoryKeys...)
		}
		from := ""
		if cur := ctx.Agent(); cur != nil {
			from = agentIdentifier(cur)
		}
		sent, err := bus.Send(Envelope{
			From:    from,
			To:      to,
			Kind:    in.Kind,
			Content: in.Content,
			Data:    in.Data,
			Memory:  shared,
			ReplyTo: in.ReplyTo,
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"id":          sent.ID,
			"to":          sent.To,
			"shared_keys": len(shared),
		}, nil
	})

	type recvIn struct {
		WaitMs int `json:"wait_ms,omitempty" desc:"Optional milliseconds to wait for an incoming message if inbox is currently empty (0 = non-blocking drain)"`
	}
	recvTool := Func("agent_recv", "Reads unread messages from this agent's inbox, optionally waiting up to wait_ms", func(ctx Context, in recvIn) (map[string]any, error) {
		bus := ctx.Bus()
		if bus == nil {
			return map[string]any{"messages": []Envelope{}}, nil
		}
		selfID := ""
		if cur := ctx.Agent(); cur != nil {
			selfID = agentIdentifier(cur)
		}
		if selfID == "" {
			return nil, fmt.Errorf("agent_recv: current agent has empty ID")
		}
		msgs := bus.Drain(selfID)
		if len(msgs) == 0 && in.WaitMs > 0 {
			waitDur := time.Duration(min(in.WaitMs, 30000)) * time.Millisecond
			wctx, cancel := context.WithTimeout(ctx, waitDur)
			defer cancel()
			if m, err := bus.Receive(wctx, selfID); err == nil {
				msgs = append(msgs, m)
				msgs = append(msgs, bus.Drain(selfID)...)
			}
		}
		return map[string]any{
			"count":    len(msgs),
			"messages": msgs,
		}, nil
	})

	type peersIn struct{}
	peersTool := Func("agent_peers", "Lists addressable peer agents on the shared bus, plus parent and named subagents", func(ctx Context, _ peersIn) (map[string]any, error) {
		var peers []string
		if ctx.Bus() != nil {
			peers = ctx.Bus().Peers()
		}
		parentID := ""
		if cur := ctx.Agent(); cur != nil && cur.Parent() != nil {
			parentID = agentIdentifier(cur.Parent())
		}
		return map[string]any{
			"peers":  peers,
			"parent": parentID,
		}, nil
	})

	return []Tool{sendTool, recvTool, peersTool}
}

type identifiableAgent interface {
	AgentID() string
}

func agentIdentifier(a Agent) string {
	if a == nil {
		return ""
	}
	if idf, ok := a.(identifiableAgent); ok {
		return idf.AgentID()
	}
	return ""
}

