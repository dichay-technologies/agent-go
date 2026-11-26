package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

// Cache is a pluggable response cache for AIGateway (e.g., in-memory, disk, or Redis).
type Cache interface {
	Get(ctx context.Context, key string) (Response, bool)
	Set(ctx context.Context, key string, resp Response, ttl time.Duration)
}

type memoryCacheEntry struct {
	resp      Response
	expiresAt time.Time
}

func cloneResponse(r Response) Response {
	out := r
	if len(r.Parts) > 0 {
		out.Parts = make([]Part, len(r.Parts))
		for i, p := range r.Parts {
			cp := p
			if len(p.Data) > 0 {
				cp.Data = append([]byte(nil), p.Data...)
			}
			out.Parts[i] = cp
		}
	}
	if len(r.ToolCalls) > 0 {
		out.ToolCalls = make([]ToolCall, len(r.ToolCalls))
		for i, tc := range r.ToolCalls {
			ctc := tc
			if len(tc.Arguments) > 0 {
				ctc.Arguments = append([]byte(nil), tc.Arguments...)
			}
			out.ToolCalls[i] = ctc
		}
	}
	return out
}

// MemoryCache is a bounded, thread-safe in-memory Cache with optional TTL and FIFO eviction.
type MemoryCache struct {
	mu         sync.RWMutex
	maxEntries int
	entries    map[string]memoryCacheEntry
	keys       []string
}

// NewMemoryCache creates a bounded MemoryCache. If maxEntries <= 0, it defaults to 1024.
func NewMemoryCache(maxEntries int) *MemoryCache {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	return &MemoryCache{
		maxEntries: maxEntries,
		entries:    make(map[string]memoryCacheEntry),
	}
}

func (c *MemoryCache) Get(_ context.Context, key string) (Response, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return Response{}, false
	}
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		c.mu.Lock()
		delete(c.entries, key)
		c.keys = slices.DeleteFunc(c.keys, func(k string) bool { return k == key })
		c.mu.Unlock()
		return Response{}, false
	}
	return cloneResponse(entry.resp), true
}

func (c *MemoryCache) Set(_ context.Context, key string, resp Response, ttl time.Duration) {
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[key]; !exists {
		for len(c.keys) > 0 && len(c.entries) >= c.maxEntries {
			oldest := c.keys[0]
			c.keys[0] = ""
			c.keys = c.keys[1:]
			delete(c.entries, oldest)
		}
		c.keys = append(c.keys, key)
	}
	c.entries[key] = memoryCacheEntry{resp: cloneResponse(resp), expiresAt: exp}
}

type diskCacheRecord struct {
	Response  Response  `json:"response"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// FileCache is a zero-dependency persistent directory Cache storing each response
// as an atomic JSON file named by SHA-256 digest.
type FileCache struct {
	dir string
	mu  sync.RWMutex
}

// NewFileCache creates a directory-backed persistent Cache at dir.
func NewFileCache(dir string) (*FileCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileCache{dir: dir}, nil
}

func (fc *FileCache) pathFor(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(fc.dir, hex.EncodeToString(sum[:])+".json")
}

func (fc *FileCache) Get(_ context.Context, key string) (Response, bool) {
	p := fc.pathFor(key)
	fc.mu.RLock()
	raw, err := os.ReadFile(p)
	fc.mu.RUnlock()
	if err != nil {
		return Response{}, false
	}
	var rec diskCacheRecord
	if json.Unmarshal(raw, &rec) != nil {
		return Response{}, false
	}
	if !rec.ExpiresAt.IsZero() && time.Now().After(rec.ExpiresAt) {
		fc.mu.Lock()
		_ = os.Remove(p)
		fc.mu.Unlock()
		return Response{}, false
	}
	return cloneResponse(rec.Response), true
}

func (fc *FileCache) Set(_ context.Context, key string, resp Response, ttl time.Duration) {
	var rec diskCacheRecord
	rec.Response = cloneResponse(resp)
	if ttl > 0 {
		rec.ExpiresAt = time.Now().Add(ttl)
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return
	}
	p := fc.pathFor(key)
	fc.mu.Lock()
	defer fc.mu.Unlock()
	tmp := p + ".tmp"
	if os.WriteFile(tmp, raw, 0o600) == nil {
		_ = os.Rename(tmp, p)
	}
}
