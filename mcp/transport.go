package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"proj.dichay.tech/agent-go/gateway"
)

// HTTPTransport implements Transport over MCP Streamable HTTP (JSON-RPC 2.0 POST,
// supporting both application/json and text/event-stream responses plus Mcp-Session-Id).
type HTTPTransport struct {
	endpoint   string
	httpClient *http.Client
	headers    map[string]string

	mu        sync.RWMutex
	sessionID string
}

// HTTPConfig configures an HTTPTransport.
type HTTPConfig struct {
	Endpoint   string
	Headers    map[string]string
	HTTPClient *http.Client
}

// NewHTTPTransport creates a Streamable HTTP MCP transport.
func NewHTTPTransport(cfg HTTPConfig) *HTTPTransport {
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	hdrs := make(map[string]string, len(cfg.Headers))
	for k, v := range cfg.Headers {
		hdrs[k] = v
	}
	return &HTTPTransport{
		endpoint:   cfg.Endpoint,
		httpClient: client,
		headers:    hdrs,
	}
}

func (t *HTTPTransport) RoundTrip(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("mcp http: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}
	t.mu.RLock()
	sid := t.sessionID
	t.mu.RUnlock()
	if sid != "" {
		req.Header.Set("Mcp-Session-Id", sid)
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mcp http: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if newSID := resp.Header.Get("Mcp-Session-Id"); newSID != "" {
		t.mu.Lock()
		t.sessionID = newSID
		t.mu.Unlock()
	}

	if resp.StatusCode == http.StatusAccepted {
		return []byte(`{}`), nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, fmt.Errorf("mcp http: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	ct := resp.Header.Get("Content-Type")
	if strings.Contains(ct, "text/event-stream") {
		for ev, sseErr := range gateway.ReadSSE(resp.Body) {
			if sseErr != nil {
				return nil, fmt.Errorf("mcp http: read sse: %w", sseErr)
			}
			data := strings.TrimSpace(ev.Data)
			if data == "" {
				continue
			}
			var probe struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  json.RawMessage `json:"error"`
			}
			if json.Unmarshal([]byte(data), &probe) == nil && (probe.ID != nil || len(probe.Result) > 0 || len(probe.Error) > 0) {
				return []byte(data), nil
			}
		}
		return nil, fmt.Errorf("mcp http: empty SSE stream without JSON-RPC response")
	}

	return io.ReadAll(io.LimitReader(resp.Body, 16<<20))
}

// StdioTransport implements Transport over a child process's stdin/stdout
// using newline-delimited JSON-RPC 2.0 messages.
type StdioTransport struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	reader *bufio.Reader
	mu     sync.Mutex
}

// NewStdioTransport starts cmd and connects to its stdin and stdout pipes.
// Call Close() when finished to terminate the subprocess.
func NewStdioTransport(cmd *exec.Cmd) (*StdioTransport, error) {
	if cmd == nil {
		return nil, fmt.Errorf("mcp stdio: cmd is nil")
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("mcp stdio: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("mcp stdio: stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("mcp stdio: start process: %w", err)
	}
	return &StdioTransport{
		cmd:    cmd,
		stdin:  stdin,
		reader: bufio.NewReaderSize(stdout, 64*1024),
	}, nil
}

func (s *StdioTransport) RoundTrip(ctx context.Context, payload []byte) ([]byte, error) {
	var sent struct {
		ID *uint64 `json:"id"`
	}
	_ = json.Unmarshal(payload, &sent)

	s.mu.Lock()
	defer s.mu.Unlock()

	line := append(bytes.TrimSpace(payload), '\n')
	if _, err := s.stdin.Write(line); err != nil {
		return nil, fmt.Errorf("mcp stdio: write: %w", err)
	}
	// One-way JSON-RPC notification expects no response frame.
	if sent.ID == nil {
		return []byte(`{}`), nil
	}

	type readOut struct {
		raw []byte
		err error
	}
	ch := make(chan readOut, 1)
	go func() {
		for {
			respLine, err := s.reader.ReadBytes('\n')
			if err != nil {
				ch <- readOut{err: err}
				return
			}
			trimmed := bytes.TrimSpace(respLine)
			if len(trimmed) == 0 {
				continue
			}
			var probe struct {
				ID *uint64 `json:"id"`
			}
			if json.Unmarshal(trimmed, &probe) == nil && probe.ID != nil && *probe.ID == *sent.ID {
				ch <- readOut{raw: trimmed}
				return
			}
		}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			return nil, fmt.Errorf("mcp stdio: read: %w", res.err)
		}
		return res.raw, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close closes stdin and terminates the underlying subprocess.
func (s *StdioTransport) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.stdin.Close()
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_ = s.cmd.Wait()
	}
	return nil
}

