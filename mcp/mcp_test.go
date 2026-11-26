package mcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"testing"

	"proj.dichay.tech/agent-go/mcp"
)

type mockTransport struct{}

func (mockTransport) RoundTrip(_ context.Context, payload []byte) ([]byte, error) {
	var req struct {
		ID     uint64          `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	switch req.Method {
	case "initialize":
		return json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  map[string]any{"protocolVersion": "2024-11-05"},
		})
	case "notifications/initialized":
		return []byte(`{}`), nil
	case "tools/list":
		var p struct {
			Cursor string `json:"cursor"`
		}
		_ = json.Unmarshal(req.Params, &p)
		if p.Cursor == "" {
			return json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result": map[string]any{
					"tools": []map[string]any{{
						"name":        "ping",
						"description": "pings target",
						"inputSchema": map[string]any{"type": "object"},
					}},
					"nextCursor": "page2",
				},
			})
		}
		return json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"tools": []map[string]any{{
					"name":        "fail_tool",
					"description": "returns isError",
					"inputSchema": map[string]any{"type": "object"},
				}},
			},
		})
	case "tools/call":
		var callParams struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(req.Params, &callParams)
		if callParams.Name == "fail_tool" {
			return json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result": map[string]any{
					"isError": true,
					"content": []map[string]string{{"type": "text", "text": "disk full"}},
				},
			})
		}
		return json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  map[string]any{"ok": true},
		})
	default:
		return nil, nil
	}
}

func TestMCPLoadToolsAndCall(t *testing.T) {
	client := mcp.NewClient(mockTransport{})
	if err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	tools, err := mcp.LoadTools(context.Background(), client)
	if err != nil {
		t.Fatalf("LoadTools failed: %v", err)
	}
	if len(tools) != 2 || tools[0].Name() != "ping" || tools[1].Name() != "fail_tool" {
		t.Fatalf("unexpected paginated tools: %+v", tools)
	}
	out, err := tools[0].Execute(nil, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok || m["ok"] != true {
		t.Fatalf("unexpected tool output: %#v", out)
	}
	if _, err := tools[1].Execute(nil, json.RawMessage(`{}`)); err == nil {
		t.Fatalf("expected error from MCP tool with isError=true")
	}
}

func TestMCPHTTPTransportJSONAndSSE(t *testing.T) {
	var seenSession string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenSession = r.Header.Get("Mcp-Session-Id")
		var req struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Mcp-Session-Id", "sess-42")
		switch req.Method {
		case "initialize":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}`))
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[{\"name\":\"sse_tool\"}]}}\n\n"))
		}
	}))
	defer srv.Close()

	tr := mcp.NewHTTPTransport(mcp.HTTPConfig{Endpoint: srv.URL})
	client := mcp.NewClient(tr)
	if err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize over HTTP: %v", err)
	}
	descs, err := client.ListTools(context.Background())
	if err != nil || len(descs) != 1 || descs[0].Name != "sse_tool" || seenSession != "sess-42" {
		t.Fatalf("unexpected HTTP SSE tools: descs=%+v sess=%q err=%v", descs, seenSession, err)
	}
}

func TestMCPStdioTransport(t *testing.T) {
	// Minimal sh responder speaking newline-delimited JSON-RPC.
	cmd := exec.Command("sh", "-c", `while IFS= read -r line; do
		case "$line" in
			*'"tools/list"'*) echo '{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"stdio_ping"}]}}' ;;
		esac
	done`)
	tr, err := mcp.NewStdioTransport(cmd)
	if err != nil {
		t.Fatalf("NewStdioTransport: %v", err)
	}
	defer func() { _ = tr.Close() }()

	client := mcp.NewClient(tr)
	descs, err := client.ListTools(context.Background())
	if err != nil || len(descs) != 1 || descs[0].Name != "stdio_ping" {
		t.Fatalf("unexpected Stdio tools: %+v err=%v", descs, err)
	}
}

