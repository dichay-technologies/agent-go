package agentgo

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/mock"
)

func TestRenderPromptAndSlicing(t *testing.T) {
	out, err := RenderPrompt("Task: {{.Task}} (Attempt {{.Attempt}})", map[string]any{
		"Task":    "fix bug",
		"Attempt": 2,
	})
	if err != nil {
		t.Fatalf("RenderPrompt failed: %v", err)
	}
	if out != "Task: fix bug (Attempt 2)" {
		t.Fatalf("unexpected rendered prompt: %q", out)
	}

	src := "line1\nfunc Foo() {}\nline3\nfunc Bar() {}\nline5"
	matches, err := SliceByRegex(src, `func \w+\(\) \{\}`, -1)
	if err != nil {
		t.Fatalf("SliceByRegex failed: %v", err)
	}
	if len(matches) != 2 || matches[0] != "func Foo() {}" || matches[1] != "func Bar() {}" {
		t.Fatalf("unexpected regex matches: %+v", matches)
	}

	lines := SliceLines(src, 2, 4)
	if lines != "func Foo() {}\nline3\nfunc Bar() {}" {
		t.Fatalf("unexpected SliceLines: %q", lines)
	}
}

func TestDataSourceAndWindowValidation(t *testing.T) {
	ctx := NewContext(context.Background(), ContextConfig{})
	ds := StaticData("readme", "hello world")
	txt, err := ds.Content(ctx)
	if err != nil || ds.Name() != "readme" || txt != "hello world" {
		t.Fatalf("unexpected DataSource result: name=%q txt=%q err=%v", ds.Name(), txt, err)
	}

	if err := ValidateWindow("sys", "short", 100); err != nil {
		t.Fatalf("expected valid window, got: %v", err)
	}
	err = ValidateWindow(strings.Repeat("a", 400), "short", 50)
	if err == nil || !strings.Contains(err.Error(), "static system prefix") {
		t.Fatalf("expected prefix overflow error, got: %v", err)
	}
}

func TestCompressContextWithMeaningVerification(t *testing.T) {
	var compCalls, verCalls atomic.Int32
	gw := gateway.New(gateway.Config{})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		if req.ResponseSchema != nil {
			if verCalls.Add(1) == 1 {
				return gateway.Response{Text: `{"ok":false,"missing":["port 8080"]}`}, nil
			}
			return gateway.Response{Text: `{"ok":true}`}, nil
		}
		if compCalls.Add(1) == 1 {
			return gateway.Response{Text: "server runs"}, nil
		}
		return gateway.Response{Text: "server runs on port 8080"}, nil
	}))

	ctx := NewContext(context.Background(), ContextConfig{Gateway: gw})
	out, err := CompressContext(ctx, "m-comp", "m-ver", strings.Repeat("server runs on port 8080; ", 20), 20)
	if err != nil {
		t.Fatalf("CompressContext failed: %v", err)
	}
	if out != "server runs on port 8080" {
		t.Fatalf("unexpected compressed text: %q", out)
	}
	if compCalls.Load() != 2 || verCalls.Load() != 2 {
		t.Fatalf("expected 2 compress + 2 verify calls, got comp=%d ver=%d", compCalls.Load(), verCalls.Load())
	}
}

