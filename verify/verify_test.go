package verify_test

import (
	"errors"
	"testing"

	agentgo "proj.dichay.tech/agent-go"
	"proj.dichay.tech/agent-go/verify"
)

func TestEnsembleVerifierAndACL(t *testing.T) {
	ev := verify.EnsembleVerifier{
		RequireAllPass: true,
		Checks: []verify.CheckFunc{
			func(_ agentgo.Context, _ any) (verify.Verdict, string, error) { return verify.Pass, "", nil },
			func(_ agentgo.Context, _ any) (verify.Verdict, string, error) { return verify.Unsure, "ambiguous", nil },
		},
	}
	if err := ev.Verify(nil, "out"); err == nil {
		t.Fatalf("expected Unsure to fail when RequireAllPass=true")
	}
	ev.RequireAllPass = false
	if err := ev.Verify(nil, "out"); err != nil {
		t.Fatalf("expected Unsure to pass when RequireAllPass=false, got %v", err)
	}

	mw := verify.ACLToolMiddleware(func(_ agentgo.Context, name string) bool {
		return name == "safe"
	})
	inv := mw(agentgo.ToolInvokerFunc(func(_ agentgo.Context, req agentgo.ToolCallRequest) (agentgo.ToolCallResult, error) {
		if len(req.Tools) != 1 || req.Tools[0].Name() != "safe" {
			t.Fatalf("unexpected filtered tools: %v", req.Tools)
		}
		return agentgo.ToolCallResult{Text: "ok"}, nil
	}))

	safeTool := agentgo.NewTool(agentgo.ToolDef{Name: "safe"})
	adminTool := agentgo.NewTool(agentgo.ToolDef{Name: "admin"})

	if _, err := inv.Invoke(nil, agentgo.ToolCallRequest{Tools: []agentgo.Tool{safeTool, adminTool}}); err != nil {
		t.Fatalf("expected safe tool to be allowed, got %v", err)
	}
	if _, err := inv.Invoke(nil, agentgo.ToolCallRequest{Tools: []agentgo.Tool{adminTool}}); !errors.Is(err, verify.ErrToolDenied) {
		t.Fatalf("expected ErrToolDenied, got %v", err)
	}
}
