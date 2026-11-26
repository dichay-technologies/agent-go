package workersai_test

import (
	"testing"

	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/cloudflare/workersai"
)

func TestDialectSingleAndBatch(t *testing.T) {
	d := workersai.Dialect{}
	suffix, _, _, err := d.BuildRequest("@cf/meta/llama-3.1-8b-instruct-fast", gateway.Request{
		SystemPrefix: "sys",
		Messages:     []gateway.Message{{Role: gateway.MsgUser, Content: "hi"}},
	})
	if err != nil || suffix != "run/@cf/meta/llama-3.1-8b-instruct-fast" {
		t.Fatalf("BuildRequest failed: suffix=%q err=%v", suffix, err)
	}
	bsuf, _, _, err := d.BuildBatchRequest("m1", []gateway.Request{{Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "1"}}}})
	if err != nil || bsuf != "run/m1?queueRequest=true" {
		t.Fatalf("BuildBatchRequest failed: %q err=%v", bsuf, err)
	}
	reqID, err := d.ParseBatchSubmit([]byte(`{"success":true,"result":{"id":"r1","status":"queued"}}`))
	if err != nil || reqID != "r1" || d.BatchPollPath("m1", reqID) != "run/m1?requestId=r1" {
		t.Fatalf("batch submit/poll failed: id=%q err=%v", reqID, err)
	}
}

