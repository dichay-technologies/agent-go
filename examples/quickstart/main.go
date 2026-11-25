package main

import (
	"context"
	"fmt"
	"log"

	"proj.dichay.tech/agent-go"
	"proj.dichay.tech/agent-go/gateway"
	"proj.dichay.tech/agent-go/gateway/mock"
)

type AppState struct {
	Notes []string
}

func main() {
	gw := gateway.New(gateway.Config{EnableCache: true})
	gw.Register("mock", mock.New("mock", func(_ context.Context, req gateway.Request) (gateway.Response, error) {
		if len(req.Tools) > 0 {
			return gateway.Response{
				ToolCalls: []gateway.ToolCall{{
					ID:        "call_1",
					Name:      "greet",
					Arguments: []byte(`{"name":"Alice"}`),
				}},
			}, nil
		}
		last := req.Messages[len(req.Messages)-1].Content
		return gateway.Response{
			Text:  fmt.Sprintf("[%s] %s", req.Model, last),
			Usage: gateway.Usage{InputTokens: 10, OutputTokens: 5},
		}, nil
	}))
	gw.AddRoute("fast", "mock", "gemini-4-argon")

	helloTool := agentgo.NewTypedTool(
		"greet",
		"Greets a user by name",
		&gateway.JSONSchema{
			Type:       "object",
			Required:   []string{"name"},
			Properties: map[string]*gateway.JSONSchema{"name": {Type: "string"}},
		},
		false,
		func(ctx agentgo.Context, in struct{ Name string `json:"name"` }) (any, error) {
			resp, err := ctx.Call(gateway.Request{
				Model:    "fast",
				Messages: []gateway.Message{{Role: gateway.MsgUser, Content: "Say hi to " + in.Name}},
			})
			if err != nil {
				return nil, err
			}
			return resp.Text, nil
		},
	)

	ag := agentgo.New[AppState](
		agentgo.WithID[AppState]("quickstart"),
		agentgo.WithProject[AppState]("demo"),
		agentgo.WithGateway[AppState](gw),
		agentgo.WithModel[AppState]("fast"),
		agentgo.WithTask[AppState]("Greet Alice"),
		agentgo.WithTools[AppState](helloTool),
		agentgo.WithHooks(agentgo.Hooks[AppState]{
			OnStep: func(_ agentgo.Context, st *agentgo.LocalState[AppState], stepID string, res agentgo.ToolCallResult) {
				st.Update(func(s *AppState) {
					s.Notes = append(s.Notes, stepID+":"+res.Text)
				})
			},
		}),
	)

	out, err := ag.Run(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Result:", out)
	fmt.Printf("Usage: %+v\n", gw.UsageByProject("demo"))
}

