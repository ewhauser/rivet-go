package main

import (
	"context"
	"testing"
)

type fakeProvider struct {
	reply string
	seen  []message
}

func (p *fakeProvider) Complete(_ context.Context, history []message) (string, error) {
	p.seen = append([]message(nil), history...)
	return p.reply, nil
}

func TestProviderBoundary(t *testing.T) {
	provider := &fakeProvider{reply: "model reply"}
	history := []message{{Role: "user", Content: "hello"}}
	reply, err := provider.Complete(context.Background(), history)
	if err != nil || reply != "model reply" || len(provider.seen) != 1 || provider.seen[0].Content != "hello" {
		t.Fatalf("provider completion = %q, %v, seen %#v", reply, err, provider.seen)
	}
	history[0].Content = "mutated"
	if provider.seen[0].Content != "hello" {
		t.Fatal("provider retained caller-owned history")
	}
}

func TestEchoProviderAndPromptIDs(t *testing.T) {
	reply, err := (echoProvider{}).Complete(context.Background(), []message{
		{Role: "assistant", Content: "old"},
		{Role: "user", Content: "latest"},
	})
	if err != nil || reply != "Echo: latest" {
		t.Fatalf("echo completion = %q, %v", reply, err)
	}
	first, err := newPromptID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newPromptID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || len(first) != len("prompt-")+16 {
		t.Fatalf("prompt IDs = %q, %q", first, second)
	}
}

func TestHasUserMessage(t *testing.T) {
	messages := []message{{RequestID: "one", Role: "assistant"}, {RequestID: "two", Role: "user"}}
	if hasUserMessage(messages, "one") || !hasUserMessage(messages, "two") {
		t.Fatalf("unexpected request membership in %#v", messages)
	}
}
