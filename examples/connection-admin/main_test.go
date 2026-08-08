package main

import "testing"

func TestConnectionLabel(t *testing.T) {
	if got := connectionLabel(map[string]string{"X-Client-Label": " header "}, "/websocket/admin?client=query"); got != "header" {
		t.Fatalf("header label = %q", got)
	}
	if got := connectionLabel(nil, "/websocket/admin?client=query"); got != "query" {
		t.Fatalf("query label = %q", got)
	}
	if got := connectionLabel(nil, "not a request URI"); got != "" {
		t.Fatalf("invalid path label = %q", got)
	}
}

func TestNormalizeDisconnect(t *testing.T) {
	code, reason, err := normalizeDisconnect(0, "")
	if err != nil || code != 4000 || reason != "disconnected by actor" {
		t.Fatalf("default disconnect = %d %q %v", code, reason, err)
	}
	code, reason, err = normalizeDisconnect(4001, " removed ")
	if err != nil || code != 4001 || reason != "removed" {
		t.Fatalf("explicit disconnect = %d %q %v", code, reason, err)
	}
	for _, code := range []int{1000, 2999, 5000} {
		if _, _, err := normalizeDisconnect(code, "invalid"); err == nil {
			t.Fatalf("invalid close code %d accepted", code)
		}
	}
}
