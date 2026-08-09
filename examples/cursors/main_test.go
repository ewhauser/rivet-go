package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/ewhauser/rivet-go/rivet"
)

func TestRegisterCursorRoom(t *testing.T) {
	registry := rivet.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := registerCursorRoom(registry, logger); err != nil {
		t.Fatal(err)
	}
}

func TestCurrentCursorStateRequiresActorConnect(t *testing.T) {
	if _, err := currentCursorState(&rivet.Context[cursorRoomState]{}); err == nil {
		t.Fatal("stateless updateCursor call succeeded")
	}
}
