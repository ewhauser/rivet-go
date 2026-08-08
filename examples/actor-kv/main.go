package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/ewhauser/rivet-go/rivet"
)

type kvState struct{}

type putTextArgs struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type getTextArgs struct {
	Key string `json:"key"`
}

type listTextArgs struct {
	Prefix  string `json:"prefix"`
	Reverse bool   `json:"reverse"`
	Limit   uint32 `json:"limit"`
}

type bytesRoundtripArgs struct {
	Key    string `json:"key"`
	Values []int  `json:"values"`
}

type textValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type textEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "actor-kv:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "actor-kv-example", "engine-visible runner name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry := rivet.NewRegistry()
	if err := registerKVStore(registry); err != nil {
		return err
	}
	return rivet.Serve(registry, rivet.Config{
		Endpoint:   endpoint,
		RunnerName: runnerName,
		TotalSlots: 16,
		LogLevel:   "info",
		Logger:     logger,
	})
}

func registerKVStore(registry *rivet.Registry) error {
	return rivet.Register(registry, "kv-store", rivet.Actor[kvState]{
		Actions: rivet.Actions[kvState]{
			"putText": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[kvState], args putTextArgs) (bool, error) {
				if err := validateKVKey(args.Key); err != nil {
					return false, err
				}
				return true, ctx.KV().Put(actionCtx, []byte(args.Key), []byte(args.Value))
			}),
			"getText": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[kvState], args getTextArgs) (textValue, error) {
				if err := validateKVKey(args.Key); err != nil {
					return textValue{}, err
				}
				value, found, err := ctx.KV().Get(actionCtx, []byte(args.Key))
				if err != nil {
					return textValue{}, err
				}
				return textValue{Key: args.Key, Value: string(value), Found: found}, nil
			}),
			"listText": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[kvState], args listTextArgs) ([]textEntry, error) {
				entries, err := ctx.KV().List(actionCtx, rivet.KVListOptions{
					Prefix:  []byte(args.Prefix),
					Reverse: args.Reverse,
					Limit:   args.Limit,
				})
				if err != nil {
					return nil, err
				}
				result := make([]textEntry, len(entries))
				for index, entry := range entries {
					result[index] = textEntry{Key: string(entry.Key), Value: string(entry.Value)}
				}
				return result, nil
			}),
			"delete": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[kvState], args getTextArgs) (bool, error) {
				if err := validateKVKey(args.Key); err != nil {
					return false, err
				}
				_, found, err := ctx.KV().Get(actionCtx, []byte(args.Key))
				if err != nil {
					return false, err
				}
				if err := ctx.KV().Delete(actionCtx, []byte(args.Key)); err != nil {
					return false, err
				}
				return found, nil
			}),
			"roundtripBytes": rivet.ActionWithContext(func(actionCtx context.Context, ctx *rivet.Context[kvState], args bytesRoundtripArgs) ([]int, error) {
				if err := validateKVKey(args.Key); err != nil {
					return nil, err
				}
				value, err := byteValues(args.Values)
				if err != nil {
					return nil, err
				}
				if err := ctx.KV().Put(actionCtx, []byte(args.Key), value); err != nil {
					return nil, err
				}
				stored, found, err := ctx.KV().Get(actionCtx, []byte(args.Key))
				if err != nil {
					return nil, err
				}
				if !found {
					return nil, rivet.ActionError{Code: "kv_roundtrip_missing", Message: "stored byte value was not found"}
				}
				result := make([]int, len(stored))
				for index, item := range stored {
					result[index] = int(item)
				}
				return result, nil
			}),
		},
	})
}

func validateKVKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return rivet.ActionError{Code: "key_required", Message: "key must not be empty"}
	}
	return nil
}

func byteValues(values []int) ([]byte, error) {
	result := make([]byte, len(values))
	for index, value := range values {
		if value < 0 || value > 255 {
			return nil, rivet.ActionError{
				Code:    "invalid_byte",
				Message: fmt.Sprintf("values[%d] must be between 0 and 255", index),
			}
		}
		result[index] = byte(value)
	}
	return result, nil
}
