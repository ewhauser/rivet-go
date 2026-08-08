package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/ewhauser/rivet-go/rivet"
)

const maxHTTPBodyBytes = 1 << 20

type httpCounterState struct {
	Count int `json:"count"`
}

type incrementRequest struct {
	Amount int `json:"amount"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "http-counter:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "http-counter-example", "engine-visible runner name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	registry := rivet.NewRegistry()
	if err := registerHTTPCounter(registry); err != nil {
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

func registerHTTPCounter(registry *rivet.Registry) error {
	return rivet.Register(registry, "http-counter", rivet.Actor[httpCounterState]{
		OnFetch: func(ctx *rivet.Context[httpCounterState], writer http.ResponseWriter, request *http.Request) {
			httpCounterHandler{ctx: ctx}.ServeHTTP(writer, request)
		},
	})
}

type httpCounterHandler struct {
	ctx *rivet.Context[httpCounterState]
}

func (h httpCounterHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.URL.Path {
	case "/count":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, http.MethodGet)
			return
		}
		writeJSON(writer, http.StatusOK, *h.ctx.State())
	case "/increment":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer, http.MethodPost)
			return
		}
		request.Body = http.MaxBytesReader(writer, request.Body, maxHTTPBodyBytes)
		input, err := decodeIncrement(request.Body)
		if err != nil {
			http.Error(writer, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
			return
		}
		previous := h.ctx.State().Count
		h.ctx.State().Count += input.Amount
		if err := h.ctx.Save(request.Context()); err != nil {
			h.ctx.State().Count = previous
			http.Error(writer, "persist counter: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(writer, http.StatusOK, *h.ctx.State())
	case "/reset":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer, http.MethodPost)
			return
		}
		previous := h.ctx.State().Count
		h.ctx.State().Count = 0
		if err := h.ctx.Save(request.Context()); err != nil {
			h.ctx.State().Count = previous
			http.Error(writer, "persist counter: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(writer, http.StatusOK, *h.ctx.State())
	default:
		http.NotFound(writer, request)
	}
}

func decodeIncrement(reader io.Reader) (incrementRequest, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var input incrementRequest
	if err := decoder.Decode(&input); err != nil {
		return incrementRequest{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return incrementRequest{}, fmt.Errorf("body must contain one JSON value")
		}
		return incrementRequest{}, err
	}
	return input, nil
}

func methodNotAllowed(writer http.ResponseWriter, allowed string) {
	writer.Header().Set("Allow", allowed)
	http.Error(writer, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
