package rivet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/ewhauser/rivet-go/internal/pump"
)

type recordedHTTPStart struct {
	status  uint16
	headers map[string]string
}

type recordedHTTPChunk struct {
	body   []byte
	finish bool
}

type recordingHTTPSession struct {
	mu     sync.Mutex
	starts []recordedHTTPStart
	chunks []recordedHTTPChunk
}

func (s *recordingHTTPSession) StartHTTPResponse(
	_ context.Context,
	_ uint64,
	status uint16,
	headers map[string]string,
	_ []byte,
	_ bool,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.starts = append(s.starts, recordedHTTPStart{status: status, headers: cloneHeaderMap(headers)})
	return nil
}

func (s *recordingHTTPSession) WriteHTTPResponseChunk(
	_ context.Context,
	_ uint64,
	body []byte,
	finish bool,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = append(s.chunks, recordedHTTPChunk{body: append([]byte(nil), body...), finish: finish})
	return nil
}

func cloneHeaderMap(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers))
	for name, value := range headers {
		cloned[name] = value
	}
	return cloned
}

func TestResponseWriterDoesNotAdvertiseUnavailableFlush(t *testing.T) {
	writer := newResponseWriter(&pump.ActorSession{}, 1, context.Background())
	if _, ok := any(writer).(http.Flusher); ok {
		t.Fatal("buffered M3 response writer advertises http.Flusher")
	}
}

func TestResponseWriterConcurrentWritesAreSafe(t *testing.T) {
	session := &recordingHTTPSession{}
	writer := newResponseWriter(session, 1, context.Background())
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 64 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, _ = writer.Write([]byte("concurrent"))
		}()
	}
	close(start)
	workers.Wait()
	if err := writer.finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if len(session.starts) != 1 {
		t.Fatalf("response starts = %d, want 1", len(session.starts))
	}
	total := 0
	finished := 0
	for _, chunk := range session.chunks {
		total += len(chunk.body)
		if chunk.finish {
			finished++
		}
	}
	if total != 64*len("concurrent") || finished != 1 {
		t.Fatalf("response chunks carried %d bytes and %d finishes", total, finished)
	}
}

func TestResponseWriterLocksStatusHeadersAndRejectsLateWrites(t *testing.T) {
	session := &recordingHTTPSession{}
	writer := newResponseWriter(session, 2, context.Background())
	writer.Header().Set("X-Before", "kept")
	writer.WriteHeader(http.StatusCreated)
	writer.Header().Set("X-After", "ignored")
	writer.WriteHeader(http.StatusAccepted)
	if _, err := writer.Write([]byte("body")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if _, err := writer.Write([]byte("late")); !errors.Is(err, errResponseWriterFinished) {
		t.Fatalf("late Write error = %v, want %v", err, errResponseWriterFinished)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if len(session.starts) != 1 || session.starts[0].status != http.StatusCreated {
		t.Fatalf("response starts = %#v", session.starts)
	}
	if session.starts[0].headers["X-Before"] != "kept" || session.starts[0].headers["X-After"] != "" {
		t.Fatalf("locked response headers = %#v", session.starts[0].headers)
	}
}

func TestResponseWriterContentLengthCoherence(t *testing.T) {
	t.Run("exact", func(t *testing.T) {
		writer := newResponseWriter(&recordingHTTPSession{}, 3, context.Background())
		writer.Header().Set("Content-Length", "4")
		if _, err := writer.Write([]byte("body")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := writer.finish(); err != nil {
			t.Fatalf("finish: %v", err)
		}
	})

	t.Run("too many", func(t *testing.T) {
		writer := newResponseWriter(&recordingHTTPSession{}, 4, context.Background())
		writer.Header().Set("Content-Length", "3")
		if written, err := writer.Write([]byte("body")); written != 0 || !errors.Is(err, http.ErrContentLength) {
			t.Fatalf("Write = %d, %v; want 0, %v", written, err, http.ErrContentLength)
		}
	})

	t.Run("too few", func(t *testing.T) {
		writer := newResponseWriter(&recordingHTTPSession{}, 5, context.Background())
		writer.Header().Set("Content-Length", "5")
		if _, err := writer.Write([]byte("body")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		var structured pump.HandlerError
		if err := writer.finish(); !errors.As(err, &structured) || structured.Code != "http_response_content_length_mismatch" {
			t.Fatalf("finish error = %v, want content-length mismatch", err)
		}
	})
}

func TestResponseWriterBoundsAggregateBodySize(t *testing.T) {
	t.Run("declared", func(t *testing.T) {
		writer := newResponseWriter(&recordingHTTPSession{}, 6, context.Background())
		writer.Header().Set("Content-Length", fmt.Sprint(maxHTTPResponseBytes+1))
		writer.WriteHeader(http.StatusOK)
		var structured pump.HandlerError
		if err := writer.finish(); !errors.As(err, &structured) || structured.Code != "http_response_body_too_large" {
			t.Fatalf("finish error = %v, want body-size error", err)
		}
	})

	t.Run("streamed", func(t *testing.T) {
		writer := newResponseWriter(&recordingHTTPSession{}, 7, context.Background())
		chunk := make([]byte, maxHTTPChunk)
		for range maxHTTPResponseBytes / maxHTTPChunk {
			if _, err := writer.Write(chunk); err != nil {
				t.Fatalf("Write within limit: %v", err)
			}
		}
		var structured pump.HandlerError
		if written, err := writer.Write([]byte{0}); written != 0 || !errors.As(err, &structured) || structured.Code != "http_response_body_too_large" {
			t.Fatalf("over-limit Write = %d, %v; want body-size error", written, err)
		}
	})
}

func TestResponseWriterRejectsUnrepresentableHeaders(t *testing.T) {
	t.Run("repeated set-cookie", func(t *testing.T) {
		writer := newResponseWriter(&recordingHTTPSession{}, 6, context.Background())
		writer.Header().Add("Set-Cookie", "first=1")
		writer.Header().Add("Set-Cookie", "second=2")
		writer.WriteHeader(http.StatusOK)
		var structured pump.HandlerError
		if err := writer.finish(); !errors.As(err, &structured) || structured.Code != "http_response_repeated_header_unsupported" {
			t.Fatalf("finish error = %v, want repeated-header error", err)
		}
	})

	t.Run("too many names", func(t *testing.T) {
		writer := newResponseWriter(&recordingHTTPSession{}, 7, context.Background())
		for index := range maxHTTPHeaders + 1 {
			writer.Header().Set(fmt.Sprintf("X-Header-%03d", index), "value")
		}
		writer.WriteHeader(http.StatusOK)
		var structured pump.HandlerError
		if err := writer.finish(); !errors.As(err, &structured) || structured.Code != "http_response_headers_too_large" {
			t.Fatalf("finish error = %v, want header-limit error", err)
		}
	})

	t.Run("oversized value", func(t *testing.T) {
		writer := newResponseWriter(&recordingHTTPSession{}, 8, context.Background())
		writer.Header().Set("X-Large", strings.Repeat("v", maxHTTPHeaderBytes+1))
		writer.WriteHeader(http.StatusOK)
		var structured pump.HandlerError
		if err := writer.finish(); !errors.As(err, &structured) || structured.Code != "http_response_header_too_large" {
			t.Fatalf("finish error = %v, want header-size error", err)
		}
	})
}
