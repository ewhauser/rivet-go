package rivet

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/ewhauser/rivet-go/internal/pump"
)

const maxHTTPChunk = 1 << 20

type responseWriter struct {
	session   *pump.ActorSession
	requestID uint64
	ctx       context.Context
	header    http.Header
	started   bool
	finished  bool
	err       error
}

func newResponseWriter(
	session *pump.ActorSession,
	requestID uint64,
	ctx context.Context,
) *responseWriter {
	return &responseWriter{
		session:   session,
		requestID: requestID,
		ctx:       ctx,
		header:    make(http.Header),
	}
}

func (w *responseWriter) Header() http.Header { return w.header }

func (w *responseWriter) WriteHeader(statusCode int) {
	if w.started || w.finished || w.err != nil {
		return
	}
	if statusCode < 100 || statusCode > 999 {
		panic(fmt.Sprintf("invalid WriteHeader code %d", statusCode))
	}
	w.started = true
	w.err = w.session.StartHTTPResponse(
		w.ctx,
		w.requestID,
		uint16(statusCode),
		flattenHeaders(w.header),
		nil,
		true,
	)
}

func (w *responseWriter) Write(body []byte) (int, error) {
	if !w.started {
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.finished {
		return 0, http.ErrBodyNotAllowed
	}
	written := 0
	for len(body) != 0 {
		chunkSize := min(len(body), maxHTTPChunk)
		if err := w.session.WriteHTTPResponseChunk(
			w.ctx,
			w.requestID,
			body[:chunkSize],
			false,
		); err != nil {
			w.err = err
			return written, err
		}
		written += chunkSize
		body = body[chunkSize:]
	}
	return written, nil
}

func (w *responseWriter) Flush() {
	if !w.started {
		w.WriteHeader(http.StatusOK)
	}
}

func (w *responseWriter) finish() error {
	if w.finished {
		return w.err
	}
	if !w.started {
		w.WriteHeader(http.StatusOK)
	}
	if w.err == nil {
		w.err = w.session.WriteHTTPResponseChunk(w.ctx, w.requestID, nil, true)
	}
	w.finished = true
	return w.err
}

func flattenHeaders(header http.Header) map[string]string {
	flattened := make(map[string]string, len(header))
	for name, values := range header {
		flattened[name] = strings.Join(values, ", ")
	}
	return flattened
}

var _ http.ResponseWriter = (*responseWriter)(nil)
var _ http.Flusher = (*responseWriter)(nil)
