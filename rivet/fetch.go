package rivet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/ewhauser/rivet-go/internal/pump"
)

const (
	maxHTTPChunk       = 1 << 20
	maxHTTPHeaders     = 256
	maxHTTPHeaderBytes = 1 << 20
)

var errResponseWriterFinished = errors.New("rivet: write after OnFetch returned")

type httpResponseSession interface {
	StartHTTPResponse(context.Context, uint64, uint16, map[string]string, []byte, bool) error
	WriteHTTPResponseChunk(context.Context, uint64, []byte, bool) error
}

type responseWriter struct {
	mu        sync.Mutex
	session   httpResponseSession
	requestID uint64
	ctx       context.Context
	header    http.Header
	started   bool
	finished  bool
	err       error
	written   int64
	length    int64
}

func newResponseWriter(
	session httpResponseSession,
	requestID uint64,
	ctx context.Context,
) *responseWriter {
	return &responseWriter{
		session:   session,
		requestID: requestID,
		ctx:       ctx,
		header:    make(http.Header),
		length:    -1,
	}
}

func (w *responseWriter) Header() http.Header { return w.header }

func (w *responseWriter) WriteHeader(statusCode int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writeHeaderLocked(statusCode)
}

func (w *responseWriter) writeHeaderLocked(statusCode int) {
	if w.started || w.finished || w.err != nil {
		return
	}
	if statusCode < 100 || statusCode > 999 {
		panic(fmt.Sprintf("invalid WriteHeader code %d", statusCode))
	}
	w.started = true
	headers, err := flattenHeaders(w.header)
	if err != nil {
		w.err = err
		return
	}
	if contentLength := w.header.Get("Content-Length"); contentLength != "" {
		w.length, err = strconv.ParseInt(contentLength, 10, 64)
		if err != nil || w.length < 0 {
			w.err = pump.HandlerError{
				Code:    "http_response_content_length_invalid",
				Message: fmt.Sprintf("invalid Content-Length %q", contentLength),
			}
			return
		}
	}
	w.err = w.session.StartHTTPResponse(
		w.ctx,
		w.requestID,
		uint16(statusCode),
		headers,
		nil,
		true,
	)
}

func (w *responseWriter) Write(body []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.started {
		w.writeHeaderLocked(http.StatusOK)
	}
	if w.finished {
		return 0, errResponseWriterFinished
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.length >= 0 && w.written+int64(len(body)) > w.length {
		w.err = pump.HandlerError{
			Code:    "http_response_content_length_mismatch",
			Message: fmt.Sprintf("writing %d bytes would exceed Content-Length %d", len(body), w.length),
			Cause:   http.ErrContentLength,
		}
		return 0, w.err
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
		w.written += int64(chunkSize)
		body = body[chunkSize:]
	}
	return written, nil
}

func (w *responseWriter) finish() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return w.err
	}
	if !w.started {
		w.writeHeaderLocked(http.StatusOK)
	}
	w.finished = true
	if w.err == nil && w.length >= 0 && w.written != w.length {
		w.err = pump.HandlerError{
			Code: "http_response_content_length_mismatch",
			Message: fmt.Sprintf(
				"wrote %d response bytes with Content-Length %d",
				w.written,
				w.length,
			),
		}
	}
	if w.err == nil {
		w.err = w.session.WriteHTTPResponseChunk(w.ctx, w.requestID, nil, true)
	}
	return w.err
}

func flattenHeaders(header http.Header) (map[string]string, error) {
	if len(header) > maxHTTPHeaders {
		return nil, pump.HandlerError{
			Code:    "http_response_headers_too_large",
			Message: fmt.Sprintf("HTTP response has more than %d header names", maxHTTPHeaders),
		}
	}
	flattened := make(map[string]string, len(header))
	for name, values := range header {
		if strings.EqualFold(name, "Set-Cookie") && len(values) > 1 {
			return nil, pump.HandlerError{
				Code:    "http_response_repeated_header_unsupported",
				Message: "multiple Set-Cookie fields cannot cross the v2.3.10 HTTP boundary",
			}
		}
		value := strings.Join(values, ", ")
		if len(name) > maxHTTPHeaderBytes || len(value) > maxHTTPHeaderBytes {
			return nil, pump.HandlerError{
				Code: "http_response_header_too_large",
				Message: fmt.Sprintf(
					"HTTP response header %q exceeds the %d-byte boundary maximum",
					name,
					maxHTTPHeaderBytes,
				),
			}
		}
		flattened[name] = value
	}
	return flattened, nil
}

var _ http.ResponseWriter = (*responseWriter)(nil)
