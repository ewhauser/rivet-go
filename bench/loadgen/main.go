// Command loadgen drives every runner through the Rivet Engine HTTP and raw
// WebSocket gateway. It intentionally imports no runner-language client SDK.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/gorilla/websocket"
)

const (
	maxResponseBytes = 1 << 20
	requestTimeout   = 30 * time.Second
)

type config struct {
	endpoint     string
	runnerName   string
	sdk          string
	variant      string
	scenario     string
	repetition   int
	warmup       time.Duration
	measure      time.Duration
	coldActors   int
	enginePID    int
	runnerPID    int
	pprofURL     string
	pprofOut     string
	pprofSeconds int
	output       string
	payloadBytes int
}

type result struct {
	SchemaVersion       int               `json:"schema_version"`
	SDK                 string            `json:"sdk"`
	Variant             string            `json:"variant"`
	Scenario            string            `json:"scenario"`
	Repetition          int               `json:"repetition"`
	StartedAt           string            `json:"started_at"`
	WarmupSeconds       float64           `json:"warmup_seconds"`
	RequestedSeconds    float64           `json:"requested_measure_seconds,omitempty"`
	TargetOperations    int               `json:"target_operations,omitempty"`
	MeasuredSeconds     float64           `json:"measured_seconds"`
	Operations          int64             `json:"operations"`
	ThroughputOpsSecond float64           `json:"throughput_ops_second"`
	Latency             latencySummary    `json:"latency_ms"`
	Errors              int64             `json:"errors"`
	WarmupErrors        int64             `json:"warmup_errors"`
	ErrorSamples        []string          `json:"error_samples,omitempty"`
	Correctness         correctnessResult `json:"correctness"`
	CPU                 cpuResult         `json:"cpu"`
	Profile             string            `json:"profile,omitempty"`
	Notes               []string          `json:"notes,omitempty"`
	Valid               bool              `json:"valid"`
}

type latencySummary struct {
	Count int64   `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

type correctnessResult struct {
	Expected int64  `json:"expected"`
	Observed int64  `json:"observed"`
	Detail   string `json:"detail"`
	OK       bool   `json:"ok"`
}

type cpuResult struct {
	Engine processSummary `json:"engine"`
	Runner processSummary `json:"runner"`
}

type processSummary struct {
	Samples       int     `json:"samples"`
	AverageCPU    float64 `json:"average_cpu_percent"`
	MaxCPU        float64 `json:"max_cpu_percent"`
	AverageRSSMiB float64 `json:"average_rss_mib"`
	MaxRSSMiB     float64 `json:"max_rss_mib"`
}

type recorder struct {
	mu      sync.Mutex
	hist    *hdrhistogram.Histogram
	errors  int64
	samples []string
}

func newRecorder() *recorder {
	return &recorder{hist: hdrhistogram.New(1, int64((60*time.Second)/time.Microsecond), 3)}
}

func (r *recorder) success(elapsed time.Duration) {
	microseconds := max(int64(1), elapsed.Microseconds())
	r.mu.Lock()
	if err := r.hist.RecordValue(min(microseconds, r.hist.HighestTrackableValue())); err != nil {
		r.recordErrorLocked(fmt.Sprintf("record latency: %v", err))
	}
	r.mu.Unlock()
}

func (r *recorder) failure(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.recordErrorLocked(err.Error())
	r.mu.Unlock()
}

func (r *recorder) recordErrorLocked(message string) {
	r.errors++
	if len(r.samples) < 20 {
		r.samples = append(r.samples, message)
	}
}

func (r *recorder) summary() latencySummary {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hist.TotalCount() == 0 {
		return latencySummary{}
	}
	return latencySummary{
		Count: r.hist.TotalCount(),
		P50:   float64(r.hist.ValueAtQuantile(50)) / 1000,
		P95:   float64(r.hist.ValueAtQuantile(95)) / 1000,
		P99:   float64(r.hist.ValueAtQuantile(99)) / 1000,
		Max:   float64(r.hist.Max()) / 1000,
	}
}

func (r *recorder) errorSnapshot() (int64, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.errors, append([]string(nil), r.samples...)
}

type gatewayClient struct {
	endpoint   string
	runnerName string
	http       *http.Client
	serial     atomic.Uint64
}

func newGatewayClient(cfg config) *gatewayClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 256
	transport.MaxConnsPerHost = 256
	return &gatewayClient{
		endpoint:   strings.TrimRight(cfg.endpoint, "/"),
		runnerName: cfg.runnerName,
		http:       &http.Client{Transport: transport, Timeout: requestTimeout},
	}
}

func (c *gatewayClient) createActor(ctx context.Context, name, label string) (string, error) {
	key := fmt.Sprintf("bench-%s-%d-%d", label, time.Now().UnixNano(), c.serial.Add(1))
	payload := struct {
		Name               string `json:"name"`
		RunnerNameSelector string `json:"runner_name_selector"`
		CrashPolicy        string `json:"crash_policy"`
		Key                string `json:"key"`
	}{
		Name:               name,
		RunnerNameSelector: c.runnerName,
		CrashPolicy:        "restart",
		Key:                key,
	}
	var response struct {
		Actor struct {
			ActorID string `json:"actor_id"`
		} `json:"actor"`
	}
	if err := c.requestJSON(ctx, http.MethodPost, "/actors?namespace=default", payload, &response); err != nil {
		return "", err
	}
	if response.Actor.ActorID == "" {
		return "", errors.New("create actor returned an empty actor_id")
	}
	return response.Actor.ActorID, nil
}

func (c *gatewayClient) action(ctx context.Context, actorID, action string, args []any) (int64, error) {
	payload := struct {
		Args []any `json:"args"`
	}{Args: args}
	var response struct {
		Output int64 `json:"output"`
	}
	path := "/gateway/" + url.PathEscape(actorID) + "/action/" + url.PathEscape(action)
	if err := c.requestJSON(ctx, http.MethodPost, path, payload, &response); err != nil {
		return 0, err
	}
	return response.Output, nil
}

func (c *gatewayClient) requestJSON(ctx context.Context, method, path string, input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer dev")
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return errors.New("response exceeds 1 MiB")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("%s %s returned %s: %s", method, path, response.Status, strings.TrimSpace(string(body)))
	}
	if output != nil {
		if err := json.Unmarshal(body, output); err != nil {
			return fmt.Errorf("decode response: %w: %s", err, body)
		}
	}
	return nil
}

type phaseCounts struct {
	byActor []atomic.Int64
}

func actionPhase(
	ctx context.Context,
	client *gatewayClient,
	actorIDs []string,
	concurrency int,
	duration time.Duration,
	record *recorder,
) phaseCounts {
	counts := phaseCounts{byActor: make([]atomic.Int64, len(actorIDs))}
	deadline := time.Now().Add(duration)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for worker := range concurrency {
		actorIndex := worker % len(actorIDs)
		go func() {
			defer workers.Done()
			for time.Now().Before(deadline) {
				started := time.Now()
				output, err := client.action(ctx, actorIDs[actorIndex], "increment", []any{1})
				elapsed := time.Since(started)
				if err != nil {
					if record != nil {
						record.failure(err)
					}
					continue
				}
				if output < 1 {
					if record != nil {
						record.failure(fmt.Errorf("increment returned non-positive output %d", output))
					}
					continue
				}
				counts.byActor[actorIndex].Add(1)
				if record != nil {
					record.success(elapsed)
				}
			}
		}()
	}
	workers.Wait()
	return counts
}

func runActionScenario(ctx context.Context, cfg config, client *gatewayClient, actorCount, concurrency int) (*result, error) {
	actorIDs := make([]string, actorCount)
	for i := range actorIDs {
		actorID, err := client.createActor(ctx, "counter", fmt.Sprintf("%s-r%d-actor-%d", cfg.scenario, cfg.repetition, i))
		if err != nil {
			return nil, fmt.Errorf("create counter actor %d: %w", i, err)
		}
		actorIDs[i] = actorID
	}

	warmupRecord := newRecorder()
	actionPhase(ctx, client, actorIDs, concurrency, cfg.warmup, warmupRecord)
	baselines := make([]int64, len(actorIDs))
	for i, actorID := range actorIDs {
		value, err := client.action(ctx, actorID, "get", []any{})
		if err != nil {
			warmupRecord.failure(fmt.Errorf("get warmup baseline for actor %d: %w", i, err))
			continue
		}
		baselines[i] = value
	}

	record := newRecorder()
	measurement, err := beginMeasurement(cfg)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	counts := actionPhase(ctx, client, actorIDs, concurrency, cfg.measure, record)
	elapsed := time.Since(started)
	profilePath, finishErr := measurement.finish()
	if finishErr != nil {
		record.failure(finishErr)
	}

	var expected, observed int64
	correct := true
	for i, actorID := range actorIDs {
		count := counts.byActor[i].Load()
		expected += baselines[i] + count
		value, getErr := client.action(ctx, actorID, "get", []any{})
		if getErr != nil {
			record.failure(fmt.Errorf("get measured total for actor %d: %w", i, getErr))
			correct = false
			continue
		}
		observed += value
		if value != baselines[i]+count {
			correct = false
			record.failure(fmt.Errorf("actor %d total = %d, want %d", i, value, baselines[i]+count))
		}
	}

	return makeResult(cfg, record, warmupRecord, elapsed, measurement.cpuSummary(), profilePath, correctnessResult{
		Expected: expected,
		Observed: observed,
		Detail:   fmt.Sprintf("%d counter actor(s) reconciled after measured increments", actorCount),
		OK:       correct && expected == observed,
	}), nil
}

func runColdStart(ctx context.Context, cfg config, client *gatewayClient) (*result, error) {
	warmupRecord := newRecorder()
	warmupDeadline := time.Now().Add(cfg.warmup)
	for i := 0; time.Now().Before(warmupDeadline); i++ {
		actorID, err := client.createActor(ctx, "counter", fmt.Sprintf("s4-r%d-warmup-%d", cfg.repetition, i))
		if err != nil {
			warmupRecord.failure(err)
			continue
		}
		if output, err := client.action(ctx, actorID, "increment", []any{1}); err != nil {
			warmupRecord.failure(err)
		} else if output != 1 {
			warmupRecord.failure(fmt.Errorf("warmup fresh counter returned %d, want 1", output))
		}
	}

	record := newRecorder()
	measurement, err := beginMeasurement(cfg)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	var observed int64
	for i := range cfg.coldActors {
		opStarted := time.Now()
		actorID, createErr := client.createActor(ctx, "counter", fmt.Sprintf("s4-r%d-measure-%d", cfg.repetition, i))
		if createErr != nil {
			record.failure(createErr)
			continue
		}
		output, actionErr := client.action(ctx, actorID, "increment", []any{1})
		if actionErr != nil {
			record.failure(actionErr)
			continue
		}
		if output != 1 {
			record.failure(fmt.Errorf("fresh counter %d returned %d, want 1", i, output))
			continue
		}
		observed++
		record.success(time.Since(opStarted))
	}
	elapsed := time.Since(started)
	profilePath, finishErr := measurement.finish()
	if finishErr != nil {
		record.failure(finishErr)
	}

	res := makeResult(cfg, record, warmupRecord, elapsed, measurement.cpuSummary(), profilePath, correctnessResult{
		Expected: int64(cfg.coldActors),
		Observed: observed,
		Detail:   "fresh actor create plus first increment returned 1",
		OK:       observed == int64(cfg.coldActors),
	})
	res.TargetOperations = cfg.coldActors
	res.RequestedSeconds = 0
	res.Notes = append(res.Notes, "S4 is count-bounded at 50 fresh actors; its measured interval is the time required for those 50 sequential operations, not an artificial 60-second pacing window.")
	return res, nil
}

type wsClient struct {
	conn *websocket.Conn
}

func (c *gatewayClient) openWebSocket(ctx context.Context, actorID, label string) (*wsClient, error) {
	endpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, err
	}
	if endpoint.Scheme == "http" {
		endpoint.Scheme = "ws"
	} else if endpoint.Scheme == "https" {
		endpoint.Scheme = "wss"
	} else {
		return nil, fmt.Errorf("unsupported endpoint scheme %q", endpoint.Scheme)
	}
	endpoint.Path = "/gateway/" + url.PathEscape(actorID) + "/websocket/echo"
	endpoint.RawQuery = "client=" + url.QueryEscape(label)
	dialer := websocket.Dialer{
		HandshakeTimeout: requestTimeout,
		Subprotocols: []string{
			"rivet",
			"rivet_target.actor",
			"rivet_actor." + actorID,
			"rivet_token.dev",
		},
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer dev")
	conn, response, err := dialer.DialContext(ctx, endpoint.String(), headers)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	conn.SetReadLimit(maxResponseBytes)
	return &wsClient{conn: conn}, nil
}

func wsPhase(clients []*wsClient, duration time.Duration, payloadBytes int, record *recorder) int64 {
	deadline := time.Now().Add(duration)
	var operations atomic.Int64
	var workers sync.WaitGroup
	workers.Add(len(clients))
	for index, client := range clients {
		go func() {
			defer workers.Done()
			payload := make([]byte, max(payloadBytes, 24))
			copy(payload[16:], []byte("rivet-go-bench-echo"))
			var sequence uint64
			for time.Now().Before(deadline) {
				sequence++
				binary.BigEndian.PutUint64(payload[0:8], uint64(index))
				binary.BigEndian.PutUint64(payload[8:16], sequence)
				started := time.Now()
				_ = client.conn.SetWriteDeadline(time.Now().Add(requestTimeout))
				if err := client.conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
					if record != nil {
						record.failure(fmt.Errorf("connection %d write: %w", index, err))
					}
					return
				}
				_ = client.conn.SetReadDeadline(time.Now().Add(requestTimeout))
				kind, echoed, err := client.conn.ReadMessage()
				elapsed := time.Since(started)
				if err != nil {
					if record != nil {
						record.failure(fmt.Errorf("connection %d read: %w", index, err))
					}
					return
				}
				if kind != websocket.BinaryMessage || !bytes.Equal(echoed, payload) {
					if record != nil {
						record.failure(fmt.Errorf("connection %d echo mismatch: kind=%d bytes=%d", index, kind, len(echoed)))
					}
					return
				}
				operations.Add(1)
				if record != nil {
					record.success(elapsed)
				}
			}
		}()
	}
	workers.Wait()
	return operations.Load()
}

func runWebSocket(ctx context.Context, cfg config, client *gatewayClient) (*result, error) {
	actorID, err := client.createActor(ctx, "echo", fmt.Sprintf("s3-r%d", cfg.repetition))
	if err != nil {
		return nil, err
	}
	clients := make([]*wsClient, 32)
	for i := range clients {
		ws, dialErr := client.openWebSocket(ctx, actorID, fmt.Sprintf("s3-r%d-%d", cfg.repetition, i))
		if dialErr != nil {
			for _, opened := range clients[:i] {
				_ = opened.conn.Close()
			}
			return nil, fmt.Errorf("open connection %d: %w", i, dialErr)
		}
		clients[i] = ws
	}
	defer func() {
		for _, client := range clients {
			_ = client.conn.Close()
		}
	}()

	warmupRecord := newRecorder()
	wsPhase(clients, cfg.warmup, cfg.payloadBytes, warmupRecord)
	record := newRecorder()
	measurement, err := beginMeasurement(cfg)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	operations := wsPhase(clients, cfg.measure, cfg.payloadBytes, record)
	elapsed := time.Since(started)
	profilePath, finishErr := measurement.finish()
	if finishErr != nil {
		record.failure(finishErr)
	}

	return makeResult(cfg, record, warmupRecord, elapsed, measurement.cpuSummary(), profilePath, correctnessResult{
		Expected: operations,
		Observed: operations,
		Detail:   fmt.Sprintf("all %d-byte binary payloads matched on 32 sequential ping-pong connections", cfg.payloadBytes),
		OK:       true,
	}), nil
}

func makeResult(
	cfg config,
	record, warmupRecord *recorder,
	elapsed time.Duration,
	cpu cpuResult,
	profile string,
	correctness correctnessResult,
) *result {
	errorsCount, samples := record.errorSnapshot()
	warmupErrors, warmupSamples := warmupRecord.errorSnapshot()
	for _, sample := range warmupSamples {
		if len(samples) >= 20 {
			break
		}
		samples = append(samples, "warmup: "+sample)
	}
	latency := record.summary()
	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(latency.Count) / elapsed.Seconds()
	}
	res := &result{
		SchemaVersion:       1,
		SDK:                 cfg.sdk,
		Variant:             cfg.variant,
		Scenario:            cfg.scenario,
		Repetition:          cfg.repetition,
		StartedAt:           time.Now().UTC().Format(time.RFC3339),
		WarmupSeconds:       cfg.warmup.Seconds(),
		RequestedSeconds:    cfg.measure.Seconds(),
		MeasuredSeconds:     elapsed.Seconds(),
		Operations:          latency.Count,
		ThroughputOpsSecond: throughput,
		Latency:             latency,
		Errors:              errorsCount,
		WarmupErrors:        warmupErrors,
		ErrorSamples:        samples,
		Correctness:         correctness,
		CPU:                 cpu,
		Profile:             profile,
	}
	res.Valid = errorsCount == 0 && warmupErrors == 0 && correctness.OK && latency.Count > 0
	return res
}

type measurementSession struct {
	monitor     *cpuMonitor
	profileDone chan error
	profilePath string
}

func beginMeasurement(cfg config) (*measurementSession, error) {
	monitor := startCPUMonitor(cfg.enginePID, cfg.runnerPID)
	session := &measurementSession{monitor: monitor}
	if cfg.pprofURL != "" || cfg.pprofOut != "" {
		if cfg.pprofURL == "" || cfg.pprofOut == "" {
			monitor.stop()
			return nil, errors.New("pprof-url and pprof-out must be supplied together")
		}
		session.profilePath = cfg.pprofOut
		session.profileDone = make(chan error, 1)
		go func() {
			session.profileDone <- captureProfile(cfg.pprofURL, cfg.pprofOut, cfg.pprofSeconds)
		}()
	}
	return session, nil
}

func (s *measurementSession) finish() (string, error) {
	s.monitor.stop()
	if s.profileDone != nil {
		if err := <-s.profileDone; err != nil {
			return "", err
		}
	}
	return s.profilePath, nil
}

func (s *measurementSession) cpuSummary() cpuResult {
	return s.monitor.summary()
}

func captureProfile(baseURL, output string, seconds int) error {
	profileURL := strings.TrimRight(baseURL, "/") + "/debug/pprof/profile?seconds=" + strconv.Itoa(seconds)
	client := &http.Client{Timeout: time.Duration(seconds+15) * time.Second}
	response, err := client.Get(profileURL)
	if err != nil {
		return fmt.Errorf("capture pprof: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("capture pprof returned %s: %s", response.Status, body)
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return err
	}
	file, err := os.Create(output)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, response.Body)
	closeErr := file.Close()
	return errors.Join(copyErr, closeErr)
}

type cpuSample struct {
	cpu float64
	rss float64
}

type cpuMonitor struct {
	enginePID int
	runnerPID int
	stopOnce  sync.Once
	stopCh    chan struct{}
	done      chan struct{}
	mu        sync.Mutex
	engine    []cpuSample
	runner    []cpuSample
}

func startCPUMonitor(enginePID, runnerPID int) *cpuMonitor {
	monitor := &cpuMonitor{
		enginePID: enginePID,
		runnerPID: runnerPID,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	go monitor.run()
	return monitor
}

func (m *cpuMonitor) run() {
	defer close(m.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	m.sample()
	for {
		select {
		case <-ticker.C:
			m.sample()
		case <-m.stopCh:
			return
		}
	}
}

func (m *cpuMonitor) sample() {
	engine, engineOK := processSample(m.enginePID)
	runner, runnerOK := processSample(m.runnerPID)
	m.mu.Lock()
	defer m.mu.Unlock()
	if engineOK {
		m.engine = append(m.engine, engine)
	}
	if runnerOK {
		m.runner = append(m.runner, runner)
	}
}

func (m *cpuMonitor) stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	<-m.done
}

func (m *cpuMonitor) summary() cpuResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cpuResult{Engine: summarizeProcess(m.engine), Runner: summarizeProcess(m.runner)}
}

func processSample(pid int) (cpuSample, bool) {
	if pid <= 0 {
		return cpuSample{}, false
	}
	output, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "%cpu=,rss=").Output()
	if err != nil {
		return cpuSample{}, false
	}
	fields := strings.Fields(string(output))
	if len(fields) != 2 {
		return cpuSample{}, false
	}
	cpu, cpuErr := strconv.ParseFloat(fields[0], 64)
	rssKiB, rssErr := strconv.ParseFloat(fields[1], 64)
	if cpuErr != nil || rssErr != nil {
		return cpuSample{}, false
	}
	return cpuSample{cpu: cpu, rss: rssKiB / 1024}, true
}

func summarizeProcess(samples []cpuSample) processSummary {
	if len(samples) == 0 {
		return processSummary{}
	}
	var cpuTotal, rssTotal, maxCPU, maxRSS float64
	for _, sample := range samples {
		cpuTotal += sample.cpu
		rssTotal += sample.rss
		maxCPU = math.Max(maxCPU, sample.cpu)
		maxRSS = math.Max(maxRSS, sample.rss)
	}
	return processSummary{
		Samples:       len(samples),
		AverageCPU:    cpuTotal / float64(len(samples)),
		MaxCPU:        maxCPU,
		AverageRSSMiB: rssTotal / float64(len(samples)),
		MaxRSSMiB:     maxRSS,
	}
}

func parseConfig() config {
	var cfg config
	flag.StringVar(&cfg.endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine gateway endpoint")
	flag.StringVar(&cfg.runnerName, "runner-name", "", "engine runner pool name")
	flag.StringVar(&cfg.sdk, "sdk", "", "runner SDK label")
	flag.StringVar(&cfg.variant, "variant", "persist", "persistence variant")
	flag.StringVar(&cfg.scenario, "scenario", "", "s1, s2, s3, or s4")
	flag.IntVar(&cfg.repetition, "repetition", 1, "repetition number")
	flag.DurationVar(&cfg.warmup, "warmup", 10*time.Second, "excluded warmup duration")
	flag.DurationVar(&cfg.measure, "measure", 60*time.Second, "measured duration for S1-S3")
	flag.IntVar(&cfg.coldActors, "cold-actors", 50, "fresh sequential actors for S4")
	flag.IntVar(&cfg.enginePID, "engine-pid", 0, "engine PID for CPU sampling")
	flag.IntVar(&cfg.runnerPID, "runner-pid", 0, "runner PID for CPU sampling")
	flag.StringVar(&cfg.pprofURL, "pprof-url", "", "optional Go runner pprof base URL")
	flag.StringVar(&cfg.pprofOut, "pprof-out", "", "optional CPU profile output path")
	flag.IntVar(&cfg.pprofSeconds, "pprof-seconds", 30, "CPU profile duration")
	flag.StringVar(&cfg.output, "output", "", "raw JSON output path")
	flag.IntVar(&cfg.payloadBytes, "payload-bytes", 64, "S3 echo payload bytes")
	flag.Parse()
	return cfg
}

func (cfg config) validate() error {
	if cfg.runnerName == "" || cfg.sdk == "" || cfg.scenario == "" || cfg.output == "" {
		return errors.New("runner-name, sdk, scenario, and output are required")
	}
	if cfg.repetition < 1 || cfg.warmup < 10*time.Second || cfg.measure <= 0 || cfg.coldActors < 1 {
		return errors.New("repetition must be positive, warmup at least 10s, measure positive, and cold-actors positive")
	}
	if cfg.variant != "persist" && cfg.variant != "no-persist" && cfg.variant != "not-applicable" {
		return fmt.Errorf("unknown variant %q", cfg.variant)
	}
	switch cfg.scenario {
	case "s1", "s2", "s3", "s4":
	default:
		return fmt.Errorf("unknown scenario %q", cfg.scenario)
	}
	return nil
}

func writeResult(path string, value *result) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(value)
	closeErr := file.Close()
	return errors.Join(encodeErr, closeErr)
}

func main() {
	cfg := parseConfig()
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}
	ctx := context.Background()
	client := newGatewayClient(cfg)
	var (
		res *result
		err error
	)
	switch cfg.scenario {
	case "s1":
		res, err = runActionScenario(ctx, cfg, client, 1, 32)
	case "s2":
		res, err = runActionScenario(ctx, cfg, client, 64, 64)
	case "s3":
		res, err = runWebSocket(ctx, cfg, client)
	case "s4":
		res, err = runColdStart(ctx, cfg, client)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
	if err := writeResult(cfg.output, res); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen: write result:", err)
		os.Exit(1)
	}
	encoded, _ := json.Marshal(res)
	fmt.Println(string(encoded))
	if !res.Valid {
		os.Exit(1)
	}
}
