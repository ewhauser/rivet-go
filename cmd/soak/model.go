package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type counterState struct {
	Value      int64  `json:"value"`
	Operations uint64 `json:"operations"`
	LastToken  string `json:"lastToken"`
	LastDelta  int64  `json:"lastDelta"`
	Checksum   uint64 `json:"checksum"`
}

type counterArgs struct {
	Token string `json:"token"`
	Delta int64  `json:"delta"`
}

type chatState struct {
	Sequence  uint64 `json:"sequence"`
	Messages  uint64 `json:"messages"`
	LastToken string `json:"lastToken"`
	Checksum  uint64 `json:"checksum"`
}

type chatReceipt struct {
	Sequence uint64 `json:"sequence"`
	Token    string `json:"token"`
}

type alarmState struct {
	Starts         uint64 `json:"starts"`
	Armed          uint64 `json:"armed"`
	Fired          uint64 `json:"fired"`
	LastNonce      string `json:"lastNonce"`
	LastFiredNonce string `json:"lastFiredNonce"`
}

type alarmArgs struct {
	Nonce       string `json:"nonce"`
	DelayMillis int64  `json:"delayMillis"`
}

func actorCounterUpdate(state *counterState, args counterArgs) {
	state.Value += args.Delta
	state.Operations++
	state.LastToken = args.Token
	state.LastDelta = args.Delta
	state.Checksum = state.Checksum*1_315_423_911 + tokenHash(args.Token) + uint64(args.Delta)
}

func truthCounterUpdate(state counterState, args counterArgs) counterState {
	return counterState{
		Value:      state.Value + args.Delta,
		Operations: state.Operations + 1,
		LastToken:  args.Token,
		LastDelta:  args.Delta,
		Checksum:   state.Checksum*1_315_423_911 + tokenHash(args.Token) + uint64(args.Delta),
	}
}

func tokenHash(value string) uint64 {
	const prime = uint64(1099511628211)
	hash := uint64(1469598103934665603)
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= prime
	}
	return hash
}

func compareCounter(label string, got, want counterState) error {
	var differences []string
	if got.Value != want.Value {
		differences = append(differences, fmt.Sprintf("value=%d want=%d", got.Value, want.Value))
	}
	if got.Operations != want.Operations {
		differences = append(differences, fmt.Sprintf("operations=%d want=%d", got.Operations, want.Operations))
	}
	if got.LastToken != want.LastToken {
		differences = append(differences, fmt.Sprintf("lastToken=%q want=%q", got.LastToken, want.LastToken))
	}
	if got.LastDelta != want.LastDelta {
		differences = append(differences, fmt.Sprintf("lastDelta=%d want=%d", got.LastDelta, want.LastDelta))
	}
	if got.Checksum != want.Checksum {
		differences = append(differences, fmt.Sprintf("checksum=%d want=%d", got.Checksum, want.Checksum))
	}
	if len(differences) != 0 {
		return fmt.Errorf("counter %s diverged: %v", label, differences)
	}
	return nil
}

func compareChat(got, want chatState) error {
	var differences []string
	if got.Sequence != want.Sequence {
		differences = append(differences, fmt.Sprintf("sequence=%d want=%d", got.Sequence, want.Sequence))
	}
	if got.Messages != want.Messages {
		differences = append(differences, fmt.Sprintf("messages=%d want=%d", got.Messages, want.Messages))
	}
	if got.LastToken != want.LastToken {
		differences = append(differences, fmt.Sprintf("lastToken=%q want=%q", got.LastToken, want.LastToken))
	}
	if got.Checksum != want.Checksum {
		differences = append(differences, fmt.Sprintf("checksum=%d want=%d", got.Checksum, want.Checksum))
	}
	if len(differences) != 0 {
		return fmt.Errorf("chat state diverged: %v", differences)
	}
	return nil
}

func compareAlarm(got, want alarmState) error {
	var differences []string
	if got.Starts != want.Starts {
		differences = append(differences, fmt.Sprintf("starts=%d want=%d", got.Starts, want.Starts))
	}
	if got.Armed != want.Armed {
		differences = append(differences, fmt.Sprintf("armed=%d want=%d", got.Armed, want.Armed))
	}
	if got.Fired != want.Fired {
		differences = append(differences, fmt.Sprintf("fired=%d want=%d", got.Fired, want.Fired))
	}
	if got.LastNonce != want.LastNonce {
		differences = append(differences, fmt.Sprintf("lastNonce=%q want=%q", got.LastNonce, want.LastNonce))
	}
	if got.LastFiredNonce != want.LastFiredNonce {
		differences = append(differences, fmt.Sprintf("lastFiredNonce=%q want=%q", got.LastFiredNonce, want.LastFiredNonce))
	}
	if len(differences) != 0 {
		return fmt.Errorf("alarm state diverged: %v", differences)
	}
	return nil
}

type receiptLedger struct {
	active        bool
	disconnecting bool
	expected      []chatReceipt
	received      []chatReceipt
	seen          map[uint64]struct{}
}

type chatOracle struct {
	mu      sync.Mutex
	state   chatState
	ledgers map[string]*receiptLedger
}

func newChatOracle() *chatOracle {
	return &chatOracle{ledgers: make(map[string]*receiptLedger)}
}

func (o *chatOracle) connect(label string) {
	o.mu.Lock()
	ledger := o.ledgers[label]
	if ledger == nil {
		ledger = &receiptLedger{seen: make(map[uint64]struct{})}
		o.ledgers[label] = ledger
	}
	ledger.active = true
	ledger.disconnecting = false
	o.mu.Unlock()
}

func (o *chatOracle) markDisconnecting(label string) {
	o.mu.Lock()
	if ledger := o.ledgers[label]; ledger != nil {
		ledger.disconnecting = true
	}
	o.mu.Unlock()
}

func (o *chatOracle) disconnect(label string) {
	o.mu.Lock()
	if ledger := o.ledgers[label]; ledger != nil {
		ledger.active = false
		ledger.disconnecting = false
	}
	o.mu.Unlock()
}

func (o *chatOracle) onMessage(state chatState, receipt chatReceipt) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	expectedState := chatState{
		Sequence:  o.state.Sequence + 1,
		Messages:  o.state.Messages + 1,
		LastToken: receipt.Token,
		Checksum:  o.state.Checksum*1_315_423_911 + tokenHash(receipt.Token) + receipt.Sequence,
	}
	if err := compareChat(state, expectedState); err != nil {
		return fmt.Errorf("chat handler diverged from truth model: %w", err)
	}
	o.state = expectedState
	for _, ledger := range o.ledgers {
		if ledger.active && !ledger.disconnecting {
			ledger.expected = append(ledger.expected, receipt)
		}
	}
	return nil
}

func (o *chatOracle) record(label string, receipt chatReceipt) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	ledger := o.ledgers[label]
	if ledger == nil {
		return fmt.Errorf("broadcast delivered to unknown client %q", label)
	}
	if _, duplicate := ledger.seen[receipt.Sequence]; duplicate {
		return fmt.Errorf("client %q received duplicate broadcast sequence %d", label, receipt.Sequence)
	}
	if len(ledger.received) != 0 && receipt.Sequence <= ledger.received[len(ledger.received)-1].Sequence {
		return fmt.Errorf("client %q sequence regressed from %d to %d", label, ledger.received[len(ledger.received)-1].Sequence, receipt.Sequence)
	}
	ledger.seen[receipt.Sequence] = struct{}{}
	ledger.received = append(ledger.received, receipt)
	return nil
}

func (o *chatOracle) stateTruth() chatState {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.state
}

func (o *chatOracle) expectedCount(label string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ledger := o.ledgers[label]; ledger != nil {
		return len(ledger.expected)
	}
	return 0
}

func (o *chatOracle) convergence(label string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	labels := []string{label}
	if label == "" {
		labels = labels[:0]
		for current := range o.ledgers {
			labels = append(labels, current)
		}
		sort.Strings(labels)
	}
	for _, current := range labels {
		ledger := o.ledgers[current]
		if ledger == nil {
			return fmt.Errorf("client ledger %q is absent", current)
		}
		if len(ledger.received) != len(ledger.expected) {
			return fmt.Errorf("client %q receipts=%d expected=%d", current, len(ledger.received), len(ledger.expected))
		}
		for index := range ledger.expected {
			got, want := ledger.received[index], ledger.expected[index]
			if got.Sequence != want.Sequence || got.Token != want.Token {
				return fmt.Errorf("client %q receipt[%d]=%#v want=%#v", current, index, got, want)
			}
		}
	}
	return nil
}

func (o *chatOracle) totals() (expected, received int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, ledger := range o.ledgers {
		expected += len(ledger.expected)
		received += len(ledger.received)
	}
	return expected, received
}

type alarmOracle struct {
	mu    sync.Mutex
	state alarmState
	fired chan string
}

func newAlarmOracle() *alarmOracle {
	return &alarmOracle{fired: make(chan string, 16)}
}

func (o *alarmOracle) onStart(state alarmState) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	want := o.state
	want.Starts++
	if err := compareAlarm(state, want); err != nil {
		return fmt.Errorf("alarm OnStart diverged: %w", err)
	}
	o.state = want
	return nil
}

func (o *alarmOracle) onArm(state alarmState, nonce string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	want := o.state
	want.Armed++
	want.LastNonce = nonce
	if err := compareAlarm(state, want); err != nil {
		return fmt.Errorf("alarm arm diverged: %w", err)
	}
	o.state = want
	return nil
}

func (o *alarmOracle) onFire(state alarmState) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	want := o.state
	want.Fired++
	want.LastFiredNonce = want.LastNonce
	if err := compareAlarm(state, want); err != nil {
		return fmt.Errorf("alarm fire diverged: %w", err)
	}
	o.state = want
	return nil
}

func (o *alarmOracle) truth() alarmState {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.state
}

type errorSink struct {
	once sync.Once
	err  chan error
}

func newErrorSink() *errorSink { return &errorSink{err: make(chan error, 1)} }

func (s *errorSink) report(err error) {
	if err == nil {
		return
	}
	s.once.Do(func() { s.err <- err })
}

func (s *errorSink) current() error {
	select {
	case err := <-s.err:
		s.err <- err
		return err
	default:
		return nil
	}
}

type workGate struct {
	mu     sync.Mutex
	paused bool
	active int
	wake   chan struct{}
}

func newWorkGate() *workGate { return &workGate{wake: make(chan struct{})} }

func (g *workGate) acquire(ctx context.Context) error {
	for {
		g.mu.Lock()
		if !g.paused {
			g.active++
			g.mu.Unlock()
			return nil
		}
		wake := g.wake
		g.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *workGate) release() {
	g.mu.Lock()
	g.active--
	g.mu.Unlock()
}

func (g *workGate) pause(ctx context.Context) error {
	g.mu.Lock()
	g.paused = true
	g.mu.Unlock()
	return waitUntil(ctx, 50*time.Millisecond, func() (bool, error) {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.active == 0, nil
	})
}

func (g *workGate) resume() {
	g.mu.Lock()
	if g.paused {
		g.paused = false
		close(g.wake)
		g.wake = make(chan struct{})
	}
	g.mu.Unlock()
}

type activationCounts struct {
	engineRestarts atomic.Int64
	disconnects    atomic.Int64
	sleepWakes     atomic.Int64
	stalls         atomic.Int64
	panics         atomic.Int64
}

func (a *activationCounts) validate() error {
	values := map[string]int64{
		"engine_restart":    a.engineRestarts.Load(),
		"client_disconnect": a.disconnects.Load(),
		"actor_sleep_wake":  a.sleepWakes.Load(),
		"stalled_ws_client": a.stalls.Load(),
		"action_panic":      a.panics.Load(),
	}
	var missing []string
	for name, value := range values {
		if value == 0 {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) != 0 {
		return fmt.Errorf("chaos knobs never activated: %v", missing)
	}
	return nil
}

type soakHooks struct {
	mu       sync.Mutex
	counters map[string]int64
	gauges   map[string]int64
	timings  map[string]struct {
		count int64
		total time.Duration
	}
}

func newSoakHooks() *soakHooks {
	return &soakHooks{
		counters: make(map[string]int64),
		gauges:   make(map[string]int64),
		timings: make(map[string]struct {
			count int64
			total time.Duration
		}),
	}
}

func (h *soakHooks) Counter(name string, delta int64) {
	h.mu.Lock()
	h.counters[name] += delta
	h.mu.Unlock()
}

func (h *soakHooks) Gauge(name string, value int64) {
	h.mu.Lock()
	h.gauges[name] = value
	h.mu.Unlock()
}

func (h *soakHooks) ObserveDuration(name string, value time.Duration) {
	h.mu.Lock()
	timing := h.timings[name]
	timing.count++
	timing.total += value
	h.timings[name] = timing
	h.mu.Unlock()
}

func (h *soakHooks) snapshot() (map[string]int64, map[string]int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	counters := make(map[string]int64, len(h.counters))
	for name, value := range h.counters {
		counters[name] = value
	}
	gauges := make(map[string]int64, len(h.gauges))
	for name, value := range h.gauges {
		gauges[name] = value
	}
	return counters, gauges
}

func (h *soakHooks) timingCount(name string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.timings[name].count
}

func waitUntil(ctx context.Context, interval time.Duration, check func() (bool, error)) error {
	if interval <= 0 {
		return errors.New("wait interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastErr error
	for {
		ok, err := check()
		if ok {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return errors.Join(ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
