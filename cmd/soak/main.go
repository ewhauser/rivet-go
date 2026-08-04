package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ewhauser/rivet-go/internal/devengine"
	"github.com/ewhauser/rivet-go/internal/ffi"
	"github.com/ewhauser/rivet-go/rivet"
	"go.uber.org/goleak"
)

type soakConfig struct {
	duration           time.Duration
	intensity          int
	clients            int
	counters           int
	seed               int64
	dataDir            string
	restartInterval    time.Duration
	disconnectInterval time.Duration
	stallInterval      time.Duration
	stallDuration      time.Duration
	panicInterval      time.Duration
	alarmDelay         time.Duration
}

type soakSummary struct {
	Duration          string           `json:"duration"`
	Seed              int64            `json:"seed"`
	CounterOperations uint64           `json:"counter_operations"`
	ChatMessages      uint64           `json:"chat_messages"`
	ExpectedReceipts  int              `json:"expected_receipts"`
	ReceivedReceipts  int              `json:"received_receipts"`
	AlarmFires        uint64           `json:"alarm_fires"`
	Chaos             map[string]int64 `json:"chaos"`
	Metrics           map[string]int64 `json:"metrics"`
	GoroutinesBefore  int              `json:"goroutines_before"`
	GoroutinesAfter   int              `json:"goroutines_after"`
	DataDir           string           `json:"data_dir"`
}

func main() {
	config, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "soak:", err)
		os.Exit(2)
	}
	summary, err := runSoak(context.Background(), config)
	if err != nil {
		fmt.Fprintln(os.Stderr, "SOAK FAIL:", err)
		os.Exit(1)
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		fmt.Fprintln(os.Stderr, "SOAK FAIL: encode summary:", err)
		os.Exit(1)
	}
	fmt.Println("SOAK PASS", string(encoded))
}

func parseFlags() (soakConfig, error) {
	config := soakConfig{}
	flag.DurationVar(&config.duration, "duration", 2*time.Minute, "active workload duration")
	flag.IntVar(&config.intensity, "intensity", 8, "approximate gateway operations per second")
	flag.IntVar(&config.clients, "clients", 4, "number of live chat WebSocket clients")
	flag.IntVar(&config.counters, "counters", 3, "number of counter actors")
	flag.Int64Var(&config.seed, "seed", 0, "random seed; zero chooses and prints one")
	flag.StringVar(&config.dataDir, "data-dir", "", "engine data directory; empty creates a preserved temporary directory")
	flag.DurationVar(&config.restartInterval, "restart-interval", 45*time.Second, "engine restart interval")
	flag.DurationVar(&config.disconnectInterval, "disconnect-interval", 12*time.Second, "client disconnect interval")
	flag.DurationVar(&config.stallInterval, "stall-interval", 20*time.Second, "stalled client interval")
	flag.DurationVar(&config.stallDuration, "stall-duration", 3*time.Second, "duration for each stalled client")
	flag.DurationVar(&config.panicInterval, "panic-interval", 30*time.Second, "sacrificial action panic interval")
	flag.DurationVar(&config.alarmDelay, "alarm-delay", 20*time.Second, "durable alarm delay")
	flag.Parse()
	if config.duration <= 0 {
		return config, errors.New("duration must be positive")
	}
	if config.intensity <= 0 {
		return config, errors.New("intensity must be positive")
	}
	if config.clients < 2 {
		return config, errors.New("clients must be at least 2")
	}
	if config.counters < 1 {
		return config, errors.New("counters must be at least 1")
	}
	for name, value := range map[string]time.Duration{
		"restart-interval":    config.restartInterval,
		"disconnect-interval": config.disconnectInterval,
		"stall-interval":      config.stallInterval,
		"stall-duration":      config.stallDuration,
		"panic-interval":      config.panicInterval,
		"alarm-delay":         config.alarmDelay,
	} {
		if value <= 0 {
			return config, fmt.Errorf("%s must be positive", name)
		}
	}
	if config.seed == 0 {
		config.seed = time.Now().UnixNano()
	}
	return config, nil
}

type soakRun struct {
	config       soakConfig
	runnerName   string
	engine       *devengine.Engine
	api          *engineAPI
	hooks        *soakHooks
	errors       *errorSink
	gate         *workGate
	activations  *activationCounts
	chatOracle   *chatOracle
	alarmOracle  *alarmOracle
	clients      *clientManager
	chatActor    string
	alarmActor   string
	counterIDs   []string
	counterTruth []counterState
	operationID  atomic.Uint64
	chaosMu      sync.Mutex
}

func runSoak(parent context.Context, config soakConfig) (soakSummary, error) {
	ctx, stopSignals := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	// Initialize os/signal before the leak baseline. The process-global signal
	// receiver is shared by all NotifyContext calls and is not a runner leak.
	warm, stopWarm := signal.NotifyContext(context.Background(), os.Interrupt)
	stopWarm()
	<-warm.Done()
	leakBaseline := goleak.IgnoreCurrent()
	goroutinesBefore := runtime.NumGoroutine()
	handlesBefore := ffi.ActiveHandleCounts()

	binary, err := devengine.Acquire(ctx)
	if err != nil {
		return soakSummary{}, fmt.Errorf("acquire engine %s: %w", devengine.Tag, err)
	}
	dataDir := config.dataDir
	if dataDir == "" {
		dataDir, err = os.MkdirTemp("", "rivet-go-soak-")
		if err != nil {
			return soakSummary{}, fmt.Errorf("create soak data directory: %w", err)
		}
	}
	port, err := devengine.ReservePortRange()
	if err != nil {
		return soakSummary{}, err
	}
	engine, err := devengine.New(binary, dataDir, port)
	if err != nil {
		return soakSummary{}, err
	}
	if err := engine.Start(ctx); err != nil {
		return soakSummary{}, err
	}
	defer engine.Kill()

	run := &soakRun{
		config:       config,
		runnerName:   fmt.Sprintf("rivet-go-soak-%d", config.seed),
		engine:       engine,
		api:          newEngineAPI(engine.Endpoint),
		hooks:        newSoakHooks(),
		errors:       newErrorSink(),
		gate:         newWorkGate(),
		activations:  &activationCounts{},
		chatOracle:   newChatOracle(),
		alarmOracle:  newAlarmOracle(),
		counterTruth: make([]counterState, config.counters),
	}
	defer run.api.close()

	registry, err := run.registry()
	if err != nil {
		return soakSummary{}, err
	}
	serveCtx, cancelServe := context.WithCancel(ctx)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- registry.Serve(serveCtx, rivet.Config{
			Endpoint:        engine.Endpoint,
			RunnerName:      run.runnerName,
			TotalSlots:      64,
			LogLevel:        "error",
			ShutdownTimeout: 10 * time.Second,
			Hooks:           run.hooks,
			Logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
				Level: slog.LevelWarn,
			})),
		})
	}()
	defer cancelServe()
	registrationCtx, cancelRegistration := context.WithTimeout(ctx, 30*time.Second)
	err = run.api.waitRunner(registrationCtx, run.runnerName, true)
	cancelRegistration()
	if err != nil {
		return soakSummary{}, fmt.Errorf("wait for runner registration: %w", err)
	}
	if err := run.createWorkload(ctx); err != nil {
		return soakSummary{}, err
	}

	activeCtx, cancelActive := context.WithCancel(ctx)
	var workload sync.WaitGroup
	run.startWorkload(activeCtx, &workload)
	started := time.Now()
	activeTimer := time.NewTimer(config.duration)
	var activeErr error
	select {
	case <-activeTimer.C:
	case activeErr = <-run.errors.err:
	case serveErr := <-serveResult:
		if serveErr == nil {
			serveErr = errors.New("runner exited before soak drain")
		}
		activeErr = serveErr
	case <-ctx.Done():
		activeErr = ctx.Err()
	}
	if !activeTimer.Stop() {
		select {
		case <-activeTimer.C:
		default:
		}
	}
	cancelActive()
	workloadDone := make(chan struct{})
	go func() {
		workload.Wait()
		close(workloadDone)
	}()
	select {
	case <-workloadDone:
	case <-time.After(90 * time.Second):
		return soakSummary{}, errors.New("workload goroutines did not stop within 90s")
	}
	if activeErr != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if run.clients != nil {
			_ = run.clients.closeAll(cleanupCtx)
		}
		cancel()
		cancelServe()
		return soakSummary{}, activeErr
	}

	finalCtx, cancelFinal := context.WithTimeout(ctx, 90*time.Second)
	defer cancelFinal()
	if err := run.gate.pause(finalCtx); err != nil {
		return soakSummary{}, fmt.Errorf("quiesce workload: %w", err)
	}
	defer run.gate.resume()
	if err := run.finalOracles(finalCtx); err != nil {
		return soakSummary{}, err
	}
	if err := run.clients.prepareRunnerDrain(finalCtx); err != nil {
		return soakSummary{}, fmt.Errorf("prepare client drain: %w", err)
	}
	cancelServe()
	if err := run.clients.waitRunnerDrain(finalCtx); err != nil {
		return soakSummary{}, fmt.Errorf("runner WebSocket drain: %w", err)
	}
	select {
	case err := <-serveResult:
		if err != nil {
			return soakSummary{}, fmt.Errorf("runner drain: %w", err)
		}
	case <-finalCtx.Done():
		return soakSummary{}, fmt.Errorf("runner did not drain: %w", finalCtx.Err())
	}
	if err := run.api.waitRunner(finalCtx, run.runnerName, false); err != nil {
		return soakSummary{}, fmt.Errorf("runner remained engine-visible: %w", err)
	}
	if err := engine.Kill(); err != nil {
		return soakSummary{}, err
	}
	run.api.close()

	if err := waitUntil(finalCtx, 100*time.Millisecond, func() (bool, error) {
		counts := ffi.ActiveHandleCounts()
		if counts == handlesBefore {
			return true, nil
		}
		return false, fmt.Errorf("native handles=%#v baseline=%#v", counts, handlesBefore)
	}); err != nil {
		return soakSummary{}, fmt.Errorf("native handle leak: %w", err)
	}
	if err := waitUntil(finalCtx, 250*time.Millisecond, func() (bool, error) {
		err := goleak.Find(leakBaseline)
		return err == nil, err
	}); err != nil {
		return soakSummary{}, fmt.Errorf("goroutine leak: %w", err)
	}

	counters, gauges := run.hooks.snapshot()
	if gauges[rivet.MetricLiveActors] != 0 || gauges[rivet.MetricLiveConnections] != 0 {
		return soakSummary{}, fmt.Errorf("nonzero final gauges: %v", gauges)
	}
	expectedReceipts, receivedReceipts := run.chatOracle.totals()
	alarmTruth := run.alarmOracle.truth()
	var counterOperations uint64
	for _, state := range run.counterTruth {
		counterOperations += state.Operations
	}
	return soakSummary{
		Duration:          time.Since(started).Round(time.Millisecond).String(),
		Seed:              config.seed,
		CounterOperations: counterOperations,
		ChatMessages:      run.chatOracle.stateTruth().Messages,
		ExpectedReceipts:  expectedReceipts,
		ReceivedReceipts:  receivedReceipts,
		AlarmFires:        alarmTruth.Fired,
		Chaos: map[string]int64{
			"engine_restarts":    run.activations.engineRestarts.Load(),
			"client_disconnects": run.activations.disconnects.Load(),
			"sleep_wakes":        run.activations.sleepWakes.Load(),
			"stalled_clients":    run.activations.stalls.Load(),
			"action_panics":      run.activations.panics.Load(),
		},
		Metrics:          counters,
		GoroutinesBefore: goroutinesBefore,
		GoroutinesAfter:  runtime.NumGoroutine(),
		DataDir:          dataDir,
	}, nil
}

func (r *soakRun) registry() (*rivet.Registry, error) {
	registry := rivet.NewRegistry()
	if err := rivet.Register(registry, "soak-counter", rivet.Actor[counterState]{
		Actions: rivet.Actions[counterState]{
			"apply": rivet.Action(func(ctx *rivet.Context[counterState], args counterArgs) (counterState, error) {
				actorCounterUpdate(ctx.State(), args)
				return *ctx.State(), nil
			}),
			"snapshot": rivet.Action(func(ctx *rivet.Context[counterState], _ struct{}) (counterState, error) {
				return *ctx.State(), nil
			}),
		},
	}); err != nil {
		return nil, err
	}
	if err := rivet.Register(registry, "soak-chat", rivet.Actor[chatState]{
		Actions: rivet.Actions[chatState]{
			"snapshot": rivet.Action(func(ctx *rivet.Context[chatState], _ struct{}) (chatState, error) {
				return *ctx.State(), nil
			}),
		},
		OnConnect: func(_ *rivet.Context[chatState], connection *rivet.Connection) error {
			label := headerValue(connection.Headers(), "x-client-label")
			if label == "" {
				return errors.New("chat client has no x-client-label")
			}
			r.chatOracle.connect(label)
			return connection.SendText("ready:" + label)
		},
		OnMessage: func(ctx *rivet.Context[chatState], connection *rivet.Connection, message rivet.Message) {
			if message.Binary {
				r.errors.report(errors.New("chat client sent a binary message"))
				_ = connection.Close(1003, "text only")
				return
			}
			token := string(message.Data)
			ctx.State().Sequence++
			ctx.State().Messages++
			ctx.State().LastToken = token
			ctx.State().Checksum = ctx.State().Checksum*1_315_423_911 + tokenHash(token) + ctx.State().Sequence
			if err := ctx.Save(context.Background()); err != nil {
				r.errors.report(fmt.Errorf("save chat state: %w", err))
				_ = connection.Close(1011, "state save failed")
				return
			}
			receipt := chatReceipt{Sequence: ctx.State().Sequence, Token: token}
			if err := ctx.Broadcast("chat", receipt); err != nil {
				r.errors.report(fmt.Errorf("broadcast chat receipt: %w", err))
				return
			}
			if err := r.chatOracle.onMessage(*ctx.State(), receipt); err != nil {
				r.errors.report(err)
			}
		},
		OnDisconnect: func(_ *rivet.Context[chatState], connection *rivet.Connection) {
			label := headerValue(connection.Headers(), "x-client-label")
			r.chatOracle.disconnect(label)
		},
	}); err != nil {
		return nil, err
	}
	if err := rivet.Register(registry, "soak-alarm", rivet.Actor[alarmState]{
		OnStart: func(ctx *rivet.Context[alarmState]) error {
			ctx.State().Starts++
			if err := ctx.Save(context.Background()); err != nil {
				return err
			}
			return r.alarmOracle.onStart(*ctx.State())
		},
		Actions: rivet.Actions[alarmState]{
			"arm": rivet.Action(func(ctx *rivet.Context[alarmState], args alarmArgs) (alarmState, error) {
				ctx.State().Armed++
				ctx.State().LastNonce = args.Nonce
				if err := ctx.ScheduleAfter(time.Duration(args.DelayMillis) * time.Millisecond); err != nil {
					return alarmState{}, err
				}
				if err := ctx.Sleep(); err != nil {
					return alarmState{}, err
				}
				if err := r.alarmOracle.onArm(*ctx.State(), args.Nonce); err != nil {
					return alarmState{}, err
				}
				return *ctx.State(), nil
			}),
			"resleep": rivet.Action(func(ctx *rivet.Context[alarmState], _ struct{}) (alarmState, error) {
				return *ctx.State(), ctx.Sleep()
			}),
			"settle": rivet.Action(func(ctx *rivet.Context[alarmState], _ struct{}) (alarmState, error) {
				return *ctx.State(), ctx.ClearSchedule()
			}),
		},
		OnAlarm: func(ctx *rivet.Context[alarmState]) error {
			ctx.State().Fired++
			ctx.State().LastFiredNonce = ctx.State().LastNonce
			if err := r.alarmOracle.onFire(*ctx.State()); err != nil {
				return err
			}
			select {
			case r.alarmOracle.fired <- ctx.State().LastFiredNonce:
			default:
				return errors.New("alarm oracle notification queue is full")
			}
			return nil
		},
	}); err != nil {
		return nil, err
	}
	if err := rivet.Register(registry, "soak-sacrificial", rivet.Actor[struct{}]{
		Actions: rivet.Actions[struct{}]{
			"panic": rivet.Action(func(*rivet.Context[struct{}], struct{}) (struct{}, error) {
				panic("intentional soak action panic")
			}),
		},
	}); err != nil {
		return nil, err
	}
	return registry, nil
}

func (r *soakRun) createWorkload(ctx context.Context) error {
	for index := range r.config.counters {
		actorID, err := r.api.createActor(ctx, "soak-counter", r.runnerName, fmt.Sprintf("counter-%d", index))
		if err != nil {
			return err
		}
		r.counterIDs = append(r.counterIDs, actorID)
	}
	chatActor, err := r.api.createActor(ctx, "soak-chat", r.runnerName, "chat")
	if err != nil {
		return err
	}
	r.chatActor = chatActor
	alarmActor, err := r.api.createActor(ctx, "soak-alarm", r.runnerName, "alarm")
	if err != nil {
		return err
	}
	r.alarmActor = alarmActor
	r.clients = newClientManager(r.engine.Endpoint, r.chatActor, r.chatOracle, r.errors)
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for range r.config.clients {
		if _, err := r.clients.connect(connectCtx); err != nil {
			return err
		}
	}
	return nil
}

func (r *soakRun) startWorkload(ctx context.Context, wait *sync.WaitGroup) {
	counterRate := time.Duration(r.config.counters) * time.Second / time.Duration(r.config.intensity)
	if counterRate < 10*time.Millisecond {
		counterRate = 10 * time.Millisecond
	}
	for index := range r.counterIDs {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			rng := rand.New(rand.NewPCG(uint64(r.config.seed)+uint64(index)+1, uint64(index)+17))
			r.runCounter(ctx, index, rng, counterRate)
		}(index)
	}
	chatRate := 2 * time.Second / time.Duration(r.config.intensity)
	if chatRate < 10*time.Millisecond {
		chatRate = 10 * time.Millisecond
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		r.runChat(ctx, chatRate)
	}()
	wait.Add(1)
	go func() {
		defer wait.Done()
		r.runAlarms(ctx)
	}()
	r.startChaos(ctx, wait)
}

func (r *soakRun) runCounter(ctx context.Context, index int, rng *rand.Rand, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := r.gate.acquire(ctx); err != nil {
			return
		}
		delta := rng.Int64N(21) - 10
		if delta == 0 {
			delta = 1
		}
		args := counterArgs{
			Token: fmt.Sprintf("counter-%d-%012d", index, r.operationID.Add(1)),
			Delta: delta,
		}
		expected := truthCounterUpdate(r.counterTruth[index], args)
		actionCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		actual, err := gatewayAction[counterState](actionCtx, r.api, r.counterIDs[index], "apply", args)
		cancel()
		if err == nil {
			err = compareCounter(fmt.Sprintf("%d immediate", index), actual, expected)
		}
		if err == nil {
			r.counterTruth[index] = expected
		}
		r.gate.release()
		if err != nil {
			if ctx.Err() == nil {
				r.errors.report(err)
			}
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (r *soakRun) runChat(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	clientIndex := 0
	for {
		if err := r.gate.acquire(ctx); err != nil {
			return
		}
		labels := r.clients.labels()
		if len(labels) == 0 {
			r.gate.release()
			r.errors.report(errors.New("chat workload has no live clients"))
			return
		}
		label := labels[clientIndex%len(labels)]
		clientIndex++
		token := fmt.Sprintf("chat-%012d", r.operationID.Add(1))
		err := r.clients.client(label).write(token)
		if err == nil {
			messageCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			err = waitUntil(messageCtx, 25*time.Millisecond, func() (bool, error) {
				state := r.chatOracle.stateTruth()
				return state.LastToken == token, nil
			})
			cancel()
		}
		r.gate.release()
		if err != nil {
			if ctx.Err() == nil {
				r.errors.report(err)
			}
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (r *soakRun) runAlarms(ctx context.Context) {
	cycle := uint64(0)
	for {
		if err := r.gate.acquire(ctx); err != nil {
			return
		}
		cycle++
		nonce := fmt.Sprintf("alarm-%012d", cycle)
		args := alarmArgs{Nonce: nonce, DelayMillis: r.config.alarmDelay.Milliseconds()}
		actionCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		actual, err := gatewayAction[alarmState](actionCtx, r.api, r.alarmActor, "arm", args)
		cancel()
		if err == nil {
			err = compareAlarm(actual, r.alarmOracle.truth())
		}
		r.gate.release()
		if err != nil {
			if ctx.Err() == nil {
				r.errors.report(fmt.Errorf("arm alarm: %w", err))
			}
			return
		}
		alarmTimer := time.NewTimer(r.config.alarmDelay + 45*time.Second)
		select {
		case fired := <-r.alarmOracle.fired:
			if !alarmTimer.Stop() {
				<-alarmTimer.C
			}
			if fired != nonce {
				r.errors.report(fmt.Errorf("alarm fired nonce=%q want=%q", fired, nonce))
				return
			}
			r.activations.sleepWakes.Add(1)
		case <-alarmTimer.C:
			r.errors.report(fmt.Errorf("alarm %s did not fire within %s", nonce, r.config.alarmDelay+45*time.Second))
			return
		case <-ctx.Done():
			if !alarmTimer.Stop() {
				select {
				case <-alarmTimer.C:
				default:
				}
			}
			return
		}
	}
}

func (r *soakRun) startChaos(ctx context.Context, wait *sync.WaitGroup) {
	r.periodicChaos(ctx, wait, min(5*time.Second, r.config.panicInterval), r.config.panicInterval, r.panicChaos)
	r.periodicChaos(ctx, wait, r.config.disconnectInterval, r.config.disconnectInterval, r.disconnectChaos)
	r.periodicChaos(ctx, wait, r.config.stallInterval, r.config.stallInterval, r.stallChaos)
	r.periodicChaos(ctx, wait, r.config.restartInterval, r.config.restartInterval, r.restartChaos)
}

func (r *soakRun) periodicChaos(
	ctx context.Context,
	wait *sync.WaitGroup,
	first, interval time.Duration,
	chaos func(context.Context, int) error,
) {
	wait.Add(1)
	go func() {
		defer wait.Done()
		timer := time.NewTimer(first)
		defer timer.Stop()
		iteration := 0
		for {
			select {
			case <-timer.C:
				iteration++
				operationCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
				err := chaos(operationCtx, iteration)
				cancel()
				if err != nil {
					if ctx.Err() == nil {
						r.errors.report(err)
					}
					return
				}
				timer.Reset(interval)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (r *soakRun) disconnectChaos(ctx context.Context, iteration int) error {
	r.chaosMu.Lock()
	defer r.chaosMu.Unlock()
	if err := r.clients.disconnectAndReplace(ctx, iteration); err != nil {
		return fmt.Errorf("disconnect chaos: %w", err)
	}
	r.activations.disconnects.Add(1)
	return nil
}

func (r *soakRun) stallChaos(ctx context.Context, iteration int) error {
	r.chaosMu.Lock()
	defer r.chaosMu.Unlock()
	if err := r.clients.stall(ctx, r.gate, iteration, r.config.stallDuration); err != nil {
		return fmt.Errorf("stall chaos: %w", err)
	}
	r.activations.stalls.Add(1)
	return nil
}

func (r *soakRun) panicChaos(ctx context.Context, iteration int) error {
	r.chaosMu.Lock()
	defer r.chaosMu.Unlock()
	if err := r.gate.acquire(ctx); err != nil {
		return err
	}
	defer r.gate.release()
	actorID, err := r.api.createActor(ctx, "soak-sacrificial", r.runnerName, fmt.Sprintf("panic-%d", iteration))
	if err != nil {
		return err
	}
	if err := expectPanicAction(ctx, r.api, actorID); err != nil {
		return err
	}
	if _, err := gatewayAction[counterState](ctx, r.api, r.counterIDs[0], "snapshot", struct{}{}); err != nil {
		return fmt.Errorf("peer counter failed after sacrificial panic: %w", err)
	}
	r.activations.panics.Add(1)
	return nil
}

func (r *soakRun) restartChaos(ctx context.Context, _ int) error {
	r.chaosMu.Lock()
	defer r.chaosMu.Unlock()
	if err := r.gate.pause(ctx); err != nil {
		return fmt.Errorf("pause before engine restart: %w", err)
	}
	defer r.gate.resume()
	if err := r.clients.closeAll(ctx); err != nil {
		return fmt.Errorf("close clients before engine restart: %w", err)
	}
	if err := r.engine.Kill(); err != nil {
		return err
	}
	restartStarted := time.Now().UnixMilli()
	if err := r.engine.Start(ctx); err != nil {
		return fmt.Errorf("restart engine: %w", err)
	}
	if err := r.api.waitRunner(ctx, r.runnerName, true); err != nil {
		return fmt.Errorf("wait for runner reconnect: %w", err)
	}
	if err := r.api.waitRunnerPingAfter(ctx, r.runnerName, restartStarted); err != nil {
		return fmt.Errorf("wait for post-restart runner ping: %w", err)
	}
	// v2.3.10 retains an old generation for a 22-second liveness window after
	// abrupt engine replacement. Demand rehydration before that window closes
	// can park behind the stale generation, so this settlement is part of the
	// pinned recovery procedure rather than an assertion delay.
	liveness := time.NewTimer(22 * time.Second)
	select {
	case <-liveness.C:
	case <-ctx.Done():
		if !liveness.Stop() {
			select {
			case <-liveness.C:
			default:
			}
		}
		return ctx.Err()
	}
	for range r.config.clients {
		if _, err := r.clients.connect(ctx); err != nil {
			return fmt.Errorf("reconnect client after engine restart: %w", err)
		}
	}
	actual, err := gatewayAction[alarmState](ctx, r.api, r.alarmActor, "resleep", struct{}{})
	if err != nil {
		return fmt.Errorf("rehydrate alarm actor after engine restart: %w", err)
	}
	if err := compareAlarm(actual, r.alarmOracle.truth()); err != nil {
		return err
	}
	r.activations.engineRestarts.Add(1)
	return nil
}

func (r *soakRun) finalOracles(ctx context.Context) error {
	if err := waitUntil(ctx, 50*time.Millisecond, func() (bool, error) {
		err := r.chatOracle.convergence("")
		return err == nil, err
	}); err != nil {
		return fmt.Errorf("broadcast convergence: %w", err)
	}
	for index, actorID := range r.counterIDs {
		actual, err := gatewayAction[counterState](ctx, r.api, actorID, "snapshot", struct{}{})
		if err != nil {
			return err
		}
		if err := compareCounter(fmt.Sprintf("%d final", index), actual, r.counterTruth[index]); err != nil {
			return err
		}
		if actual.Operations == 0 {
			return fmt.Errorf("counter %d executed zero operations", index)
		}
	}
	chatActual, err := gatewayAction[chatState](ctx, r.api, r.chatActor, "snapshot", struct{}{})
	if err != nil {
		return err
	}
	if err := compareChat(chatActual, r.chatOracle.stateTruth()); err != nil {
		return err
	}
	if chatActual.Messages == 0 {
		return errors.New("chat actor handled zero messages")
	}
	alarmActual, err := gatewayAction[alarmState](ctx, r.api, r.alarmActor, "settle", struct{}{})
	if err != nil {
		return err
	}
	if err := compareAlarm(alarmActual, r.alarmOracle.truth()); err != nil {
		return err
	}
	if alarmActual.Armed == 0 || alarmActual.Fired == 0 {
		return fmt.Errorf("alarm convergence has armed=%d fired=%d", alarmActual.Armed, alarmActual.Fired)
	}
	if err := r.activations.validate(); err != nil {
		return err
	}
	counters, _ := r.hooks.snapshot()
	requiredMetrics := []string{
		rivet.MetricEventsPolled,
		rivet.MetricCommandsSubmitted,
		rivet.MetricActorStarts,
		rivet.MetricActorStops,
		rivet.MetricActorPanics,
	}
	for _, name := range requiredMetrics {
		if counters[name] == 0 {
			return fmt.Errorf("metric %s was never activated", name)
		}
	}
	if r.hooks.timingCount(rivet.MetricPollLatency) == 0 {
		return fmt.Errorf("metric %s was never activated", rivet.MetricPollLatency)
	}
	return nil
}

func headerValue(headers map[string]string, name string) string {
	for key, value := range headers {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}
