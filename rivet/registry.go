package rivet

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ewhauser/rivet-go/internal/ffi"
	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/ewhauser/rivet-go/internal/wire"
)

const (
	defaultEndpoint   = "http://127.0.0.1:6420"
	defaultNamespace  = "default"
	defaultRunnerName = "rivet-go"
	defaultLogLevel   = "info"
)

// Config controls engine registration. Version is the engine-visible runner
// version. TotalSlots remains a stable boundary field but v2.3.10 does not
// expose a CoreRegistry setter for it.
type Config struct {
	Endpoint   string
	Namespace  string
	RunnerName string
	Version    uint32
	TotalSlots uint32
	LogLevel   string
}

// Registry owns typed actor registrations and may serve only once at a time.
type Registry struct {
	serving atomic.Bool
	mu      sync.RWMutex
	actors  map[string]pump.ActorHandler
}

func NewRegistry() *Registry {
	return &Registry{actors: make(map[string]pump.ActorHandler)}
}

// Register adds one typed actor definition to registry. Registrations are
// immutable while Serve is running so the engine manifest and Go dispatcher
// always describe the same actor names and actions.
func Register[T any](registry *Registry, name string, actor Actor[T]) error {
	if registry == nil {
		return errors.New("rivet registry is nil")
	}
	if strings.TrimSpace(name) == "" {
		return errors.New("actor name must not be empty")
	}
	if registry.serving.Load() {
		return errors.New("cannot register an actor while registry is serving")
	}
	if len(actor.Actions) > 1_024 {
		return fmt.Errorf("actor %q must contain at most 1024 actions", name)
	}
	for actionName, handler := range actor.Actions {
		if strings.TrimSpace(actionName) == "" {
			return fmt.Errorf("actor %q action names must not be empty", name)
		}
		if handler == nil {
			return fmt.Errorf("actor %q action %q has a nil handler", name, actionName)
		}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.serving.Load() {
		return errors.New("cannot register an actor while registry is serving")
	}
	if _, exists := registry.actors[name]; exists {
		return fmt.Errorf("actor %q is already registered", name)
	}
	registry.actors[name] = &actorAdapter[T]{definition: actor}
	return nil
}

// Serve registers this runner and its actor manifest with the configured
// engine, then blocks until ctx is canceled, graceful drain completes, or a
// fatal runtime error is returned.
func (r *Registry) Serve(ctx context.Context, config Config) error {
	if r == nil {
		return errors.New("rivet registry is nil")
	}
	if ctx == nil {
		return errors.New("serve context is nil")
	}
	if !r.serving.CompareAndSwap(false, true) {
		return errors.New("registry is already serving")
	}
	defer r.serving.Store(false)

	actorNames, actorActions, handlers := r.snapshotActors()
	config = withDefaults(config)
	encoded, err := wire.EncodeRunnerConfig(wire.RunnerConfig{
		EngineEndpoint: config.Endpoint,
		Namespace:      config.Namespace,
		RunnerName:     config.RunnerName,
		Version:        config.Version,
		TotalSlots:     config.TotalSlots,
		ActorNames:     actorNames,
		ActorActions:   actorActions,
		LogLevel:       config.LogLevel,
	})
	if err != nil {
		return err
	}
	result, err := ffi.NewRunner(encoded)
	if err != nil {
		return fmt.Errorf("load native runner: %w", err)
	}
	if result.Error != nil {
		defer result.Error.Close()
		payload, decodeErr := result.Error.Payload()
		if decodeErr != nil {
			return fmt.Errorf("start native runner: decode error: %w", decodeErr)
		}
		return fmt.Errorf("start native runner: %w", payload)
	}
	if result.Runner == nil {
		return errors.New("start native runner: native constructor returned neither runner nor error")
	}

	return pump.NewWithHandlers(result.Runner, handlers).Run(ctx)
}

func (r *Registry) snapshotActors() (
	[]string,
	map[string][]string,
	map[string]pump.ActorHandler,
) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.actors))
	handlers := make(map[string]pump.ActorHandler, len(r.actors))
	actions := make(map[string][]string, len(r.actors))
	for name, handler := range r.actors {
		names = append(names, name)
		handlers[name] = handler
		if definition, ok := handler.(interface{ actionNames() []string }); ok {
			actionNames := definition.actionNames()
			sort.Strings(actionNames)
			actions[name] = actionNames
		}
	}
	sort.Strings(names)
	return names, actions, handlers
}

func withDefaults(config Config) Config {
	if config.Endpoint == "" {
		config.Endpoint = defaultEndpoint
	}
	if config.Namespace == "" {
		config.Namespace = defaultNamespace
	}
	if config.RunnerName == "" {
		config.RunnerName = defaultRunnerName
	}
	if config.Version == 0 {
		config.Version = 1
	}
	if config.TotalSlots == 0 {
		config.TotalSlots = 1
	}
	if config.LogLevel == "" {
		config.LogLevel = defaultLogLevel
	}
	return config
}
