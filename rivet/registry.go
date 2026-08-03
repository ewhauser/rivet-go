package rivet

import (
	"context"
	"errors"
	"fmt"
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

// Config controls engine registration for an empty M1 registry. Version is
// the engine-visible runner version and TotalSlots is retained in the stable
// boundary for the actor-capable milestones.
type Config struct {
	Endpoint   string
	Namespace  string
	RunnerName string
	Version    uint32
	TotalSlots uint32
	LogLevel   string
}

// Registry is the actor registration owner. M1 has no actor registration
// methods; M2 and M3 add actors and actions without changing Serve.
type Registry struct {
	serving atomic.Bool
}

func NewRegistry() *Registry {
	return &Registry{}
}

// Serve registers this zero-actor runner with the configured engine and blocks
// until ctx is canceled, graceful drain completes, or a fatal runtime error is
// returned.
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

	config = withDefaults(config)
	encoded, err := wire.EncodeRunnerConfig(wire.RunnerConfig{
		EngineEndpoint: config.Endpoint,
		Namespace:      config.Namespace,
		RunnerName:     config.RunnerName,
		Version:        config.Version,
		TotalSlots:     config.TotalSlots,
		ActorNames:     []string{},
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

	return pump.New(result.Runner).Run(ctx)
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
