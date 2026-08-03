package rivet

import "testing"

func TestConfigDefaults(t *testing.T) {
	config := withDefaults(Config{})
	if config.Endpoint != defaultEndpoint ||
		config.Namespace != defaultNamespace ||
		config.RunnerName != defaultRunnerName ||
		config.Version != 1 ||
		config.TotalSlots != 1 ||
		config.LogLevel != defaultLogLevel {
		t.Fatalf("unexpected defaults: %#v", config)
	}
}
