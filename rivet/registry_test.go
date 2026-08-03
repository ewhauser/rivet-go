package rivet

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

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

func TestRegisterBuildsSortedManifestAndRejectsDuplicates(t *testing.T) {
	registry := NewRegistry()
	if err := Register(registry, "zeta", Actor[struct{}]{}); err != nil {
		t.Fatalf("Register zeta: %v", err)
	}
	if err := Register(registry, "alpha", Actor[int]{}); err != nil {
		t.Fatalf("Register alpha: %v", err)
	}
	if err := Register(registry, "alpha", Actor[int]{}); err == nil {
		t.Fatal("duplicate Register succeeded")
	}
	names, handlers := registry.snapshotActors()
	if !reflect.DeepEqual(names, []string{"alpha", "zeta"}) {
		t.Fatalf("actor names = %#v", names)
	}
	if len(handlers) != 2 || handlers["alpha"] == nil || handlers["zeta"] == nil {
		t.Fatalf("actor handlers = %#v", handlers)
	}
}

type binaryState struct {
	Value byte
}

func (s *binaryState) MarshalBinary() ([]byte, error) {
	if s.Value == 0xff {
		return nil, errors.New("sentinel encode failure")
	}
	return []byte{s.Value}, nil
}

func (s *binaryState) UnmarshalBinary(data []byte) error {
	if len(data) != 1 {
		return errors.New("binary state must contain one byte")
	}
	s.Value = data[0]
	return nil
}

func TestStateSerdeJSONAndBinaryOverride(t *testing.T) {
	type jsonState struct {
		Count int `json:"count"`
	}
	jsonValue := jsonState{Count: 7}
	encoded, err := encodeState(&jsonValue)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeState[jsonState](encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != jsonValue {
		t.Fatalf("JSON state = %#v, want %#v", decoded, jsonValue)
	}

	binaryValue := binaryState{Value: 9}
	encoded, err = encodeState(&binaryValue)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, []byte{9}) {
		t.Fatalf("binary state encoding = %v", encoded)
	}
	binaryDecoded, err := decodeState[binaryState](encoded)
	if err != nil {
		t.Fatal(err)
	}
	if binaryDecoded != binaryValue {
		t.Fatalf("binary state = %#v, want %#v", binaryDecoded, binaryValue)
	}
}
