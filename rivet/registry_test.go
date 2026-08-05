package rivet

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
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
	if err := Register(registry, "zeta", Actor[struct{}]{
		HibernateWebSockets: true,
		Database:            true,
		Actions: Actions[struct{}]{
			"z": Action(func(*Context[struct{}], struct{}) (struct{}, error) { return struct{}{}, nil }),
			"a": Action(func(*Context[struct{}], struct{}) (struct{}, error) { return struct{}{}, nil }),
		},
	}); err != nil {
		t.Fatalf("Register zeta: %v", err)
	}
	if err := Register(registry, "alpha", Actor[int]{}); err != nil {
		t.Fatalf("Register alpha: %v", err)
	}
	if err := Register(registry, "alpha", Actor[int]{HibernateWebSockets: true}); err == nil {
		t.Fatal("duplicate Register succeeded")
	}
	names, actions, hibernateWebSockets, databases, handlers := registry.snapshotActors()
	if !reflect.DeepEqual(names, []string{"alpha", "zeta"}) {
		t.Fatalf("actor names = %#v", names)
	}
	if len(handlers) != 2 || handlers["alpha"] == nil || handlers["zeta"] == nil {
		t.Fatalf("actor handlers = %#v", handlers)
	}
	if len(actions) != 2 || !reflect.DeepEqual(actions["zeta"], []string{"a", "z"}) {
		t.Fatalf("actor action manifests = %#v", actions)
	}
	if !reflect.DeepEqual(hibernateWebSockets, map[string]bool{"alpha": false, "zeta": true}) {
		t.Fatalf("actor WebSocket hibernation manifest = %#v", hibernateWebSockets)
	}
	if !reflect.DeepEqual(databases, map[string]bool{"alpha": false, "zeta": true}) {
		t.Fatalf("actor database manifest = %#v", databases)
	}
}

func TestRegisterEnforcesActorManifestBound(t *testing.T) {
	registry := NewRegistry()
	for index := range maxRegisteredActors {
		name := fmt.Sprintf("actor-%04d", index)
		if err := Register(registry, name, Actor[struct{}]{}); err != nil {
			t.Fatalf("Register %s: %v", name, err)
		}
	}
	if err := Register(registry, "actor-over-limit", Actor[struct{}]{}); err == nil ||
		!strings.Contains(err.Error(), "at most 1024 actors") {
		t.Fatalf("over-limit actor registration error = %v", err)
	}
	if err := Register(registry, "actor-0000", Actor[struct{}]{HibernateWebSockets: true}); err == nil ||
		!strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate registration at bound error = %v", err)
	}
	_, _, hibernation, databases, _ := registry.snapshotActors()
	if hibernation["actor-0000"] {
		t.Fatal("rejected duplicate changed the actor hibernation manifest")
	}
	if databases["actor-0000"] {
		t.Fatal("rejected duplicate changed the actor database manifest")
	}
}

func TestRegisterRejectsInvalidActionManifest(t *testing.T) {
	if err := Register(NewRegistry(), "counter", Actor[struct{}]{
		Actions: Actions[struct{}]{" ": Action(func(*Context[struct{}], struct{}) (struct{}, error) {
			return struct{}{}, nil
		})},
	}); err == nil || !strings.Contains(err.Error(), "action names must not be empty") {
		t.Fatalf("empty action name error = %v", err)
	}

	if err := Register(NewRegistry(), "counter", Actor[struct{}]{
		Actions: Actions[struct{}]{"increment": nil},
	}); err == nil || !strings.Contains(err.Error(), "nil handler") {
		t.Fatalf("nil action handler error = %v", err)
	}

	if err := Register(NewRegistry(), "counter", Actor[struct{}]{
		Actions: Actions[struct{}]{internalAlarmAction: Action(
			func(*Context[struct{}], struct{}) (struct{}, error) { return struct{}{}, nil },
		)},
	}); err == nil || !strings.Contains(err.Error(), "reserved for durable alarms") {
		t.Fatalf("reserved alarm action error = %v", err)
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

type emptyBinaryState struct {
	Decoded bool
}

func (*emptyBinaryState) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

func (s *emptyBinaryState) UnmarshalBinary(data []byte) error {
	if data == nil {
		return errors.New("persisted empty state was reported as absent")
	}
	if len(data) != 0 {
		return errors.New("empty binary state contained data")
	}
	s.Decoded = true
	return nil
}

func TestStateSerdeDistinguishesAbsentFromPersistedEmpty(t *testing.T) {
	fresh, err := decodeState[emptyBinaryState](nil)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Decoded {
		t.Fatal("first start invoked the binary state decoder")
	}

	rehydrated, err := decodeState[emptyBinaryState]([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if !rehydrated.Decoded {
		t.Fatal("persisted zero-length state did not invoke the binary state decoder")
	}
}

func TestStateSerdeErrorsAreReturned(t *testing.T) {
	value := binaryState{Value: 0xff}
	if _, err := encodeState(&value); err == nil || !strings.Contains(err.Error(), "sentinel encode failure") {
		t.Fatalf("binary encode error = %v", err)
	}
	if _, err := decodeState[binaryState]([]byte{1, 2}); err == nil || err.Error() != "decode actor binary state: binary state must contain one byte" {
		t.Fatalf("binary decode error = %v", err)
	}
	type unsupportedJSONState struct {
		Value chan int `json:"value"`
	}
	unsupported := unsupportedJSONState{Value: make(chan int)}
	if _, err := encodeState(&unsupported); err == nil {
		t.Fatal("unsupported JSON state encoded successfully")
	}
}
