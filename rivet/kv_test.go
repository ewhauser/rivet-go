package rivet

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ewhauser/rivet-go/internal/wire"
)

type fakeKVSession struct {
	getKey       []byte
	listPrefix   []byte
	listReverse  bool
	listLimit    *uint32
	putKey       []byte
	putValue     []byte
	deleteKey    []byte
	getValue     []byte
	getFound     bool
	listEntries  []wire.KVEntry
	operationErr error
}

func (s *fakeKVSession) KVGet(_ context.Context, key []byte) ([]byte, bool, error) {
	s.getKey = append([]byte(nil), key...)
	return s.getValue, s.getFound, s.operationErr
}

func (s *fakeKVSession) KVList(
	_ context.Context,
	prefix []byte,
	reverse bool,
	limit *uint32,
) ([]wire.KVEntry, error) {
	s.listPrefix = append([]byte(nil), prefix...)
	s.listReverse = reverse
	if limit != nil {
		value := *limit
		s.listLimit = &value
	}
	return s.listEntries, s.operationErr
}

func (s *fakeKVSession) KVPut(_ context.Context, key, value []byte) error {
	s.putKey = append([]byte(nil), key...)
	s.putValue = append([]byte(nil), value...)
	return s.operationErr
}

func (s *fakeKVSession) KVDelete(_ context.Context, key []byte) error {
	s.deleteKey = append([]byte(nil), key...)
	return s.operationErr
}

func TestKVOperationsExposeTypedPublicSurface(t *testing.T) {
	session := &fakeKVSession{
		getValue: []byte("stored"),
		getFound: true,
		listEntries: []wire.KVEntry{{
			Key:   []byte("items/a"),
			Value: []byte("one"),
		}},
	}
	store := &KV{session: session}
	ctx := context.Background()

	value, found, err := store.Get(ctx, []byte("item"))
	if err != nil || !found || !bytes.Equal(value, []byte("stored")) {
		t.Fatalf("Get = %q, %t, %v", value, found, err)
	}
	if !bytes.Equal(session.getKey, []byte("item")) {
		t.Fatalf("Get key = %q", session.getKey)
	}
	value[0] = 'X'
	if !bytes.Equal(session.getValue, []byte("stored")) {
		t.Fatal("Get returned mutable session-owned data")
	}

	entries, err := store.List(ctx, KVListOptions{
		Prefix:  []byte("items/"),
		Reverse: true,
		Limit:   7,
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !bytes.Equal(session.listPrefix, []byte("items/")) || !session.listReverse ||
		session.listLimit == nil || *session.listLimit != 7 {
		t.Fatalf("List options = prefix %q reverse %t limit %v", session.listPrefix, session.listReverse, session.listLimit)
	}
	if len(entries) != 1 || !bytes.Equal(entries[0].Key, []byte("items/a")) ||
		!bytes.Equal(entries[0].Value, []byte("one")) {
		t.Fatalf("List entries = %#v", entries)
	}
	entries[0].Key[0] = 'X'
	entries[0].Value[0] = 'X'
	if !bytes.Equal(session.listEntries[0].Key, []byte("items/a")) ||
		!bytes.Equal(session.listEntries[0].Value, []byte("one")) {
		t.Fatal("List returned mutable session-owned data")
	}

	if err := store.Put(ctx, []byte("new"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !bytes.Equal(session.putKey, []byte("new")) || !bytes.Equal(session.putValue, []byte("value")) {
		t.Fatalf("Put = key %q value %q", session.putKey, session.putValue)
	}
	if err := store.Delete(ctx, []byte("old")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !bytes.Equal(session.deleteKey, []byte("old")) {
		t.Fatalf("Delete key = %q", session.deleteKey)
	}
}

func TestKVValidationAndErrors(t *testing.T) {
	if _, _, err := (*KV)(nil).Get(context.Background(), []byte("key")); err == nil {
		t.Fatal("nil KV Get succeeded")
	}
	store := &KV{session: &fakeKVSession{}}
	if err := store.Put(nil, []byte("key"), []byte("value")); err == nil {
		t.Fatal("Put with nil context succeeded")
	}
	want := errors.New("core rejected operation")
	store.session = &fakeKVSession{operationErr: want}
	if err := store.Delete(context.Background(), []byte("key")); !errors.Is(err, want) {
		t.Fatalf("Delete error = %v, want %v", err, want)
	}
	if contextKV := (*Context[struct{}])(nil).KV(); contextKV != nil {
		t.Fatalf("nil Context KV = %#v, want nil", contextKV)
	}
}
