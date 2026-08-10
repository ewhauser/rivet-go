package rivet

import (
	"context"
	"errors"

	"github.com/ewhauser/rivet-go/internal/pump"
	"github.com/ewhauser/rivet-go/internal/wire"
)

// KVEntry is one key-value pair returned by KV.List. Key and Value are owned
// by the caller.
type KVEntry struct {
	Key   []byte
	Value []byte
}

// KVListOptions controls an actor KV list operation. A zero Limit leaves the
// result count unbounded by the caller.
type KVListOptions struct {
	Prefix  []byte
	Reverse bool
	Limit   uint32
}

type actorKVSession interface {
	KVGet(context.Context, []byte) ([]byte, bool, error)
	KVList(context.Context, []byte, bool, *uint32) ([]wire.KVEntry, error)
	KVPut(context.Context, []byte, []byte) error
	KVDelete(context.Context, []byte) error
}

// KV is one actor generation's low-level key-value store. Keys and values are
// arbitrary bytes. Methods copy caller-owned input before it crosses the
// native boundary.
//
// Deprecated: Prefer typed actor state or Context.DB for new actors.
type KV struct {
	session actorKVSession
}

func newKV(session *pump.ActorSession) *KV {
	return &KV{session: session}
}

// Get returns a copy of the value for key. Found distinguishes a missing key
// from a present key whose value is empty.
func (k *KV) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	if err := k.validate(ctx); err != nil {
		return nil, false, err
	}
	value, found, err = k.session.KVGet(ctx, append([]byte(nil), key...))
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), value...), found, nil
}

// List returns copies of entries matching options.Prefix.
func (k *KV) List(ctx context.Context, options KVListOptions) ([]KVEntry, error) {
	if err := k.validate(ctx); err != nil {
		return nil, err
	}
	var limit *uint32
	if options.Limit != 0 {
		value := options.Limit
		limit = &value
	}
	entries, err := k.session.KVList(
		ctx,
		append([]byte(nil), options.Prefix...),
		options.Reverse,
		limit,
	)
	if err != nil {
		return nil, err
	}
	result := make([]KVEntry, len(entries))
	for index, entry := range entries {
		result[index] = KVEntry{
			Key:   append([]byte(nil), entry.Key...),
			Value: append([]byte(nil), entry.Value...),
		}
	}
	return result, nil
}

// Put stores value at key.
func (k *KV) Put(ctx context.Context, key, value []byte) error {
	if err := k.validate(ctx); err != nil {
		return err
	}
	return k.session.KVPut(
		ctx,
		append([]byte(nil), key...),
		append([]byte(nil), value...),
	)
}

// Delete removes key. Deleting a missing key succeeds.
func (k *KV) Delete(ctx context.Context, key []byte) error {
	if err := k.validate(ctx); err != nil {
		return err
	}
	return k.session.KVDelete(ctx, append([]byte(nil), key...))
}

func (k *KV) validate(ctx context.Context) error {
	if k == nil || k.session == nil {
		return errors.New("actor KV is unavailable")
	}
	if ctx == nil {
		return errors.New("KV context is nil")
	}
	return ctx.Err()
}
