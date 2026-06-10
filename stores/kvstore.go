package stores

import (
	"errors"
)

// ErrNotFound defines an error if a key is not found in the key value store.
var ErrNotFound = errors.New("key not found")

// KVStore defines an interface to be optionally passed into NewKVCache.
// There are some existing implementations like NewInMemKVStore and NewNatsKVStore.
// By default, the in-memory implementation is used.
type KVStore interface {
	Store(key string, data []byte) error
	Load(key string) ([]byte, error)
}

// SnapshotKVStore is an optional extension of KVStore that allows the
// KVCache FSM to take and restore raft snapshots and to reset the store
// (RT-13042 M8). KVStore implementations that do not implement it cannot be
// snapshotted: raft auto-snapshots (SnapshotInterval / SnapshotThreshold)
// and cache resets will fail with an explicit error for such stores. The
// in-memory store (NewInMemKVStore) implements it.
type SnapshotKVStore interface {
	KVStore

	// Items returns a copy of all key/value pairs in the store.
	Items() (map[string][]byte, error)
	// SetItems replaces the entire store content with the given pairs.
	SetItems(items map[string][]byte) error
	// Clear removes all pairs from the store.
	Clear() error
}
