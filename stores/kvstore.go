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
