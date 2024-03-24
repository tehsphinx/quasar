package stores

import (
	"errors"
)

var ErrNotFound = errors.New("key not found")

type KVStore interface {
	Store(key string, data []byte) error
	Load(key string) ([]byte, error)
}
