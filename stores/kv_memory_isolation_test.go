package stores

import (
	"testing"
)

// TestInMemKVStore_NoAliasing covers RT-13042 m23: the in-memory KV store must
// own its data. Mutating the slice returned by Load, or the slice passed to
// Store, must not change what a subsequent Load returns.
func TestInMemKVStore_NoAliasing(t *testing.T) {
	kv := NewInMemKVStore()

	// Copy-in: mutating the slice passed to Store must not leak into the store.
	in := []byte("v1")
	if err := kv.Store("k", in); err != nil {
		t.Fatal(err)
	}
	in[0] = 'X'
	got, err := kv.Load("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v1" {
		t.Fatalf("Store aliased its input: Load = %q, want %q", got, "v1")
	}

	// Copy-out: mutating the slice returned by Load must not corrupt the store.
	got[0] = 'Y'
	again, err := kv.Load("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != "v1" {
		t.Fatalf("Load leaked backing array: Load = %q, want %q", again, "v1")
	}
}
