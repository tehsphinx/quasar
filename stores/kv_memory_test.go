package stores

import (
	"testing"
)

// TestInMemoryKVSnapshotStore covers the SnapshotKVStore extension added for
// RT-13042 M8: Items copies out the full content, SetItems replaces it, and
// Clear empties the store.
func TestInMemoryKVSnapshotStore(t *testing.T) {
	kv, ok := NewInMemKVStore().(SnapshotKVStore)
	if !ok {
		t.Fatal("in-memory KV store does not implement SnapshotKVStore")
	}

	if r := kv.Store("k1", []byte("v1")); r != nil {
		t.Fatal(r)
	}
	if r := kv.Store("k2", []byte("v2")); r != nil {
		t.Fatal(r)
	}

	items, err := kv.Items()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || string(items["k1"]) != "v1" || string(items["k2"]) != "v2" {
		t.Fatalf("unexpected items: %v", items)
	}

	// Items must return a copy: mutating it must not affect the store.
	items["k1"][0] = 'X'
	got, err := kv.Load("k1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v1" {
		t.Fatalf("Items leaked internal state: store now holds %q", got)
	}

	if r := kv.SetItems(map[string][]byte{"k3": []byte("v3")}); r != nil {
		t.Fatal(r)
	}
	if _, err := kv.Load("k1"); err == nil {
		t.Fatal("SetItems did not replace existing content")
	}
	got, err = kv.Load("k3")
	if err != nil || string(got) != "v3" {
		t.Fatalf("Load(k3) = %q, %v", got, err)
	}

	if r := kv.Clear(); r != nil {
		t.Fatal(r)
	}
	if _, err := kv.Load("k3"); err == nil {
		t.Fatal("Clear did not empty the store")
	}
}

func TestInMemoryKV(t *testing.T) {
	tests := []struct {
		name     string
		key, val string
	}{
		{
			name: "value",
			key:  "key1",
			val:  "val1",
		},
	}

	stable := NewInMemKVStore()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if r := stable.Store(tt.key, []byte(tt.val)); r != nil {
				t.Error(r)
			}

			got, r := stable.Load(tt.key)
			if r != nil {
				t.Error(r)
			}
			if string(got) != tt.val {
				t.Errorf("got: %s, expected: %s\n", got, tt.val)
			}
		})
	}
}
