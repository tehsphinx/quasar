package stores

import (
	"encoding/binary"
	"testing"
)

// TestStableInMemory_SharedKeyspace covers RT-13042 M22: byte and uint64 values
// must share a single keyspace, so Get sees a value written by SetUint64 for the
// same key (and GetUint64 sees a value written by Set).
func TestStableInMemory_SharedKeyspace(t *testing.T) {
	stable := NewStableInMemory()

	if err := stable.SetUint64([]byte("k"), 42); err != nil {
		t.Fatal(err)
	}
	got, err := stable.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if u := binary.BigEndian.Uint64(got); u != 42 {
		t.Fatalf("Get after SetUint64: got %d, want 42", u)
	}

	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, 7)
	if err := stable.Set([]byte("k2"), val); err != nil {
		t.Fatal(err)
	}
	gotU, err := stable.GetUint64([]byte("k2"))
	if err != nil {
		t.Fatal(err)
	}
	if gotU != 7 {
		t.Fatalf("GetUint64 after Set: got %d, want 7", gotU)
	}
}
