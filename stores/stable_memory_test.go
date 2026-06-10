package stores

import (
	"strconv"
	"sync"
	"testing"
)

func TestStableInMemory(t *testing.T) {
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

	stable := NewStableInMemory()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if r := stable.Set([]byte(tt.key), []byte(tt.val)); r != nil {
				t.Error(r)
			}

			got, r := stable.Get([]byte(tt.key))
			if r != nil {
				t.Error(r)
			}
			if string(got) != tt.val {
				t.Errorf("got: %s, expected: %s\n", got, tt.val)
			}
		})
	}
}

// TestStableInMemory_ConcurrentAccess covers RT-13042 C2: during quorum
// recovery the old and new raft instance share the same stable store and
// access it concurrently. Fails under -race without internal locking.
func TestStableInMemory_ConcurrentAccess(t *testing.T) {
	stable := NewStableInMemory()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			key := []byte("CurrentTerm" + strconv.Itoa(i%2))
			for j := 0; j < 1000; j++ {
				if err := stable.Set(key, []byte{byte(j)}); err != nil {
					t.Error(err)
				}
				if _, err := stable.Get(key); err != nil {
					t.Error(err)
				}
				if err := stable.SetUint64(key, uint64(j)); err != nil {
					t.Error(err)
				}
				if _, err := stable.GetUint64(key); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}
