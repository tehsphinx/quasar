package stores

import (
	"testing"
)

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
