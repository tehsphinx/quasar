package stores

import (
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
