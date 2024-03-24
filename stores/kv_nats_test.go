package stores

import (
	"testing"

	"github.com/nats-io/nats.go"
)

func TestNatsKV(t *testing.T) {
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

	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		t.Skip("test needs NATS running on localhost:4222")
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Skip("NATS JetStream must be enabled")
	}

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "quasar_test",
	})
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if r := js.DeleteKeyValue("quasar_test"); r != nil {
			t.Error(r)
		}
	}()

	stable := NewNatsKVStore(kv)

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
