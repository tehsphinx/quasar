package stores

import (
	"cmp"
	"os"
	"testing"

	"github.com/nats-io/nats.go"
)

// natsURL is the NATS server the store tests dial. Override with NATS_URL.
var natsURL = cmp.Or(os.Getenv("NATS_URL"), "nats://localhost:4222")

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

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("NATS not available at %s: %v", natsURL, err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Skip("NATS JetStream must be enabled")
	}

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: t.Name(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r := js.DeleteKeyValue(t.Name()); r != nil {
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
