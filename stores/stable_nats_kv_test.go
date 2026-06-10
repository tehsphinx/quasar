package stores

import (
	"testing"

	"github.com/nats-io/nats.go"
)

func TestStableNatsKV(t *testing.T) {
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
		t.Fatal(err)
	}
	defer func() {
		if r := js.DeleteKeyValue("quasar_test"); r != nil {
			t.Error(r)
		}
	}()

	stable := NewStableNatsKV(kv)

	// raft.StableStore contract: a missing key is a zero value with nil error,
	// not an error. raft.NewRaft fails on a fresh bucket otherwise (RT-13042 C4).
	t.Run("missing key", func(t *testing.T) {
		got, r := stable.Get([]byte("missing"))
		if r != nil {
			t.Errorf("Get on missing key returned error: %v", r)
		}
		if len(got) != 0 {
			t.Errorf("Get on missing key returned %q, expected empty", got)
		}

		gotUint, r := stable.GetUint64([]byte("missing"))
		if r != nil {
			t.Errorf("GetUint64 on missing key returned error: %v", r)
		}
		if gotUint != 0 {
			t.Errorf("GetUint64 on missing key returned %d, expected 0", gotUint)
		}
	})

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
