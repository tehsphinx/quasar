package discoveries

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar/internal/frozennats"
)

// TestNATSDiscoveryConnLeavesAFrozenServer freezes the NATS server under the
// connection the pings travel on. A peer that is not heard for the alive window
// counts as dead, so that connection has to notice within seconds. The
// connection the discovery was given, on the client defaults, is the control:
// it is still attached (RT-15911).
func TestNATSDiscoveryConnLeavesAFrozenServer(t *testing.T) {
	srv := frozennats.Run(t)
	given, err := nats.Connect(srv.URL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer given.Close()

	conn := NewNATSDiscovery(given).dial("quasar-test-discovery")
	if conn == given {
		t.Fatal("the discovery connection fell back to the given connection")
	}
	defer conn.Close()

	srv.Freeze()
	frozenAt := time.Now()
	const within = 8 * time.Second
	for conn.IsConnected() {
		if time.Since(frozenAt) > within {
			t.Fatalf("the discovery connection is still attached to the frozen server after %s", within)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("the discovery connection left the frozen server after %s", time.Since(frozenAt).Round(100*time.Millisecond))

	if !given.IsConnected() {
		t.Error("the given connection left too; it should be on its own ping settings")
	}
}
