package quasar

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tehsphinx/quasar/transports"
)

// injectOrderFSM records cache state observed at Inject time so a test can
// assert Inject ran before raft / the consume goroutines were wired up.
type injectOrderFSM struct {
	injected        bool
	raftNilAtInject bool
}

func (f *injectOrderFSM) Inject(inj *FSMInjector) {
	f.injected = true
	// The m11 fix runs Inject before newRaft, so raft is not wired yet.
	f.raftNilAtInject = inj.cache.raft() == nil
}

func (f *injectOrderFSM) ApplyCmd([]byte) error       { return nil }
func (f *injectOrderFSM) Restore(io.ReadCloser) error { return nil }
func (f *injectOrderFSM) Reset() error                { return nil }
func (f *injectOrderFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, errors.New("injectOrderFSM: no snapshots")
}

// TestInjectRunsBeforeRaftWiring is the RT-13042 m11 regression test. Inject
// used to run only after newCache had already created raft and started the
// consume/persisted goroutines, so an early apply could reach a user FSM whose
// injector was still nil. Inject must now run before raft is wired up.
func TestInjectRunsBeforeRaftWiring(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fsm := &injectOrderFSM{}
	_, tr := transports.NewInmemTransport("")
	c, err := NewCache(ctx, fsm, WithLocalID("solo"), WithTransport(tr), WithBootstrap(true))
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })

	if !fsm.injected {
		t.Fatal("FSM was never injected")
	}
	if !fsm.raftNilAtInject {
		t.Fatal("Inject ran after raft was already wired up (m11 race window)")
	}
}
