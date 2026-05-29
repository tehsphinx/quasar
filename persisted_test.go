// Copyright (c) RealTyme SA. All rights reserved.

package quasar_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/matryer/is"
	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/examples/generic/exampleFSM"
	"github.com/tehsphinx/quasar/transports"
)

// TestPersistedFIFO_ThreeNodeWriteFromAnyNode brings up a 3-node Inmem
// cluster with persisted-FIFO enabled, writes from each node in turn,
// and verifies every replica sees every write.
//
// The persisted-FIFO path routes ALL writes through the queue — both
// leader-self and follower writes — so this is the smoke test for the
// dispatcher in Cache.apply and the consumer / reply plumbing in
// runPersistedApplyLoop.
func TestPersistedFIFO_ThreeNodeWriteFromAnyNode(t *testing.T) {
	ctxMain, cancelMain := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelMain()

	asrt := is.New(t)

	addr1, t1 := transports.NewInmemTransport("")
	addr2, t2 := transports.NewInmemTransport("")
	addr3, t3 := transports.NewInmemTransport("")

	t1.Connect(addr2, t2)
	t1.Connect(addr3, t3)
	t2.Connect(addr1, t1)
	t2.Connect(addr3, t3)
	t3.Connect(addr1, t1)
	t3.Connect(addr2, t2)

	hub := transports.NewInmemQueueHub()
	transports.ConnectInmemQueueHub(hub, t1, t2, t3)

	servers := []raft.Server{
		{ID: "cache1", Address: addr1, Suffrage: raft.Voter},
		{ID: "cache2", Address: addr2, Suffrage: raft.Voter},
		{ID: "cache3", Address: addr3, Suffrage: raft.Voter},
	}

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	fsm3 := exampleFSM.NewInMemoryFSM()

	c1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(t1),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)
	defer c1.Shutdown()

	c2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(t2),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)
	defer c2.Shutdown()

	c3, err := quasar.NewCache(ctxMain, fsm3,
		quasar.WithLocalID("cache3"),
		quasar.WithTransport(t3),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)
	defer c3.Shutdown()

	asrt.NoErr(c1.WaitReady(ctxMain))
	asrt.NoErr(c2.WaitReady(ctxMain))
	asrt.NoErr(c3.WaitReady(ctxMain))

	// One write from each node. Persisted-FIFO means every write hits the
	// hub, the leader's consumer drains it, and the reply travels back.
	type write struct {
		from *exampleFSM.InMemoryFSM
		name string
	}
	writes := []write{
		{from: fsm1, name: "Alex"},
		{from: fsm2, name: "Bea"},
		{from: fsm3, name: "Cara"},
	}
	for _, w := range writes {
		writeCtx, cancel := context.WithTimeout(ctxMain, 5*time.Second)
		err := w.from.SetMusician(writeCtx, exampleFSM.Musician{
			Name: w.name, Age: 30, Instruments: []string{"queue"},
		})
		cancel()
		asrt.NoErr(err)
	}

	// All nodes must see all writes.
	readCtx, cancelRead := context.WithTimeout(ctxMain, 5*time.Second)
	defer cancelRead()
	err = waitForCondition(readCtx, func() error {
		for _, fsm := range []*exampleFSM.InMemoryFSM{fsm1, fsm2, fsm3} {
			for _, w := range writes {
				got, e := fsm.GetMusicianKnownLatest(readCtx, w.name)
				if e != nil {
					return fmt.Errorf("get %q: %w", w.name, e)
				}
				if got.Name != w.name {
					return fmt.Errorf("missing %q", w.name)
				}
			}
		}
		return nil
	})
	asrt.NoErr(err)
}

// TestPersistedFIFO_WithoutHub_StaysOnLegacyPath confirms the
// dispatcher is a no-op when the transport is not configured with a
// persisted-FIFO hub — i.e. the existing leader-local / leader-RPC
// path stays in place for clusters that don't opt in.
func TestPersistedFIFO_WithoutHub_StaysOnLegacyPath(t *testing.T) {
	asrt := is.New(t)
	ctxMain, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr1, t1 := transports.NewInmemTransport("")
	addr2, t2 := transports.NewInmemTransport("")
	t1.Connect(addr2, t2)
	t2.Connect(addr1, t1)
	// No queue hub attached: SupportsPersisted() should be false on
	// both transports.
	asrt.Equal(t1.SupportsPersisted(), false)
	asrt.Equal(t2.SupportsPersisted(), false)

	servers := []raft.Server{
		{ID: "n1", Address: addr1, Suffrage: raft.Voter},
		{ID: "n2", Address: addr2, Suffrage: raft.Nonvoter},
	}

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	c1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("n1"),
		quasar.WithTransport(t1),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)
	defer c1.Shutdown()
	c2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("n2"),
		quasar.WithTransport(t2),
		quasar.WithSuffrage(raft.Nonvoter),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)
	defer c2.Shutdown()

	asrt.NoErr(c1.WaitReady(ctxMain))
	asrt.NoErr(c2.WaitReady(ctxMain))

	// Plain write still works through the legacy path.
	writeCtx, cancelW := context.WithTimeout(ctxMain, 3*time.Second)
	defer cancelW()
	asrt.NoErr(fsm1.SetMusician(writeCtx, exampleFSM.Musician{Name: "Legacy"}))

	readCtx, cancelR := context.WithTimeout(ctxMain, 3*time.Second)
	defer cancelR()
	got, err := fsm2.GetMusicianKnownLatest(readCtx, "Legacy")
	asrt.NoErr(err)
	asrt.Equal(got.Name, "Legacy")
}
