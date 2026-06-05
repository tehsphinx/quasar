package quasar_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/matryer/is"
	"github.com/nats-io/nats.go"
	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/discoveries"
	"github.com/tehsphinx/quasar/examples/generic/exampleFSM"
	"github.com/tehsphinx/quasar/stores"
	"github.com/tehsphinx/quasar/transports"
)

func TestSingleCache(t *testing.T) {
	type test struct {
		name      string
		storeVals []exampleFSM.Musician
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: []exampleFSM.Musician{
				{
					Name:        "Roberto",
					Age:         43,
					Instruments: []string{"violin", "guitar"},
				},
				{
					Name:        "Angela",
					Age:         24,
					Instruments: []string{"piano", "flute"},
				},
				{
					Name:        "Eva",
					Age:         28,
					Instruments: []string{"ukulele", "saxophone", "bass guitar"},
				},
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	fsm := exampleFSM.NewInMemoryFSM()

	cache, err := quasar.NewCache(ctxMain, fsm,
		quasar.WithBootstrap(true),
	)
	asrtMain.NoErr(err)
	defer cache.Shutdown()

	err = cache.WaitReady(ctxMain)
	asrtMain.NoErr(err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctxMain, 2*time.Second)
			defer cancel()

			asrt := asrtMain.New(t)

			for _, v := range tt.storeVals {
				r := fsm.SetMusician(ctx, v)
				asrt.NoErr(r)
			}

			for _, v := range tt.storeVals {
				got, r := fsm.GetMusicianLocal(v.Name)
				asrt.NoErr(r)

				asrt.Equal(got, v)
			}

			for _, v := range tt.storeVals {
				got, r := fsm.GetMusicianMaster(ctx, v.Name)
				asrt.NoErr(r)

				asrt.Equal(got, v)
			}

			for _, v := range tt.storeVals {
				got, r := fsm.GetMusicianKnownLatest(ctx, v.Name)
				asrt.NoErr(r)

				asrt.Equal(got, v)
			}
		})
	}
}

func TestCacheRemoveAndShutdown(t *testing.T) {
	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrt := is.New(t)

	addr1, transport1 := transports.NewInmemTransport("")
	addr2, transport2 := transports.NewInmemTransport("")
	addr3, transport3 := transports.NewInmemTransport("")

	transport1.Connect(addr2, transport2)
	transport1.Connect(addr3, transport3)
	transport2.Connect(addr1, transport1)
	transport2.Connect(addr3, transport3)
	transport3.Connect(addr1, transport1)
	transport3.Connect(addr2, transport2)

	servers := []raft.Server{
		{ID: "cache1", Address: addr1, Suffrage: raft.Voter},
		{ID: "cache2", Address: addr2, Suffrage: raft.Voter},
		{ID: "cache3", Address: addr3, Suffrage: raft.Voter},
	}

	cache1, err := quasar.NewCache(ctxMain, exampleFSM.NewInMemoryFSM(),
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)
	defer cache1.Shutdown()

	cache2, err := quasar.NewCache(ctxMain, exampleFSM.NewInMemoryFSM(),
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)

	cache3, err := quasar.NewCache(ctxMain, exampleFSM.NewInMemoryFSM(),
		quasar.WithLocalID("cache3"),
		quasar.WithTransport(transport3),
		quasar.WithServers(servers),
	)
	asrt.NoErr(err)
	defer cache3.Shutdown()

	err = cache1.WaitReady(ctxMain)
	asrt.NoErr(err)
	err = cache2.WaitReady(ctxMain)
	asrt.NoErr(err)
	err = cache3.WaitReady(ctxMain)
	asrt.NoErr(err)

	err = cache2.RemoveAndShutdown(ctxMain)
	asrt.NoErr(err)

	err = waitForCondition(ctxMain, func() error {
		srvs, waitErr := cache1.GetServerList()
		if waitErr != nil {
			return waitErr
		}
		if len(srvs) != 2 {
			return fmt.Errorf("expected 2 servers after removal, got %d", len(srvs))
		}

		for _, server := range srvs {
			if server.ID == "cache2" {
				return fmt.Errorf("cache2 should have been removed from raft")
			}
		}
		return nil
	})
	asrt.NoErr(err)
}

func TestCacheClusterTCP(t *testing.T) {
	type test struct {
		name      string
		storeVals []exampleFSM.Musician
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: []exampleFSM.Musician{
				{
					Name:        "Roberto",
					Age:         43,
					Instruments: []string{"violin", "guitar"},
				},
				{
					Name:        "Angela",
					Age:         24,
					Instruments: []string{"piano", "flute"},
				},
				{
					Name:        "Eva",
					Age:         28,
					Instruments: []string{"ukulele", "saxophone", "bass guitar"},
				},
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	fsm3 := exampleFSM.NewInMemoryFSM()

	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTCPTransport(":28230", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28230}),
		quasar.WithServers([]raft.Server{
			{ID: "cache1", Address: "localhost:28230"},
			{ID: "cache2", Address: "localhost:28231"},
			{ID: "cache3", Address: "localhost:28232"},
		}),
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()

	cache2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTCPTransport(":28231", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28231}),
	)
	asrtMain.NoErr(err)
	defer cache2.Shutdown()

	cache3, err := quasar.NewCache(ctxMain, fsm3,
		quasar.WithLocalID("cache3"),
		quasar.WithTCPTransport(":28232", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28232}),
	)
	asrtMain.NoErr(err)
	defer cache3.Shutdown()

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("WAIT DONE")

	fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2, fsm3}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, fsm := range fsms {
				t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(ctxMain, 2*time.Second)
					defer cancel()

					asrtWrite := asrtMain.New(t)

					for _, v := range tt.storeVals {
						r := fsm.SetMusician(ctx, v)
						asrtWrite.NoErr(r)
					}

					for j, readFSM := range fsms {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianMaster(ctx, v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianLocal(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianKnownLatest(ctx, v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}
						})
					}
				})
			}
		})
	}
}

func TestCacheClusterNATS(t *testing.T) {
	type test struct {
		name      string
		storeVals []exampleFSM.Musician
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: []exampleFSM.Musician{
				{
					Name:        "Roberto",
					Age:         43,
					Instruments: []string{"violin", "guitar"},
				},
				{
					Name:        "Angela",
					Age:         24,
					Instruments: []string{"piano", "flute"},
				},
				{
					Name:        "Eva",
					Age:         28,
					Instruments: []string{"ukulele", "saxophone", "bass guitar"},
				},
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	nc1, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc2, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc3, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()
	defer nc3.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, t.Name(), "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, t.Name(), "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, t.Name(), "cache3")
	asrtMain.NoErr(err)

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	fsm3 := exampleFSM.NewInMemoryFSM()

	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithServers([]raft.Server{
			{ID: "cache1", Address: "cache1"},
			{ID: "cache2", Address: "cache2"},
			{ID: "cache3", Address: "cache3"},
		}),
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()

	cache2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithServers([]raft.Server{
			{ID: "cache1", Address: "cache1"},
			{ID: "cache2", Address: "cache2"},
			{ID: "cache3", Address: "cache3"},
		}),
	)
	asrtMain.NoErr(err)
	defer cache2.Shutdown()

	cache3, err := quasar.NewCache(ctxMain, fsm3,
		quasar.WithLocalID("cache3"),
		quasar.WithTransport(transport3),
		quasar.WithServers([]raft.Server{
			{ID: "cache1", Address: "cache1"},
			{ID: "cache2", Address: "cache2"},
			{ID: "cache3", Address: "cache3"},
		}),
	)
	asrtMain.NoErr(err)
	defer cache3.Shutdown()

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("WAIT DONE")

	fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2, fsm3}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, fsm := range fsms {
				t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(ctxMain, 2*time.Second)
					defer cancel()

					asrtWrite := asrtMain.New(t)

					for _, v := range tt.storeVals {
						r := fsm.SetMusician(ctx, v)
						asrtWrite.NoErr(r)
					}

					for j, readFSM := range fsms {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianMaster(ctx, v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianLocal(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianKnownLatest(ctx, v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}
						})
					}
				})
			}
		})
	}
}

type testPersistStore struct {
	store func(data stores.StoreData) error
}

func (s testPersistStore) Store(data stores.StoreData) error {
	return s.store(data)
}

func TestNatsFail(t *testing.T) {
	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	nc1, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc2, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, t.Name(), "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, t.Name(), "cache2")
	asrtMain.NoErr(err)

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()

	persistStore := testPersistStore{store: func(data stores.StoreData) error {
		return io.ErrNoProgress
	}}

	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithServers([]raft.Server{
			{ID: "cache1", Address: "cache1"},
			{ID: "cache2", Address: "cache2"},
			{ID: "cache3", Address: "cache3"},
		}),
		quasar.WithPersistentStore(persistStore),
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()

	cache2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithServers([]raft.Server{
			{ID: "cache1", Address: "cache1"},
			{ID: "cache2", Address: "cache2"},
			{ID: "cache3", Address: "cache3"},
		}),
		quasar.WithPersistentStore(persistStore),
	)
	asrtMain.NoErr(err)
	defer cache2.Shutdown()

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("WAIT DONE")

	fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2}

	t.Run("error propagation", func(t *testing.T) {
		for i, fsm := range fsms {
			t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctxMain, 2*time.Second)
				defer cancel()

				asrtWrite := asrtMain.New(t)

				r := fsm.SetMusician(ctx, exampleFSM.Musician{
					Name:        "Alex",
					Age:         23,
					Instruments: []string{"flute"},
				})
				asrtWrite.True(r != nil)
			})
		}
	})
}

func TestInstallSnapshot(t *testing.T) {
	type test struct {
		name      string
		storeVals []exampleFSM.Musician
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: []exampleFSM.Musician{
				{
					Name:        "Roberto",
					Age:         43,
					Instruments: []string{"violin", "guitar"},
				},
				{
					Name:        "Angela",
					Age:         24,
					Instruments: []string{"piano", "flute"},
				},
				{
					Name:        "Eva",
					Age:         28,
					Instruments: []string{"ukulele", "saxophone", "bass guitar"},
				},
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	nc1, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc2, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc3, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()
	defer nc3.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, t.Name(), "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, t.Name(), "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, t.Name(), "cache3")
	asrtMain.NoErr(err)

	raftCfg := raft.DefaultConfig()
	raftCfg.TrailingLogs = 5

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctxMain, 10*time.Second)
			defer cancel()

			asrt := asrtMain.New(t)
			fsm1 := exampleFSM.NewInMemoryFSM()
			fsm2 := exampleFSM.NewInMemoryFSM()
			fsm3 := exampleFSM.NewInMemoryFSM()

			cache1, err := quasar.NewCache(ctx, fsm1,
				quasar.WithLocalID("cache1"),
				quasar.WithRaftConfig(raftCfg),
				quasar.WithTransport(transport1),
				quasar.WithServers([]raft.Server{
					{ID: "cache1", Address: "cache1"},
					{ID: "cache2", Address: "cache2"},
					{ID: "cache3", Address: "cache3"},
				}),
			)
			asrt.NoErr(err)
			defer cache1.Shutdown()

			cache2, err := quasar.NewCache(ctx, fsm2,
				quasar.WithRaftConfig(raftCfg),
				quasar.WithTransport(transport2),
			)
			asrt.NoErr(err)
			defer cache2.Shutdown()

			err = cache1.WaitReady(ctx)
			asrt.NoErr(err)
			fmt.Println("WAIT DONE")

			t.Run("write cache", func(t *testing.T) {
				asrtWrite := asrt.New(t)

				for i := 0; i < 100; i++ {
					for _, v := range tt.storeVals {
						r := fsm1.SetMusician(ctx, v)
						asrtWrite.NoErr(r)
					}
				}
			})

			err = cache1.ForceSnapshot()
			asrt.NoErr(err)

			cache3, err := quasar.NewCache(ctx, fsm3,
				quasar.WithRaftConfig(raftCfg),
				quasar.WithTransport(transport3),
			)
			asrt.NoErr(err)
			defer cache3.Shutdown()

			err = cache3.WaitReady(ctx)
			asrt.NoErr(err)
			fmt.Println("3rd cache is up")

			t.Run("read cache", func(t *testing.T) {
				asrtRead := asrt.New(t)

				for _, v := range tt.storeVals {
					got, r := fsm3.GetMusicianMaster(ctx, v.Name)
					fmt.Println("getting values", got, r)
					asrtRead.NoErr(r)

					asrtRead.Equal(got, v)
				}
			})
		})
	}
}

func TestCacheClusterNATSDiscovery(t *testing.T) {
	type test struct {
		name      string
		storeVals []exampleFSM.Musician
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: []exampleFSM.Musician{
				{
					Name:        "Roberto",
					Age:         43,
					Instruments: []string{"violin", "guitar"},
				},
				{
					Name:        "Angela",
					Age:         24,
					Instruments: []string{"piano", "flute"},
				},
				{
					Name:        "Eva",
					Age:         28,
					Instruments: []string{"ukulele", "saxophone", "bass guitar"},
				},
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	nc1, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc2, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc3, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()
	defer nc3.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, t.Name(), "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, t.Name(), "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, t.Name(), "cache3")
	asrtMain.NoErr(err)

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	fsm3 := exampleFSM.NewInMemoryFSM()

	discovery1 := discoveries.NewNATSDiscovery(nc1)
	discovery2 := discoveries.NewNATSDiscovery(nc2)
	discovery3 := discoveries.NewNATSDiscovery(nc3)

	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithDiscovery(discovery1),
		quasar.WithName(t.Name()),
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()

	cache2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithDiscovery(discovery2),
		quasar.WithName(t.Name()),
	)
	asrtMain.NoErr(err)
	defer cache2.Shutdown()

	cache3, err := quasar.NewCache(ctxMain, fsm3,
		quasar.WithLocalID("cache3"),
		quasar.WithTransport(transport3),
		quasar.WithDiscovery(discovery3),
		quasar.WithName(t.Name()),
	)
	asrtMain.NoErr(err)
	defer cache3.Shutdown()

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	err = cache2.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	err = cache3.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("WAIT DONE")

	fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2, fsm3}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctxMain, 2*time.Second)
			defer cancel()

			for i, fsm := range fsms {
				t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
					asrtWrite := asrtMain.New(t)

					for _, v := range tt.storeVals {
						r := fsm.SetMusician(ctx, v)
						asrtWrite.NoErr(r)
					}

					for j, readFSM := range fsms {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianMaster(ctx, v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianLocal(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianKnownLatest(ctx, v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}
						})
					}
				})
			}
		})
	}
}

func TestCacheClusterNATSDiscoveryNonVoter(t *testing.T) {
	type test struct {
		name      string
		storeVals []exampleFSM.Musician
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: []exampleFSM.Musician{
				{
					Name:        "Roberto",
					Age:         43,
					Instruments: []string{"violin", "guitar"},
				},
				{
					Name:        "Angela",
					Age:         24,
					Instruments: []string{"piano", "flute"},
				},
				{
					Name:        "Eva",
					Age:         28,
					Instruments: []string{"ukulele", "saxophone", "bass guitar"},
				},
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	nc1, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc2, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc3, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()
	defer nc3.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, t.Name(), "cache1")
	asrtMain.NoErr(err)
	fsm1 := exampleFSM.NewInMemoryFSM()
	discovery1 := discoveries.NewNATSDiscovery(nc1)
	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithDiscovery(discovery1),
		quasar.WithName(t.Name()),
		quasar.WithSuffrage(raft.Nonvoter),
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()

	transport2, err := transports.NewNATSTransport(ctxMain, nc2, t.Name(), "cache2")
	asrtMain.NoErr(err)
	fsm2 := exampleFSM.NewInMemoryFSM()
	discovery2 := discoveries.NewNATSDiscovery(nc2)
	cache2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithDiscovery(discovery2),
		quasar.WithName(t.Name()),
		quasar.WithSuffrage(raft.Nonvoter),
	)
	asrtMain.NoErr(err)
	defer cache2.Shutdown()

	transport3, err := transports.NewNATSTransport(ctxMain, nc3, t.Name(), "cache3")
	asrtMain.NoErr(err)
	fsm3 := exampleFSM.NewInMemoryFSM()
	discovery3 := discoveries.NewNATSDiscovery(nc3)
	cache3, err := quasar.NewCache(ctxMain, fsm3,
		quasar.WithLocalID("cache3"),
		quasar.WithTransport(transport3),
		quasar.WithDiscovery(discovery3),
		quasar.WithName(t.Name()),
	)
	asrtMain.NoErr(err)
	defer cache3.Shutdown()

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	err = cache2.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	err = cache3.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("WAIT DONE")

	asrtMain.Equal(cache1.GetLeader().ID, raft.ServerID("cache3"))

	asrtMain.Equal(cache1.GetLeader().Suffrage, raft.Voter)
	asrtMain.Equal(cache2.GetLeader().Suffrage, raft.Voter)
	asrtMain.Equal(cache3.GetLeader().Suffrage, raft.Voter)

	fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2, fsm3}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctxMain, 2*time.Second)
			defer cancel()

			for i, fsm := range fsms {
				t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
					asrtWrite := asrtMain.New(t)

					for _, v := range tt.storeVals {
						r := fsm.SetMusician(ctx, v)
						asrtWrite.NoErr(r)
					}

					for j, readFSM := range fsms {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianMaster(ctx, v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianLocal(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianKnownLatest(ctx, v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}
						})
					}
				})
			}
		})
	}
}

func TestCacheDiscoveryRestart(t *testing.T) {
	// Enable to verify if cache shuts down properly. Will fail with other tests running as well.
	// defer func() {
	// 	time.Sleep(100 * time.Millisecond)
	// 	goleak.VerifyNone(t)
	// }()

	type test struct {
		name      string
		storeVals []exampleFSM.Musician
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: []exampleFSM.Musician{
				{
					Name:        "Roberto",
					Age:         43,
					Instruments: []string{"violin", "guitar"},
				},
				{
					Name:        "Angela",
					Age:         24,
					Instruments: []string{"piano", "flute"},
				},
				{
					Name:        "Eva",
					Age:         28,
					Instruments: []string{"ukulele", "saxophone", "bass guitar"},
				},
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	nc1, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc2, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc3, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()
	defer nc3.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, t.Name(), "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, t.Name(), "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, t.Name(), "cache3")
	asrtMain.NoErr(err)

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	fsm3 := exampleFSM.NewInMemoryFSM()

	discovery1 := discoveries.NewNATSDiscovery(nc1)

	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithDiscovery(discovery1),
		quasar.WithName(t.Name()),
	)
	asrtMain.NoErr(err)

	for age := 0; age < 10; age++ {
		func() {
			ctx, cancel := context.WithTimeout(ctxMain, 5*time.Second)
			defer cancel()

			discovery2 := discoveries.NewNATSDiscovery(nc2)
			cache2, e := quasar.NewCache(ctx, fsm2,
				quasar.WithLocalID("cache2"),
				quasar.WithTransport(transport2),
				quasar.WithDiscovery(discovery2),
				quasar.WithName(t.Name()),
			)
			asrtMain.NoErr(e)

			discovery3 := discoveries.NewNATSDiscovery(nc3)
			cache3, e := quasar.NewCache(ctx, fsm3,
				quasar.WithLocalID("cache3"),
				quasar.WithTransport(transport3),
				quasar.WithDiscovery(discovery3),
				quasar.WithName(t.Name()),
			)
			asrtMain.NoErr(e)

			e = cache1.WaitReady(ctx)
			asrtMain.NoErr(e)
			e = cache2.WaitReady(ctx)
			asrtMain.NoErr(e)
			e = cache3.WaitReady(ctx)
			asrtMain.NoErr(e)
			fmt.Println("WAIT DONE")

			fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2, fsm3}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					for writeIndex, fsm := range fsms {
						t.Run("write cache "+strconv.Itoa(writeIndex), func(t *testing.T) {
							asrtWrite := asrtMain.New(t)

							for _, v := range tt.storeVals {
								v.Age = age*3 + writeIndex
								r := fsm.SetMusician(ctx, v)
								asrtWrite.NoErr(r)
							}

							for j, readFSM := range fsms {
								t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
									asrtRead := asrtWrite.New(t)

									for _, v := range tt.storeVals {
										got, r := readFSM.GetMusicianMaster(ctx, v.Name)
										asrtRead.NoErr(r)

										v.Age = age*3 + writeIndex
										asrtRead.Equal(got, v)
									}

									for _, v := range tt.storeVals {
										got, r := readFSM.GetMusicianLocal(v.Name)
										asrtRead.NoErr(r)

										v.Age = age*3 + writeIndex
										asrtRead.Equal(got, v)
									}

									for _, v := range tt.storeVals {
										got, r := readFSM.GetMusicianKnownLatest(ctx, v.Name)
										asrtRead.NoErr(r)

										v.Age = age*3 + writeIndex
										asrtRead.Equal(got, v)
									}
								})
							}
						})
					}
				})
			}
			e = cache3.Shutdown()
			asrtMain.NoErr(e)
			e = cache2.Shutdown()
			asrtMain.NoErr(e)
		}()
	}

	err = cache1.Shutdown()
	asrtMain.NoErr(err)
}

func TestCacheDiscoveryRestartNonVoter(t *testing.T) {
	// Enable to verify if cache shuts down properly. Will fail with other tests running as well.
	// defer func() {
	// 	time.Sleep(100 * time.Millisecond)
	// 	goleak.VerifyNone(t)
	// }()

	type test struct {
		name      string
		storeVals []exampleFSM.Musician
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: []exampleFSM.Musician{
				{
					Name:        "Roberto",
					Age:         43,
					Instruments: []string{"violin", "guitar"},
				},
				{
					Name:        "Angela",
					Age:         24,
					Instruments: []string{"piano", "flute"},
				},
				{
					Name:        "Eva",
					Age:         28,
					Instruments: []string{"ukulele", "saxophone", "bass guitar"},
				},
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	nc1, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc2, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, t.Name(), "cache1")
	asrtMain.NoErr(err)

	fsm1 := exampleFSM.NewInMemoryFSM()
	discovery1 := discoveries.NewNATSDiscovery(nc1)

	// Stable voter / leader for the whole test. Only the nonvoter restarts each
	// iteration, so the cluster stays healthy (no forked history) and Reset is
	// always issued against a healthy leader+quorum — its documented precondition
	// after the RT-12994 redesign. (The previous version recreated the *voter*
	// fresh every iteration, manufacturing a fork that the old out-of-band Reset
	// repaired as a side effect; fork repair now belongs to quorum recovery /
	// instance-ID adoption, covered by the quorum-loss suite.)
	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithDiscovery(discovery1),
		quasar.WithName(t.Name()),
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()
	asrtMain.NoErr(cache1.WaitReady(ctxMain))

	for age := 0; age < 3; age++ {
		func() {
			ctx, cancel := context.WithTimeout(ctxMain, 15*time.Second)
			defer cancel()

			// A fresh nonvoter joins via discovery each iteration (the "restart").
			// It rejoins the stable leader cleanly and catches up via replication.
			transport2, e := transports.NewNATSTransport(ctx, nc2, t.Name(), "cache2")
			asrtMain.NoErr(e)
			fsm2 := exampleFSM.NewInMemoryFSM()
			discovery2 := discoveries.NewNATSDiscovery(nc2)
			cache2, e := quasar.NewCache(ctx, fsm2,
				quasar.WithLocalID("cache2"),
				quasar.WithTransport(transport2),
				quasar.WithDiscovery(discovery2),
				quasar.WithName(t.Name()),
				quasar.WithSuffrage(raft.Nonvoter),
			)
			asrtMain.NoErr(e)

			asrtMain.NoErr(cache2.WaitReady(ctx))
			asrtMain.NoErr(cache1.WaitReady(ctx))
			asrtMain.Equal(cache1.GetLeader().ID, raft.ServerID("cache1"))

			fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2}

			// Reset, forwarded from the nonvoter to the leader, is a replicated
			// raft-log command: it clears content on every member. From iteration
			// 1 on, the rejoined nonvoter first caught up to the previous
			// iteration's entries, which the reset must then remove everywhere.
			asrtMain.NoErr(cache2.Reset(ctx))
			for fi, fsm := range fsms {
				readFSM, idx := fsm, fi
				asrtMain.NoErr(waitForCondition(ctx, func() error {
					for _, tt := range tests {
						for _, v := range tt.storeVals {
							if _, r := readFSM.GetMusicianLocal(v.Name); r == nil {
								return fmt.Errorf("cache%d still has %s after reset", idx+1, v.Name)
							}
						}
					}
					return nil
				}))
			}

			// New writes from every node replicate to all members after the reset.
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					for writeIndex, fsm := range fsms {
						t.Run("write cache "+strconv.Itoa(writeIndex+1), func(t *testing.T) {
							asrtWrite := asrtMain.New(t)

							writeAge := age*2 + writeIndex

							for _, v := range tt.storeVals {
								v.Age = writeAge
								r := fsm.SetMusician(ctx, v)
								asrtWrite.NoErr(r)
							}

							for j, readFSM := range fsms {
								t.Run("read cache "+strconv.Itoa(j+1), func(t *testing.T) {
									asrtRead := asrtWrite.New(t)

									for _, v := range tt.storeVals {
										got, r := readFSM.GetMusicianMaster(ctx, v.Name)
										asrtRead.NoErr(r)

										v.Age = writeAge
										asrtRead.Equal(got, v)
									}

									for _, v := range tt.storeVals {
										got, r := readFSM.GetMusicianKnownLatest(ctx, v.Name)
										asrtRead.NoErr(r)

										v.Age = writeAge
										asrtRead.Equal(got, v)
									}
								})
							}
						})
					}
				})
			}

			asrtMain.NoErr(cache2.Shutdown())
		}()
	}
}

func TestVoterNodeRestart(t *testing.T) {
	ctxMain, cancelMain := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	nc1, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc2, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc3, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()
	defer nc3.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, t.Name(), "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, t.Name(), "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, t.Name(), "cache3")
	asrtMain.NoErr(err)

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	fsm3 := exampleFSM.NewInMemoryFSM()

	discovery1 := discoveries.NewNATSDiscovery(nc1)
	discovery2 := discoveries.NewNATSDiscovery(nc2)
	discovery3 := discoveries.NewNATSDiscovery(nc3)

	// Create cache1 as voter (leader)
	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithDiscovery(discovery1),
		quasar.WithName(t.Name()),
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()

	// Create cache2 and cache3 as non-voters
	cache2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithDiscovery(discovery2),
		quasar.WithName(t.Name()),
		quasar.WithSuffrage(raft.Nonvoter),
	)
	asrtMain.NoErr(err)
	defer cache2.Shutdown()

	cache3, err := quasar.NewCache(ctxMain, fsm3,
		quasar.WithLocalID("cache3"),
		quasar.WithTransport(transport3),
		quasar.WithDiscovery(discovery3),
		quasar.WithName(t.Name()),
		quasar.WithSuffrage(raft.Nonvoter),
	)
	asrtMain.NoErr(err)
	defer cache3.Shutdown()

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	err = cache2.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	err = cache3.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("All caches ready")

	// Verify cache1 is the leader
	asrtMain.Equal(cache1.GetLeader().ID, raft.ServerID("cache1"))

	// Add 50 entries from different nodes
	ctx, cancel := context.WithTimeout(ctxMain, 10*time.Second)
	defer cancel()

	fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2, fsm3}
	for i := 0; i < 50; i++ {
		fsm := fsms[i%3]
		musician := exampleFSM.Musician{
			Name:        fmt.Sprintf("Musician%d", i),
			Age:         20 + i,
			Instruments: []string{fmt.Sprintf("instrument%d", i)},
		}
		err = fsm.SetMusician(ctx, musician)
		asrtMain.NoErr(err)
	}
	fmt.Println("Added 50 entries")

	// Verify all entries are present on all nodes
	for i := 0; i < 50; i++ {
		for _, fsm := range fsms {
			got, err := fsm.GetMusicianMaster(ctx, fmt.Sprintf("Musician%d", i))
			asrtMain.NoErr(err)
			asrtMain.Equal(got.Name, fmt.Sprintf("Musician%d", i))
			asrtMain.Equal(got.Age, 20+i)
		}
	}
	fmt.Println("Verified all entries on all nodes")

	// RT-12994: the redesigned Reset is a replicated raft-log command issued
	// against a HEALTHY cluster (leader + quorum). It clears content on every
	// member and the cluster keeps serving afterwards.
	//
	// The previous version of this test restarted the voter with a fresh FSM to
	// manufacture a forked history and relied on Reset to repair it. Fork repair
	// is no longer Reset's job by design — it belongs to quorum recovery /
	// instance-ID adoption (covered by the quorum-loss suite), so the reset is
	// now exercised on the healthy cluster it is meant for.
	err = cache1.Reset(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("Cache reset complete")

	ctx2, cancel2 := context.WithTimeout(ctxMain, 15*time.Second)
	defer cancel2()

	// The reset replicates to every node; the pre-reset entries must be gone.
	for fi, fsm := range fsms {
		readFSM := fsm
		idx := fi
		e := waitForCondition(ctx2, func() error {
			for i := 0; i < 50; i++ {
				if _, r := readFSM.GetMusicianLocal(fmt.Sprintf("Musician%d", i)); r == nil {
					return fmt.Errorf("cache%d still has Musician%d after reset", idx+1, i)
				}
			}
			return nil
		})
		asrtMain.NoErr(e)
	}
	fmt.Println("Verified reset cleared all nodes")

	// The cluster keeps accepting writes after the reset; new entries replicate
	// to every node and remain readable.
	for i := 50; i < 60; i++ {
		fsm := fsms[i%3]
		musician := exampleFSM.Musician{
			Name:        fmt.Sprintf("Musician%d", i),
			Age:         20 + i,
			Instruments: []string{fmt.Sprintf("instrument%d", i)},
		}
		err = fsm.SetMusician(ctx2, musician)
		asrtMain.NoErr(err)
	}
	fmt.Println("Added 10 new entries after reset")

	for i := 50; i < 60; i++ {
		for _, fsm := range fsms {
			got, err := fsm.GetMusicianMaster(ctx2, fmt.Sprintf("Musician%d", i))
			asrtMain.NoErr(err)
			asrtMain.Equal(got.Name, fmt.Sprintf("Musician%d", i))
			asrtMain.Equal(got.Age, 20+i)
		}
	}
	fmt.Println("Verified all new entries on all nodes after reset")
}

func TestCacheDiscoveryAutoPrune(t *testing.T) {
	ctxMain, cancelMain := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	nc1, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc2, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	nc3, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()
	defer nc3.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, t.Name(), "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, t.Name(), "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, t.Name(), "cache3")
	asrtMain.NoErr(err)

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	fsm3 := exampleFSM.NewInMemoryFSM()

	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithDiscovery(discoveries.NewNATSDiscovery(nc1)),
		quasar.WithName(t.Name()),
		quasar.WithAutoPrune(6*time.Second),
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()

	cache2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithDiscovery(discoveries.NewNATSDiscovery(nc2)),
		quasar.WithName(t.Name()),
		quasar.WithAutoPrune(6*time.Second),
	)
	asrtMain.NoErr(err)
	defer cache2.Shutdown()

	cache3, err := quasar.NewCache(ctxMain, fsm3,
		quasar.WithLocalID("cache3"),
		quasar.WithTransport(transport3),
		quasar.WithDiscovery(discoveries.NewNATSDiscovery(nc3)),
		quasar.WithName(t.Name()),
		quasar.WithAutoPrune(6*time.Second),
	)
	asrtMain.NoErr(err)
	defer cache3.Shutdown()

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	err = cache2.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	err = cache3.WaitReady(ctxMain)
	asrtMain.NoErr(err)

	servers, err := cache1.GetServerList()
	asrtMain.NoErr(err)
	asrtMain.Equal(len(servers), 3)

	err = cache2.Shutdown()
	asrtMain.NoErr(err)

	err = waitForCondition(ctxMain, func() error {
		srvs, waitErr := cache1.GetServerList()
		if waitErr != nil {
			return waitErr
		}
		if len(srvs) != 2 {
			return fmt.Errorf("expected 2 servers after pruning, got %d", len(srvs))
		}

		for _, server := range srvs {
			if server.ID == "cache2" {
				return fmt.Errorf("cache2 should have been pruned")
			}
		}
		return nil
	})
	asrtMain.NoErr(err)
}

func waitForCondition(ctx context.Context, fn func() error) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	for {
		if err := fn(); err == nil {
			return nil
		} else {
			fmt.Printf("waiting for condition: %s\n", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
