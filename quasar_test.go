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

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, "test_cache1", "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, "test_cache1", "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, "test_cache1", "cache3")
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

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, "test_cache1", "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, "test_cache1", "cache2")
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

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, "test_cache2", "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, "test_cache2", "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, "test_cache2", "cache3")
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

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, "test_cache", "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, "test_cache", "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, "test_cache", "cache3")
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
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()

	cache2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithDiscovery(discovery2),
	)
	asrtMain.NoErr(err)
	defer cache2.Shutdown()

	cache3, err := quasar.NewCache(ctxMain, fsm3,
		quasar.WithLocalID("cache3"),
		quasar.WithTransport(transport3),
		quasar.WithDiscovery(discovery3),
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

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, "test_cache", "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, "test_cache", "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, "test_cache", "cache3")
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
		quasar.WithSuffrage(raft.Nonvoter),
	)
	asrtMain.NoErr(err)
	defer cache1.Shutdown()

	cache2, err := quasar.NewCache(ctxMain, fsm2,
		quasar.WithLocalID("cache2"),
		quasar.WithTransport(transport2),
		quasar.WithDiscovery(discovery2),
		quasar.WithSuffrage(raft.Nonvoter),
	)
	asrtMain.NoErr(err)
	defer cache2.Shutdown()

	cache3, err := quasar.NewCache(ctxMain, fsm3,
		quasar.WithLocalID("cache3"),
		quasar.WithTransport(transport3),
		quasar.WithDiscovery(discovery3),
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

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, "test_cache", "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, "test_cache", "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, "test_cache", "cache3")
	asrtMain.NoErr(err)

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	fsm3 := exampleFSM.NewInMemoryFSM()

	discovery1 := discoveries.NewNATSDiscovery(nc1)

	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithDiscovery(discovery1),
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
			)
			asrtMain.NoErr(e)

			discovery3 := discoveries.NewNATSDiscovery(nc3)
			cache3, e := quasar.NewCache(ctx, fsm3,
				quasar.WithLocalID("cache3"),
				quasar.WithTransport(transport3),
				quasar.WithDiscovery(discovery3),
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
	nc3, err := nats.Connect("localhost:4222")
	asrtMain.NoErr(err)
	defer nc1.Close()
	defer nc2.Close()
	defer nc3.Close()

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, "test_cache", "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, "test_cache", "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, "test_cache", "cache3")
	asrtMain.NoErr(err)

	fsm1 := exampleFSM.NewInMemoryFSM()
	fsm2 := exampleFSM.NewInMemoryFSM()
	fsm3 := exampleFSM.NewInMemoryFSM()

	discovery1 := discoveries.NewNATSDiscovery(nc1)

	cache1, err := quasar.NewCache(ctxMain, fsm1,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithDiscovery(discovery1),
		quasar.WithSuffrage(raft.Nonvoter),
	)
	asrtMain.NoErr(err)

	for age := 0; age < 3; age++ {
		func() {
			ctx, cancel := context.WithTimeout(ctxMain, 5*time.Second)
			defer cancel()

			discovery2 := discoveries.NewNATSDiscovery(nc2)
			cache2, e := quasar.NewCache(ctx, fsm2,
				quasar.WithLocalID("cache2"),
				quasar.WithTransport(transport2),
				quasar.WithDiscovery(discovery2),
			)
			asrtMain.NoErr(e)

			discovery3 := discoveries.NewNATSDiscovery(nc3)
			cache3, e := quasar.NewCache(ctx, fsm3,
				quasar.WithLocalID("cache3"),
				quasar.WithTransport(transport3),
				quasar.WithDiscovery(discovery3),
				quasar.WithSuffrage(raft.Nonvoter),
			)
			asrtMain.NoErr(e)

			e = cache2.WaitReady(ctx)
			asrtMain.NoErr(e)
			e = cache3.WaitReady(ctx)
			asrtMain.NoErr(e)
			e = cache1.WaitReady(ctx)
			asrtMain.NoErr(e)
			fmt.Println("WAIT DONE")

			e = cache2.Reset(ctx)
			asrtMain.NoErr(e)

			e = cache2.WaitReady(ctx)
			asrtMain.NoErr(e)
			e = cache3.WaitReady(ctx)
			asrtMain.NoErr(e)
			e = cache1.WaitReady(ctx)
			asrtMain.NoErr(e)
			fmt.Println("after reset: WAIT DONE")

			fmt.Println("cache1 leader", cache1.GetLeader())
			fmt.Println(cache1.GetServerList())
			fmt.Println("cache2 leader", cache2.GetLeader())
			fmt.Println(cache2.GetServerList())
			fmt.Println("cache3 leader", cache3.GetLeader())
			fmt.Println(cache3.GetServerList())

			asrtMain.Equal(cache1.GetLeader().Suffrage, raft.Voter)
			asrtMain.Equal(cache2.GetLeader().Suffrage, raft.Voter)
			asrtMain.Equal(cache3.GetLeader().Suffrage, raft.Voter)

			fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2, fsm3}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					for writeIndex, fsm := range fsms {
						t.Run("write cache "+strconv.Itoa(writeIndex+1), func(t *testing.T) {
							asrtWrite := asrtMain.New(t)

							writeAge := age*3 + writeIndex

							for _, v := range tt.storeVals {
								v.Age = writeAge
								fmt.Println("writing", v.Name, v.Age, v.Instruments)
								r := fsm.SetMusician(ctx, v)
								asrtWrite.NoErr(r)
							}

							for j, readFSM := range fsms {
								t.Run("read cache "+strconv.Itoa(j+1), func(t *testing.T) {
									asrtRead := asrtWrite.New(t)

									for _, v := range tt.storeVals {
										got, r := readFSM.GetMusicianMaster(ctx, v.Name)
										asrtRead.NoErr(r)

										fmt.Println("reading", got.Name, got.Age, got.Instruments)

										v.Age = writeAge
										asrtRead.Equal(got, v)
									}

									for _, v := range tt.storeVals {
										got, r := readFSM.GetMusicianLocal(v.Name)
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
			e = cache3.Shutdown()
			asrtMain.NoErr(e)
			e = cache2.Shutdown()
			asrtMain.NoErr(e)
		}()
	}

	err = cache1.Shutdown()
	asrtMain.NoErr(err)
}
