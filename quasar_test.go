package quasar_test

import (
	"context"
	"fmt"
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
			asrt := asrtMain.New(t)

			for _, v := range tt.storeVals {
				r := fsm.SetMusician(v)
				asrt.NoErr(r)
			}

			for _, v := range tt.storeVals {
				got, r := fsm.GetMusicianLocal(v.Name)
				asrt.NoErr(r)

				asrt.Equal(got, v)
			}

			for _, v := range tt.storeVals {
				got, r := fsm.GetMusicianMaster(v.Name)
				asrt.NoErr(r)

				asrt.Equal(got, v)
			}

			for _, v := range tt.storeVals {
				got, r := fsm.GetMusicianKnownLatest(v.Name)
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
					asrtWrite := asrtMain.New(t)

					for _, v := range tt.storeVals {
						r := fsm.SetMusician(v)
						asrtWrite.NoErr(r)
					}

					for j, readFSM := range fsms {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianMaster(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianLocal(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianKnownLatest(v.Name)
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
					asrtWrite := asrtMain.New(t)

					for _, v := range tt.storeVals {
						r := fsm.SetMusician(v)
						asrtWrite.NoErr(r)
					}

					for j, readFSM := range fsms {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianMaster(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianLocal(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianKnownLatest(v.Name)
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
			asrt := asrtMain.New(t)
			fsm1 := exampleFSM.NewInMemoryFSM()
			fsm2 := exampleFSM.NewInMemoryFSM()
			fsm3 := exampleFSM.NewInMemoryFSM()

			cache1, err := quasar.NewCache(ctxMain, fsm1,
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

			cache2, err := quasar.NewCache(ctxMain, fsm2,
				quasar.WithRaftConfig(raftCfg),
				quasar.WithTransport(transport2),
			)
			asrt.NoErr(err)
			defer cache2.Shutdown()

			err = cache1.WaitReady(ctxMain)
			asrt.NoErr(err)
			fmt.Println("WAIT DONE")

			t.Run("write cache", func(t *testing.T) {
				asrtWrite := asrt.New(t)

				for i := 0; i < 100; i++ {
					for _, v := range tt.storeVals {
						r := fsm1.SetMusician(v)
						asrtWrite.NoErr(r)
					}
				}
			})

			err = cache1.ForceSnapshot()
			asrt.NoErr(err)

			cache3, err := quasar.NewCache(ctxMain, fsm3,
				quasar.WithRaftConfig(raftCfg),
				quasar.WithTransport(transport3),
			)
			asrt.NoErr(err)
			defer cache3.Shutdown()

			err = cache3.WaitReady(ctxMain)
			asrt.NoErr(err)
			fmt.Println("3rd cache is up")

			t.Run("read cache", func(t *testing.T) {
				asrtRead := asrt.New(t)

				for _, v := range tt.storeVals {
					got, r := fsm3.GetMusicianMaster(v.Name)
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
			for i, fsm := range fsms {
				t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
					asrtWrite := asrtMain.New(t)

					for _, v := range tt.storeVals {
						r := fsm.SetMusician(v)
						asrtWrite.NoErr(r)
					}

					for j, readFSM := range fsms {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianMaster(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianLocal(v.Name)
								asrtRead.NoErr(r)

								asrtRead.Equal(got, v)
							}

							for _, v := range tt.storeVals {
								got, r := readFSM.GetMusicianKnownLatest(v.Name)
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

	r := cache1.WaitReady(ctxMain)
	asrtMain.NoErr(r)

	for age := 0; age < 20; age++ {
		cache2, e := quasar.NewCache(ctxMain, fsm2,
			quasar.WithLocalID("cache2"),
			quasar.WithTransport(transport2),
			quasar.WithDiscovery(discovery2),
		)
		asrtMain.NoErr(e)

		cache3, e := quasar.NewCache(ctxMain, fsm3,
			quasar.WithTransport(transport3),
			quasar.WithDiscovery(discovery3),
		)
		asrtMain.NoErr(e)

		r = cache2.WaitReady(ctxMain)
		asrtMain.NoErr(r)
		e = cache3.WaitReady(ctxMain)
		asrtMain.NoErr(e)
		fmt.Println("WAIT DONE")

		fsms := []*exampleFSM.InMemoryFSM{fsm1, fsm2, fsm3}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				for i, fsm := range fsms {
					t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
						asrtWrite := asrtMain.New(t)

						for _, v := range tt.storeVals {
							v.Age = age
							r := fsm.SetMusician(v)
							asrtWrite.NoErr(r)
						}

						for j, readFSM := range fsms {
							t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
								asrtRead := asrtWrite.New(t)

								for _, v := range tt.storeVals {
									got, r := readFSM.GetMusicianMaster(v.Name)
									asrtRead.NoErr(r)

									v.Age = age
									asrtRead.Equal(got, v)
								}

								for _, v := range tt.storeVals {
									got, r := readFSM.GetMusicianLocal(v.Name)
									asrtRead.NoErr(r)

									v.Age = age
									asrtRead.Equal(got, v)
								}

								for _, v := range tt.storeVals {
									got, r := readFSM.GetMusicianKnownLatest(v.Name)
									asrtRead.NoErr(r)

									v.Age = age
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
	}

	err = cache1.Shutdown()
	asrtMain.NoErr(err)
}
