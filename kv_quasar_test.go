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
	"github.com/tehsphinx/quasar/transports"
)

func TestSingleKVCache(t *testing.T) {
	type test struct {
		name      string
		storeVals map[string]string
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: map[string]string{
				"key1": "val1",
				"key2": "val2",
				"key3": "val3",
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	cache, err := quasar.NewKVCache(ctxMain,
		quasar.WithBootstrap(true),
	)
	asrtMain.NoErr(err)

	err = cache.WaitReady(ctxMain)
	asrtMain.NoErr(err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctxMain, 2*time.Second)
			defer cancel()

			asrt := asrtMain.New(t)

			for k, v := range tt.storeVals {
				_, r := cache.Store(ctx, k, []byte(v))
				asrt.NoErr(r)
			}

			for k, v := range tt.storeVals {
				got, r := cache.LoadLocal(ctx, k)
				asrt.NoErr(r)

				asrt.Equal(got, []byte(v))
			}

			for k, v := range tt.storeVals {
				got, r := cache.Load(ctx, k)
				asrt.NoErr(r)

				asrt.Equal(got, []byte(v))
			}
		})
	}
}

func TestKVCacheClusterTCP(t *testing.T) {
	type test struct {
		name      string
		storeVals map[string]string
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: map[string]string{
				"key1": "val1",
				"key2": "val2",
				"key3": "val3",
			},
		},
	}

	ctxMain, cancelMain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelMain()

	asrtMain := is.New(t)

	cache1, err := quasar.NewKVCache(ctxMain,
		quasar.WithLocalID("cache1"),
		quasar.WithTCPTransport(":28233", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28233}),
		quasar.WithServers([]raft.Server{
			{ID: "cache1", Address: "localhost:28233"},
			{ID: "cache2", Address: "localhost:28234"},
			{ID: "cache3", Address: "localhost:28235"},
		}),
	)
	asrtMain.NoErr(err)

	cache2, err := quasar.NewKVCache(ctxMain,
		quasar.WithLocalID("cache2"),
		quasar.WithTCPTransport(":28234", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28234}),
	)
	asrtMain.NoErr(err)

	cache3, err := quasar.NewKVCache(ctxMain,
		quasar.WithLocalID("cache3"),
		quasar.WithTCPTransport(":28235", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28235}),
	)
	asrtMain.NoErr(err)

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("WAIT DONE")

	caches := []*quasar.KVCache{cache1, cache2, cache3}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			for i, cache := range caches {
				t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(ctxMain, 2*time.Second)
					defer cancel()

					asrtWrite := asrtMain.New(t)

					for k, v := range tt.storeVals {
						_, r := cache.Store(ctx, k+strconv.Itoa(i), []byte(v+strconv.Itoa(i)))
						asrtWrite.NoErr(r)
					}

					for j, readCache := range caches {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for k, v := range tt.storeVals {
								got, r := readCache.Load(ctx, k+strconv.Itoa(i))
								asrtRead.NoErr(r)

								asrtRead.Equal(got, []byte(v+strconv.Itoa(i)))
							}

							for k, v := range tt.storeVals {
								got, r := readCache.LoadLocal(ctx, k+strconv.Itoa(i))
								asrtRead.NoErr(r)

								asrtRead.Equal(got, []byte(v+strconv.Itoa(i)))
							}
						})
					}
				})
			}
		})
	}
}

func TestKVCacheClusterNATS(t *testing.T) {
	type test struct {
		name      string
		storeVals map[string]string
	}
	tests := []test{
		{
			name: "sub test",
			storeVals: map[string]string{
				"key1": "val1",
				"key2": "val2",
				"key3": "val3",
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

	transport1, err := transports.NewNATSTransport(ctxMain, nc1, "test_cache", "cache1")
	asrtMain.NoErr(err)
	transport2, err := transports.NewNATSTransport(ctxMain, nc2, "test_cache", "cache2")
	asrtMain.NoErr(err)
	transport3, err := transports.NewNATSTransport(ctxMain, nc3, "test_cache", "cache3")
	asrtMain.NoErr(err)

	cache1, err := quasar.NewKVCache(ctxMain,
		quasar.WithLocalID("cache1"),
		quasar.WithTransport(transport1),
		quasar.WithServers([]raft.Server{
			{ID: "cache1", Address: "cache1"},
			{ID: "cache2", Address: "cache2"},
			{ID: "cache3", Address: "cache3"},
		}),
	)
	asrtMain.NoErr(err)

	cache2, err := quasar.NewKVCache(ctxMain,
		quasar.WithTransport(transport2),
	)
	asrtMain.NoErr(err)

	cache3, err := quasar.NewKVCache(ctxMain,
		quasar.WithTransport(transport3),
	)
	asrtMain.NoErr(err)

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("WAIT DONE")

	caches := []*quasar.KVCache{cache1, cache2, cache3}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, cache := range caches {
				t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(ctxMain, 2*time.Second)
					defer cancel()

					asrtWrite := asrtMain.New(t)

					for k, v := range tt.storeVals {
						_, r := cache.Store(ctx, k+strconv.Itoa(i), []byte(v+strconv.Itoa(i)))
						asrtWrite.NoErr(r)
					}

					for j, readCache := range caches {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for k, v := range tt.storeVals {
								got, r := readCache.Load(ctx, k+strconv.Itoa(i))
								asrtRead.NoErr(r)

								asrtRead.Equal(got, []byte(v+strconv.Itoa(i)))
							}

							for k, v := range tt.storeVals {
								got, r := readCache.LoadLocal(ctx, k+strconv.Itoa(i))
								asrtRead.NoErr(r)

								asrtRead.Equal(got, []byte(v+strconv.Itoa(i)))
							}
						})
					}
				})
			}
		})
	}
}
