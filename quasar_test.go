package quasar

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/matryer/is"
)

func TestSingleCache(t *testing.T) {
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

	cache, err := NewCache(ctxMain,
		WithBootstrap(true),
	)
	asrtMain.NoErr(err)

	err = cache.WaitReady(ctxMain)
	asrtMain.NoErr(err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			asrt := asrtMain.New(t)

			for k, v := range tt.storeVals {
				_, r := cache.Store(k, []byte(v))
				asrt.NoErr(r)
			}

			for k, v := range tt.storeVals {
				got, r := cache.LoadLocal(k)
				asrt.NoErr(r)

				asrt.Equal(got, []byte(v))
			}

			for k, v := range tt.storeVals {
				got, r := cache.Load(k)
				asrt.NoErr(r)

				asrt.Equal(got, []byte(v))
			}
		})
	}
}

func TestCacheCluster(t *testing.T) {
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

	cache1, err := NewCache(ctxMain,
		WithLocalID("cache1"),
		WithTCPTransport(":28230", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28230}),
		WithServers([]raft.Server{
			{ID: "cache1", Address: "localhost:28230"},
			{ID: "cache2", Address: "localhost:28231"},
			{ID: "cache3", Address: "localhost:28232"},
		}),
	)
	asrtMain.NoErr(err)

	cache2, err := NewCache(ctxMain,
		WithTCPTransport(":28231", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28231}),
	)
	asrtMain.NoErr(err)

	cache3, err := NewCache(ctxMain,
		WithTCPTransport(":28232", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28232}),
	)
	asrtMain.NoErr(err)

	err = cache1.WaitReady(ctxMain)
	asrtMain.NoErr(err)
	fmt.Println("WAIT DONE")

	caches := []*Cache{cache1, cache2, cache3}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, cache := range caches {
				t.Run("write cache "+strconv.Itoa(i), func(t *testing.T) {
					asrtWrite := asrtMain.New(t)

					for k, v := range tt.storeVals {
						_, r := cache.Store(k+strconv.Itoa(i), []byte(v+strconv.Itoa(i)))
						asrtWrite.NoErr(r)
					}

					for j, readCache := range caches {
						t.Run("read cache "+strconv.Itoa(j), func(t *testing.T) {
							asrtRead := asrtWrite.New(t)

							for k, v := range tt.storeVals {
								got, r := readCache.LoadLocal(k + strconv.Itoa(i))
								asrtRead.NoErr(r)

								asrtRead.Equal(got, []byte(v+strconv.Itoa(i)))
							}

							for k, v := range tt.storeVals {
								got, r := readCache.Load(k + strconv.Itoa(i))
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
