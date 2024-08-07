package main

import (
	"context"
	"fmt"
	"time"

	"github.com/tehsphinx/quasar"
)

func main() {
	cache, err := quasar.NewKVCache(context.Background(), quasar.WithBootstrap(true))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if r := cache.WaitReady(ctx); r != nil {
		panic(r)
	}
	// Wait for cluster to be up and running. (Elected a leader.)

	// set a value
	uid, err := cache.Store(ctx, "key1", []byte("abc"))
	if err != nil {
		panic(err)
	}

	// get a value
	data, err := cache.Load(ctx, "key1")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))

	// get a value
	data, err = cache.LoadLocal(ctx, "key1")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))

	// get a value
	data, err = cache.LoadLocal(ctx, "key1", quasar.WaitForUID(uid))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}
