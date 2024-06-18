package main

import (
	"context"
	"fmt"
	"time"

	"github.com/tehsphinx/quasar"
	"github.com/tehsphinx/quasar/examples/generic/exampleFSM"
)

/* This example showcases a cache with a custom-built FSM implementation. The advantages are:
- the API can be built on top of the business model
- if data is unmarshalled on incoming Apply calls, reads don't need to unmarshal data -> much faster.
*/

func main() {
	// Instantiate the custom-built FSM implementation.
	fsm := exampleFSM.NewInMemoryFSM()

	cache, err := quasar.NewCache(context.Background(), fsm, quasar.WithBootstrap(true))
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
	if r := fsm.SetMusician(exampleFSM.Musician{
		Name:        "Roberto",
		Age:         32,
		Instruments: []string{"guitar"},
	}); r != nil {
		panic(r)
	}

	// get a value
	musician, err := fsm.GetMusicianMaster("Roberto")
	if err != nil {
		panic(err)
	}
	fmt.Println(musician)

	// get a value
	musician, err = fsm.GetMusicianKnownLatest("Roberto")
	if err != nil {
		panic(err)
	}
	fmt.Println(musician)

	// get a value
	musician, err = fsm.GetMusicianLocal("Roberto")
	if err != nil {
		panic(err)
	}
	fmt.Println(musician)
}
