package main

import (
	"fmt"

	"github.com/tehsphinx/quasar"
)

func main() {
	// addresses := []string{
	// 	"localhost:28224",
	// }
	cache, err := quasar.NewCache(nil)
	if err != nil {
		panic(err)
	}

	// set a value
	uid, err := cache.Store("key1", []byte("abc"))
	if err != nil {
		panic(err)
	}

	// get a value
	data, err := cache.Load("key1")
	if err != nil {
		panic(err)
	}
	fmt.Println(data)

	// get a value
	data, err = cache.LoadLocal("key1")
	if err != nil {
		panic(err)
	}
	fmt.Println(data)

	// get a value
	data, err = cache.LoadLocal("key1", quasar.WaitForUID(uid))
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
}
