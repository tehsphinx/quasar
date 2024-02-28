package main

import (
	"fmt"

	"github.com/tehsphinx/quasar"
)

func main() {
	addresses := []string{
		"localhost:28224",
	}
	cache, err := quasar.NewCache(addresses)
	if err != nil {
		panic(err)
	}

	// set a value
	uid, err := cache.Set("key1", []byte("abc"))
	if err != nil {
		panic(err)
	}

	// get a value
	data, err := cache.Get("key1")
	if err != nil {
		panic(err)
	}
	fmt.Println(data)

	// get a value
	data, err = cache.GetLocal("key1")
	if err != nil {
		panic(err)
	}
	fmt.Println(data)

	// get a value
	data, err = cache.GetLocal("key1", quasar.WaitForUID(uid))
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
}
