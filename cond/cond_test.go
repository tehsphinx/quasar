// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file was blatantly stolen from https://cs.opensource.google/go/go/+/refs/tags/go1.20.4:src/sync/cond_test.go.

package cond_test

import (
	"sync"
	"testing"

	"github.com/tehsphinx/quasar/cond"
)

func TestCondBroadcast(t *testing.T) {
	var m sync.Mutex
	c := cond.New(&m)
	n := 200
	running := make(chan int, n)
	awake := make(chan int, n)
	exit := false
	for i := 0; i < n; i++ {
		go func(g int) {
			m.Lock()
			for !exit {
				running <- g
				c.Wait()
				awake <- g
			}
			m.Unlock()
		}(i)
	}
	for i := 0; i < n; i++ {
		for i := 0; i < n; i++ {
			<-running // Will deadlock unless n are running.
		}
		if i == n-1 {
			m.Lock()
			exit = true
			m.Unlock()
		}
		select {
		case <-awake:
			t.Fatal("goroutine not asleep")
		default:
		}
		m.Lock()
		c.Broadcast()
		m.Unlock()
		seen := make([]bool, n)
		for i := 0; i < n; i++ {
			g := <-awake
			if seen[g] {
				t.Fatal("goroutine woke up twice", g)
			}
			seen[g] = true
		}
	}
	select {
	case <-running:
		t.Fatal("goroutine did not exit")
	default:
	}
	c.Broadcast()
}

func BenchmarkSync1(b *testing.B) {
	benchmarkSync(b, 1)
}

func BenchmarkBroad1(b *testing.B) {
	benchmarkBroad(b, 1)
}

func BenchmarkSync2(b *testing.B) {
	benchmarkSync(b, 2)
}

func BenchmarkBroad2(b *testing.B) {
	benchmarkBroad(b, 2)
}

func BenchmarkSync4(b *testing.B) {
	benchmarkSync(b, 4)
}

func BenchmarkBroad4(b *testing.B) {
	benchmarkBroad(b, 4)
}

func BenchmarkSync8(b *testing.B) {
	benchmarkSync(b, 8)
}

func BenchmarkBroad8(b *testing.B) {
	benchmarkBroad(b, 8)
}

func BenchmarkSync16(b *testing.B) {
	benchmarkSync(b, 16)
}

func BenchmarkBroad16(b *testing.B) {
	benchmarkBroad(b, 16)
}

func BenchmarkSync32(b *testing.B) {
	benchmarkSync(b, 32)
}

func BenchmarkBroad32(b *testing.B) {
	benchmarkBroad(b, 32)
}

func BenchmarkSync64(b *testing.B) {
	benchmarkSync(b, 64)
}

func BenchmarkBroad64(b *testing.B) {
	benchmarkBroad(b, 64)
}

func BenchmarkSync128(b *testing.B) {
	benchmarkSync(b, 128)
}

func BenchmarkBroad128(b *testing.B) {
	benchmarkBroad(b, 128)
}

func benchmarkBroad(b *testing.B, waiters int) {
	b.Helper()

	c := cond.New(&sync.Mutex{})
	done := make(chan bool)
	id := 0

	for routine := 0; routine < waiters+1; routine++ {
		go func() {
			for i := 0; i < b.N; i++ {
				c.L.Lock()
				if id == -1 {
					c.L.Unlock()
					break
				}
				id++
				if id == waiters+1 {
					id = 0
					c.Broadcast()
				} else {
					c.Wait()
				}
				c.L.Unlock()
			}
			c.L.Lock()
			id = -1
			c.Broadcast()
			c.L.Unlock()
			done <- true
		}()
	}
	for routine := 0; routine < waiters+1; routine++ {
		<-done
	}
}

func benchmarkSync(b *testing.B, waiters int) {
	b.Helper()

	c := sync.NewCond(&sync.Mutex{})
	done := make(chan bool)
	id := 0

	for routine := 0; routine < waiters+1; routine++ {
		go func() {
			for i := 0; i < b.N; i++ {
				c.L.Lock()
				if id == -1 {
					c.L.Unlock()
					break
				}
				id++
				if id == waiters+1 {
					id = 0
					c.Broadcast()
				} else {
					c.Wait()
				}
				c.L.Unlock()
			}
			c.L.Lock()
			id = -1
			c.Broadcast()
			c.L.Unlock()
			done <- true
		}()
	}
	for routine := 0; routine < waiters+1; routine++ {
		<-done
	}
}
