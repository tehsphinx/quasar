// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestInmemQueueHub_StopConsumerDoesNotLeakWatchdog is the RT-13042 m21(b)
// regression test. Each startConsumer spawned a watchdog goroutine that only
// exited when its own ctx was cancelled. A leadership flip that ends a claim
// through stopConsumer (leaving the start ctx alive) used to leak one
// goroutine per flip. stopConsumer must wake the watchdog so it exits with
// the claim.
func TestInmemQueueHub_StopConsumerDoesNotLeakWatchdog(t *testing.T) {
	hub := NewInmemQueueHub()
	_, consumer := NewInmemTransport("")
	ConnectInmemQueueHub(hub, consumer)

	// Long-lived ctx that is never cancelled, so the only thing that can
	// retire a watchdog is stopConsumer.
	ctx := context.Background()

	// Warm up one cycle so any one-time goroutines are already running.
	if _, err := consumer.StartPersistedConsumer(ctx); err != nil {
		t.Fatalf("StartPersistedConsumer: %v", err)
	}
	if err := consumer.StopPersistedConsumer(); err != nil {
		t.Fatalf("StopPersistedConsumer: %v", err)
	}
	waitGoroutinesSettle()
	before := runtime.NumGoroutine()

	const flips = 50
	for n := 0; n < flips; n++ {
		if _, err := consumer.StartPersistedConsumer(ctx); err != nil {
			t.Fatalf("StartPersistedConsumer #%d: %v", n, err)
		}
		if err := consumer.StopPersistedConsumer(); err != nil {
			t.Fatalf("StopPersistedConsumer #%d: %v", n, err)
		}
	}

	waitGoroutinesSettle()
	after := runtime.NumGoroutine()

	// Each leaked watchdog would add one goroutine per flip. Allow a small
	// slack for scheduler noise; pre-fix this grows by ~flips.
	if after-before > flips/2 {
		t.Fatalf("watchdog goroutines leaked across %d flips: before=%d after=%d", flips, before, after)
	}
}

// waitGoroutinesSettle gives just-released watchdog goroutines a moment to
// observe their release signal and exit before we count.
func waitGoroutinesSettle() {
	for i := 0; i < 20; i++ {
		runtime.Gosched()
		time.Sleep(5 * time.Millisecond)
	}
}
