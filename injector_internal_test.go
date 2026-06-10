package quasar

import (
	"context"
	"testing"
	"time"
)

// TestGetWaitCtxCapsLongDeadline is the RT-13042 m3 regression test.
// A caller budget longer than maxWait used to collapse to defaultWait (5s);
// it must now be capped at maxWait. An at-or-under-cap deadline is honored,
// and a deadline-less context falls back to defaultWait.
func TestGetWaitCtxCapsLongDeadline(t *testing.T) {
	t.Run("long deadline capped at maxWait", func(t *testing.T) {
		parent, cancelParent := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancelParent()

		ctx, cancel := getWaitCtx(parent)
		defer cancel()

		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected a deadline")
		}
		got := time.Until(deadline)
		// Pre-fix this collapsed to defaultWait (5s).
		if got <= defaultWait {
			t.Fatalf("60s caller budget collapsed to %v (<= defaultWait %v)", got, defaultWait)
		}
		if got > maxWait+time.Second {
			t.Fatalf("expected cap near maxWait %v, got %v", maxWait, got)
		}
	})

	t.Run("short deadline honored", func(t *testing.T) {
		parent, cancelParent := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelParent()

		ctx, cancel := getWaitCtx(parent)
		defer cancel()

		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected a deadline")
		}
		if got := time.Until(deadline); got <= defaultWait || got > 10*time.Second+time.Second {
			t.Fatalf("expected the 10s caller deadline honored, got %v", got)
		}
	})

	t.Run("no deadline uses defaultWait", func(t *testing.T) {
		ctx, cancel := getWaitCtx(context.Background())
		defer cancel()

		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected a default deadline")
		}
		if got := time.Until(deadline); got > defaultWait+time.Second {
			t.Fatalf("expected defaultWait %v, got %v", defaultWait, got)
		}
	})
}
