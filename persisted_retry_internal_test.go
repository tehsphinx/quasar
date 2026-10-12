// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tehsphinx/quasar/pb/v1"
	"github.com/tehsphinx/quasar/transports"
)

var errApplyRejected = errors.New("apply rejected")

// stubPersistTransport answers every StorePersisted with err once the caller's
// ctx is done, the way a late transport failure arrives.
type stubPersistTransport struct {
	*transports.InmemTransport

	err error
}

func (s *stubPersistTransport) StorePersisted(ctx context.Context, _ *pb.Store, _ transports.PersistedStoreOpts,
) (*pb.StoreResponse, error) {
	<-ctx.Done()
	return nil, s.err
}

// TestRoutePersistedRetryingOnlyAfterPubAck is the RT-14885 regression:
// under WithRetry, only a reply wait that failed after the PubAck is
// ErrRetrying. A failure before it queued nothing and is an error, even when
// it arrives after the caller's deadline. Before the fix, cases "publish
// timeout" and "apply error" came back as ErrRetrying and pgwatcher acked a
// CDC whose write was lost.
func TestRoutePersistedRetryingOnlyAfterPubAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	noReply := fmt.Errorf("%w: %w", transports.ErrPersistedNoReply, context.DeadlineExceeded)
	cases := []struct {
		name  string
		err   error
		retry bool
		want  bool
	}{
		{"reply wait failed", noReply, true, true},
		{"publish timeout after the caller's deadline",
			fmt.Errorf("publish persisted store: %w", context.DeadlineExceeded), true, false},
		{"apply error after the caller's deadline", errApplyRejected, true, false},
		{"non-retry reply wait failed", noReply, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, inner := transports.NewInmemTransport("")
			transports.ConnectInmemQueueHub(transports.NewInmemQueueHub(), inner)
			tr := &stubPersistTransport{InmemTransport: inner, err: tc.err}

			c, err := NewCache(ctx, &stubFSM{}, WithLocalID("solo"), WithTransport(tr), WithBootstrap(true))
			if err != nil {
				t.Fatalf("NewCache: %v", err)
			}
			t.Cleanup(func() { _ = c.Shutdown() })
			if err := c.WaitReady(ctx); err != nil {
				t.Fatalf("WaitReady: %v", err)
			}

			var opts []StoreOption
			if tc.retry {
				opts = append(opts, WithRetry())
			}
			storeCtx, storeCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer storeCancel()
			_, err = c.store(storeCtx, "key", []byte("v"), opts...)
			if err == nil {
				t.Fatal("store succeeded, want an error")
			}
			if got := errors.Is(err, ErrRetrying); got != tc.want {
				t.Fatalf("errors.Is(%v, ErrRetrying) = %v, want %v", err, got, tc.want)
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("error %v does not wrap the transport error %v", err, tc.err)
			}
		})
	}
}
