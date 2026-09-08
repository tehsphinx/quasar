// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"context"
)

// itemSink returns a PersistedApplyFunc that hands every item to the returned
// channel WITHOUT settling it, for tests that want to inspect or settle
// deliveries from the test body rather than from the apply func.
//
// Not settling is safe: the transport keeps one unsettled item per partition,
// so a partition whose item is parked in the channel simply delivers nothing
// further until the test settles it. The send is bounded by the consumer's ctx
// so a test that stops reading cannot wedge a puller on teardown — the same
// obligation a real apply has.
func itemSink(buf int) (PersistedApplyFunc, <-chan PersistedItem) {
	items := make(chan PersistedItem, buf)
	return func(ctx context.Context, item PersistedItem) {
		select {
		case items <- item:
		case <-ctx.Done():
			_ = item.Nack(context.WithoutCancel(ctx))
		}
	}, items
}
