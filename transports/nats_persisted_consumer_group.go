package transports

import (
	"context"
	"sync"
)

// natsPersistedConsumerGroup owns the per-shard pullers and the channel that
// reports the group has stopped. A single context.CancelFunc stops every
// puller; the WaitGroup gates closing done until all have exited.
type natsPersistedConsumerGroup struct {
	done      chan struct{}
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	consumers []*natsPersistedConsumer
}

// launch starts a puller goroutine on pullCtx for every consumer in the group.
// Each consumer must already have been opened with start.
func (g *natsPersistedConsumerGroup) launch(pullCtx context.Context) {
	for _, c := range g.consumers {
		g.wg.Add(1)
		go c.run(pullCtx)
	}
}

// stopContexts tears down the pull subscriptions opened so far. Used on the
// startConsumer error path, before any puller goroutine has been launched.
func (g *natsPersistedConsumerGroup) stopContexts() {
	for _, c := range g.consumers {
		c.stopMctx()
	}
}

// stop cancels every puller and Naks each shard's in-flight item for prompt
// handover to the next leader. The done channel is closed by the closer
// goroutine once all pullers have exited.
//
// cancel runs before stopMctx so a puller racing a reconnect observes the
// cancellation and stops the messages context it just opened itself (see
// reconnect); stopMctx here unblocks whichever context is currently live.
//
// The per-shard drains run concurrently. Each waits up to the consumer's
// AckWait for its item to be settled by the apply side, so draining them in
// sequence would cost shards x AckWait in the worst case. In practice the
// cancellation above makes every puller settle-or-Nack its own item in
// parallel already, but now that each puller applies its own items (RT-14337)
// all shards can genuinely be mid-apply at once, which is exactly when the
// sequential version would have been slowest.
func (g *natsPersistedConsumerGroup) stop() {
	g.cancel()

	var wg sync.WaitGroup
	for _, c := range g.consumers {
		wg.Add(1)
		go func(c *natsPersistedConsumer) {
			defer wg.Done()
			c.stopMctx()
			c.drainInflight()
		}(c)
	}
	wg.Wait()
}
