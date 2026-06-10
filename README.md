# Quasar Cache

Quasar is a distributed, embeddable Go cache with eventual consistency built on
the [HashiCorp Raft](https://github.com/hashicorp/raft) consensus algorithm.
Every node holds a full copy of the data and applies writes through a replicated
log, so reads can be served locally while writes are linearized through the Raft
leader. Quasar is designed to keep serving — and to recover — even when a
cluster loses quorum.

> **Status:** in active development. The core replication, recovery, and NATS
> transport paths are exercised by an extensive test suite (including `-race`
> and quorum-loss scenarios), but the API is not yet frozen. `KVCache` in
> particular is experimental (see [Key/value cache](#quick-start)). Pin a
> commit and review changes before depending on it.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Concepts](#concepts)
- [Choosing a transport](#choosing-a-transport)
- [Discovery and clustering](#discovery-and-clustering)
- [Reads and consistency](#reads-and-consistency)
- [Writing a custom FSM](#writing-a-custom-fsm)
- [Persistence](#persistence)
- [Resilience and recovery](#resilience-and-recovery)
- [Options reference](#options-reference)
- [Operational notes](#operational-notes)

## Install

```bash
go get github.com/tehsphinx/quasar
```

Requires Go 1.21+.

## Quick start

The simplest way to try Quasar is the built-in key/value cache, bootstrapped as
a single-node cluster:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/tehsphinx/quasar"
)

func main() {
	ctx := context.Background()

	cache, err := quasar.NewKVCache(ctx, quasar.WithBootstrap(true))
	if err != nil {
		panic(err)
	}
	defer cache.Shutdown()

	// Wait for the cluster to elect a leader before serving traffic.
	ready, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cache.WaitReady(ready); err != nil {
		panic(err)
	}

	// Writes go through the Raft leader and return the log index (UID) of the
	// committed entry.
	uid, err := cache.Store(ctx, "key1", []byte("hello"))
	if err != nil {
		panic(err)
	}

	// Load reads through the leader's latest index: read-your-writes across the
	// whole cluster.
	val, err := cache.Load(ctx, "key1")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val)) // hello

	// LoadLocal serves from the local replica without contacting the leader.
	// Pass WaitForUID to wait until a specific write has been applied locally.
	val, err = cache.LoadLocal(ctx, "key1", quasar.WaitForUID(uid))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val)) // hello
}
```

See [`examples/basic`](examples/basic) for a runnable version.

> The key/value cache is **experimental**: its API may change, and its
> snapshot/restore support requires a `stores.KVStore` that also implements
> `stores.SnapshotKVStore` (the default in-memory store does). For full control
> over the data model, supply your own FSM via
> [`NewCache`](#writing-a-custom-fsm).

## Concepts

- **Cache vs. KVCache.** `NewCache` is the general-purpose constructor: you
  supply your own [`FSM`](#writing-a-custom-fsm) (finite state machine) and own
  the data model. `NewKVCache` is a ready-made key/value layer built on top of
  `Cache`.
- **FSM.** The state machine that applies committed log entries to your data.
  Quasar replicates opaque byte commands; your FSM decides what they mean.
- **UID.** Every committed write is assigned a monotonic Raft log index, surfaced
  as a `uint64` UID. UIDs are the unit of consistency: you wait for a UID to be
  applied locally before reading.
- **Transport.** How Raft RPCs (and forwarded writes) travel between nodes —
  NATS, TCP, or a custom implementation.
- **Discovery.** How nodes find each other, join the cluster, and get pruned
  when they disappear.

A cache is created once and runs until `Shutdown`. All public methods take a
`context.Context` so calls can carry deadlines.

## Choosing a transport

Quasar ships two production transports plus an in-memory one for tests. Pick
exactly one.

### NATS

NATS is the recommended transport: it pairs naturally with NATS-based discovery
and the persisted-FIFO queue.

```go
import "github.com/nats-io/nats.go"

nc, err := nats.Connect(nats.DefaultURL)
// ...

cache, err := quasar.NewKVCache(ctx,
	quasar.WithName("my-cache"),
	quasar.WithLocalID("node-a"),
	quasar.WithNatsTransport(nc),
)
```

For finer control (timeouts, log output, message size, persisted queue) build
the transport directly and pass it with `WithTransport`:

```go
import "github.com/tehsphinx/quasar/transports"

tr, err := transports.NewNATSTransport(ctx, nc, "my-cache", "node-a",
	transports.WithNATSTimeout(5*time.Second),
	transports.WithNATSHeartbeatTimeout(time.Second),
)
// ...
cache, err := quasar.NewCache(ctx, fsm, quasar.WithTransport(tr))
```

### TCP

```go
cache, err := quasar.NewKVCache(ctx,
	quasar.WithTCPTransport(":28224", &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 28224}),
)
```

`bindAddr` is the local listen address; the second argument is the address peers
should dial. Pass `""` / `nil` to use the defaults (`:28224` / `127.0.0.1`).

## Discovery and clustering

You can form a cluster three ways:

1. **Static membership.** Bootstrap one node with the full server list:

   ```go
   servers := []raft.Server{
       {ID: "node-a", Address: "10.0.0.1:28224"},
       {ID: "node-b", Address: "10.0.0.2:28224"},
       {ID: "node-c", Address: "10.0.0.3:28224"},
   }
   cache, err := quasar.NewCache(ctx, fsm,
       quasar.WithTCPTransport(":28224", extAddr),
       quasar.WithServers(servers), // only the bootstrapping node sets this
   )
   ```

2. **Single-node bootstrap.** `WithBootstrap(true)` starts a one-node cluster;
   other nodes join later via discovery or `AddVoter` from the leader.

3. **Dynamic discovery.** Let nodes find each other automatically. With NATS:

   ```go
   import "github.com/tehsphinx/quasar/discoveries"

   cache, err := quasar.NewKVCache(ctx,
       quasar.WithNatsTransport(nc),
       quasar.WithDiscovery(discoveries.NewNATSDiscovery(nc)),
       quasar.WithBootstrapWait(5*time.Second), // wait for peers before bootstrapping alone
       quasar.WithAutoPrune(30*time.Second),    // drop peers unseen for 30s
   )
   ```

   With discovery, a fresh voter waits `WithBootstrapWait` for any peer to
   announce itself before bootstrapping a single-node cluster — this prevents two
   nodes from each starting their own universe on a cold start.

**Voters vs. nonvoters.** By default a node is a Raft voter. Use
`WithSuffrage(raft.Nonvoter)` for read-only replicas that never participate in
elections and never block quorum.

## Reads and consistency

Quasar is eventually consistent, but gives you explicit control over how fresh a
read must be:

- **`Load`** asks the leader for its latest committed index, waits for the local
  replica to reach it, then reads. This is cluster-wide read-your-writes at the
  cost of a round-trip to the leader.
- **`LoadLocal`** reads the local replica immediately. Combine with
  `WaitForUID(uid)` (the UID returned by `Store`) to read-your-own-writes without
  contacting the leader.
- **`WaitReady(ctx)`** blocks until the node sees a leader — call it after
  construction before serving traffic.

In a custom FSM the same waits are available through the injected `FSMInjector`
(`WaitFor`, `WaitForMasterLatest`, `WaitForKnownLatest`).

## Writing a custom FSM

Implement the `FSM` interface and pass it to `NewCache`. Quasar replicates the
bytes you hand to `FSMInjector.Store`; your `ApplyCmd` receives them on every
node once committed.

```go
type FSM interface {
	Inject(fsm *quasar.FSMInjector) // receives the handle used to Store/Wait
	ApplyCmd(cmd []byte) error      // apply a committed command to your state
	Snapshot() (raft.FSMSnapshot, error)
	Restore(snapshot io.ReadCloser) error
	Reset() error                   // wipe state (used by Reset/HardReset)
}
```

A minimal counter FSM:

```go
type counterFSM struct {
	inj *quasar.FSMInjector
	mu  sync.Mutex
	n   int64
}

func (f *counterFSM) Inject(inj *quasar.FSMInjector) { f.inj = inj }

func (f *counterFSM) ApplyCmd(cmd []byte) error {
	delta, _ := binary.Varint(cmd)
	f.mu.Lock()
	f.n += delta
	f.mu.Unlock()
	return nil
}

func (f *counterFSM) Reset() error { f.mu.Lock(); f.n = 0; f.mu.Unlock(); return nil }

// Snapshot / Restore: serialize and reload f.n (see hashicorp/raft FSMSnapshot).

// Add replicates a delta through Raft and returns its UID.
func (f *counterFSM) Add(ctx context.Context, delta int64) (uint64, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, delta)
	return f.inj.Store(ctx, buf[:n])
}
```

```go
fsm := &counterFSM{}
cache, err := quasar.NewCache(ctx, fsm, quasar.WithBootstrap(true))
```

`FSMInjector` also exposes `IsLeader`, `HasLeader`, and `CacheID`.

> **Idempotency:** the persisted-FIFO path (below) is at-least-once — a command
> can be applied more than once across leadership changes. If you enable it, make
> `ApplyCmd` idempotent.

## Persistence

By default Quasar keeps Raft state in memory (state is rebuilt from peers on
restart). Two opt-in mechanisms add durability:

- **`WithPersistentStore(store)`** persists FSM data via a
  `stores.PersistentStorage` implementation, so a node can recover its data
  across restarts.
- **Persisted-FIFO queue (NATS only).** `transports.WithNATSPersistedQueue`
  backs writes with a JetStream WorkQueue stream: a write is published to the
  stream and the next leader applies it in strict FIFO order, so **a missing
  leader no longer blocks writes** — the publish lands durably and is applied
  when leadership returns.

  ```go
  tr, err := transports.NewNATSTransport(ctx, nc, "my-cache", "node-a",
      transports.WithNATSPersistedQueue("", // default stream name
          transports.WithPersistedMaxDeliver(16),
          transports.WithPersistedAckWait(10*time.Second),
      ),
  )
  ```

  This path is at-least-once; see the idempotency note above.

The `stores` package provides in-memory and NATS-KV implementations of the
stable and KV stores.

## Resilience and recovery

Quasar's design goal is to recover even on quorum loss. The relevant knobs:

- **`WithQuorumRecovery(after)`** — for the two-site, 2-voter deployment. If the
  local voter is leaderless for `after` and no other voter has been seen via
  discovery, it rebuilds Raft as a single-voter cluster so writes can continue.
  Missing voters rejoin later.

  > **Warning:** forced recovery is a controlled split-brain risk. If the
  > "missing" voter is actually alive behind a transient partition, recent
  > unreplicated writes from that side can be dropped when the partition heals.
  > Set `after` long enough that link blips don't trigger it (minimum 6s).

- **`WithAutoPrune(after)`** — remove peers not seen via discovery for `after`
  (minimum 6s). Off by default.
- **`WithBootstrapWait(after)`** — see [discovery](#discovery-and-clustering).
- **`WithNoLeaderTimeout(d)`** — how long a write waits for a leader before
  returning `ErrNoLeader`.

Manual recovery and lifecycle:

- **`Reset(ctx)`** — soft reset: replicate a cache-wipe command through the log.
- **`HardReset(ctx)`** — last-resort wipe and rebuild across the cluster. Issue
  it from the node you want to survive as leader.
- **`Snapshot` / `Restore` / `ForceSnapshot`** — capture and reload FSM state.
- **`Shutdown`** / **`RemoveAndShutdown(ctx)`** — stop the node, optionally
  removing it from the configuration first.
- **`GetLeader`, `GetServerList`, `IsLeader`, `GetRaftStatus`, `InstanceID`** —
  inspect cluster state.

## Options reference

Passed to `NewCache` / `NewKVCache`:

| Option                                          | Purpose                                                                             |
|-------------------------------------------------|-------------------------------------------------------------------------------------|
| `WithName(string)`                              | Cache name; isolates traffic between caches sharing a network.                      |
| `WithLocalID(string)`                           | Stable node ID. **Set this** — a random UUID resets Raft identity on every restart. |
| `WithBootstrap(bool)`                           | Start a single-node cluster.                                                        |
| `WithServers([]raft.Server)`                    | Bootstrap with a static multi-node configuration.                                   |
| `WithSuffrage(raft.ServerSuffrage)`             | Voter (default) or `Nonvoter`.                                                      |
| `WithNatsTransport(*nats.Conn)`                 | Use NATS for Raft communication.                                                    |
| `WithTCPTransport(bind, ext)`                   | Use TCP for Raft communication.                                                     |
| `WithTransport(transports.Transport)`           | Use a custom/preconfigured transport (overrides the above).                         |
| `WithDiscovery(Discovery)`                      | Enable automatic peer discovery.                                                    |
| `WithBootstrapWait(d)`                          | Wait for peers before cold-start bootstrap (discovery only).                        |
| `WithAutoPrune(d)`                              | Remove peers unseen for `d` (min 6s).                                               |
| `WithQuorumRecovery(d)`                         | Auto-recover a stranded voter after `d` (min 6s).                                   |
| `WithNoLeaderTimeout(d)`                        | Write wait-for-leader timeout.                                                      |
| `WithKVStore(stores.KVStore)`                   | Backing store for `KVCache`.                                                        |
| `WithPersistentStore(stores.PersistentStorage)` | Durable FSM storage for `Cache`.                                                    |
| `WithRaftConfig(*raft.Config)`                  | Custom Raft tuning (LocalID still comes from `WithLocalID`).                        |
| `WithHclogLogger` / `WithSlogLogger`            | Wire logging (priority: hclog > slog).                                              |

Load options: `WaitForUID(uid)` — wait for a specific write before reading.

## Operational notes

- **Always set `WithLocalID`** to a value that is stable across restarts;
  otherwise the node presents as a brand-new member each time it starts.
- **Call `WaitReady`** after construction before serving requests.
- The **key/value cache is experimental** — its API may change and its
  snapshot/restore support requires a `stores.KVStore` that also implements
  `stores.SnapshotKVStore` (the in-memory store does).
- For NATS deployments, pair `WithNatsTransport` with
  `discoveries.NewNATSDiscovery` and consider the persisted-FIFO queue for
  write availability during leader flaps.
